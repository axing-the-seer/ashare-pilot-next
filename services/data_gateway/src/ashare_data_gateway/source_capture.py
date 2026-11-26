"""Read and store the existing content-addressed source capture format.

This narrow module does not initiate acquisition or infer market-time availability.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path


def encode(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode()


def store(root: Path, prefix: str, value: dict) -> str:
    """Store complete content atomically without replacing a retained object."""
    if prefix not in {"source-table", "source-receipt"}:
        raise ValueError("invalid source capture prefix")
    if root.is_symlink() or any(parent.is_symlink() for parent in root.parents):
        raise ValueError("redirected source capture root")
    raw = encode(value)
    identity = prefix + "-sha256-" + hashlib.sha256(raw).hexdigest()
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or any(parent.is_symlink() for parent in root.parents):
        raise ValueError("redirected source capture root")
    path = root / (identity + ".json")
    with tempfile.NamedTemporaryFile(dir=root, prefix=".source-", delete=False) as out:
        temporary = Path(out.name)
        out.write(raw)
        out.flush()
        os.fsync(out.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != raw:
                raise ValueError("source capture content collision") from None
    finally:
        temporary.unlink()
    return identity


def read(root: Path, identity: str) -> dict:
    if not isinstance(identity, str) or not re.fullmatch(
        r"(?:source-table|source-receipt)-sha256-[a-f0-9]{64}", identity
    ):
        raise ValueError("invalid source capture identity")
    path = root / (identity + ".json")
    if path.is_symlink() or root.is_symlink() or path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("invalid source capture path")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != identity.rsplit("-", 1)[1]:
        raise ValueError("source capture hash mismatch")
    return json.loads(raw)
