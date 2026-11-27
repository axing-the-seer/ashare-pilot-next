"""Immutable local research artifact I/O shared by cache producers and readers."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from ashare_obs import event


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_regular(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError("research input must be a regular file")
    return path.read_bytes()


def file_digest(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise ValueError("panel artifact cannot be redirected")
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def publish(path: Path, content: bytes) -> None:
    """Publish without replacement, accepting only byte-identical duplicates."""
    fd, temporary = tempfile.mkstemp(prefix=".artifact-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if read_regular(path) != content:
                raise ValueError("immutable model artifact collision") from None
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)
    runtime = next((p for p in path.parents if p.name == "runtime"), None)
    if runtime is not None:
        event(runtime.parent, "artifact", "published", status="ok", datasetId=path.stem)
