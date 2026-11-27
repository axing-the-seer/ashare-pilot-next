"""Build a hash-bound, memory-mapped cache for model-v2 research."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import numpy as np

from .canonical import canonical_json_bytes
from .daily_inference_inputs import _apply_risk_history, _load_recent_prices
from .immutable_artifacts import digest, read_regular
from .immutable_artifacts import file_digest as _file_digest
from .immutable_artifacts import publish as _publish


def _hashed_json(root: Path, identifier: str, prefix: str) -> tuple[dict, bytes]:
    if not re.fullmatch(rf"{re.escape(prefix)}-sha256-[a-f0-9]{{64}}", identifier):
        raise ValueError(f"invalid {prefix} identifier")
    raw = read_regular(root / f"{identifier}.json")
    if identifier != f"{prefix}-sha256-{digest(raw)}":
        raise ValueError(f"{prefix} hash mismatch")
    return json.loads(raw), raw


def _extract_panel(panel_path, risk_document, output):
    with np.load(panel_path, allow_pickle=False) as panel:
        x, y = panel["features"], panel["labels"]
        days, symbols = panel["days"], panel["symbols"]
        exclusions = _apply_risk_history(x, y, days, symbols, risk_document)
        np.save(output / "features.npy", x, allow_pickle=False)
        np.save(output / "labels.npy", y, allow_pickle=False)
        np.save(output / "days.npy", days, allow_pickle=False)
        np.save(output / "symbols.npy", symbols, allow_pickle=False)
        (output / "risk-exclusions.json").write_text(json.dumps(exclusions))


def _extract_overlay(overlay_path, output):
    with np.load(overlay_path, allow_pickle=False) as overlay:
        for source, target in (
            ("features", "financial.npy"),
            ("industries", "industries.npy"),
            ("industry_labels", "industry-labels.npy"),
        ):
            np.save(output / target, overlay[source], allow_pickle=False)


def _extract_prices(repo, panel_manifest, days_path, symbols_path, first_index, as_of, output):
    days = np.load(days_path, allow_pickle=False, mmap_mode="r")
    symbols = np.load(symbols_path, allow_pickle=False, mmap_mode="r")
    prices = _load_recent_prices(
        repo,
        panel_manifest,
        days,
        symbols,
        first_index=first_index,
        as_of=as_of,
    )
    for name in ("opens", "closes", "volumes", "amounts"):
        np.save(output / f"{name}.npy", prices[name], allow_pickle=False)
    metadata = {
        "days": prices["days"],
        "catalog_sha256": prices["catalog_sha256"],
        "price_basis": prices["price_basis"],
    }
    (output / "price-metadata.json").write_text(json.dumps(metadata))


def _run_child(target, args):
    process = multiprocessing.get_context("spawn").Process(target=target, args=args)
    process.start()
    process.join()
    if process.exitcode != 0:
        raise RuntimeError(f"cache extraction child failed: {target.__name__}")


def build(
    repo: Path,
    panel_manifest_id: str,
    overlay_manifest_id: str,
    risk_id: str,
    first_index: int,
    *,
    as_of: datetime,
):
    if as_of.tzinfo is None or as_of.utcoffset() is None or first_index < 0:
        raise ValueError("cache requires an aware as_of and non-negative first_index")
    data_root = repo / "runtime/all-market-data"
    panel_manifest, panel_raw = _hashed_json(
        data_root / "panels", panel_manifest_id, "market-panel-manifest"
    )
    overlay_manifest, overlay_raw = _hashed_json(
        data_root / "fundamentals/overlays",
        overlay_manifest_id,
        "market-fundamental-overlay-manifest",
    )
    risk_document, risk_raw = _hashed_json(
        data_root / "risk", risk_id, "market-risk-history"
    )
    if (
        overlay_manifest.get("base_manifest_id") != panel_manifest_id
        or datetime.fromisoformat(panel_manifest["as_of"]) > as_of
        or datetime.fromisoformat(overlay_manifest["as_of"]) > as_of
        or datetime.fromisoformat(risk_document["as_of"]) > as_of
    ):
        raise ValueError("cache source binding is unavailable")
    panel_id = panel_manifest["panel_id"]
    overlay_id = overlay_manifest["overlay_id"]
    panel_path = data_root / "panels" / f"{panel_id}.npz"
    overlay_path = data_root / "fundamentals/overlays" / f"{overlay_id}.npz"
    if panel_id != "market-panel-sha256-" + _file_digest(panel_path):
        raise ValueError("panel hash mismatch")
    if overlay_id != "market-fundamental-overlay-sha256-" + _file_digest(overlay_path):
        raise ValueError("overlay hash mismatch")

    root = data_root / "model-v2-cache"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(dir=root, prefix=".building-"))
    try:
        _run_child(_extract_panel, (panel_path, risk_document, temporary))
        _run_child(_extract_overlay, (overlay_path, temporary))
        days = np.load(temporary / "days.npy", allow_pickle=False, mmap_mode="r")
        if first_index >= len(days):
            raise ValueError("price cache first_index is outside the panel")
        _run_child(
            _extract_prices,
            (
                repo,
                panel_manifest,
                temporary / "days.npy",
                temporary / "symbols.npy",
                first_index,
                as_of,
                temporary,
            ),
        )
        price_metadata_raw = read_regular(temporary / "price-metadata.json")
        risk_exclusions_raw = read_regular(temporary / "risk-exclusions.json")
        files = []
        for path in sorted(temporary.glob("*.npy")):
            array = np.load(path, allow_pickle=False, mmap_mode="r")
            files.append(
                {
                    "path": path.name,
                    "sha256": _file_digest(path),
                    "shape": list(array.shape),
                    "dtype": str(array.dtype),
                    "bytes": path.stat().st_size,
                }
            )
        document = {
            "format": "daily-model-v2-cache/v1",
            "as_of": as_of.isoformat(),
            "panel_manifest_id": panel_manifest_id,
            "panel_manifest_sha256": digest(panel_raw),
            "overlay_manifest_id": overlay_manifest_id,
            "overlay_manifest_sha256": digest(overlay_raw),
            "risk_history_id": risk_id,
            "risk_history_sha256": digest(risk_raw),
            "risk_exclusions": json.loads(risk_exclusions_raw),
            "price_first_index": first_index,
            "price_metadata": json.loads(price_metadata_raw),
            "files": files,
            "code_sha256": digest(read_regular(Path(__file__))),
        }
        raw = canonical_json_bytes(document)
        identifier = "daily-model-v2-cache-sha256-" + digest(raw)
        target = root / identifier
        if target.exists():
            if target.is_symlink() or not target.is_dir():
                raise ValueError("cache target is not a directory")
            shutil.rmtree(temporary)
        else:
            os.rename(temporary, target)
        _publish(root / f"{identifier}.json", raw)
        return {
            "cache_id": identifier,
            "files": len(files),
            "bytes": sum(int(item["bytes"]) for item in files),
            "risk_exclusions": document["risk_exclusions"],
            "price_sessions": len(document["price_metadata"]["days"]),
        }
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--panel-manifest-id", required=True)
    parser.add_argument("--overlay-manifest-id", required=True)
    parser.add_argument("--risk-id", required=True)
    parser.add_argument("--first-index", type=int)
    parser.add_argument("--as-of", type=datetime.fromisoformat, required=True)
    parser.add_argument(
        "--verify-cache-id",
        help="read and validate an existing immutable cache without training or publishing",
    )
    args = vars(parser.parse_args(argv))
    cache_id = args.pop("verify_cache_id")
    if cache_id is None:
        if args["first_index"] is None:
            parser.error("--first-index is required when building a cache")
        result = build(**args)
    else:
        if args.pop("first_index") is not None:
            parser.error("--first-index is only valid when building a cache")
        from .daily_model_v2_inputs import SEMANTICS_SHA256, close_inputs, load_inputs

        inputs = load_inputs(**args, cache_id=cache_id)
        try:
            result = {
                "cache_id": cache_id,
                "validated": True,
                "semantic_sha256": SEMANTICS_SHA256,
                "days": int(len(inputs["days"])),
                "symbols": int(len(inputs["symbols"])),
                "base_shape": list(inputs["x"].shape),
                "financial_shape": list(inputs["financial"].shape),
                "risk_exclusions": inputs["risk_exclusions"],
            }
        finally:
            close_inputs(inputs)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
