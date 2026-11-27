"""Read the existing v2 model cache with source and feature bindings intact.

The frozen feature semantics are the existing production50 registry document,
not a new model or a change to its financial calculations. The normal cache
command exposes this reader while the personal training entry is migrated.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from importlib.resources import files
from pathlib import Path

import numpy as np
from ashare_quant_core.daily_selection import validate_panel_axes

from .immutable_artifacts import digest, file_digest, read_regular

SEMANTICS_SHA256 = "07b586cfb06aa1012f935e883946f7623f3c2bac4556225c1a8a8359562ffcb4"


def _semantics() -> tuple[dict, ...]:
    raw = files(__package__).joinpath("daily_model_v2_semantics.json").read_bytes()
    if digest(raw) != SEMANTICS_SHA256:
        raise ValueError("model input semantic snapshot hash mismatch")
    document = json.loads(raw)
    return tuple(document["features"])


class RowMappedArray:
    """Copy one requested partition and close its mapping immediately."""

    def __init__(self, path: Path):
        self.path = path
        array = np.load(path, allow_pickle=False, mmap_mode="r")
        try:
            self.shape = array.shape
            self.ndim = len(self.shape)
        finally:
            array._mmap.close()

    def __len__(self):
        return self.shape[0]

    def __getitem__(self, key):
        array = np.load(self.path, allow_pickle=False, mmap_mode="r")
        try:
            return np.array(array[key], copy=True)
        finally:
            array._mmap.close()


def _validate_values(values, specs):
    if values.ndim != 2 or values.shape[1] != len(specs) or values.shape[0] == 0:
        raise ValueError("nonempty named feature matrix required")
    if np.isinf(values).any():
        raise ValueError("infinite feature values")
    for i, spec in enumerate(specs):
        known = values[np.isfinite(values[:, i]), i]
        lo, hi = spec["bounds"]
        if (lo is not None and np.any(known < lo - 1e-7)) or (
            hi is not None and np.any(known > hi + 1e-7)
        ):
            raise ValueError("feature unit/range mismatch: " + spec["name"])
        if spec["discrete"] and np.any(~np.isin(known, spec["discrete"])):
            raise ValueError("invalid discrete feature value: " + spec["name"])


def validate_partitions(base, financial, *, base_names, financial_names):
    """Apply the original ordered production50 validation, including NaNs."""
    specs = _semantics()
    names = tuple(item["name"] for item in specs)
    if tuple(base_names) + tuple(financial_names) != names or len(base_names) != 36:
        raise ValueError("model input order, units or semantics mismatch")
    if (
        base.shape[:-1] != financial.shape[:-1]
        or base.shape[-1] != len(base_names)
        or financial.shape[-1] != len(financial_names)
    ):
        raise ValueError("unaligned named model inputs")
    base_specs, financial_specs = specs[:36], specs[36:]
    if base.ndim == 2:
        _validate_values(base, base_specs)
        _validate_values(financial, financial_specs)
    else:
        for i in range(len(base)):
            _validate_values(base[i], base_specs)
            _validate_values(financial[i], financial_specs)


def _hashed_json(root: Path, identifier: str, prefix: str) -> tuple[dict, bytes]:
    if not re.fullmatch(rf"{re.escape(prefix)}-sha256-[a-f0-9]{{64}}", identifier):
        raise ValueError(f"invalid {prefix} identifier")
    raw = read_regular(root / f"{identifier}.json")
    if identifier != f"{prefix}-sha256-{digest(raw)}":
        raise ValueError(f"{prefix} hash mismatch")
    return json.loads(raw), raw


def validate_panel_manifest(panel, *, as_of):
    specs = _semantics()
    base_names = tuple(item["name"] for item in specs[:36])
    if (
        panel.get("format") != "all-market-panel/v3"
        or tuple(panel.get("features", ())) != base_names
        or tuple(panel.get("horizons", ())) != (5, 10, 20)
        or datetime.fromisoformat(panel["as_of"]) > as_of
    ):
        raise ValueError("invalid composite panel")
    return base_names


def validate_overlay_manifest(overlay, panel_manifest_id, *, as_of):
    financial_names = tuple(item["name"] for item in _semantics()[36:])
    if (
        overlay.get("format") != "market-fundamental-overlay-manifest/v1"
        or overlay.get("base_manifest_id") != panel_manifest_id
        or tuple(overlay.get("features", ())) != financial_names
        or datetime.fromisoformat(overlay["as_of"]) > as_of
    ):
        raise ValueError("invalid fundamental overlay manifest")
    return financial_names


def load_inputs(repo, panel_manifest_id, overlay_manifest_id, risk_id, cache_id, *, as_of):
    """Load the same cache/source bindings used by the existing v2 research reader."""
    if not isinstance(as_of, datetime) or as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("model inputs require an aware as_of")
    data_root = Path(repo) / "runtime/all-market-data"
    panel, panel_raw = _hashed_json(
        data_root / "panels", panel_manifest_id, "market-panel-manifest"
    )
    base_names = validate_panel_manifest(panel, as_of=as_of)
    overlay, overlay_raw = _hashed_json(
        data_root / "fundamentals/overlays", overlay_manifest_id,
        "market-fundamental-overlay-manifest",
    )
    financial_names = validate_overlay_manifest(overlay, panel_manifest_id, as_of=as_of)
    risk, risk_raw = _hashed_json(data_root / "risk", risk_id, "market-risk-history")
    if datetime.fromisoformat(risk["as_of"]) > as_of:
        raise ValueError("risk history unavailable at as_of")
    cache, cache_raw = _hashed_json(data_root / "model-v2-cache", cache_id, "daily-model-v2-cache")
    if (
        cache.get("format") != "daily-model-v2-cache/v1"
        or cache.get("panel_manifest_id") != panel_manifest_id
        or cache.get("panel_manifest_sha256") != digest(panel_raw)
        or cache.get("overlay_manifest_id") != overlay_manifest_id
        or cache.get("overlay_manifest_sha256") != digest(overlay_raw)
        or cache.get("risk_history_id") != risk_id
        or cache.get("risk_history_sha256") != digest(risk_raw)
        or datetime.fromisoformat(cache["as_of"]) > as_of
    ):
        raise ValueError("model-v2 cache source binding mismatch")
    cache_root = data_root / "model-v2-cache" / cache_id
    arrays = {}
    try:
        for binding in cache["files"]:
            path = cache_root / binding["path"]
            if not path.resolve().is_relative_to(cache_root.resolve()):
                raise ValueError("model-v2 cache path escapes root")
            if binding["sha256"] != file_digest(path):
                raise ValueError("model-v2 cache file hash mismatch")
            value = np.load(path, allow_pickle=False, mmap_mode="r")
            if list(value.shape) != binding["shape"] or str(value.dtype) != binding["dtype"]:
                value._mmap.close()
                raise ValueError("model-v2 cache array contract mismatch")
            if binding["path"] in arrays:
                value._mmap.close()
                raise ValueError("duplicate model-v2 cache array")
            arrays[binding["path"]] = value

        x, y = arrays["features.npy"], arrays["labels.npy"]
        financial, industries = arrays["financial.npy"], arrays["industries.npy"]
        days, symbols = arrays["days.npy"], arrays["symbols.npy"]
        validate_panel_axes(
            x, y, days, symbols, feature_count=len(base_names), horizon_count=len(panel["horizons"])
        )
        validate_partitions(
            RowMappedArray(cache_root / "features.npy"),
            RowMappedArray(cache_root / "financial.npy"),
            base_names=base_names,
            financial_names=financial_names,
        )
        if x.shape[:2] != financial.shape[:2] or x.shape[:2] != industries.shape:
            raise ValueError("model-v2 cache arrays are not aligned")
        price_first_index = int(cache["price_first_index"])
        price_days = cache["price_metadata"]["days"]
        if not 0 <= price_first_index < len(days) or price_days != [
            str(value) for value in days[price_first_index:]
        ]:
            raise ValueError("model-v2 price cache is not aligned")
        if any(
            arrays[f"{name}.npy"].shape != (len(price_days), len(symbols))
            for name in ("opens", "closes", "volumes", "amounts")
        ):
            raise ValueError("model-v2 price cache arrays are not aligned")
        retained = {
            "features.npy", "labels.npy", "financial.npy", "industries.npy",
            "industry-labels.npy", "days.npy", "symbols.npy", "opens.npy", "closes.npy",
            "volumes.npy", "amounts.npy",
        }
        for name, array in arrays.items():
            if name not in retained:
                array._mmap.close()
        return {
            "panel_manifest": panel,
            "panel_manifest_raw": panel_raw,
            "overlay_manifest": overlay,
            "overlay_manifest_raw": overlay_raw,
            "risk_raw": risk_raw,
            "cache_raw": cache_raw,
            "risk_exclusions": cache["risk_exclusions"],
            "x": x,
            "y": y,
            "financial": financial,
            "industries": industries,
            "industry_labels": arrays["industry-labels.npy"],
            "days": days,
            "symbols": symbols,
            "prices": {
                "days": price_days,
                "opens": arrays["opens.npy"],
                "closes": arrays["closes.npy"],
                "volumes": arrays["volumes.npy"],
                "amounts": arrays["amounts.npy"],
                "catalog_sha256": cache["price_metadata"]["catalog_sha256"],
                "price_basis": cache["price_metadata"]["price_basis"],
            },
            "price_first_index": price_first_index,
        }
    except BaseException:
        for array in arrays.values():
            array._mmap.close()
        raise


def close_inputs(inputs):
    """Release mappings after a caller finishes with a successfully read cache."""
    for key in ("x", "y", "financial", "industries", "industry_labels", "days", "symbols"):
        inputs[key]._mmap.close()
    for key in ("opens", "closes", "volumes", "amounts"):
        inputs["prices"][key]._mmap.close()
