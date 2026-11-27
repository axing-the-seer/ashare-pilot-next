"""Hash-bound price/risk readers shared without importing research orchestration."""

import json
from datetime import datetime
from pathlib import Path

import numpy as np
from ashare_quant_core.daily_selection import apply_risk_history

from .immutable_artifacts import digest, read_regular


def _apply_risk_history(x, y, days, symbols, document):
    return apply_risk_history(x, y, days, symbols, document, array_ops=np)


def _load_recent_prices(
    repo: Path,
    manifest: dict[str, object],
    days: np.ndarray,
    symbols: np.ndarray,
    *,
    first_index: int,
    as_of: datetime,
):
    """Load hash-bound raw prices and factors only for the evaluated window."""
    root = repo / "runtime/all-market-data"
    catalog_id = str(manifest["catalog_id"])
    catalog_raw = read_regular(root / "catalogs" / f"{catalog_id}.json")
    if catalog_id != "market-catalog-sha256-" + digest(catalog_raw):
        raise ValueError("catalog hash mismatch")
    catalog = json.loads(catalog_raw)
    if datetime.fromisoformat(catalog["generated_at"]) > as_of:
        raise ValueError("future catalog")

    selected_days = [str(value) for value in days[first_index:]]
    day_index = {value: index for index, value in enumerate(selected_days)}
    symbol_index = {str(value): index for index, value in enumerate(symbols)}
    shape = (len(selected_days), len(symbols))
    opens = np.full(shape, np.nan, dtype=np.float32)
    closes = np.full(shape, np.nan, dtype=np.float32)
    volumes = np.full(shape, np.nan, dtype=np.float32)
    amounts = np.full(shape, np.nan, dtype=np.float64)
    factors = np.full(shape, np.nan, dtype=np.float32)

    bindings = {item["dataset_id"]: item["manifest_sha256"] for item in catalog["bindings"]}
    entries = [*catalog["histories"], *catalog["daily_partitions"]]
    for entry in entries:
        identifier = entry["dataset_id"]
        if identifier not in bindings:
            raise ValueError("catalog price binding is incomplete")
        raw = read_regular(root / "manifests" / f"{identifier}.json")
        if digest(raw) != bindings[identifier]:
            raise ValueError("price manifest hash mismatch")
        price_manifest = json.loads(raw)
        artifact = price_manifest["files"][0]
        folder = root / "datasets" / identifier
        path = folder / artifact["path"]
        if not path.resolve().is_relative_to(folder.resolve()):
            raise ValueError("dataset path escapes root")
        content = read_regular(path)
        if digest(content) != artifact["sha256"]:
            raise ValueError("price content hash mismatch")
        for row in json.loads(content):
            i = day_index.get(row["trade_date"])
            j = symbol_index.get(row["symbol"])
            if i is None or j is None:
                continue
            if np.isfinite(opens[i, j]):
                raise ValueError("overlapping price row")
            opens[i, j] = float(row["open"])
            closes[i, j] = float(row["close"])
            volumes[i, j] = float(row["volume"])
            amounts[i, j] = float(row["amount"])

    factor_ids = manifest.get("factor_ids")
    if not isinstance(factor_ids, list) or not factor_ids:
        raise ValueError("panel has no factor bindings")
    for identifier in factor_ids:
        raw = read_regular(root / "factors" / f"{identifier}.json")
        if identifier != "market-factors-sha256-" + digest(raw):
            raise ValueError("factor hash mismatch")
        document = json.loads(raw)
        if datetime.fromisoformat(document["observed_at"]) > as_of:
            raise ValueError("future factor partition")
        for row in document["records"]:
            i = day_index.get(row["date"])
            j = symbol_index.get(row["symbol"])
            if i is None or j is None:
                continue
            if np.isfinite(factors[i, j]):
                raise ValueError("overlapping factor row")
            factors[i, j] = float(row["factor"])

    missing = np.isfinite(opens) & ~np.isfinite(factors)
    if missing.any():
        raise ValueError(f"recent prices are missing {int(missing.sum())} factors")
    base_factors = np.full(len(symbols), np.nan, dtype=np.float32)
    for j in range(len(symbols)):
        available = factors[:, j][np.isfinite(factors[:, j])]
        if available.size:
            base_factors[j] = available[0]
    scale = factors / base_factors[np.newaxis, :]
    return {
        "days": selected_days,
        "opens": opens * scale,
        "closes": closes * scale,
        "volumes": volumes,
        "amounts": amounts,
        "catalog_sha256": digest(catalog_raw),
        "price_basis": "raw_ohlc_times_factor_ratio_from_window_start",
    }
