"""Synthetic producer/consumer checks for the normal v2 cache command."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
from ashare_research_app.daily_model_v2_inputs import (
    SEMANTICS_SHA256,
    RowMappedArray,
    _semantics,
    validate_partitions,
)
from ashare_research_app.immutable_artifacts import publish

AS_OF = "2026-11-26T23:00:00+08:00"


def _digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _json(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _publish_json(folder, prefix, document):
    folder.mkdir(parents=True, exist_ok=True)
    raw = _json(document)
    identifier = prefix + "-sha256-" + _digest(raw)
    (folder / f"{identifier}.json").write_bytes(raw)
    return identifier


def make_sources(repo: Path):
    """Construct a tiny, deterministic, entirely synthetic source bundle."""
    root = repo / "runtime/all-market-data"
    for folder in ("panels", "fundamentals/overlays", "risk", "factors", "datasets", "manifests"):
        (root / folder).mkdir(parents=True, exist_ok=True)
    days = np.array(["2026-11-23", "2026-11-24", "2026-11-25"])
    symbols = np.array(["SYNTH_A", "SYNTH_B", "SYNTH_C"])
    base = np.zeros((3, 3, 36), dtype=np.float32)
    base[0, 0, 0] = np.nan
    labels = np.arange(27, dtype=np.float32).reshape(3, 3, 3) / 100
    financial = np.zeros((3, 3, 14), dtype=np.float32)
    financial[2, 2, 0] = 0.25
    industries = np.array([[0, 1, 1]] * 3, dtype=np.int16)

    panel_tmp = root / "panels/tmp.npz"
    np.savez(panel_tmp, features=base, labels=labels, days=days, symbols=symbols)
    panel_id = "market-panel-sha256-" + _digest(panel_tmp.read_bytes())
    panel_tmp.rename(root / "panels" / f"{panel_id}.npz")
    overlay_tmp = root / "fundamentals/overlays/tmp.npz"
    np.savez(
        overlay_tmp, features=financial, industries=industries,
        industry_labels=np.array(["synthetic-0", "synthetic-1"]),
    )
    overlay_id = "market-fundamental-overlay-sha256-" + _digest(overlay_tmp.read_bytes())
    overlay_tmp.rename(root / "fundamentals/overlays" / f"{overlay_id}.npz")

    bars = [
        {"trade_date": str(day), "symbol": str(symbol), "open": 10, "close": 11,
         "volume": 100, "amount": 1000}
        for day in days for symbol in symbols
    ]
    raw_bars = _json(bars)
    dataset_id = "synthetic-dataset"
    folder = root / "datasets" / dataset_id
    folder.mkdir()
    (folder / "bars.json").write_bytes(raw_bars)
    price_manifest = _json({"files": [{"path": "bars.json", "sha256": _digest(raw_bars)}]})
    (root / "manifests" / f"{dataset_id}.json").write_bytes(price_manifest)
    catalog_id = _publish_json(root / "catalogs", "market-catalog", {
        "generated_at": AS_OF,
        "bindings": [{"dataset_id": dataset_id, "manifest_sha256": _digest(price_manifest)}],
        "histories": [{"dataset_id": dataset_id}], "daily_partitions": [],
    })
    factor_id = _publish_json(root / "factors", "market-factors", {
        "observed_at": AS_OF,
        "records": [{"date": str(day), "symbol": str(symbol), "factor": 1.0}
                    for day in days for symbol in symbols],
    })
    specs = _semantics()
    panel_manifest_id = _publish_json(root / "panels", "market-panel-manifest", {
        "format": "all-market-panel/v3", "features": [v["name"] for v in specs[:36]],
        "horizons": [5, 10, 20], "as_of": AS_OF, "panel_id": panel_id,
        "catalog_id": catalog_id, "factor_ids": [factor_id],
    })
    overlay_manifest_id = _publish_json(
        root / "fundamentals/overlays", "market-fundamental-overlay-manifest", {
            "format": "market-fundamental-overlay-manifest/v1", "as_of": AS_OF,
            "base_manifest_id": panel_manifest_id, "overlay_id": overlay_id,
            "features": [v["name"] for v in specs[36:]],
        },
    )
    risk_id = _publish_json(root / "risk", "market-risk-history", {
        "format": "market-risk-history/v1", "policy_id": "exclude-st-and-delisting-names/v1",
        "as_of": AS_OF, "coverage": {"start": str(days[0]), "end": str(days[-1])},
        "records": [{"symbol": "SYNTH_B", "start_date": str(days[1]), "end_date": str(days[1])}],
    })
    return {"repo": repo, "panel_manifest_id": panel_manifest_id,
            "overlay_manifest_id": overlay_manifest_id, "risk_id": risk_id,
            "as_of": AS_OF, "days": days, "symbols": symbols, "base": base,
            "labels": labels, "financial": financial}


def run_cache(source, *, verify=None, as_of=AS_OF, env=None):
    command = [sys.executable, "-m", "ashare_research_app.daily_model_v2_cache",
               "--repo", str(source["repo"]), "--panel-manifest-id", source["panel_manifest_id"],
               "--overlay-manifest-id", source["overlay_manifest_id"],
               "--risk-id", source["risk_id"],
               "--as-of", as_of]
    command.extend(["--first-index", "1"] if verify is None else ["--verify-cache-id", verify])
    child_env = dict(os.environ if env is None else env)
    child_env.update(OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")
    return subprocess.run(command, text=True, capture_output=True, timeout=30, env=child_env)


def _snapshot(root):
    return {str(p.relative_to(root)): _digest(p.read_bytes())
            for p in root.rglob("*") if p.is_file()}


def test_normal_cache_command_builds_and_reads_same_bound_arrays(tmp_path):
    source = make_sources(tmp_path)
    result = run_cache(source)
    assert result.returncode == 0, result.stderr
    output = json.loads(result.stdout)
    cache_id = output["cache_id"]
    cache_root = tmp_path / "runtime/all-market-data/model-v2-cache" / cache_id
    assert output["risk_exclusions"] == {"excluded_rows": 1, "excluded_symbols": 1}

    expected_base, expected_labels = source["base"].copy(), source["labels"].copy()
    expected_base[1, 1, :] = np.nan
    expected_labels[1, 1, :] = np.nan
    for name, expected in (("features", expected_base), ("labels", expected_labels),
                           ("financial", source["financial"]), ("days", source["days"]),
                           ("symbols", source["symbols"])):
        array_path = cache_root / f"{name}.npy"
        np.testing.assert_equal(np.load(array_path, allow_pickle=False), expected)
        expected_bytes = io.BytesIO()
        np.save(expected_bytes, expected, allow_pickle=False)
        assert _digest(array_path.read_bytes()) == _digest(expected_bytes.getvalue())

    before = _snapshot(tmp_path / "runtime")
    checked = run_cache(source, verify=cache_id)
    assert checked.returncode == 0, checked.stderr
    detail = json.loads(checked.stdout)
    assert detail["validated"] is True and detail["semantic_sha256"] == SEMANTICS_SHA256
    assert detail["base_shape"] == [3, 3, 36] and detail["financial_shape"] == [3, 3, 14]
    assert _snapshot(tmp_path / "runtime") == before
    future = run_cache(source, verify=cache_id, as_of="2026-11-26T22:00:00+08:00")
    assert future.returncode != 0 and "invalid composite panel" in future.stderr
    with (cache_root / "features.npy").open("ab") as changed:
        changed.write(b"tamper")
    corrupt = run_cache(source, verify=cache_id)
    assert corrupt.returncode != 0 and "file hash mismatch" in corrupt.stderr


@pytest.mark.parametrize("variation,expected", [
    ("valid", None), ("nan", None), ("infinity", "infinite feature values"),
    ("bound", "feature unit/range mismatch"), ("discrete", "invalid discrete feature value"),
])
def test_row_mapping_preserves_feature_values_and_validation(tmp_path, variation, expected):
    specs = _semantics()
    base_names = tuple(v["name"] for v in specs[:36])
    financial_names = tuple(v["name"] for v in specs[36:])
    base = np.zeros((4, 3, 36), dtype=np.float32)
    financial = np.zeros((4, 3, 14), dtype=np.float32)
    if variation == "nan":
        base[1, 1, 0] = np.nan
    elif variation == "infinity":
        financial[1, 0, 0] = np.inf
    elif variation == "bound":
        base[2, 1, 0] = 2
    elif variation == "discrete":
        financial[2, 0, 7] = 0.5
    np.save(tmp_path / "base.npy", base)
    np.save(tmp_path / "financial.npy", financial)
    ordinary = [np.load(tmp_path / f"{name}.npy", mmap_mode="r")
                for name in ("base", "financial")]
    mapped = [RowMappedArray(tmp_path / f"{name}.npy") for name in ("base", "financial")]
    outcomes = []
    for x, y in (ordinary, mapped):
        try:
            validate_partitions(x, y, base_names=base_names, financial_names=financial_names)
        except ValueError as error:
            outcomes.append(str(error))
        else:
            outcomes.append(None)
    assert outcomes[0] == outcomes[1]
    assert (outcomes[0] is None) if expected is None else expected in outcomes[0]
    np.testing.assert_equal(mapped[0][0], ordinary[0][0])
    for value in ordinary:
        value._mmap.close()


def test_mapping_closes_and_rows_are_independent(tmp_path, monkeypatch):
    original = np.arange(24, dtype=np.float32).reshape(3, 4, 2)
    path = tmp_path / "array.npy"
    np.save(path, original)
    load = np.load
    seen = []

    def record(*args, **kwargs):
        result = load(*args, **kwargs)
        seen.append(result)
        return result

    monkeypatch.setattr(np, "load", record)
    mapped = RowMappedArray(path)
    assert mapped.shape == original.shape and mapped.ndim == 3 and len(mapped) == 3
    assert seen[-1]._mmap.closed
    row = mapped[1]
    assert seen[-1]._mmap.closed
    np.testing.assert_equal(row, original[1])
    row[0, 0] = -1
    np.testing.assert_equal(load(path), original)


def test_feature_order_and_alignment_are_required():
    specs = _semantics()
    base_names = tuple(v["name"] for v in specs[:36])
    financial_names = tuple(v["name"] for v in specs[36:])
    base = np.zeros((2, 3, 36), dtype=np.float32)
    financial = np.zeros((2, 3, 14), dtype=np.float32)
    with pytest.raises(ValueError, match="order, units or semantics"):
        validate_partitions(
            base, financial, base_names=base_names[::-1], financial_names=financial_names
        )
    with pytest.raises(ValueError, match="unaligned"):
        validate_partitions(
            base, financial[:, :2], base_names=base_names, financial_names=financial_names
        )


def test_immutable_publication_accepts_identical_bytes_only(tmp_path):
    path = tmp_path / "same.json"
    publish(path, b"one")
    publish(path, b"one")
    with pytest.raises(ValueError, match="collision"):
        publish(path, b"different")
    assert path.read_bytes() == b"one"
