"""Synthetic producer/consumer checks for the read-only daily-source slice."""

from __future__ import annotations

import hashlib
import json
import socket
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from ashare_data_gateway.daily_source_verification import FIELDS, verify_daily_partition
from ashare_data_gateway.dataset_publication import (
    prepare_normalized_dataset,
    publish_normalized_dataset,
    source_identity_from_base_url,
)
from ashare_data_gateway.normalization import normalize_daily_bars
from ashare_data_gateway.pilot_acquire import main
from ashare_data_gateway.source_capture import read, store
from ashare_data_gateway.tushare_models import DailyBarRecord
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[3]
DAY = date(2023, 10, 31)
AS_OF = datetime(2023, 10, 31, 10, 0, tzinfo=UTC)


@pytest.fixture
def evidence(tmp_path):
    publication_root = tmp_path / "published"
    source_root = tmp_path / "runtime" / "upstream-source-data"
    row = DailyBarRecord("000001.SZ", DAY, 10, 11, 9, 10.5, 100, 100)
    normalized = normalize_daily_bars(
        [row], requested_symbols=[row.ts_code], window_start=DAY, window_end=DAY
    )
    prepared = prepare_normalized_dataset(
        normalized,
        as_of=DAY,
        generated_at=datetime(2023, 10, 31, 8, 0, tzinfo=UTC),
        source_identity=source_identity_from_base_url("https://api.tushare.pro"),
        source_version="all-market-day/v1",
    )
    publish_normalized_dataset(publication_root=publication_root, prepared=prepared)
    table = {
        "fields": list(FIELDS),
        "items": [["000001.SZ", "20231031", 10, 11, 9, 10.5, 100, 100]],
    }
    table_id = store(source_root / "tables", "source-table", table)
    receipt = {
        "format": "source-capture/v1",
        "datasetId": "tushare.daily",
        "source": "tushare",
        "api": "daily",
        "requestApi": "daily",
        "params": {"trade_date": "20231031"},
        "scope": "market",
        "requestedAt": "2023-10-31T07:10:00+00:00",
        "receivedAt": "2023-10-31T07:15:00+00:00",
        "status": "available",
        "complete": True,
        "rows": 1,
        "requestedFields": list(FIELDS),
        "tableId": table_id,
        "duplicateRows": 0,
        "normalPipeline": True,
        "reusedCheckpoint": False,
    }
    receipt_id = store(source_root / "receipts/tushare.daily", "source-receipt", receipt)
    return {
        "publication_root": publication_root,
        "source_root": source_root,
        "dataset_id": prepared.dataset_id,
        "manifest_sha256": hashlib.sha256(prepared.manifest_bytes).hexdigest(),
        "trade_date": DAY,
        "rows": 1,
        "receipt_id": receipt_id,
        "as_of": AS_OF,
        "receipt": receipt,
        "table": table,
    }


def _arguments(evidence):
    return {k: v for k, v in evidence.items() if k not in {"receipt", "table"}}


def test_existing_public_entry_verifies_a_bound_receipt_without_network(
    evidence, monkeypatch, capsys
):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(socket, "socket", lambda *a, **kw: pytest.fail("network is not needed"))
    result = main(
        [
            "--verify-source-receipt", evidence["receipt_id"],
            "--source-root", str(evidence["source_root"]),
            "--publication-root", str(evidence["publication_root"]),
            "--dataset-id", evidence["dataset_id"],
            "--manifest-sha256", evidence["manifest_sha256"],
            "--trade-date", DAY.isoformat(),
            "--expected-rows", "1",
            "--as-of", AS_OF.isoformat(),
        ]
    )
    assert result == 0
    doc = json.loads(capsys.readouterr().out)
    schema_path = ROOT / "contracts/schemas/daily-source-verification.schema.json"
    schema = json.loads(schema_path.read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(doc)
    assert doc["source_received_at"] == "2023-10-31T07:15:00+00:00"
    assert doc["partition_generated_at"] == "2023-10-31T08:00:00+00:00"
    assert doc["historical_pit_qualified"] is False
    assert doc["binding_origin"] == "caller-supplied"
    assert doc["source_link"] == "matching-content-only"


@pytest.mark.parametrize(
    ("field", "bad", "error"),
    [
        ("complete", False, "incomplete or untrusted"),
        ("reusedCheckpoint", True, "incomplete or untrusted"),
        ("duplicateRows", 1, "incomplete or untrusted"),
        ("duplicateRows", False, "incomplete or untrusted"),
        ("scope", "security", "date or scope"),
        ("params", {"trade_date": "20231030"}, "date or scope"),
        ("receivedAt", "2023-10-31T11:00:00+00:00", "unavailable at cutoff"),
        ("requestedAt", "2023-10-31T06:59:00+00:00", "before its final market close"),
        ("rows", 2, "receipt and table differ"),
        ("rows", True, "receipt and table differ"),
        ("tableId", "source-receipt-sha256-" + "0" * 64, "source table identity"),
    ],
)
def test_untrusted_or_wrong_receipts_do_not_verify(evidence, field, bad, error):
    altered = {**evidence["receipt"], field: bad}
    evidence["receipt_id"] = store(
        evidence["source_root"] / "receipts/tushare.daily", "source-receipt", altered
    )
    with pytest.raises(ValueError, match=error):
        verify_daily_partition(**_arguments(evidence))


def test_wrong_binding_cutoff_and_tampered_table_are_rejected(evidence):
    with pytest.raises(ValueError, match="manifest differs"):
        verify_daily_partition(**{**_arguments(evidence), "manifest_sha256": "0" * 64})
    with pytest.raises(ValueError, match="cutoff"):
        verify_daily_partition(**{**_arguments(evidence), "as_of": datetime(2023, 10, 31)})
    with pytest.raises(ValueError, match="receipt identity"):
        verify_daily_partition(
            **{**_arguments(evidence), "receipt_id": "source-table-sha256-" + "0" * 64}
        )
    with pytest.raises(ValueError, match="differs from raw source"):
        verify_daily_partition(**{**_arguments(evidence), "rows": 2})
    path = evidence["source_root"] / "tables" / (evidence["receipt"]["tableId"] + ".json")
    path.write_text("{}")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_daily_partition(**_arguments(evidence))


def test_content_change_with_valid_new_hash_cannot_match_published_partition(evidence):
    altered = {**evidence["table"], "items": [["000001.SZ", "20231031", 10, 11, 9, 10, 100, 100]]}
    changed_table = store(evidence["source_root"] / "tables", "source-table", altered)
    changed_receipt = {**evidence["receipt"], "tableId": changed_table}
    evidence["receipt_id"] = store(
        evidence["source_root"] / "receipts/tushare.daily", "source-receipt", changed_receipt
    )
    with pytest.raises(ValueError, match="differs from raw source"):
        verify_daily_partition(**_arguments(evidence))


@pytest.mark.parametrize(
    ("item", "message"),
    [
        (["000001.SZ", "20231030", 10, 11, 9, 10.5, 100, 100], "escape their request"),
        (["000001.SZ", "20231031", True, 11, 9, 10.5, 100, 100], "numeric, got bool"),
        (["000001.SZ", "20231031", "10", 11, 9, 10.5, 100, 100], "numeric, got str"),
    ],
)
def test_source_row_must_match_scope_and_public_client_value_types(evidence, item, message):
    altered = {**evidence["table"], "items": [item]}
    table_id = store(evidence["source_root"] / "tables", "source-table", altered)
    receipt = {**evidence["receipt"], "tableId": table_id}
    evidence["receipt_id"] = store(
        evidence["source_root"] / "receipts/tushare.daily", "source-receipt", receipt
    )
    with pytest.raises(ValueError, match=message):
        verify_daily_partition(**_arguments(evidence))


def test_source_objects_are_immutable_and_recheck_their_hash(evidence):
    root = evidence["source_root"] / "receipts/tushare.daily"
    identity = evidence["receipt_id"]
    assert store(root, "source-receipt", evidence["receipt"]) == identity
    assert read(root, identity) == evidence["receipt"]
    (root / (identity + ".json")).write_text("{}")
    with pytest.raises(ValueError, match="content collision"):
        store(root, "source-receipt", evidence["receipt"])
