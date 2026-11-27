"""The normal paged client captures its actual response without a second request."""

import json
from datetime import UTC, date, datetime, timedelta

import pytest
from ashare_data_gateway import source_capture
from ashare_data_gateway.source_capture import read
from ashare_data_gateway.source_catalog import DEFINITIONS, definitions
from ashare_data_gateway.tushare_client import TushareClient
from ashare_data_gateway.tushare_transport import HttpJsonTransport, TransportResponse

FIELDS = ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount")
ROWS = (
    ("000001.SZ", "20260925", 10, 11, 9, 10, 100, 100),
    ("000002.SZ", "20260925", 20, 21, 19, 20, 200, 200),
    ("600001.SH", "20260925", 30, 31, 29, 30, 300, 300),
)


def test_http_day_capture_keeps_full_response_and_actual_two_times(tmp_path, monkeypatch):
    root = tmp_path / "runtime/upstream-source-data"
    monkeypatch.setenv("TUSHARE_SOURCE_CAPTURE_ROOT", str(root))
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-ONLY")
    started = datetime(2026, 9, 25, 7, 1, tzinfo=UTC)
    received = started + timedelta(seconds=12)
    original = source_capture.response_observer
    monkeypatch.setattr(source_capture, "receipt_time", lambda: started)
    monkeypatch.setattr(
        source_capture, "response_observer", lambda path: original(path, clock=lambda: received)
    )
    transport = HttpJsonTransport(base_url="https://example.test")
    calls = []

    def send(request, *, token):
        calls.append(request.offset)
        return TransportResponse(0, "", FIELDS, ROWS[:2] if request.offset == 0 else ROWS[2:])

    monkeypatch.setattr(transport, "send", send)
    captured = []
    bars = TushareClient(transport, page_size=2).fetch_daily_by_date(
        trade_date=date(2026, 9, 25), capture_sink=captured.append
    )
    assert len(bars) == 3 and calls == [0, 2] and len(captured) == 1
    receipt = captured[0]
    assert receipt["requestedAt"] == started.isoformat()
    assert receipt["receivedAt"] == received.isoformat()
    assert receipt["complete"] and receipt["normalPipeline"] and not receipt["reusedCheckpoint"]
    assert receipt["scope"] == "market" and receipt["params"] == {"trade_date": "20260925"}
    retained = read(root / "receipts/tushare.daily", receipt["receiptId"])
    table = read(root / "tables", retained["tableId"])
    assert table["fields"] == list(FIELDS) and table["items"] == [list(row) for row in ROWS]
    assert "TEST-ONLY" not in str(retained) and "TEST-ONLY" not in str(table)


def test_bad_second_page_has_no_complete_receipt(tmp_path, monkeypatch):
    root = tmp_path / "runtime/upstream-source-data"
    monkeypatch.setenv("TUSHARE_SOURCE_CAPTURE_ROOT", str(root))
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-ONLY")
    transport = HttpJsonTransport(base_url="https://example.test")

    def send(request, *, token):
        if request.offset == 0:
            return TransportResponse(0, "", FIELDS, ROWS[:2])
        return TransportResponse(0, "", ("invalid",), ((1,),))

    monkeypatch.setattr(transport, "send", send)
    with pytest.raises(ValueError, match="response fields"):
        TushareClient(transport, page_size=2).fetch_daily_by_date(trade_date=date(2026, 9, 25))
    assert not root.exists()


def test_archive_failure_preserves_response_but_cannot_supply_recovery_proof(
    tmp_path, monkeypatch, caplog
):
    root = tmp_path / "runtime/upstream-source-data"
    monkeypatch.setenv("TUSHARE_SOURCE_CAPTURE_ROOT", str(root))
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-ONLY")
    transport = HttpJsonTransport(base_url="https://example.test")
    monkeypatch.setattr(
        transport, "send", lambda request, *, token: TransportResponse(0, "", FIELDS, ROWS[:1])
    )

    def failed_store(*args, **kwargs):
        raise OSError("PRIVATE-DIAGNOSTIC")

    monkeypatch.setattr(source_capture, "store", failed_store)
    captured = []
    bars = TushareClient(transport).fetch_daily_by_date(
        trade_date=date(2026, 9, 25), capture_sink=captured.append
    )
    assert len(bars) == 1 and captured == [None]
    assert transport.source_capture_status["failed"] == 1
    assert "source_capture_failed:OSError" in caplog.text
    assert "PRIVATE-DIAGNOSTIC" not in caplog.text and "TEST-ONLY" not in caplog.text


def test_capture_root_is_explicit_and_bounded(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_SOURCE_CAPTURE_ROOT", raising=False)
    assert not hasattr(HttpJsonTransport(base_url="https://example.test"), "on_complete_response")
    monkeypatch.setenv("TUSHARE_SOURCE_CAPTURE_ROOT", str(tmp_path / "elsewhere"))
    with pytest.raises(ValueError, match="explicit source capture runtime"):
        HttpJsonTransport(base_url="https://example.test")


def test_definition_slice_has_its_own_identity_and_accepts_explicit_full_format(tmp_path):
    sliced = json.loads(DEFINITIONS.read_text())
    assert sliced["format"] == "source-field-definitions-slice/v1"
    assert {row["id"] for row in definitions()} == {"tushare.daily", "tushare.trade_cal"}
    path = tmp_path / "full.json"
    path.write_text(
        json.dumps({"format": "source-field-definitions/v1", "datasets": sliced["datasets"]})
    )
    assert definitions(path) == sliced["datasets"]
    sliced["scope"] = ["tushare.daily"]
    path.write_text(json.dumps(sliced))
    with pytest.raises(ValueError, match="unsupported source definitions"):
        definitions(path)


def test_unmigrated_api_does_not_claim_a_capture(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TUSHARE_SOURCE_CAPTURE_ROOT", str(tmp_path / "runtime/upstream-source-data")
    )
    transport = HttpJsonTransport(base_url="https://example.test")
    fields = ("trade_date", "close")
    monkeypatch.setattr(
        transport,
        "send",
        lambda request, *, token: TransportResponse(0, "", fields, (("20260925", 10),)),
    )
    result = TushareClient(transport)._fetch_all_pages(
        api_name="daily_basic", params=(("trade_date", "20260925"),), fields=fields, token="TEST"
    )
    assert len(result.items) == 1 and transport.source_capture_status["skipped"] == 1
    assert not (tmp_path / "runtime/upstream-source-data").exists()
