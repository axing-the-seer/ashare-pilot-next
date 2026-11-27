"""Exercise the scheduled module's real entry, transport and shared budget offline."""

import itertools
import json
import sqlite3
import sys
from datetime import UTC, datetime, timedelta

import ashare_obs
from ashare_data_gateway import market_daily, source_capture, tushare_transport
from ashare_data_gateway.source_capture import read

FIELDS = ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount")
ROWS = (("000001.SZ", "20260925", 10, 11, 9, 10, 100, 100),)


def test_normal_cli_recovers_only_after_two_real_pipeline_receipts(tmp_path, monkeypatch, capsys):
    source = tmp_path / "runtime/upstream-source-data"
    output = tmp_path / "runtime/all-market-data"
    budget = tmp_path / "quota"
    monkeypatch.setenv("TUSHARE_SOURCE_CAPTURE_ROOT", str(source))
    monkeypatch.setenv("TUSHARE_BUDGET_DIR", str(budget))
    monkeypatch.setenv("TUSHARE_HTTP_URL", "https://example.test")
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-ONLY")

    base = datetime(2026, 9, 25, 7, 1, tzinfo=UTC)
    ticks = itertools.count()

    def clock():
        return base + timedelta(seconds=next(ticks))

    observe = source_capture.response_observer
    monkeypatch.setattr(source_capture, "receipt_time", clock)
    monkeypatch.setattr(
        source_capture, "response_observer", lambda root: observe(root, clock=clock)
    )
    monkeypatch.setattr(ashare_obs, "receipt_time", lambda: base + timedelta(hours=1))
    requests = []

    def fake_io(request, *, timeout_seconds):
        payload = json.loads(request.data)
        requests.append(payload["api_name"])
        if payload["api_name"] == "trade_cal":
            fields = ["exchange", "cal_date", "is_open", "pretrade_date"]
            rows = [["SSE", "20260925", 1, "20260924"]]
        else:
            assert payload["api_name"] == "daily" and payload["params"]["trade_date"] == "20260925"
            fields, rows = FIELDS, ROWS
        return json.dumps(
            {"code": 0, "msg": "", "data": {"fields": fields, "items": rows}}
        ).encode()

    monkeypatch.setattr(tushare_transport, "_perform_io", fake_io)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "market_daily",
            "--start",
            "2026-09-25",
            "--end",
            "2026-09-25",
            "--generated-at",
            "2026-09-25T07:00:00+00:00",
            "--publication-root",
            str(output),
        ],
    )
    market_daily.main()
    first = json.loads(capsys.readouterr().out)["partitions"][0]
    index = output / "market-days/2026-09-25.json"
    original_index = index.read_bytes()
    dataset = output / "datasets" / first["dataset_id"] / "daily-bars.json"
    original_dataset = dataset.read_bytes()
    index.unlink()  # Only the synthetic index is removed to simulate interruption.

    market_daily.main()
    recovered = json.loads(capsys.readouterr().out)["partitions"][0]
    assert recovered["recovered"] and recovered["dataset_id"] == first["dataset_id"]
    assert index.read_bytes() == original_index and dataset.read_bytes() == original_dataset
    proof = json.loads(
        (output / "market-day-recoveries" / f"{recovered['recovery_proof_id']}.json").read_text()
    )
    first_capture, retry_capture = proof["matching_capture"], proof["retry_capture"]
    assert first_capture["receipt_id"] != retry_capture["receipt_id"]
    assert first_capture["received_at"] != retry_capture["received_at"]
    assert first_capture["table_id"] == retry_capture["table_id"]
    saved = read(source / "receipts/tushare.daily", retry_capture["receipt_id"])
    assert saved["normalPipeline"] and saved["complete"] and saved["rows"] == 1
    assert requests == ["trade_cal", "daily", "trade_cal", "daily"]
    ledgers = list(budget.glob("*.sqlite3"))
    assert len(ledgers) == 1
    with sqlite3.connect(ledgers[0]) as connection:
        assert connection.execute("SELECT COUNT(*) FROM reservations").fetchone()[0] == 4
