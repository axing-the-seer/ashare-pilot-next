import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from threading import Barrier

import pytest
from ashare_data_gateway import market_daily
from ashare_data_gateway.daily_calendar import encode, publish
from ashare_data_gateway.source_capture import read, response_observer
from ashare_data_gateway.tushare_client import TushareClient
from ashare_data_gateway.tushare_transport import TransportResponse

DAY = date(2026, 9, 25)
FIELDS = ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount")
ROWS = (("000001.SZ", "20260925", 10.0, 11.0, 9.0, 10.0, 100.0, 100.0),)


def instant(minute):
    return datetime(2026, 9, 25, 7, minute, tzinfo=UTC)


class CapturedTransport:
    def __init__(self, source_root, minute, *, rows=ROWS, barrier=None, captured_rows=None):
        self.rows = rows
        self.minute = minute
        self.barrier = barrier
        self.captured_rows = captured_rows
        self.observer = response_observer(source_root, clock=lambda: instant(minute))
        self.requests = 0

    def begin_logical_call(self, api):
        self.requested = instant(self.minute - 1).isoformat()

    def send(self, request, *, token):
        self.requests += 1
        return TransportResponse(0, "", FIELDS, self.rows)

    def on_complete_response(self, *, api_name, params, response):
        if self.captured_rows is not None:
            response = TransportResponse(0, "", FIELDS, self.captured_rows)
        receipt = self.observer(
            api_name=api_name, params=params, response=response, requested_at=self.requested
        )
        if self.barrier:
            self.barrier.wait(timeout=5)
        return receipt


@pytest.fixture
def case(tmp_path, monkeypatch):
    source = tmp_path / "runtime/upstream-source-data"
    root = tmp_path / "runtime/all-market-data"
    monkeypatch.setenv("TUSHARE_TOKEN", "FAKE-TEST-ONLY")
    monkeypatch.setenv("TUSHARE_SOURCE_CAPTURE_ROOT", str(source))
    return root, source


def acquire(root, source, minute, *, rows=ROWS, captured_rows=None, barrier=None):
    transport = CapturedTransport(
        source, minute, rows=rows, barrier=barrier, captured_rows=captured_rows
    )
    value = market_daily.acquire_day(
        TushareClient(transport),
        trade_date=DAY,
        generated_at=instant(2),
        publication_root=root,
        source_base_url="https://api.tushare.pro",
    )
    return value, transport


def interrupted(root, source):
    first, _ = acquire(root, source, 1)
    pointer = root / "market-days" / f"{DAY}.json"
    original = pointer.read_bytes()
    pointer.unlink()
    return first, original


def test_interruption_recovers_without_replacing_committed_objects_or_source_times(case):
    root, source = case
    first, original = interrupted(root, source)
    dataset_id = first["dataset_id"]
    manifest = root / "manifests" / (dataset_id + ".json")
    data = root / "datasets" / dataset_id / "daily-bars.json"
    saved = (manifest.read_bytes(), data.read_bytes())
    result, retry = acquire(root, source, 4)
    assert result["dataset_id"] == dataset_id and not result["cached"]
    assert (root / "market-days" / f"{DAY}.json").read_bytes() == original
    assert (manifest.read_bytes(), data.read_bytes()) == saved
    proof = json.loads(
        (root / "market-day-recoveries" / (result["recovery_proof_id"] + ".json")).read_bytes()
    )
    assert proof["matching_capture"]["received_at"] == instant(1).isoformat()
    assert proof["retry_capture"]["received_at"] == instant(4).isoformat()
    assert (
        proof["source_table_id"]
        == read(source / "receipts/tushare.daily", proof["retry_capture"]["receipt_id"])["tableId"]
    )
    again = market_daily.acquire_day(
        TushareClient(retry),
        trade_date=DAY,
        generated_at=instant(5),
        publication_root=root,
        source_base_url="https://api.tushare.pro",
    )
    assert again["cached"] is True and retry.requests == 1


def test_recovery_can_retry_after_proof_but_before_day_index(case, monkeypatch):
    root, source = case
    first, original = interrupted(root, source)
    real_publish = market_daily.publish

    def interrupt_day_index(root_arg, name, content):
        if root_arg.name == "market-days":
            raise OSError("simulated process interruption before day index")
        return real_publish(root_arg, name, content)

    monkeypatch.setattr(market_daily, "publish", interrupt_day_index)
    with pytest.raises(OSError, match="simulated process interruption"):
        acquire(root, source, 4)
    assert len(list((root / "market-day-recoveries").glob("*.json"))) == 1
    assert not (root / "market-days" / f"{DAY}.json").exists()
    monkeypatch.setattr(market_daily, "publish", real_publish)
    result, _ = acquire(root, source, 5)
    assert result["dataset_id"] == first["dataset_id"]
    assert (root / "market-days" / f"{DAY}.json").read_bytes() == original
    assert len(list((root / "market-day-recoveries").glob("*.json"))) == 2


def test_same_raw_rows_in_different_provider_order_keep_both_source_hashes(case):
    root, source = case
    other = ("600001.SH", "20260925", 10.0, 11.0, 9.0, 10.0, 100.0, 100.0)
    first, _ = acquire(root, source, 1, rows=(*ROWS, other))
    (root / "market-days" / f"{DAY}.json").unlink()
    result, _ = acquire(root, source, 4, rows=(other, *ROWS))
    assert result["dataset_id"] == first["dataset_id"] and result["recovered"] is True
    proof = json.loads(
        (root / "market-day-recoveries" / (result["recovery_proof_id"] + ".json")).read_bytes()
    )
    assert proof["matching_capture"]["table_id"] != proof["retry_capture"]["table_id"]
    assert proof["matching_capture"]["received_at"] == instant(1).isoformat()
    assert proof["retry_capture"]["received_at"] == instant(4).isoformat()


def test_vendor_revision_creates_new_id_without_rewriting_or_claiming_recovery(case):
    root, source = case
    first, _ = interrupted(root, source)
    ident = first["dataset_id"]
    manifest = root / "manifests" / (ident + ".json")
    data = root / "datasets" / ident / "daily-bars.json"
    previous = manifest.read_bytes(), data.read_bytes()
    revised = (("000001.SZ", "20260925", 10.0, 11.0, 9.0, 10.2, 100.0, 100.0),)
    result, _ = acquire(root, source, 4, rows=revised)
    assert result["dataset_id"] != ident
    assert "recovered" not in result and "recovery_proof_id" not in result
    assert (manifest.read_bytes(), data.read_bytes()) == previous
    assert (
        json.loads((root / "market-days" / f"{DAY}.json").read_bytes())["dataset_id"]
        == (result["dataset_id"])
    )


@pytest.mark.parametrize("target", ["data", "manifest", "auxiliary", "aux_index"])
def test_recovery_rejects_corrupt_committed_parts(case, target):
    root, source = case
    first, _ = interrupted(root, source)
    ident = first["dataset_id"]
    targets = {
        "data": root / "datasets" / ident / "daily-bars.json",
        "manifest": root / "manifests" / (ident + ".json"),
        "auxiliary": root / "auxiliary" / ident / "raw/market-day-receipt.json",
        "aux_index": root / "auxiliary" / ident / "auxiliary-index.json",
    }
    path = targets[target]
    path.write_bytes(path.read_bytes() + b" ")
    damaged = path.read_bytes()
    with pytest.raises((ValueError, OSError)):
        acquire(root, source, 4)
    assert path.read_bytes() == damaged
    assert not (root / "market-days" / f"{DAY}.json").exists()


def test_recovery_fails_closed_if_independent_source_missing(case):
    root, source = case
    interrupted(root, source)
    for path in (source / "receipts/tushare.daily").glob("*.json"):
        path.unlink()
    with pytest.raises(ValueError, match="independent matching"):
        acquire(root, source, 4)
    assert not (root / "market-days" / f"{DAY}.json").exists()


def test_recovery_does_not_fabricate_a_missing_capture_root(case, monkeypatch):
    root, source = case
    interrupted(root, source)
    monkeypatch.delenv("TUSHARE_SOURCE_CAPTURE_ROOT")
    with pytest.raises(ValueError, match="retained source receipt"):
        acquire(root, source, 4)
    assert not (root / "market-days" / f"{DAY}.json").exists()


def test_recovery_rejects_capture_that_does_not_match_input(case):
    root, source = case
    interrupted(root, source)
    wrong = (("000001.SZ", "20260925", 10.0, 11.0, 9.0, 10.1, 100.0, 100.0),)
    with pytest.raises(ValueError, match="source table differs"):
        acquire(root, source, 4, captured_rows=wrong)
    assert not (root / "market-days" / f"{DAY}.json").exists()


def test_recovery_rejects_corrupt_retained_source_table(case, monkeypatch):
    root, source = case
    interrupted(root, source)
    observe = CapturedTransport.on_complete_response

    def corrupt_after_capture(self, **kwargs):
        receipt = observe(self, **kwargs)
        table = source / "tables" / (receipt["tableId"] + ".json")
        table.write_bytes(table.read_bytes() + b" ")
        return receipt

    monkeypatch.setattr(CapturedTransport, "on_complete_response", corrupt_after_capture)
    with pytest.raises(ValueError, match="hash mismatch"):
        acquire(root, source, 4)
    assert not (root / "market-days" / f"{DAY}.json").exists()


def test_recovery_does_not_claim_a_future_source_receipt(case, monkeypatch):
    import ashare_obs

    root, source = case
    interrupted(root, source)
    monkeypatch.setattr(ashare_obs, "receipt_time", lambda: instant(3))
    with pytest.raises(ValueError, match="source unavailable at cutoff"):
        acquire(root, source, 4)
    assert not (root / "market-days" / f"{DAY}.json").exists()


def test_recovery_does_not_overwrite_racing_different_day_pointer(case, monkeypatch):
    root, source = case
    interrupted(root, source)
    rival_root = root.parent / "rival"
    rival_rows = (("000001.SZ", "20260925", 10.0, 11.0, 9.0, 10.2, 100.0, 100.0),)
    rival, _ = acquire(rival_root, source, 3, rows=rival_rows)
    ident = rival["dataset_id"]
    for directory in ("datasets", "auxiliary"):
        shutil.copytree(rival_root / directory / ident, root / directory / ident)
    shutil.copy2(
        rival_root / "manifests" / (ident + ".json"), root / "manifests" / (ident + ".json")
    )
    real_publish = market_daily.publish
    other = (rival_root / "market-days" / f"{DAY}.json").read_bytes()

    def racing(root_arg, name, content):
        if root_arg.name == "market-days":
            publish(root_arg, name, other)
        return real_publish(root_arg, name, content)

    monkeypatch.setattr(market_daily, "publish", racing)
    with pytest.raises(ValueError, match="collision"):
        acquire(root, source, 4)
    assert (root / "market-days" / f"{DAY}.json").read_bytes() == other


def test_different_concurrent_inputs_cannot_replace_winning_pointer(case):
    root, source = case
    barrier = Barrier(2)
    alternate = (("000001.SZ", "20260925", 10.0, 11.0, 9.0, 10.2, 100.0, 100.0),)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(acquire, root, source, minute, rows=rows, barrier=barrier)
            for minute, rows in ((3, ROWS), (4, alternate))
        ]
        successes, failures = [], []
        for future in futures:
            try:
                successes.append(future.result(timeout=10)[0])
            except ValueError as exc:
                failures.append(str(exc))
    assert len(successes) == 1 and failures == ["calendar evidence collision"]
    pointer = json.loads((root / "market-days" / f"{DAY}.json").read_bytes())
    assert pointer["dataset_id"] == successes[0]["dataset_id"]
    assert len(list((root / "manifests").glob("*.json"))) == 2


def test_concurrent_equal_inputs_publish_single_immutable_dataset(case):
    root, source = case
    barrier = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(acquire, root, source, minute, barrier=barrier) for minute in (3, 4)]
        results = [future.result(timeout=10)[0] for future in futures]
    assert len({result["dataset_id"] for result in results}) == 1
    assert len(list((root / "manifests").glob("*.json"))) == 1
    assert len(list((root / "market-days").glob("*.json"))) == 1
    assert sum("recovery_proof_id" in result for result in results) == 1


def test_wrong_date_in_existing_receipt_cannot_pass_as_cached(case):
    root, source = case
    acquire(root, source, 3)
    path = root / "market-days" / f"{DAY}.json"
    receipt = json.loads(path.read_bytes())
    receipt["trade_date"] = "2026-09-24"
    path.write_bytes(encode(receipt))
    with pytest.raises(ValueError, match="date mismatch"):
        acquire(root, source, 4)


@pytest.mark.parametrize("invalid", [True, 1.0, "1"])
def test_invalid_count_in_existing_receipt_cannot_pass_as_cached(case, invalid):
    root, source = case
    acquire(root, source, 3)
    path = root / "market-days" / f"{DAY}.json"
    receipt = json.loads(path.read_bytes())
    receipt["rows"] = invalid
    path.write_bytes(encode(receipt))
    with pytest.raises(ValueError, match="invalid counts"):
        acquire(root, source, 4)


def test_unsupported_capture_client_does_not_recover_without_proof(case):
    root, source = case
    interrupted(root, source)

    class NoCapture:
        def fetch_daily_by_date(self, *, trade_date):
            from ashare_data_gateway.tushare_models import DailyBarRecord

            return (DailyBarRecord("000001.SZ", DAY, 10, 11, 9, 10, 100, 100),)

    with pytest.raises(ValueError, match="this acquisition's retained source receipt"):
        market_daily.acquire_day(
            NoCapture(),
            trade_date=DAY,
            generated_at=instant(4),
            publication_root=root,
            source_base_url="https://api.tushare.pro",
        )
    assert not (root / "market-days" / f"{DAY}.json").exists()
