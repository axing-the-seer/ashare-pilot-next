"""Restart, real process contention, cooldown and fail-closed transport coverage."""

import hashlib
import json
import multiprocessing
import sqlite3
import urllib.error
import urllib.request
from datetime import date

import pytest
from ashare_data_gateway.persistent_budget import PersistentRequestBudget
from ashare_data_gateway.request_budget import RequestBudgetExceeded, shared_budget
from ashare_data_gateway.tushare_client import TushareClient
from ashare_data_gateway.tushare_transport import HttpJsonTransport, TushareRateLimitError


class Clock:
    def __init__(self, now=100000.0):
        self.now = now
        self.waits = []

    def read(self):
        return self.now

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds


def budget(path, clock, **kwargs):
    return PersistentRequestBudget(path=path, clock=clock.read, sleep=clock.sleep, **kwargs)


def test_restart_retains_spacing_and_rolling_cap(tmp_path):
    path, clock = tmp_path / "quota.sqlite3", Clock(86399)
    budget(path, clock, max_requests=2).acquire()
    budget(path, clock, max_requests=2).acquire()
    assert clock.waits == [2]
    # Midnight doesn't reset the rolling window; a more permissive process can't either.
    with pytest.raises(RequestBudgetExceeded, match="24-hour"):
        budget(path, clock, min_interval=0, max_requests=50).acquire()
    clock.now = 86399 + 86400
    budget(path, clock, max_requests=2).acquire()
    with pytest.raises(RequestBudgetExceeded, match="24-hour"):
        budget(path, clock, max_requests=2).acquire()


def test_cooldown_persists_and_extends_without_shortening(tmp_path):
    path, clock = tmp_path / "quota.sqlite3", Clock()
    first = budget(path, clock)
    first.acquire()
    first.pause(600)
    second = budget(path, clock)
    second.pause(60)
    clock.now += 599
    with pytest.raises(RequestBudgetExceeded, match="cooldown"):
        second.acquire()
    clock.now += 1
    second.acquire()


def test_concurrent_cooldown_rechecked_after_spacing_wait(tmp_path):
    path, clock = tmp_path / "quota.sqlite3", Clock()
    budget(path, clock).acquire()
    contender = budget(path, clock)

    def sleep(seconds):
        budget(path, clock).pause(120)
        clock.sleep(seconds)

    contender._sleep = sleep
    with pytest.raises(RequestBudgetExceeded, match="cooldown"):
        contender.acquire()
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM reservations").fetchone()[0] == 1


def test_backwards_clock_stops_without_resetting_usage(tmp_path):
    path, clock = tmp_path / "quota.sqlite3", Clock()
    budget(path, clock).acquire()
    clock.now -= 1
    with pytest.raises(RequestBudgetExceeded, match="backwards"):
        budget(path, clock).acquire()


def test_damaged_ledger_stops_before_transport_io(tmp_path, monkeypatch):
    path = tmp_path / "quota.sqlite3"
    original = b"damaged quota state"
    path.write_bytes(original)
    calls = []
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: calls.append(1))
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-NOT-A-REAL-KEY")
    client = TushareClient(HttpJsonTransport(budget=PersistentRequestBudget(path=path)))
    with pytest.raises(RequestBudgetExceeded, match="unavailable"):
        client.fetch_trade_cal(
            exchange="SSE", start_date=date(2026, 9, 25), end_date=date(2026, 9, 25)
        )
    assert calls == []
    assert path.read_bytes() == original


def test_actual_http429_survives_transport_recreation(tmp_path, monkeypatch):
    path, clock = tmp_path / "quota.sqlite3", Clock()
    calls = []

    def limit(*args, **kwargs):
        calls.append(1)
        raise urllib.error.HTTPError(
            "https://example.test", 429, "limited", {"Retry-After": "900"}, None
        )

    monkeypatch.setattr(urllib.request, "urlopen", limit)
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-NOT-A-REAL-KEY")
    client = TushareClient(HttpJsonTransport(budget=budget(path, clock)), max_retries=3)
    with pytest.raises(TushareRateLimitError):
        client.fetch_trade_cal(
            exchange="SSE", start_date=date(2026, 9, 25), end_date=date(2026, 9, 25)
        )
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT interval_seconds FROM policy").fetchone()[0] == 5
    clock.now += 899
    recreated = TushareClient(HttpJsonTransport(budget=budget(path, clock)))
    with pytest.raises(RequestBudgetExceeded, match="cooldown"):
        recreated.fetch_trade_cal(
            exchange="SSE", start_date=date(2026, 9, 25), end_date=date(2026, 9, 25)
        )
    assert calls == [1]


def test_limit_slowdown_survives_old_worker_and_keeps_usage(tmp_path):
    path, clock = tmp_path / "quota.sqlite3", Clock()
    original = budget(path, clock)
    original.acquire()
    original.pause(60)
    clock.now += 60
    old_worker = budget(path, clock, min_interval=0)
    old_worker.acquire()
    old_worker.acquire()
    assert clock.waits == [5]
    old_worker.pause(60)
    clock.now += 60
    original.acquire()
    original.acquire()
    assert clock.waits == [5, 10]
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM reservations").fetchone()[0] == 5
        assert connection.execute("SELECT maximum FROM policy").fetchone()[0] == 5000


@pytest.mark.parametrize(("initial", "expected"), [(20, 40), (90, 90)])
def test_limit_backoff_never_reduces_a_stricter_spacing_policy(tmp_path, initial, expected):
    path, clock = tmp_path / "quota.sqlite3", Clock()
    budget(path, clock, min_interval=initial).pause(60)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT interval_seconds FROM policy").fetchone()[0] == expected


def test_repeated_http429_adapts_beyond_ten_seconds_without_resetting_usage(tmp_path, monkeypatch):
    path, clock = tmp_path / "quota.sqlite3", Clock()
    calls = []

    def limit(*args, **kwargs):
        calls.append(1)
        raise urllib.error.HTTPError("https://example.test", 429, "limited", {}, None)

    monkeypatch.setattr(urllib.request, "urlopen", limit)
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-NOT-A-REAL-KEY")
    for count, expected in enumerate((5, 10, 20, 40, 60, 60), start=1):
        recreated = TushareClient(HttpJsonTransport(budget=budget(path, clock)), max_retries=3)
        with pytest.raises(TushareRateLimitError):
            recreated.fetch_trade_cal(
                exchange="SSE", start_date=date(2026, 9, 25), end_date=date(2026, 9, 25)
            )
        with sqlite3.connect(path) as connection:
            assert (
                connection.execute("SELECT interval_seconds FROM policy").fetchone()[0] == expected
            )
            assert connection.execute("SELECT COUNT(*) FROM reservations").fetchone()[0] == count
            assert connection.execute("SELECT maximum FROM policy").fetchone()[0] == 5000
        clock.now += 60
    assert len(calls) == 6


def _contender(path, start, output):
    limiter = PersistentRequestBudget(path=path, min_interval=0.01, max_requests=7)
    start.wait(timeout=10)
    accepted = 0
    for _ in range(7):
        try:
            limiter.acquire()
            accepted += 1
        except RequestBudgetExceeded:
            break
    output.put(accepted)


def test_independent_processes_share_one_cap_and_spacing(tmp_path):
    path = tmp_path / "quota.sqlite3"
    context = multiprocessing.get_context("spawn")
    start, output = context.Event(), context.Queue()
    processes = [context.Process(target=_contender, args=(path, start, output)) for _ in range(3)]
    for process in processes:
        process.start()
    try:
        start.set()
        assert sum(output.get(timeout=15) for _ in processes) == 7
        for process in processes:
            process.join(timeout=15)
            assert process.exitcode == 0
        with sqlite3.connect(path) as connection:
            times = [
                r[0]
                for r in connection.execute(
                    "SELECT started_at FROM reservations ORDER BY started_at"
                )
            ]
        assert len(times) == 7
        assert all(b - a >= 0.009 for a, b in zip(times, times[1:], strict=False))
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        output.close()


def test_default_factory_groups_credentials_and_paths_by_provider(monkeypatch, tmp_path):
    monkeypatch.setenv("TUSHARE_BUDGET_DIR", str(tmp_path))
    first = shared_budget("https://user:SECRET-CANARY@example.test/v1?key=KEY-CANARY")
    assert first is shared_budget("https://example.test/v2")
    assert first is shared_budget("https://EXAMPLE.test:443/")
    assert isinstance(first, PersistentRequestBudget)
    assert "CANARY" not in str(first.path)
    assert first is not shared_budget("https://different.test")


def test_default_transport_keeps_existing_75s_policy_cooldown_and_rate_failure(
    monkeypatch, tmp_path
):
    from ashare_data_gateway import persistent_budget, request_budget

    monkeypatch.setenv("TUSHARE_BUDGET_DIR", str(tmp_path))
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-NOT-A-REAL-KEY")
    monkeypatch.delenv("TUSHARE_SOURCE_CAPTURE_ROOT", raising=False)
    clock = Clock()
    provider = "https://same-provider.test"
    name = hashlib.sha256(b"same-provider.test:443").hexdigest() + ".sqlite3"
    path = tmp_path / name
    seed = budget(path, clock, min_interval=75, max_requests=5000)
    seed.acquire()
    seed.pause(120)

    actual_budget = persistent_budget.PersistentRequestBudget

    def restarted_budget(*, path):
        # The default constructor still requests 2 seconds; the existing
        # durable ledger must win. The fake clock avoids waiting in a test.
        return actual_budget(path=path, clock=clock.read, sleep=clock.sleep)

    monkeypatch.setattr(persistent_budget, "PersistentRequestBudget", restarted_budget)
    calls = []
    limited = False

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def read(self):
            data = {
                "fields": ["exchange", "cal_date", "is_open", "pretrade_date"],
                "items": [["SSE", "20260925", 1, "20260924"]],
            }
            return json.dumps({"code": 0, "msg": "", "data": data}).encode()

    def urlopen(*args, **kwargs):
        calls.append(1)
        if limited:
            raise urllib.error.HTTPError(provider, 429, "limited", {"Retry-After": "900"}, None)
        return Response()

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)

    def client():
        return TushareClient(HttpJsonTransport(base_url=provider), max_retries=3)

    def fetch(instance):
        return instance.fetch_trade_cal(
            exchange="SSE", start_date=date(2026, 9, 25), end_date=date(2026, 9, 25)
        )

    with pytest.raises(RequestBudgetExceeded, match="cooldown"):
        fetch(client())
    assert calls == []
    clock.now += 120
    assert len(fetch(client())) == 1
    # Drop only this synthetic path's process cache to model a new worker.
    request_budget._BUDGETS.pop(str(path))
    assert len(fetch(client())) == 1 and clock.waits == [75]
    limited = True
    with pytest.raises(TushareRateLimitError):
        fetch(client())
    assert len(calls) == 3  # A 429 stops immediately, even with max_retries=3.
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT interval_seconds, maximum, blocked_until FROM policy"
        ).fetchone()
        reservations = connection.execute("SELECT COUNT(*) FROM reservations").fetchone()[0]
    assert row == (75.0, 5000, clock.now + 900) and reservations == 4
    request_budget._BUDGETS.pop(str(path))
    with pytest.raises(RequestBudgetExceeded, match="cooldown"):
        fetch(client())
    assert len(calls) == 3
