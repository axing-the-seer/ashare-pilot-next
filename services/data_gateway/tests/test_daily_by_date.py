"""The real day-scoped client path keeps pagination, units and evidence together."""

from datetime import date

import pytest
from ashare_data_gateway.tushare_client import DailyMarketDataUnavailable, TushareClient
from ashare_data_gateway.tushare_transport import TransportResponse

FIELDS = ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount")
ROWS = (
    ("000001.SZ", "20260925", 10.0, 11.0, 9.0, 10.0, 100.0, 100.0),
    ("000002.SZ", "20260925", 20.0, 21.0, 19.0, 20.0, 200.0, 200.0),
    ("600001.SH", "20260925", 30.0, 31.0, 29.0, 30.0, 300.0, 300.0),
)


class Wire:
    def __init__(self, pages):
        self.pages = list(pages)
        self.requests = []
        self.observed = []
        self.started = []

    def begin_logical_call(self, api):
        self.started.append(api)

    def send(self, request, *, token):
        self.requests.append(request)
        return self.pages.pop(0)

    def on_complete_response(self, **result):
        self.observed.append(result)
        return {"receiptId": "synthetic-complete-capture"}


@pytest.fixture(autouse=True)
def synthetic_token(monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", "TEST-ONLY")


def response(rows, fields=FIELDS):
    return TransportResponse(0, "", fields, rows)


def test_day_api_captures_one_complete_ordered_response_after_pagination():
    wire = Wire([response(ROWS[:2]), response((tuple(reversed(ROWS[2])),), FIELDS[::-1])])
    captured = []
    bars = TushareClient(wire, page_size=2).fetch_daily_by_date(
        trade_date=date(2026, 9, 25), capture_sink=captured.append
    )
    assert [r.offset for r in wire.requests] == [0, 2]
    assert all(r.params == (("trade_date", "20260925"),) for r in wire.requests)
    assert wire.started == ["daily"]
    assert len(wire.observed) == 1 and wire.observed[0]["response"].items == ROWS
    assert captured == [{"receiptId": "synthetic-complete-capture"}]
    assert [(r.ts_code, r.vol, r.amount) for r in bars] == [
        ("000001.SZ", 100.0, 100.0),
        ("000002.SZ", 200.0, 200.0),
        ("600001.SH", 300.0, 300.0),
    ]


def test_invalid_page_never_emits_a_completed_capture():
    wire = Wire([response(ROWS[:2]), response((("bad",),), ("unexpected",))])
    captured = []
    with pytest.raises(ValueError, match="response fields"):
        TushareClient(wire, page_size=2).fetch_daily_by_date(
            trade_date=date(2026, 9, 25), capture_sink=captured.append
        )
    assert not wire.observed and not captured


@pytest.mark.parametrize(
    "fields,expected",
    [
        (FIELDS + ("pre_close", "change", "pct_chg", "ah_vol", "ah_amount"),
         DailyMarketDataUnavailable),
        (FIELDS + ("unknown",), ValueError),
    ],
)
def test_empty_provider_shape_is_distinguished_from_unknown_schema(fields, expected):
    wire = Wire([response((), fields)])
    with pytest.raises(expected) as error:
        TushareClient(wire).fetch_daily_by_date(trade_date=date(2026, 9, 25))
    if expected is ValueError:
        assert not isinstance(error.value, DailyMarketDataUnavailable)
    assert not wire.observed


@pytest.mark.parametrize(
    "rows,problem",
    [
        ((ROWS[0], ROWS[0]), "duplicate primary key"),
        ((("000001.SZ", "20260924", *ROWS[0][2:]),), "outside request window"),
    ],
)
def test_day_api_rejects_duplicate_symbols_and_other_dates(rows, problem):
    with pytest.raises(ValueError, match=problem):
        TushareClient(Wire([response(rows)])).fetch_daily_by_date(trade_date=date(2026, 9, 25))


def test_absent_observer_is_explicit_in_capture_sink():
    class PlainWire:
        def send(self, request, *, token):
            return response((ROWS[0],))

    captured = []
    TushareClient(PlainWire()).fetch_daily_by_date(
        trade_date=date(2026, 9, 25), capture_sink=captured.append
    )
    assert captured == [None]
