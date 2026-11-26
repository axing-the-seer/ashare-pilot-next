"""Verify one retained market-day receipt against one immutable daily partition.

This is the market-day branch of the existing training source verifier. It is
read-only and does not prove the catalog binding, historical point-in-time
availability, company risk, model quality, or an executable trade.
"""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from .dataset_publication import load_published_dataset
from .normalization import normalize_daily_bars
from .source_capture import read
from .tushare_models import (
    DailyBarRecord,
    validate_finite_float,
    validate_symbol,
    validate_trade_date,
)

FIELDS = ("ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount")
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("source evidence time is missing")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("source evidence time must be aware")
    return result


def verify_daily_partition(
    *,
    publication_root: Path,
    source_root: Path,
    dataset_id: str,
    manifest_sha256: str,
    trade_date: date,
    rows: int,
    receipt_id: str,
    as_of: datetime,
) -> dict:
    """Verify the caller's explicit binding; do not promote it into a catalog."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("source evidence cutoff must be aware")
    if not re.fullmatch(r"[a-f0-9]{64}", manifest_sha256):
        raise ValueError("invalid manifest hash")
    if not re.fullmatch(r"source-receipt-sha256-[a-f0-9]{64}", receipt_id):
        raise ValueError("invalid daily source receipt identity")
    if type(rows) is not int or rows <= 0:
        raise ValueError("expected partition row count must be positive")

    loaded = load_published_dataset(publication_root=publication_root, dataset_id=dataset_id)
    if hashlib.sha256(loaded.manifest_bytes).hexdigest() != manifest_sha256:
        raise ValueError("catalog partition manifest differs")
    if _time(loaded.manifest["generated_at"]) > as_of:
        raise ValueError("partition unavailable at cutoff")
    receipt = read(source_root / "receipts/tushare.daily", receipt_id)
    if (
        receipt.get("format") != "source-capture/v1"
        or receipt.get("datasetId") != "tushare.daily"
        or receipt.get("api") != "daily"
        or receipt.get("requestApi") != "daily"
        or receipt.get("source") != "tushare"
        or receipt.get("complete") is not True
        or receipt.get("status") != "available"
        or receipt.get("normalPipeline") is not True
        or receipt.get("reusedCheckpoint") is not False
        or type(receipt.get("duplicateRows")) is not int
        or receipt.get("duplicateRows") != 0
    ):
        raise ValueError("daily source receipt is incomplete or untrusted")
    requested, received = _time(receipt.get("requestedAt")), _time(receipt.get("receivedAt"))
    if requested > received or received > as_of:
        raise ValueError("daily source unavailable at cutoff")
    table_id = receipt.get("tableId")
    if not isinstance(table_id, str) or not re.fullmatch(
        r"source-table-sha256-[a-f0-9]{64}", table_id
    ):
        raise ValueError("invalid daily source table identity")
    table = read(source_root / "tables", table_id)
    fields = table.get("fields")
    if (
        not isinstance(fields, list)
        or not all(isinstance(field, str) for field in fields)
        or len(fields) != len(set(fields))
        or not set(FIELDS).issubset(fields)
        or receipt.get("requestedFields") != fields
        or not isinstance(table.get("items"), list)
        or not table["items"]
        or type(receipt.get("rows")) is not int
        or len(table["items"]) != receipt.get("rows")
    ):
        raise ValueError("daily source receipt and table differ")
    if receipt.get("params") != {"trade_date": trade_date.strftime("%Y%m%d")} or receipt.get(
        "scope"
    ) != "market":
        raise ValueError("daily source date or scope mismatch")
    if requested < datetime.combine(trade_date, time(15), tzinfo=SHANGHAI):
        raise ValueError("daily source requested before its final market close")

    raw_rows = []
    seen = set()
    for item in table["items"]:
        if not isinstance(item, list) or len(item) != len(fields):
            raise ValueError("daily source row shape mismatch")
        row = dict(zip(fields, item, strict=True))
        day = validate_trade_date(row["trade_date"], context="daily source")
        symbol = validate_symbol(row["ts_code"], context="daily source")
        key = (symbol, day)
        if key in seen:
            raise ValueError("duplicate daily source row")
        seen.add(key)
        if day != trade_date:
            raise ValueError("daily source rows escape their request")
        raw_rows.append(
            DailyBarRecord(
                symbol,
                day,
                *(
                    validate_finite_float(row[k], context="daily source", field=k)
                    for k in FIELDS[2:]
                ),
            )
        )
    symbols = sorted({row.ts_code for row in raw_rows})
    normalized = normalize_daily_bars(
        raw_rows, requested_symbols=symbols, window_start=trade_date, window_end=trade_date
    )
    if normalized != loaded.records or len(normalized) != rows:
        raise ValueError("normalized partition differs from raw source")
    return {
        "contract_id": "daily-source-verification",
        "schema_version": "1.0.0",
        "dataset_id": dataset_id,
        "manifest_sha256": manifest_sha256,
        "trade_date": trade_date.isoformat(),
        "as_of": as_of.isoformat(),
        "binding_origin": "caller-supplied",
        "source_link": "matching-content-only",
        "source_kind": "captured_daily",
        "source_receipt_id": receipt_id,
        "table_id": table_id,
        "source_received_at": received.isoformat(),
        "partition_generated_at": _time(loaded.manifest["generated_at"]).isoformat(),
        "available_at": received.isoformat(),
        "historical_pit_qualified": False,
        "rows": len(normalized),
    }
