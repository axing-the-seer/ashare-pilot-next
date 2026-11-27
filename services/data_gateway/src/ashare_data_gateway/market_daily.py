"""Immutable whole-market daily partitions using the existing paced provider client.

One date per partition permits interrupted backfills to resume without copying or
mutating earlier evidence. Universe eligibility and model acceptance are separate.
"""

import argparse
import hashlib
import json
import math
import os
from datetime import date, datetime, timedelta
from pathlib import Path

from .daily_calendar import encode, publish
from .dataset_publication import (
    AuxiliaryArtifact,
    load_published_auxiliary_artifacts,
    load_published_dataset,
    prepare_normalized_dataset,
    publish_normalized_dataset,
    source_identity_from_base_url,
)
from .normalization import normalize_daily_bars
from .tushare_client import DailyMarketDataUnavailable, TushareClient
from .tushare_transport import HttpJsonTransport


def _recover_published_day(*, root, prepared, raw, trade_date, captured):
    """Verify an already committed day before restoring its missing index.

    A matching independent capture is evidence of equal source content, not a
    claim that it was the original publication's exact network response.
    """
    from ashare_obs import receipt_time

    from .daily_source_verification import FIELDS, _time, verify_daily_partition
    from .source_capture import read

    loaded = load_published_dataset(publication_root=root, dataset_id=prepared.dataset_id)
    existing_manifest = loaded.manifest
    new_manifest = prepared.manifest
    if {k: v for k, v in existing_manifest.items() if k != "generated_at"} != {
        k: v for k, v in new_manifest.items() if k != "generated_at"
    } or loaded.data_bytes != prepared.data_bytes:
        raise ValueError("existing daily dataset differs from acquired input")
    if (
        load_published_auxiliary_artifacts(publication_root=root, dataset_id=prepared.dataset_id)
        != prepared.auxiliary_artifacts
    ):
        raise ValueError("existing daily auxiliary evidence differs")

    source_path = os.environ.get("TUSHARE_SOURCE_CAPTURE_ROOT")
    if not source_path or len(captured) != 1 or not isinstance(captured[0], dict):
        raise ValueError("daily recovery requires this acquisition's retained source receipt")
    source = Path(source_path)
    folder = source / "receipts" / "tushare.daily"
    tables = source / "tables"
    if (
        not source.is_absolute()
        or source.is_symlink()
        or any(parent.is_symlink() for parent in source.parents)
        or (source / "receipts").is_symlink()
        or folder.is_symlink()
        or not folder.is_dir()
        or tables.is_symlink()
        or not tables.is_dir()
    ):
        raise ValueError("daily recovery source capture layout invalid")
    current_id = captured[0].get("receiptId")
    if not isinstance(current_id, str):
        raise ValueError("daily acquisition source receipt identity missing")
    current = read(folder, current_id)
    if {**current, "receiptId": current_id} != captured[0]:
        raise ValueError("daily acquisition source receipt differs from capture")
    if current.get("targetDate") != trade_date.strftime("%Y%m%d"):
        raise ValueError("daily acquisition source target date differs")
    expected = sorted(
        (
            r.ts_code,
            r.trade_date.strftime("%Y%m%d"),
            r.open,
            r.high,
            r.low,
            r.close,
            r.vol,
            r.amount,
        )
        for r in raw
    )

    def same_raw_rows(table):
        rows = table.get("items")
        if table.get("fields") != list(FIELDS) or not isinstance(rows, list):
            return False
        if len(rows) != len(expected):
            return False
        for row in rows:
            if (
                not isinstance(row, list)
                or len(row) != len(FIELDS)
                or not isinstance(row[0], str)
                or not isinstance(row[1], str)
                or any(
                    type(value) not in {int, float} or not math.isfinite(value)
                    for value in row[2:]
                )
            ):
                return False
        return sorted(map(tuple, rows)) == expected

    table_id = current.get("tableId")
    table = read(tables, table_id)
    if not same_raw_rows(table):
        raise ValueError("daily acquisition source table differs from parsed input")

    paths = sorted(folder.glob("source-receipt-sha256-*.json"))
    if len(paths) > 2000:
        raise ValueError("daily source receipt inventory bound exceeded")
    manifest_hash = hashlib.sha256(loaded.manifest_bytes).hexdigest()
    cutoff = receipt_time()
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("daily recovery observation time is invalid")
    def verify(receipt_id):
        return verify_daily_partition(
            publication_root=root,
            source_root=source,
            dataset_id=prepared.dataset_id,
            manifest_sha256=manifest_hash,
            trade_date=trade_date,
            rows=len(raw),
            receipt_id=receipt_id,
            as_of=cutoff,
        )

    verify(current_id)
    matches = []
    for path in paths:
        if path.stem == current_id:
            continue
        try:
            other = read(folder, path.stem)
            if other.get("targetDate") != trade_date.strftime("%Y%m%d"):
                continue
            if not same_raw_rows(read(tables, other.get("tableId"))):
                continue
            verify(path.stem)
        except (OSError, ValueError, KeyError, TypeError):
            continue
        matches.append((other["receivedAt"], path.stem, other))
    if not matches:
        raise ValueError("independent matching daily source evidence unavailable")
    _, other_id, other = min(matches, key=lambda item: (_time(item[0]), item[1]))
    proof = {
        "format": "market-day-recovery/v1",
        "dataset_id": prepared.dataset_id,
        "manifest_sha256": manifest_hash,
        "source_table_id": table_id,
        "matching_capture": {
            "receipt_id": other_id,
            "table_id": other["tableId"],
            "requested_at": other["requestedAt"],
            "received_at": other["receivedAt"],
        },
        "retry_capture": {
            "receipt_id": current_id,
            "table_id": current["tableId"],
            "requested_at": current["requestedAt"],
            "received_at": current["receivedAt"],
        },
        "meaning": "matching retained raw rows; original publication response is not inferred",
    }
    content = encode(proof)
    proof_id = "market-day-recovery-sha256-" + hashlib.sha256(content).hexdigest()
    publish(root / "market-day-recoveries", proof_id + ".json", content)
    return proof_id


def acquire_day(client, *, trade_date, generated_at, publication_root, source_base_url):
    receipts = publication_root / "market-days"
    receipt = receipts / f"{trade_date}.json"
    if receipt.exists():
        if receipt.is_symlink():
            raise ValueError("daily receipt cannot be redirected")
        result = json.loads(receipt.read_bytes())
        if (
            not isinstance(result, dict)
            or type(result.get("rows")) is not int
            or type(result.get("symbols")) is not int
        ):
            raise ValueError("daily receipt has invalid counts")
        loaded = load_published_dataset(
            publication_root=publication_root, dataset_id=result["dataset_id"]
        )
        if (
            result["trade_date"] != trade_date.isoformat()
            or loaded.manifest["as_of"] != trade_date.isoformat()
            or loaded.manifest["dataset_family_id"] != "all-ashare-daily/v1"
            or any(r.trade_date != trade_date for r in loaded.records)
            or result["rows"] != len(loaded.records)
            or result["symbols"] != len({r.symbol for r in loaded.records})
        ):
            raise ValueError("daily receipt date mismatch")
        return {**result, "cached": True}
    captured = []
    if isinstance(client, TushareClient):
        raw = client.fetch_daily_by_date(trade_date=trade_date, capture_sink=captured.append)
    else:
        raw = client.fetch_daily_by_date(trade_date=trade_date)
    if not raw:
        raise DailyMarketDataUnavailable("provider returned no market bars")
    symbols = tuple(sorted({r.ts_code for r in raw}))
    records = normalize_daily_bars(
        raw, requested_symbols=symbols, window_start=trade_date, window_end=trade_date
    )
    prepared = prepare_normalized_dataset(
        records,
        as_of=trade_date,
        generated_at=generated_at,
        source_identity=source_identity_from_base_url(source_base_url),
        source_version="all-market-day/v1",
        dataset_family_id="all-ashare-daily/v1",
        auxiliary_artifacts=(
            AuxiliaryArtifact(
                relative_path="raw/market-day-receipt.json",
                content=encode(
                    {
                        "api": "daily",
                        "trade_date": trade_date.isoformat(),
                        "request_scope": "all-market",
                        "rows": len(raw),
                        "vendor_volume_unit": "lot-100-shares",
                        "vendor_amount_unit": "thousand-CNY",
                    }
                ),
            ),
        ),
    )
    proof_id = None
    try:
        paths = publish_normalized_dataset(publication_root=publication_root, prepared=prepared)
        dataset_id = paths.dataset_id
    except FileExistsError:
        proof_id = _recover_published_day(
            root=publication_root,
            prepared=prepared,
            raw=raw,
            trade_date=trade_date,
            captured=captured,
        )
        dataset_id = prepared.dataset_id
    result = {
        "dataset_id": dataset_id,
        "trade_date": trade_date.isoformat(),
        "symbols": len(symbols),
        "rows": len(records),
    }
    publish(receipts, receipt.name, encode(result))
    return {
        **result,
        "cached": False,
        **({"recovered": True, "recovery_proof_id": proof_id} if proof_id else {}),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--generated-at", required=True)
    p.add_argument("--publication-root", type=Path, required=True)
    args = p.parse_args()
    captured = datetime.fromisoformat(args.generated_at)
    if args.start > args.end or captured.tzinfo is None or args.end > captured.date():
        raise ValueError("invalid explicit acquisition window")
    transport = HttpJsonTransport()
    client = TushareClient(transport)
    calendar = client.fetch_trade_cal(exchange="SSE", start_date=args.start, end_date=args.end)
    expected = {args.start + timedelta(days=i) for i in range((args.end - args.start).days + 1)}
    if len(calendar) != len(expected) or {r.cal_date for r in calendar} != expected:
        raise ValueError("provider calendar coverage is incomplete")
    days = sorted(r.cal_date for r in calendar if r.is_open)
    if not days or days[-1] != args.end:
        raise ValueError("end must be a provider trading session")
    results = []
    for day in days:
        results.append(
            acquire_day(
                client,
                trade_date=day,
                generated_at=captured,
                publication_root=args.publication_root,
                source_base_url=transport.base_url,
            )
        )
    print(
        json.dumps({"days": len(results), "partitions": results, "production_state_changed": False})
    )


if __name__ == "__main__":
    main()
