"""Publish complete provider calendar evidence without interpreting market state."""

import argparse
import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

from .tushare_client import TushareClient
from .tushare_transport import HttpJsonTransport


def encode(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()


def publish(root, name, content):
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise ValueError("calendar directory cannot be redirected")
    path = root / name
    fd, temp = tempfile.mkstemp(dir=root, prefix=".calendar-")
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(content)
            out.flush()
            os.fsync(out.fileno())
        try:
            os.link(temp, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != content:
                raise ValueError("calendar evidence collision") from None
    finally:
        Path(temp).unlink(missing_ok=True)
    return path


def acquire(client, *, start, end, published_at, root):
    if published_at.tzinfo is None or published_at.utcoffset() is None or start > end:
        raise ValueError("invalid calendar interval or publication timestamp")
    rows = client.fetch_trade_cal(exchange="SSE", start_date=start, end_date=end)
    expected = {start + timedelta(days=i) for i in range((end - start).days + 1)}
    if len(rows) != len(expected) or {r.cal_date for r in rows} != expected:
        raise ValueError("provider calendar coverage is incomplete")
    raw = encode([{"day": str(r.cal_date), "open": r.is_open} for r in rows])
    source_hash = hashlib.sha256(raw).hexdigest()
    publish(root, f"calendar-source-{source_hash}.json", raw)
    document = {
        "contract_id": "trading-calendar",
        "schema_version": "1.0.0",
        "calendar_id": "daily-tushare-" + source_hash + "/v1",
        "timezone": "Asia/Shanghai",
        "published_at": published_at.isoformat(),
        "coverage_start": str(start),
        "coverage_end": str(end),
        "source": "tushare trade_cal",
        "source_url": "urn:tera:tushare:trade-cal",
        "source_sha256": source_hash,
        "sessions": sorted(str(r.cal_date) for r in rows if r.is_open),
    }
    content = encode(document)
    return publish(root, "calendar-" + hashlib.sha256(content).hexdigest() + ".json", content)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--published-at", required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    args = p.parse_args()
    path = acquire(
        TushareClient(HttpJsonTransport()),
        start=args.start,
        end=args.end,
        published_at=datetime.fromisoformat(args.published_at.replace("Z", "+00:00")),
        root=args.output_dir,
    )
    print(json.dumps({"path": str(path)}))


if __name__ == "__main__":
    main()
