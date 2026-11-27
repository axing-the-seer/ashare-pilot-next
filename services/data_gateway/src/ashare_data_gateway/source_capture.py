"""Read and store the existing content-addressed source capture format.

This narrow module does not initiate acquisition or infer market-time availability.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

from ashare_obs import receipt_time

from .source_catalog import definitions


def encode(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode()


def store(root: Path, prefix: str, value: dict) -> str:
    """Store complete content atomically without replacing a retained object."""
    if prefix not in {"source-table", "source-receipt"}:
        raise ValueError("invalid source capture prefix")
    if root.is_symlink() or any(parent.is_symlink() for parent in root.parents):
        raise ValueError("redirected source capture root")
    raw = encode(value)
    identity = prefix + "-sha256-" + hashlib.sha256(raw).hexdigest()
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or any(parent.is_symlink() for parent in root.parents):
        raise ValueError("redirected source capture root")
    path = root / (identity + ".json")
    with tempfile.NamedTemporaryFile(dir=root, prefix=".source-", delete=False) as out:
        temporary = Path(out.name)
        out.write(raw)
        out.flush()
        os.fsync(out.fileno())
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != raw:
                raise ValueError("source capture content collision") from None
    finally:
        temporary.unlink()
    return identity


def read(root: Path, identity: str) -> dict:
    if not isinstance(identity, str) or not re.fullmatch(
        r"(?:source-table|source-receipt)-sha256-[a-f0-9]{64}", identity
    ):
        raise ValueError("invalid source capture identity")
    path = root / (identity + ".json")
    if path.is_symlink() or root.is_symlink() or path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("invalid source capture path")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != identity.rsplit("-", 1)[1]:
        raise ValueError("source capture hash mismatch")
    return json.loads(raw)


FINANCIAL_MARKET_APIS = {
    "tushare.income": "income_vip",
    "tushare.balancesheet": "balancesheet_vip",
    "tushare.cashflow": "cashflow_vip",
}


def validate_scope(params, scope):
    if params.get("con_code") and scope != "security":
        raise ValueError("constituent security selection requires security scope")
    if params.get("ts_code") and scope not in {"security", "sector"}:
        raise ValueError("explicit code selection cannot claim market scope")


def validate_selection(table, fields, items, params):
    """A successful HTTP response cannot silently substitute another source slice."""
    selections = {
        name: params[name]
        for name in ("trade_date", "ann_date", "report_date", "ts_code", "con_code", "month")
        if isinstance(params.get(name), str) and params[name] and name in fields
    }
    period = params.get("period") or (
        params.get("end_date") if table["api"] == "disclosure_date" else None
    )
    if period and "end_date" in fields:
        selections["end_date"] = period
    if table["api"] == "stk_surv" and params.get("trade_date") and "surv_date" in fields:
        selections["surv_date"] = params["trade_date"]
    for name, expected in selections.items():
        index = fields.index(name)
        allowed = expected.split(",") if name == "ts_code" else [expected]
        if any(row[index] not in allowed for row in items):
            raise ValueError("source response differs from explicit selection")


def response_observer(root: Path, *, clock=receipt_time):
    """Retain completed normal-pipeline responses, without fetching a second copy.

    This opt-in observer runs only after the existing client's full pagination
    and field checks. Reused checkpoint pages never acquire a new source time.
    """
    if (
        not root.is_absolute()
        or root.name != "upstream-source-data"
        or root.parent.name != "runtime"
    ):
        raise ValueError("explicit source capture runtime required")

    def observe(*, api_name, params, response, reused=False, requested_at=None):
        table = next(
            (
                t
                for t in definitions()
                if t["source"] == "tushare"
                and (t["api"] == api_name or FINANCIAL_MARKET_APIS.get(t["id"]) == api_name)
            ),
            None,
        )
        if table is None:
            return None
        selection = dict(params)
        if any(k.lower() in {"token", "password", "authorization", "api_key"} for k in selection):
            raise ValueError("credentials cannot be retained as selection")
        if not set(response.fields) <= set(table["fields"]):
            raise ValueError("normal acquisition returned unregistered source fields")
        validate_selection(table, response.fields, response.items, selection)
        scope = (
            "security"
            if selection.get("ts_code") or selection.get("con_code")
            else "reference"
            if api_name in {"trade_cal", "stock_basic", "bak_basic", "index_basic"}
            else "market"
        )
        validate_scope(selection, scope)
        identifier = store(
            root / "tables", "source-table", {"fields": response.fields, "items": response.items}
        )
        directory = root / "receipts" / table["id"]
        if reused and directory.exists():
            paths = list(directory.glob("source-receipt-sha256-*.json"))
            if len(paths) > 2000:
                raise ValueError("source receipt inventory bound exceeded")
            for path in paths:
                old = read(directory, path.stem)
                if old.get("tableId") == identifier and old.get("complete"):
                    return old
        received_time = clock()
        if requested_at is not None:
            requested_time = datetime.fromisoformat(requested_at)
            if requested_time.utcoffset() is None or requested_time > received_time:
                raise ValueError("invalid observed request start")
        received = received_time.isoformat()
        receipt = {
            "format": "source-capture/v1",
            "datasetId": table["id"],
            "source": "tushare",
            "api": table["api"],
            "requestApi": api_name,
            "params": selection,
            "scope": scope,
            "requestedAt": requested_at,
            "receivedAt": received,
            "targetDate": selection.get("trade_date"),
            "pages": [],
            "tableId": identifier,
            "complete": True,
            "rows": len(response.items),
            "status": "available" if response.items else "empty",
            "requestedFields": list(response.fields),
            "unrequestedDeclaredFields": [f for f in table["fields"] if f not in response.fields],
            "duplicateRows": len(response.items) - len({encode(row) for row in response.items}),
            "normalPipeline": True,
            "reusedCheckpoint": reused,
            "validation": "source-shape-only",
        }
        if reused:
            receipt.update(acquired=False, sourceReceivedAt=None)
        identity = store(directory, "source-receipt", receipt)
        return {**receipt, "receiptId": identity}

    return observe
