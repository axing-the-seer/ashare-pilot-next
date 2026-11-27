"""Local operational diagnostics, separate from immutable financial evidence."""

import json
import os
import re
import stat
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path


def receipt_time():
    """Operational receipt clock; never a substitute for the provider's market time."""
    return datetime.now(UTC)


def event(root, component, name, **metadata):
    """Allowlisted metadata only; never log values, prompts or exception text."""
    try:
        runtime = Path(root) / "runtime"
        if not runtime.is_dir() or runtime.is_symlink():
            return
        directory = runtime / "diagnostics"
        if directory.is_symlink():
            return
        directory.mkdir(mode=0o700, exist_ok=True)
        if not all(re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", v) for v in (component, name)):
            return
        stamp = datetime.fromtimestamp(time.time(), UTC).isoformat(timespec="milliseconds")
        trace = os.environ.get("TERA_TRACE_ID", "")
        if not re.fullmatch(r"[a-f0-9-]{36}", trace):
            trace = str(uuid.uuid4())
        record = dict(version=1, at=stamp, component=component, event=name, traceId=trace)
        for key, value in metadata.items():
            if key in {
                "operation",
                "reason",
                "source",
                "securityKey",
                "trialId",
                "datasetId",
                "code",
            }:
                if (
                    isinstance(value, str)
                    and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,120}", value)
                    and "://" not in value
                    and ".." not in value
                ):
                    record[key] = value
            elif key == "status" and value in {"ok", "failed", "rejected", "duplicate", "pending"}:
                record[key] = value
            elif key == "durationMs" and type(value) in {int, float} and 0 <= value < 1e12:
                record[key] = round(value)
        # Research readers are short-lived processes. A file per PID creates
        # thousands of entries, and pruning them in event() stalls real reads.
        # One bounded O_APPEND write is shared by today's Python producers.
        # Writer bucket 0 keeps the existing installed Node filename contract.
        target = directory / f"{stamp[:10]}-python-0.jsonl"
        if target.is_symlink():
            return
        if target.exists() and target.stat().st_size >= 1024 * 1024:
            target.rename(directory / f"{target.stem}-{time.time_ns()}.jsonl")
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(json.dumps(record, separators=(",", ":")).encode() + b"\n")
    except (OSError, ValueError, TypeError):
        # Loss of diagnostics must never change signal/financial behavior.
        return


def prune_diagnostics(root):
    """Explicit maintenance, never part of a financial or page-read request.

    The running Tera bridge also performs this maintenance asynchronously.
    Keep legacy PID logs readable until their normal retention expires.
    """
    runtime = Path(root) / "runtime"
    directory = runtime / "diagnostics"
    if runtime.is_symlink() or directory.is_symlink() or not directory.is_dir():
        return
    files = []
    for file in directory.iterdir():
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}-(?:node|python)(?:-\d+)?(?:-\d+)?\.jsonl", file.name):
            try:
                info = file.lstat()
                if stat.S_ISREG(info.st_mode):
                    files.append((file, info))
            except OSError:
                continue
    size = 0
    today = receipt_time().date().isoformat()
    for file, info in sorted(files, key=lambda pair: pair[1].st_mtime, reverse=True):
        size += info.st_size
        if file.name != f"{today}-python-0.jsonl" and (
            size > 64 * 1024 * 1024 or time.time() - info.st_mtime > 7 * 86400
        ):
            file.unlink(missing_ok=True)


def failure_site(error):
    """Last internal module/function/line, without messages, values or paths."""
    site = None
    tb = error.__traceback__
    while tb:
        module = tb.tb_frame.f_globals.get("__name__", "")
        if module == "__main__":
            code_file = Path(tb.tb_frame.f_code.co_filename)
            if code_file.parent.name in {"ashare_research_app", "ashare_data_gateway"}:
                module = code_file.parent.name + "." + code_file.stem
        function = tb.tb_frame.f_code.co_name
        if module.startswith(
            ("ashare_research_app.", "ashare_data_gateway.", "ashare_quant_core.")
        ) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", function):
            site = f"{module}.{function}:L{tb.tb_lineno}"
        tb = tb.tb_next
    return site
