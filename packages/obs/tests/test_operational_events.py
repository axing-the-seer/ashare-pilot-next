import json
import os
import re
from pathlib import Path

from ashare_obs import event, prune_diagnostics


def test_private_payloads_are_omitted_and_old_diagnostics_pruned(tmp_path):
    directory = tmp_path / "runtime/diagnostics"
    directory.mkdir(parents=True)
    old = directory / "2026-01-01-python-1.jsonl"
    old.write_text("old")
    os.utime(old, (0, 0))
    event(
        tmp_path,
        "test",
        "safe",
        operation="capture",
        status="ok",
        durationMs=20,
        price=100,
        prompt="PRIVATE",
        error="SECRET",
    )
    assert old.exists()  # retention must not run in the request's critical path
    prune_diagnostics(tmp_path)
    assert not old.exists()
    file = next(directory.glob("*.jsonl"))
    raw = file.read_text()
    assert "PRIVATE" not in raw and "SECRET" not in raw and "price" not in raw
    assert json.loads(raw)["durationMs"] == 20
    assert file.stat().st_mode & 0o777 == 0o600


def test_missing_runtime_and_symlink_fail_open(tmp_path):
    event(tmp_path, "test", "safe")
    assert not (tmp_path / "runtime").exists()
    (tmp_path / "runtime").mkdir()
    (tmp_path / "runtime/diagnostics").symlink_to(tmp_path)
    event(tmp_path, "test", "safe")
    assert not list(tmp_path.glob("*.jsonl"))


def test_event_never_enumerates_history_and_shares_daily_file(tmp_path, monkeypatch):
    (tmp_path / "runtime").mkdir()

    def blocked(*args, **kwargs):
        raise AssertionError("page-read logging must not scan history")

    monkeypatch.setattr(Path, "iterdir", blocked)
    for _ in range(10):
        event(tmp_path, "test", "safe", status="ok")
    files = os.listdir(tmp_path / "runtime/diagnostics")
    assert len(files) == 1 and files[0].endswith("-python-0.jsonl")
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}-(?:node|python)-\d+(?:-\d+)?\.jsonl", files[0])
    assert len((tmp_path / "runtime/diagnostics" / files[0]).read_text().splitlines()) == 10


def test_short_processes_append_complete_records_without_pid_file_growth(tmp_path):
    import subprocess
    import sys

    (tmp_path / "runtime").mkdir()
    script = (
        'from ashare_obs import event; import sys; '
        '[event(sys.argv[1], "test", "safe", status="ok") for _ in range(20)]'
    )
    children = [subprocess.Popen([sys.executable, "-c", script, str(tmp_path)]) for _ in range(6)]
    assert all(child.wait(timeout=15) == 0 for child in children)
    files = list((tmp_path / "runtime/diagnostics").glob("*.jsonl"))
    assert len(files) == 1
    rows = [json.loads(line) for line in files[0].read_text().splitlines()]
    assert len(rows) == 120 and all(row["event"] == "safe" for row in rows)
