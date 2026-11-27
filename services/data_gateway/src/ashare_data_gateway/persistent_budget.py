"""Durable acquisition quota, never a market timestamp or research input.

The conservative cap is a rolling 24-hour local policy, not a claimed vendor
billing window. SQLite transactions coordinate processes without a new service.
Only reservation timestamps and cooldowns are stored; no keys, quotes or symbols.
"""

from __future__ import annotations

import math
import os
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from .request_budget import RequestBudget, RequestBudgetExceeded

WINDOW_SECONDS = 86400.0


class PersistentRequestBudget(RequestBudget):
    def __init__(
        self,
        *,
        path: Path,
        min_interval: float = 2.0,
        max_requests: int = 5000,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        super().__init__(
            min_interval=min_interval, max_requests=max_requests, clock=clock, sleep=sleep
        )
        self.path = path

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Never silently reset a damaged state file or follow a redirected database.
        if self.path.is_symlink():
            raise RequestBudgetExceeded("quota ledger cannot be a symbolic link")
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(descriptor)
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS policy ("
                "id INTEGER PRIMARY KEY CHECK(id=1), last_start REAL NOT NULL, "
                "last_seen REAL NOT NULL, blocked_until REAL NOT NULL, "
                "interval_seconds REAL NOT NULL, maximum INTEGER NOT NULL)"
            )
            connection.execute("CREATE TABLE IF NOT EXISTS reservations (started_at REAL NOT NULL)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS reservation_time ON reservations(started_at)"
            )
            connection.execute(
                "INSERT OR IGNORE INTO policy VALUES (1,0,0,0,?,?)",
                (self._interval, self._maximum),
            )
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _state(self, connection: sqlite3.Connection) -> tuple[float, float, float, float, int]:
        now = self._clock()
        row = connection.execute(
            "SELECT last_start,last_seen,blocked_until,interval_seconds,maximum FROM policy"
        ).fetchone()
        if (
            row is None
            or any(not isinstance(v, (float, int)) or not math.isfinite(v) or v < 0 for v in row)
            or not isinstance(row[4], int)
            or row[4] < 1
            or not math.isfinite(now)
            or now < 0
        ):
            raise RequestBudgetExceeded("invalid quota ledger or clock; stop acquisition")
        last_start, last_seen, blocked_until, interval, maximum = row
        if now < max(last_seen, last_start):
            raise RequestBudgetExceeded("quota clock moved backwards; stop acquisition")
        # A less restrictive process cannot relax an existing shared policy.
        interval, maximum = max(interval, self._interval), min(maximum, self._maximum)
        connection.execute(
            "UPDATE policy SET last_seen=?,interval_seconds=?,maximum=? WHERE id=1",
            (now, interval, maximum),
        )
        return now, last_start, blocked_until, interval, maximum

    def acquire(self) -> None:
        while True:
            try:
                with self._transaction() as connection:
                    now, last_start, blocked_until, interval, maximum = self._state(connection)
                    if now < blocked_until:
                        raise RequestBudgetExceeded("vendor cooldown active; stop this acquisition")
                    connection.execute(
                        "DELETE FROM reservations WHERE started_at<=?", (now - WINDOW_SECONDS,)
                    )
                    count = connection.execute("SELECT COUNT(*) FROM reservations").fetchone()[0]
                    if count >= maximum:
                        raise RequestBudgetExceeded(
                            "rolling 24-hour request budget exhausted; stop this acquisition"
                        )
                    wait = max(0.0, last_start + interval - now)
                    if wait == 0:
                        connection.execute("INSERT INTO reservations VALUES (?)", (now,))
                        connection.execute("UPDATE policy SET last_start=? WHERE id=1", (now,))
                        self.requests += 1
                        return
            except (OSError, sqlite3.Error):
                raise RequestBudgetExceeded("quota ledger unavailable; stop acquisition") from None
            # Release the database lock while waiting, then re-check shared cooldown/cap.
            self._sleep(wait)

    def pause(self, seconds: float) -> None:
        if not math.isfinite(seconds) or seconds < 0:
            raise RequestBudgetExceeded("invalid vendor cooldown; stop acquisition")
        try:
            with self._transaction() as connection:
                now, _, blocked_until, interval, _ = self._state(connection)
                # A cooldown alone resumes the same rejected burst cadence.
                # Keep a conservative slowdown across old/new worker processes;
                # never relax a user's stricter interval or reset reservations.
                # Some configured gateways still reject requests at ten-second
                # spacing. Repeated rejection must keep slowing the shared
                # cadence rather than replaying that rejected rate forever.
                slower = max(interval, min(60.0, max(5.0, interval * 2)))
                connection.execute(
                    "UPDATE policy SET blocked_until=?,interval_seconds=? WHERE id=1",
                    (max(blocked_until, now + max(60.0, seconds)), slower),
                )
        except (OSError, sqlite3.Error):
            raise RequestBudgetExceeded("quota ledger unavailable; stop acquisition") from None
