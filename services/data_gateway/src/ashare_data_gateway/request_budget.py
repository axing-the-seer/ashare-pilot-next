"""Injectable request pacing and the durable default provider allowance."""

from __future__ import annotations

import hashlib
import math
import os
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlsplit


class RequestBudgetExceeded(RuntimeError):
    """Stop the job without issuing another vendor request."""


class RequestBudget:
    def __init__(
        self,
        *,
        min_interval: float = 2.0,
        max_requests: int = 5000,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not math.isfinite(min_interval) or min_interval < 0 or max_requests < 1:
            raise ValueError("invalid request budget")
        self._interval = min_interval
        self._maximum = max_requests
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next = 0.0
        self._blocked_until = 0.0
        self.requests = 0

    def acquire(self) -> None:
        with self._lock:
            if self._clock() < self._blocked_until:
                raise RequestBudgetExceeded("vendor cooldown active; stop this acquisition")
            if self.requests >= self._maximum:
                raise RequestBudgetExceeded("local request budget exhausted; stop this acquisition")
            while self._clock() < self._next:
                self._sleep(self._next - self._clock())
            self.requests += 1
            self._next = self._clock() + self._interval

    def pause(self, seconds: float) -> None:
        with self._lock:
            self._blocked_until = max(self._blocked_until, self._clock() + max(60.0, seconds))


_BUDGETS: dict[str, RequestBudget] = {}
_LOCK = threading.Lock()


def shared_budget(source: str) -> RequestBudget:
    """Share a durable provider allowance across processes and working directories."""
    from .persistent_budget import PersistentRequestBudget

    parsed = urlsplit(source)
    # Tokens, credentials, paths and query strings cannot create fresh allowances.
    port = parsed.port or {"https": 443, "http": 80}.get(parsed.scheme)
    identity = f"{parsed.hostname}:{port}" if parsed.hostname else source
    source_id = hashlib.sha256(identity.encode()).hexdigest()
    cache = Path.home() / ("Library/Caches" if sys.platform == "darwin" else ".cache")
    root = Path(os.environ.get("TUSHARE_BUDGET_DIR", cache / "ashare-pilot-next/source-budgets"))
    path = root.resolve() / f"{source_id}.sqlite3"
    with _LOCK:
        return _BUDGETS.setdefault(str(path), PersistentRequestBudget(path=path))
