"""Order watcher — the notification mechanism available today: polls the order list and emits
status *transitions* (QUEUED→PROCESSED, PROCESSED→SENT, …) exactly once each.

    from aiotic.watch import OrderWatcher, Transition

    def on_change(t: Transition) -> None:
        if t.to == OrderStatusValue.ATTENTION:
            notify_team(t.order)

    OrderWatcher(client, on_transition=on_change, interval=20).run_forever()
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Protocol

from .client import AioticClient
from .models import OrderStatus, OrderStatusValue

log = logging.getLogger("aiotic.watch")


@dataclass(slots=True, frozen=True)
class Transition:
    request_id: str
    from_status: OrderStatusValue | None  # None when the order is seen for the first time
    to: OrderStatusValue
    order: OrderStatus


class WatchState(Protocol):
    def last_status(self, request_id: str) -> str | None: ...

    def set_status(self, request_id: str, status: str) -> None: ...


class InMemoryWatchState:
    def __init__(self) -> None:
        self._d: dict[str, str] = {}

    def last_status(self, request_id: str) -> str | None:
        return self._d.get(request_id)

    def set_status(self, request_id: str, status: str) -> None:
        self._d[request_id] = status


class SqliteWatchState:
    def __init__(self, path: str = "aiotic-watch-state.db"):
        self.path = path
        self._lock = threading.Lock()
        with sqlite3.connect(path) as c:
            c.execute("CREATE TABLE IF NOT EXISTS orders (request_id TEXT PRIMARY KEY, status TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")

    def last_status(self, request_id: str) -> str | None:
        with sqlite3.connect(self.path) as c:
            row = c.execute("SELECT status FROM orders WHERE request_id = ?", (request_id,)).fetchone()
            return row[0] if row else None

    def set_status(self, request_id: str, status: str) -> None:
        with self._lock, sqlite3.connect(self.path) as c:
            c.execute("INSERT OR REPLACE INTO orders (request_id, status, ts) VALUES (?, ?, CURRENT_TIMESTAMP)", (request_id, status))


class OrderWatcher:
    """Polls ``GET /order_status/list`` (newest first) and calls ``on_transition`` for every status change.

    ``pages`` limits how deep each poll looks (orders older than that are assumed settled).
    ``only`` restricts callbacks to certain target statuses.
    """

    def __init__(
        self,
        client: AioticClient,
        *,
        on_transition: Callable[[Transition], None],
        state: WatchState | None = None,
        interval: float = 20.0,
        page_size: int = 200,
        pages: int = 2,
        only: Iterable[OrderStatusValue] | None = None,
        seed_silently: bool = True,
    ):
        self.client = client
        self.on_transition = on_transition
        self.state = state or InMemoryWatchState()
        self.interval = interval
        self.page_size = page_size
        self.pages = pages
        self.only = set(only) if only else None
        self.seed_silently = seed_silently
        self._stop = threading.Event()
        self._seeded = False

    def run_once(self) -> list[Transition]:
        """One poll. Returns the transitions found (and already dispatched)."""
        found: list[Transition] = []
        first_run = not self._seeded
        for order in self.client.orders.iter_all(size=self.page_size, max_pages=self.pages):
            rid = str(order.request_id)
            prev = self.state.last_status(rid)
            cur = order.status.value
            if prev == cur:
                continue
            self.state.set_status(rid, cur)
            if first_run and self.seed_silently and prev is None:
                continue  # learn the current state without firing for history
            t = Transition(rid, OrderStatusValue(prev) if prev else None, order.status, order)
            if self.only and t.to not in self.only:
                continue
            found.append(t)
            try:
                self.on_transition(t)
            except Exception:
                log.exception("on_transition failed for %s", rid)
        self._seeded = True
        return found

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("watch poll failed")
            self._stop.wait(self.interval)

    def stop(self) -> None:
        self._stop.set()


def transition_to_json(t: Transition) -> str:
    return json.dumps({"request_id": t.request_id, "from": t.from_status, "to": t.to, "order_number": t.order.result.order_number if t.order.result else None, "erp_ref": t.order.erp_ref, "timestamp": t.order.timestamp.isoformat()}, default=str)


__all__ = ["OrderWatcher", "Transition", "WatchState", "InMemoryWatchState", "SqliteWatchState", "transition_to_json"]

_ = time  # re-exported for callers that patch sleep in tests
