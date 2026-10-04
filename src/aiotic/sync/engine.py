from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from ..client import AioticClient
from ..errors import AioticError, AioticNotFoundError
from ..models import CustomerProductUpsert, CustomerUpsert, ProductUpsert

log = logging.getLogger("aiotic.sync")


class ChangeKind(StrEnum):
    CUSTOMER = "customer"
    PRODUCT = "product"
    MAPPING = "mapping"


class ChangeOp(StrEnum):
    UPSERT = "upsert"
    DELETE = "delete"


@dataclass(slots=True, frozen=True)
class ChangeEvent:
    """One change in your ERP, in AIOTIC terms. ``key`` is the AIOTIC identity of the record."""

    kind: ChangeKind
    op: ChangeOp
    key: tuple[str, ...]
    data: dict[str, Any] = field(default_factory=dict)

    # -- constructors -------------------------------------------------------------------
    @classmethod
    def customer_upsert(cls, number: str, **fields: Any) -> "ChangeEvent":
        return cls(ChangeKind.CUSTOMER, ChangeOp.UPSERT, (number,), CustomerUpsert(**fields).model_dump(exclude_none=True, mode="json"))

    @classmethod
    def customer_delete(cls, number: str) -> "ChangeEvent":
        return cls(ChangeKind.CUSTOMER, ChangeOp.DELETE, (number,))

    @classmethod
    def product_upsert(cls, item_number: str, language_code: str, *, description: str, remark: str | None = None) -> "ChangeEvent":
        return cls(ChangeKind.PRODUCT, ChangeOp.UPSERT, (item_number, language_code), ProductUpsert(description=description, remark=remark).model_dump(exclude_none=True))

    @classmethod
    def product_delete(cls, item_number: str, language_code: str) -> "ChangeEvent":
        return cls(ChangeKind.PRODUCT, ChangeOp.DELETE, (item_number, language_code))

    @classmethod
    def mapping_upsert(cls, customer_number: str, customer_item_number: str, *, item_number: str, language_code: str) -> "ChangeEvent":
        return cls(ChangeKind.MAPPING, ChangeOp.UPSERT, (customer_number, customer_item_number), CustomerProductUpsert(item_number=item_number, language_code=language_code).model_dump())

    @classmethod
    def mapping_delete(cls, customer_number: str, customer_item_number: str) -> "ChangeEvent":
        return cls(ChangeKind.MAPPING, ChangeOp.DELETE, (customer_number, customer_item_number))

    # -- helpers ------------------------------------------------------------------------
    @property
    def state_key(self) -> str:
        return f"{self.kind}:" + "|".join(self.key)

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(self.data, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:24]


class StateStore(Protocol):
    """Remembers the fingerprint of the last record sent per key, and watermarks for polling sources."""

    def get(self, key: str) -> str | None: ...

    def set(self, key: str, fingerprint: str) -> None: ...

    def delete(self, key: str) -> None: ...

    def keys(self, prefix: str) -> Iterator[str]: ...

    def get_watermark(self, name: str) -> str | None: ...

    def set_watermark(self, name: str, value: str) -> None: ...


class InMemoryStateStore:
    def __init__(self) -> None:
        self._d: dict[str, str] = {}
        self._w: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self._d.get(key)

    def set(self, key: str, fingerprint: str) -> None:
        self._d[key] = fingerprint

    def delete(self, key: str) -> None:
        self._d.pop(key, None)

    def keys(self, prefix: str) -> Iterator[str]:
        return iter([k for k in self._d if k.startswith(prefix)])

    def get_watermark(self, name: str) -> str | None:
        return self._w.get(name)

    def set_watermark(self, name: str, value: str) -> None:
        self._w[name] = value


class HashStateStore:
    """SQLite-backed :class:`StateStore` (one file per integration service)."""

    def __init__(self, path: str = "aiotic-sync-state.db"):
        self.path = path
        self._lock = threading.Lock()
        with sqlite3.connect(path) as c:
            c.execute("CREATE TABLE IF NOT EXISTS sent (k TEXT PRIMARY KEY, fp TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")
            c.execute("CREATE TABLE IF NOT EXISTS watermark (name TEXT PRIMARY KEY, v TEXT)")

    def get(self, key: str) -> str | None:
        with sqlite3.connect(self.path) as c:
            row = c.execute("SELECT fp FROM sent WHERE k = ?", (key,)).fetchone()
            return row[0] if row else None

    def set(self, key: str, fingerprint: str) -> None:
        with self._lock, sqlite3.connect(self.path) as c:
            c.execute("INSERT OR REPLACE INTO sent (k, fp, ts) VALUES (?, ?, CURRENT_TIMESTAMP)", (key, fingerprint))

    def delete(self, key: str) -> None:
        with self._lock, sqlite3.connect(self.path) as c:
            c.execute("DELETE FROM sent WHERE k = ?", (key,))

    def keys(self, prefix: str) -> Iterator[str]:
        with sqlite3.connect(self.path) as c:
            return iter([r[0] for r in c.execute("SELECT k FROM sent WHERE k LIKE ?", (prefix + "%",)).fetchall()])

    def get_watermark(self, name: str) -> str | None:
        with sqlite3.connect(self.path) as c:
            row = c.execute("SELECT v FROM watermark WHERE name = ?", (name,)).fetchone()
            return row[0] if row else None

    def set_watermark(self, name: str, value: str) -> None:
        with self._lock, sqlite3.connect(self.path) as c:
            c.execute("INSERT OR REPLACE INTO watermark (name, v) VALUES (?, ?)", (name, value))


@dataclass(slots=True)
class SyncReport:
    sent: int = 0
    deleted: int = 0
    skipped_unchanged: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)

    @property
    def duration(self) -> float:
        return time.time() - self.started_at

    def __str__(self) -> str:
        return f"sent={self.sent} deleted={self.deleted} unchanged={self.skipped_unchanged} failed={self.failed} in {self.duration:.1f}s"


class SyncEngine:
    """Applies :class:`ChangeEvent`s to AIOTIC, skipping records whose fingerprint did not change."""

    ORDER = (ChangeKind.CUSTOMER, ChangeKind.PRODUCT, ChangeKind.MAPPING)  # FK order for upserts

    def __init__(self, client: AioticClient, *, state: StateStore | None = None, concurrency: int = 4, dry_run: bool = False):
        self.client = client
        self.state = state or InMemoryStateStore()
        self.concurrency = max(1, concurrency)
        self.dry_run = dry_run

    # -- single event ---------------------------------------------------------------------
    def apply(self, ev: ChangeEvent, *, report: SyncReport | None = None, force: bool = False) -> bool:
        """Apply one change. Returns True when a request was made (False when unchanged/skipped)."""
        rep = report or SyncReport()
        if ev.op == ChangeOp.UPSERT and not force and self.state.get(ev.state_key) == ev.fingerprint:
            rep.skipped_unchanged += 1
            return False
        if self.dry_run:
            log.info("dry-run: %s %s %s", ev.op, ev.kind, ev.key)
            rep.sent += 1
            return True
        try:
            if ev.op == ChangeOp.UPSERT:
                self._upsert(ev)
                self.state.set(ev.state_key, ev.fingerprint)
                rep.sent += 1
            else:
                self._delete(ev)
                self.state.delete(ev.state_key)
                rep.deleted += 1
            return True
        except AioticNotFoundError:
            if ev.op == ChangeOp.DELETE:  # already gone — fine
                self.state.delete(ev.state_key)
                rep.deleted += 1
                return True
            rep.failed += 1
            rep.errors.append(f"{ev.kind} {ev.key}: referenced customer/product missing (upsert those first)")
            return False
        except AioticError as exc:
            rep.failed += 1
            rep.errors.append(f"{ev.kind} {ev.key}: {exc}")
            log.warning("sync failed for %s %s: %s", ev.kind, ev.key, exc)
            return False

    def _upsert(self, ev: ChangeEvent) -> None:
        if ev.kind == ChangeKind.CUSTOMER:
            self.client.customers.upsert(ev.key[0], ev.data)
        elif ev.kind == ChangeKind.PRODUCT:
            self.client.products.upsert(ev.key[0], ev.key[1], ev.data)
        else:
            self.client.customer_products.upsert(ev.key[0], ev.key[1], ev.data)

    def _delete(self, ev: ChangeEvent) -> None:
        if ev.kind == ChangeKind.CUSTOMER:
            self.client.customers.delete(ev.key[0])
        elif ev.kind == ChangeKind.PRODUCT:
            self.client.products.delete(ev.key[0], ev.key[1])
        else:
            self.client.customer_products.delete(ev.key[0], ev.key[1])

    # -- batches -------------------------------------------------------------------------
    def apply_many(self, events: Iterable[ChangeEvent], *, force: bool = False) -> SyncReport:
        """Apply a batch: upserts in FK order (customers → products → mappings), deletes in reverse, in parallel per kind."""
        rep = SyncReport()
        evs = list(events)
        upserts = [e for e in evs if e.op == ChangeOp.UPSERT]
        deletes = [e for e in evs if e.op == ChangeOp.DELETE]
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            for kind in self.ORDER:
                list(pool.map(lambda e: self.apply(e, report=rep, force=force), [e for e in upserts if e.kind == kind]))
            for kind in reversed(self.ORDER):
                list(pool.map(lambda e: self.apply(e, report=rep), [e for e in deletes if e.kind == kind]))
        log.info("sync batch: %s", rep)
        return rep

    # -- reconciliation ------------------------------------------------------------------
    def reconcile(
        self,
        *,
        customers: Iterable[ChangeEvent] | None = None,
        products: Iterable[ChangeEvent] | None = None,
        mappings: Iterable[ChangeEvent] | None = None,
        delete_missing: bool = True,
    ) -> SyncReport:
        """Safety net: given the *complete* current data set as upsert events, send only what changed
        and delete what disappeared (compared with the fingerprints remembered from earlier runs)."""
        rep = SyncReport()
        for kind, source in ((ChangeKind.CUSTOMER, customers), (ChangeKind.PRODUCT, products), (ChangeKind.MAPPING, mappings)):
            if source is None:
                continue
            present: set[str] = set()
            batch: list[ChangeEvent] = []
            for ev in source:
                present.add(ev.state_key)
                batch.append(ev)
            with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
                list(pool.map(lambda e: self.apply(e, report=rep), batch))
            if delete_missing:
                gone = [k for k in self.state.keys(f"{kind}:") if k not in present]
                for k in gone:
                    parts = tuple(k.split(":", 1)[1].split("|"))
                    self.apply(ChangeEvent(kind, ChangeOp.DELETE, parts), report=rep)
        log.info("reconcile: %s", rep)
        return rep

    def bootstrap_state_from_aiotic(self) -> int:
        """Seed the state store from what AIOTIC already holds, so the first reconcile does not re-send everything."""
        n = 0
        for c in self.client.customers.iter_all():
            ev = ChangeEvent.customer_upsert(c.number, **c.model_dump(include=set(CustomerUpsert.model_fields) - {"id"}, exclude_none=True))
            self.state.set(ev.state_key, ev.fingerprint)
            n += 1
        for p in self.client.products.iter_all():
            ev = ChangeEvent.product_upsert(p.item_number, p.language_code, description=p.description, remark=p.remark)
            self.state.set(ev.state_key, ev.fingerprint)
            n += 1
        for cp in self.client.customer_products.iter_all():
            ev = ChangeEvent.mapping_upsert(cp.customer_number, cp.customer_item_number, item_number=cp.item_number, language_code=cp.language_code)
            self.state.set(ev.state_key, ev.fingerprint)
            n += 1
        return n


class PollingChangeSource:
    """For ERPs without events: poll ``fetch_changed_since(watermark) -> (events, new_watermark)`` on an interval.

    Typical implementation: ``SELECT ... WHERE updated_at > ? ORDER BY updated_at`` over your ERP tables.
    """

    def __init__(self, name: str, fetch_changed_since: Callable[[str | None], tuple[list[ChangeEvent], str | None]], engine: SyncEngine, *, interval: float = 60.0):
        self.name, self.fetch, self.engine, self.interval = name, fetch_changed_since, engine, interval
        self._stop = threading.Event()

    def run_once(self) -> SyncReport:
        wm = self.engine.state.get_watermark(self.name)
        events, new_wm = self.fetch(wm)
        rep = self.engine.apply_many(events)
        if new_wm and rep.failed == 0:
            self.engine.state.set_watermark(self.name, new_wm)
        elif new_wm:
            log.warning("watermark %s NOT advanced: %d failures (will retry next run)", self.name, rep.failed)
        return rep

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:  # keep the loop alive; the next run retries
                log.exception("polling source %s failed", self.name)
            self._stop.wait(self.interval)

    def stop(self) -> None:
        self._stop.set()


def iso_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
