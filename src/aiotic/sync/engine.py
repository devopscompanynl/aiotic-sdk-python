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
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from ..client import AioticClient
from ..errors import AioticError, AioticNotFoundError
from ..models import CustomerProductUpsert, CustomerUpsert, ProductUpsert

if TYPE_CHECKING:
    from .bulk import BulkSyncReport

log = logging.getLogger("aiotic.sync")


class ChangeKind(StrEnum):
    CUSTOMER = "customer"
    PRODUCT = "product"
    MAPPING = "mapping"


class ChangeOp(StrEnum):
    UPSERT = "upsert"
    DELETE = "delete"


def _upsert_data(model: Any) -> dict[str, Any]:
    """The body of an upsert. ``source_updated_at`` is written in UTC, so that the same moment always gives the
    same body and the same fingerprint, whatever offset your ERP reported it in."""
    data = model.model_dump(exclude_none=True, mode="json")
    if model.source_updated_at is not None:
        data["source_updated_at"] = model.source_updated_at.astimezone(timezone.utc).isoformat()
    return data


@dataclass(slots=True, frozen=True)
class ChangeEvent:
    """One change in your ERP, in AIOTIC terms. ``key`` is the AIOTIC identity of the record.

    An upsert may carry ``source_updated_at``: when your ERP last changed the record, as a ``datetime`` with a
    zone or an RFC 3339 string with one. AIOTIC stores it, and an incremental sync continues from the newest one
    (:class:`CursorChangeSource`). An upsert without it clears the value AIOTIC held.
    """

    kind: ChangeKind
    op: ChangeOp
    key: tuple[str, ...]
    data: dict[str, Any] = field(default_factory=dict)

    # -- constructors -------------------------------------------------------------------
    @classmethod
    def customer_upsert(cls, number: str, **fields: Any) -> "ChangeEvent":
        """``name`` is required, as the API requires it; every other field of ``CustomerUpsert`` is optional."""
        return cls(ChangeKind.CUSTOMER, ChangeOp.UPSERT, (number,), _upsert_data(CustomerUpsert(**fields)))

    @classmethod
    def customer_delete(cls, number: str) -> "ChangeEvent":
        return cls(ChangeKind.CUSTOMER, ChangeOp.DELETE, (number,))

    @classmethod
    def product_upsert(cls, item_number: str, language_code: str, *, description: str, remark: str | None = None, source_updated_at: datetime | str | None = None) -> "ChangeEvent":
        return cls(ChangeKind.PRODUCT, ChangeOp.UPSERT, (item_number, language_code), _upsert_data(ProductUpsert(description=description, remark=remark, source_updated_at=source_updated_at)))

    @classmethod
    def product_delete(cls, item_number: str, language_code: str) -> "ChangeEvent":
        return cls(ChangeKind.PRODUCT, ChangeOp.DELETE, (item_number, language_code))

    @classmethod
    def mapping_upsert(cls, customer_number: str, customer_item_number: str, *, item_number: str, language_code: str, source_updated_at: datetime | str | None = None) -> "ChangeEvent":
        return cls(ChangeKind.MAPPING, ChangeOp.UPSERT, (customer_number, customer_item_number), _upsert_data(CustomerProductUpsert(item_number=item_number, language_code=language_code, source_updated_at=source_updated_at)))

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

    @property
    def source_updated_at(self) -> datetime | None:
        """The ``source_updated_at`` this upsert carries, in UTC; ``None`` for a delete or an upsert without one."""
        value = self.data.get("source_updated_at")
        return datetime.fromisoformat(value) if value else None


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
    not_attempted: int = 0  # events left alone because an earlier one failed (``apply_in_order`` only)

    @property
    def duration(self) -> float:
        return time.time() - self.started_at

    def __str__(self) -> str:
        left = f" not_attempted={self.not_attempted}" if self.not_attempted else ""
        return f"sent={self.sent} deleted={self.deleted} unchanged={self.skipped_unchanged} failed={self.failed}{left} in {self.duration:.1f}s"


class SyncEngine:
    """Applies :class:`ChangeEvent`s to AIOTIC, skipping records whose fingerprint did not change.

    Events for the same record are always applied in the order given and never concurrently; different records
    run in parallel. The report's counters are updated under a lock, so ``failed == 0`` is a reliable signal.
    """

    ORDER = (ChangeKind.CUSTOMER, ChangeKind.PRODUCT, ChangeKind.MAPPING)  # FK order for upserts

    def __init__(self, client: AioticClient, *, state: StateStore | None = None, concurrency: int = 4, dry_run: bool = False):
        self.client = client
        self.state = state or InMemoryStateStore()
        self.concurrency = max(1, concurrency)
        self.dry_run = dry_run
        self._report_lock = threading.Lock()

    def _count(self, rep: SyncReport, field_name: str, error: str | None = None) -> None:
        with self._report_lock:
            setattr(rep, field_name, getattr(rep, field_name) + 1)
            if error:
                rep.errors.append(error)

    # -- single event ---------------------------------------------------------------------
    def apply(self, ev: ChangeEvent, *, report: SyncReport | None = None, force: bool = False) -> bool:
        """Apply one change. Returns True when a request was made (False when unchanged/skipped)."""
        rep = report or SyncReport()
        if ev.op == ChangeOp.UPSERT and not force and self.state.get(ev.state_key) == ev.fingerprint:
            self._count(rep, "skipped_unchanged")
            return False
        if self.dry_run:
            log.info("dry-run: %s %s %s", ev.op, ev.kind, ev.key)
            self._count(rep, "sent")
            return True
        try:
            if ev.op == ChangeOp.UPSERT:
                self._upsert(ev)
                self.state.set(ev.state_key, ev.fingerprint)
                self._count(rep, "sent")
            else:
                self._delete(ev)
                self.state.delete(ev.state_key)
                self._count(rep, "deleted")
            return True
        except AioticNotFoundError:
            if ev.op == ChangeOp.DELETE:  # already gone — fine
                self.state.delete(ev.state_key)
                self._count(rep, "deleted")
                return True
            self._count(rep, "failed", f"{ev.kind} {ev.key}: referenced customer/product missing (upsert those first)")
            return False
        except AioticError as exc:
            self._count(rep, "failed", f"{ev.kind} {ev.key}: {exc}")
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
    # The order in which independent events of one wave are sent: parents before children for upserts, children
    # before parents for deletes. Dependent events never share a wave (see _plan).
    PHASES = (
        (ChangeOp.UPSERT, ChangeKind.CUSTOMER),
        (ChangeOp.UPSERT, ChangeKind.PRODUCT),
        (ChangeOp.UPSERT, ChangeKind.MAPPING),
        (ChangeOp.DELETE, ChangeKind.MAPPING),
        (ChangeOp.DELETE, ChangeKind.PRODUCT),
        (ChangeOp.DELETE, ChangeKind.CUSTOMER),
    )

    @staticmethod
    def _plan(events: list[ChangeEvent]) -> list[set[int]]:
        """For every event of a batch, the earlier events it has to wait for.

        * the previous event of the same record (events of one record run in the order given, never concurrently);
        * for a mapping upsert: the latest earlier event of its customer and of its product, so parents exist first;
        * for a customer or product delete: every earlier mapping event, so children go before parents.
        All edges point backwards in the batch, so the plan is always a DAG and always makes progress.
        """
        deps: list[set[int]] = [set() for _ in events]
        last_of_record: dict[str, int] = {}
        mapping_events: list[int] = []
        for i, ev in enumerate(events):
            prev = last_of_record.get(ev.state_key)
            if prev is not None:
                deps[i].add(prev)
            if ev.kind == ChangeKind.MAPPING:
                if ev.op == ChangeOp.UPSERT:
                    parents = (f"{ChangeKind.CUSTOMER}:{ev.key[0]}", f"{ChangeKind.PRODUCT}:{ev.data.get('item_number')}|{ev.data.get('language_code')}")
                    deps[i].update(j for j in (last_of_record.get(k) for k in parents) if j is not None)
                mapping_events.append(i)
            elif ev.op == ChangeOp.DELETE:
                deps[i].update(mapping_events)
            last_of_record[ev.state_key] = i
        return deps

    def _run_batch(self, pool: ThreadPoolExecutor, events: Iterable[ChangeEvent], rep: SyncReport, *, force: bool = False) -> None:
        """Run a batch in waves: an event runs once everything it depends on has run; within a wave the phases of
        :data:`PHASES` go one after another and the events of one phase run in parallel."""
        batch = list(events)
        deps = self._plan(batch)
        done: set[int] = set()
        remaining = list(range(len(batch)))
        while remaining:
            ready = [i for i in remaining if deps[i] <= done]  # never empty: the earliest remaining event is ready
            for op, kind in self.PHASES:
                wave = [i for i in ready if batch[i].op == op and batch[i].kind == kind]
                if wave:
                    list(pool.map(lambda i: self.apply(batch[i], report=rep, force=force), wave))
                    done.update(wave)
            remaining = [i for i in remaining if i not in done]

    def apply_many(self, events: Iterable[ChangeEvent], *, force: bool = False) -> SyncReport:
        """Apply a batch.

        Events for the same record are applied one after another, in the order given, never concurrently, so
        ``delete C1`` followed by ``upsert C1`` ends with C1 present and ``upsert v1``, ``upsert v2`` ends with v2.
        Dependencies between records are kept as well: a mapping is created after the customer and the product it
        refers to, and a customer or product is deleted after the mappings that came before it in the batch. A batch
        that creates and then removes a customer, a product and their mapping therefore succeeds in source order.
        Independent events run in parallel, parents before children for upserts and children before parents for
        deletes.
        """
        rep = SyncReport()
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            self._run_batch(pool, events, rep, force=force)
        log.info("sync batch: %s", rep)
        return rep

    def apply_in_order(self, events: Iterable[ChangeEvent], *, force: bool = False) -> SyncReport:
        """Apply events one at a time, in exactly the order given, and stop at the first one that fails.

        This is what a sync that continues from a moment in time needs (:class:`CursorChangeSource`): when the
        changes go oldest first and nothing is sent after a failure, nothing newer than an unsent change is ever
        stored, so the next run starts before the failed change and not after it. ``report.not_attempted`` counts
        the events that were left alone.
        """
        rep = SyncReport()
        batch = list(events)
        for position, ev in enumerate(batch):
            self.apply(ev, report=rep, force=force)
            if rep.failed:
                rep.not_attempted = len(batch) - position - 1
                break
        log.info("sync in order: %s", rep)
        return rep

    # -- bulk sync -----------------------------------------------------------------------
    def bulk_sync(
        self,
        *,
        customers: Iterable[Any] | None = None,
        products: Iterable[Any] | None = None,
        mappings: Iterable[Any] | None = None,
        max_delete_fraction: float = 0.10,
        compare: str = "keys",
        approve_deletions: bool = False,
        approve_skipped: bool = False,
    ) -> "BulkSyncReport":
        """Replace complete data sets in AIOTIC, one call per data set, after checking what that would remove.

        Pass every data set you want replaced as its **complete** content: upsert events (the same ones
        ``reconcile()`` takes), dicts or bulk row models. Customers go first, then products, then mappings.

        AIOTIC removes whatever a bulk sync does not contain, so before anything is sent the engine compares each
        data set with what AIOTIC holds and raises :class:`aiotic.sync.BulkSyncRefused` when

        * more than ``max_delete_fraction`` (10 %) of the records AIOTIC holds would be removed, which is what a
          truncated or filtered export looks like: pass ``approve_deletions=True`` for a clean-up you intend;
        * a mapping points at a customer or product that AIOTIC will not hold after this run: AIOTIC would skip
          that row and remove the mapping it holds under the same key. Pass ``approve_skipped=True`` to send anyway;
          the mappings such rows remove still count as deletions against ``max_delete_fraction``.

        Nothing is sent unless every data set passes. ``compare="count"`` compares only the number of records
        instead of the keys (one request instead of a listing; it cannot see an export of the right size with the
        wrong records). With ``dry_run=True`` on the engine the checks run and nothing is sent.

        The engine's state store is not involved: a bulk sync does not read or write fingerprints.
        """
        from .bulk import run_bulk_sync

        return run_bulk_sync(
            self.client,
            {ChangeKind.CUSTOMER: customers, ChangeKind.PRODUCT: products, ChangeKind.MAPPING: mappings},
            max_delete_fraction=max_delete_fraction,
            compare=compare,
            approve_deletions=approve_deletions,
            approve_skipped=approve_skipped,
            dry_run=self.dry_run,
        )

    # -- reconciliation ------------------------------------------------------------------
    def reconcile(
        self,
        *,
        customers: Iterable[ChangeEvent] | None = None,
        products: Iterable[ChangeEvent] | None = None,
        mappings: Iterable[ChangeEvent] | None = None,
        delete_missing: bool = True,
        allow_empty: bool = False,
    ) -> SyncReport:
        """Safety net: given the *complete* current data set as upsert events, send only what changed
        and delete what disappeared (compared with the fingerprints remembered from earlier runs).

        An **empty** data set for a kind that has records in the state store is treated as a broken export,
        not as "delete everything": nothing is deleted, the report gets one failure that says so. Pass
        ``allow_empty=True`` (CLI: ``--allow-empty``) when the data set really is empty and the deletions
        are intended.
        """
        rep = SyncReport()
        for kind, source in ((ChangeKind.CUSTOMER, customers), (ChangeKind.PRODUCT, products), (ChangeKind.MAPPING, mappings)):
            if source is None:
                continue
            batch = list(source)
            present = {ev.state_key for ev in batch if ev.op == ChangeOp.UPSERT}
            with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
                self._run_batch(pool, batch, rep)
            if delete_missing:
                gone = [k for k in self.state.keys(f"{kind}:") if k not in present]
                if gone and not present and not allow_empty:
                    self._count(rep, "failed", f"{kind}: the data set is empty but {len(gone)} record(s) were sent earlier; refusing to delete them (pass allow_empty=True / --allow-empty when the data set really is empty)")
                    continue
                for k in gone:
                    parts = tuple(k.split(":", 1)[1].split("|"))
                    self.apply(ChangeEvent(kind, ChangeOp.DELETE, parts), report=rep)
        log.info("reconcile: %s", rep)
        return rep

    def bootstrap_state_from_aiotic(self) -> int:
        """Seed the state store from what AIOTIC already holds, so the first reconcile does not re-send everything.

        Products without a description (AIOTIC may return them, an upsert cannot write them) are not seeded:
        the next reconcile sends them once with your description, which is the right outcome.
        """
        n = 0
        skipped = 0
        nameless = 0
        for c in self.client.customers.iter_all():
            if not (c.name or "").strip():  # stored before the API required a name; an upsert cannot write it
                nameless += 1
                continue
            ev = ChangeEvent.customer_upsert(c.number, **c.model_dump(include=set(CustomerUpsert.model_fields) - {"id"}, exclude_none=True))
            self.state.set(ev.state_key, ev.fingerprint)
            n += 1
        for p in self.client.products.iter_all():
            if p.description is None:
                skipped += 1
                continue
            ev = ChangeEvent.product_upsert(p.item_number, p.language_code, description=p.description, remark=p.remark, source_updated_at=p.source_updated_at)
            self.state.set(ev.state_key, ev.fingerprint)
            n += 1
        for cp in self.client.customer_products.iter_all():
            ev = ChangeEvent.mapping_upsert(cp.customer_number, cp.customer_item_number, item_number=cp.item_number, language_code=cp.language_code, source_updated_at=cp.source_updated_at)
            self.state.set(ev.state_key, ev.fingerprint)
            n += 1
        if skipped:
            log.warning("bootstrap: %d product(s) in AIOTIC have no description and were not seeded", skipped)
        if nameless:
            log.warning("bootstrap: %d customer(s) in AIOTIC have no name and were not seeded", nameless)
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


class CursorChangeSource:
    """Incremental sync without local state: the sync continues from the newest ``source_updated_at`` AIOTIC holds.

    ``fetch_changed_since(since)`` returns the changes of **one** data set that your ERP made **at or after**
    ``since`` (everything when ``since`` is ``None``), **oldest change first**, as upsert events that carry
    ``source_updated_at`` and, where your ERP reports them, delete events. Typical implementation:
    ``SELECT ... WHERE updated_at >= ? ORDER BY updated_at``.

    Each run reads the cursor from AIOTIC, steps back by ``margin`` and asks for the changes from there. Three rules
    keep a change from being lost:

    * changes are sent one at a time, oldest first, and the run **stops at the first one that fails**. Nothing
      newer than an unsent change is stored, so the cursor cannot move past it;
    * the start is inclusive and a margin earlier: several records can share one change moment, and "after the
      cursor" would skip the ones that were not sent yet. Records sent twice are harmless: upserts are idempotent;
    * every upsert must carry ``source_updated_at``. An upsert without it clears the value in AIOTIC and the
      cursor would fall back.

    A change that keeps failing (a mapping whose product does not exist, a customer without a name) holds back
    every later change of that data set until it is fixed in the ERP or left out by ``fetch_changed_since``; each
    run logs it as an error. Deleted records leave no moment behind in AIOTIC, so a delete is repeated on later runs
    until a newer upsert moves the cursor; a delete of a record that is already gone counts as done.
    """

    def __init__(
        self,
        kind: ChangeKind | str,
        fetch_changed_since: Callable[[datetime | None], Iterable[ChangeEvent]],
        engine: SyncEngine,
        *,
        margin: timedelta = timedelta(seconds=1),
        interval: float = 60.0,
        language_code: str | None = None,
    ):
        self.kind = ChangeKind(kind)
        self.fetch, self.engine, self.margin, self.interval, self.language_code = fetch_changed_since, engine, margin, interval, language_code
        self._stop = threading.Event()

    def cursor(self) -> datetime | None:
        """The newest ``source_updated_at`` AIOTIC holds for this data set (UTC), or ``None`` when there is none."""
        client = self.engine.client
        if self.kind == ChangeKind.CUSTOMER:
            return client.customers.latest_source_update()
        if self.kind == ChangeKind.PRODUCT:
            return client.products.latest_source_update(language_code=self.language_code)
        return client.customer_products.latest_source_update()

    def run_once(self) -> SyncReport:
        cursor = self.cursor()
        events = list(self.fetch(cursor - self.margin if cursor else None))
        previous: datetime | None = None
        for ev in events:
            if ev.kind != self.kind:
                raise ValueError(f"cursor source for {self.kind} was given a {ev.kind} event; use one source per data set")
            if ev.op != ChangeOp.UPSERT:
                continue
            moment = ev.source_updated_at
            if moment is None:
                raise ValueError(f"{ev.kind} {ev.key}: an upsert in a cursor sync needs source_updated_at (without it AIOTIC clears the value and the cursor falls back)")
            if previous is not None and moment < previous:
                raise ValueError(f"{ev.kind} {ev.key}: changes must come oldest first ({moment.isoformat()} follows {previous.isoformat()})")
            previous = moment
        rep = self.engine.apply_in_order(events)
        if rep.failed:
            log.error("cursor sync %s stopped at a failed change; %d later change(s) wait for it: %s", self.kind, rep.not_attempted, rep.errors[-1] if rep.errors else "")
        return rep

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:  # keep the loop alive; the next run starts from the cursor again
                log.exception("cursor source %s failed", self.kind)
            self._stop.wait(self.interval)

    def stop(self) -> None:
        self._stop.set()


def iso_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")
