"""Bulk sync with a safeguard: replace complete data sets, but look at what that removes first.

A bulk sync (``POST /customer/bulk``, ``/product/bulk``, ``/customer-product/bulk``) carries a data set's
complete content. AIOTIC stores it and **removes everything it does not contain**; that is the API's contract, and
it is what makes one call enough. It also means that an export that was cut short, filtered by mistake or taken from
the wrong company removes real data, and that the call still answers 200.

The checks in this module run on your side, before anything is sent. They are the SDK's, not the server's: the
server only refuses an empty data set.

* **Deletions.** Each data set is compared with what AIOTIC holds. When more than ``max_delete_fraction`` of the
  held records would be removed, the run is refused.
* **Mapping targets.** Every mapping must point at a customer and a product that AIOTIC will hold after this run.
  AIOTIC skips a row that does not, and removes the mapping it held under that row's key. Such a mapping is
  counted as a deletion, so approving skipped rows never approves what they remove.

Use it through :meth:`aiotic.sync.SyncEngine.bulk_sync`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ValidationError

from .. import models as m
from ..client import AioticClient
from .engine import ChangeEvent, ChangeKind, ChangeOp

log = logging.getLogger("aiotic.sync")

ORDER = (ChangeKind.CUSTOMER, ChangeKind.PRODUCT, ChangeKind.MAPPING)  # parents before the mappings that refer to them
PLURAL = {ChangeKind.CUSTOMER: "customers", ChangeKind.PRODUCT: "products", ChangeKind.MAPPING: "mappings"}
ROW_MODEL: dict[ChangeKind, type[BaseModel]] = {
    ChangeKind.CUSTOMER: m.CustomerBulkRow,
    ChangeKind.PRODUCT: m.ProductBulkRow,
    ChangeKind.MAPPING: m.CustomerProductBulkRow,
}
KEY_FIELDS = {
    ChangeKind.CUSTOMER: ("number",),
    ChangeKind.PRODUCT: ("item_number", "language_code"),
    ChangeKind.MAPPING: ("customer_number", "customer_item_number"),
}
EXAMPLES = 10  # how many keys a refusal names

Key = tuple[str, ...]


@dataclass(slots=True)
class BulkPlan:
    """What a bulk sync of one data set would do, worked out before it is sent."""

    kind: ChangeKind
    rows: int  # records in the data set that would be sent
    held: int  # records AIOTIC holds now
    deletions: int  # records AIOTIC holds that this run removes: the ones the data set does not contain, and the mappings held under the key of a row AIOTIC will skip; a lower bound when not ``exact``
    exact: bool  # True when keys were compared, False when only the totals were
    new: int | None = None  # records AIOTIC does not hold yet and will store; unknown when not ``exact``
    deletion_examples: list[str] = field(default_factory=list)
    unknown_targets: int = 0  # mappings whose customer or product AIOTIC will not hold
    replaced_by_skipped: int = 0  # of the deletions: mappings AIOTIC holds under the key of such a row; unknown (0) when not ``exact``
    unknown_target_examples: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)  # why the safeguard refuses this data set; empty when it passes

    def __str__(self) -> str:
        new = "?" if self.new is None else str(self.new)
        bound = "" if self.exact else "at least "
        extra = f", {self.unknown_targets} with an unknown customer or product" if self.unknown_targets else ""
        return f"{PLURAL[self.kind]}: {self.rows} to send, {self.held} held, {new} new, {bound}{self.deletions} to remove{extra}"


class BulkSyncRefused(Exception):
    """The safeguard stopped a bulk sync before anything was sent. ``plans`` holds one :class:`BulkPlan` per data
    set; the ones with ``problems`` are why. Nothing in AIOTIC changed."""

    def __init__(self, plans: list[BulkPlan]):
        self.plans = plans
        problems = [problem for plan in plans for problem in plan.problems]
        super().__init__("bulk sync refused before sending; nothing changed in AIOTIC.\n- " + "\n- ".join(problems))


@dataclass(slots=True)
class BulkSyncReport:
    """What a bulk sync run did. ``results`` holds AIOTIC's answer per data set that was sent."""

    plans: list[BulkPlan] = field(default_factory=list)
    results: dict[ChangeKind, m.BulkResult] = field(default_factory=dict)
    dry_run: bool = False
    skipped_approved: bool = False
    started_at: float = field(default_factory=time.time)

    @property
    def duration(self) -> float:
        return time.time() - self.started_at

    @property
    def deleted(self) -> int:
        """Records AIOTIC removed because the data sets did not contain them."""
        return sum(r.deleted for r in self.results.values())

    @property
    def mappings_deleted(self) -> int:
        """Mappings AIOTIC removed together with a removed customer or product."""
        return sum(getattr(r, "mappings_deleted", 0) for r in self.results.values())

    @property
    def skipped_total(self) -> int:
        """Mapping rows AIOTIC did not store because it holds no such customer or product."""
        return sum(getattr(r, "skipped_total", 0) for r in self.results.values())

    @property
    def ok(self) -> bool:
        """False when AIOTIC skipped mapping rows that the caller did not approve beforehand."""
        return self.skipped_approved or self.skipped_total == 0

    def __str__(self) -> str:
        if self.dry_run:
            return "dry run, nothing sent: " + "; ".join(str(p) for p in self.plans)
        parts = []
        for kind in ORDER:
            r = self.results.get(kind)
            if r is None:
                continue
            text = f"{PLURAL[kind]}: inserted={r.inserted} updated={r.updated} unchanged={r.unchanged} deleted={r.deleted}"
            if isinstance(r, m.CustomerProductBulkResult):
                text += f" skipped={r.skipped_total}"
            else:
                text += f" mappings_deleted={getattr(r, 'mappings_deleted', 0)}"
            if r.attempts > 1:
                text += f" (sent {r.attempts} times)"
            parts.append(text)
        return "; ".join(parts) + f" in {self.duration:.1f}s"


# ------------------------------------------------------------------------------------------------
# Reading the data sets
# ------------------------------------------------------------------------------------------------


def _row(kind: ChangeKind, item: Any) -> dict[str, Any] | None:
    """One row as a plain dict with its key fields, or None for a delete event (the record is simply absent)."""
    if isinstance(item, ChangeEvent):
        if item.kind != kind:
            raise ValueError(f"a {item.kind} event was passed in the {PLURAL[kind]} data set")
        if item.op == ChangeOp.DELETE:
            return None
        return {**dict(zip(KEY_FIELDS[kind], item.key)), **item.data}
    if isinstance(item, BaseModel):
        return item.model_dump(exclude_none=True)
    return dict(item)


def read_rows(kind: ChangeKind, source: Iterable[Any]) -> dict[Key, dict[str, Any]]:
    """Validate one data set and index it by key. A row the API would refuse, and a key that occurs twice, raise
    ``ValueError`` here: both mean the export is broken, and AIOTIC would refuse the whole call for it."""
    rows: dict[Key, dict[str, Any]] = {}
    model = ROW_MODEL[kind]
    for position, item in enumerate(source):
        raw = _row(kind, item)
        if raw is None:
            continue
        try:
            row = model.model_validate(raw).model_dump(mode="json", exclude_none=True)
        except ValidationError as exc:
            first = exc.errors()[0]
            where = ".".join(str(part) for part in first["loc"]) or "row"
            raise ValueError(f"{PLURAL[kind]} row {position}: {where}: {first['msg']}") from exc
        key = tuple(str(row[f]) for f in KEY_FIELDS[kind])
        if key in rows:
            raise ValueError(f"{PLURAL[kind]} row {position}: {' / '.join(key)} occurs twice in the data set")
        rows[key] = row
    if not rows:
        raise ValueError(f"the {PLURAL[kind]} data set is empty. A bulk sync replaces the whole data set and AIOTIC refuses an empty one; remove records with delete calls")
    return rows


# ------------------------------------------------------------------------------------------------
# What AIOTIC holds
# ------------------------------------------------------------------------------------------------


def held_keys(client: AioticClient, kind: ChangeKind) -> set[Key]:
    if kind == ChangeKind.CUSTOMER:
        return {(c.number,) for c in client.customers.iter_all(size=1000)}
    if kind == ChangeKind.PRODUCT:
        return {(p.item_number, p.language_code) for p in client.products.iter_all(size=1000)}
    return {(cp.customer_number, cp.customer_item_number) for cp in client.customer_products.iter_all(size=1000)}


def held_total(client: AioticClient, kind: ChangeKind) -> int:
    if kind == ChangeKind.CUSTOMER:
        return client.customers.list(size=1).total
    if kind == ChangeKind.PRODUCT:
        return client.products.list(size=1).total
    return client.customer_products.list(size=1).total


# ------------------------------------------------------------------------------------------------
# The safeguard
# ------------------------------------------------------------------------------------------------


def _examples(keys: Iterable[Key]) -> list[str]:
    return [" / ".join(k) for k in sorted(keys)[:EXAMPLES]]


def plan_bulk_sync(
    client: AioticClient,
    sets: dict[ChangeKind, dict[Key, dict[str, Any]]],
    *,
    max_delete_fraction: float,
    compare: str,
    approve_deletions: bool,
    approve_skipped: bool,
) -> list[BulkPlan]:
    """Work out, without sending anything, what replacing each data set would do, and what the safeguard objects to."""
    if compare not in ("keys", "count"):
        raise ValueError("compare must be 'keys' or 'count'")
    plans: list[BulkPlan] = []
    held: dict[ChangeKind, set[Key]] = {}

    def keys_of(kind: ChangeKind) -> set[Key]:
        if kind not in held:
            held[kind] = held_keys(client, kind)
        return held[kind]

    for kind in ORDER:
        rows = sets.get(kind)
        if rows is None:
            continue
        # A mapping row whose customer or product AIOTIC will not hold is skipped, and the mapping held under its key
        # is removed. So such a row counts as absent, whether or not skipped rows are approved.
        unknown: list[tuple[Key, dict[str, Any]]] = []
        customers: set[Key] = set()
        if kind == ChangeKind.MAPPING:
            # After this run AIOTIC holds exactly the customers and products that are sent now, or else the ones it has.
            customers = set(sets[ChangeKind.CUSTOMER]) if sets.get(ChangeKind.CUSTOMER) is not None else keys_of(ChangeKind.CUSTOMER)
            products = set(sets[ChangeKind.PRODUCT]) if sets.get(ChangeKind.PRODUCT) is not None else keys_of(ChangeKind.PRODUCT)
            unknown = [
                (key, row)
                for key, row in rows.items()
                if (key[0],) not in customers or (str(row["item_number"]), str(row["language_code"])) not in products
            ]
        stored = set(rows) - {key for key, _ in unknown}  # the rows AIOTIC will store

        if compare == "keys":
            current = keys_of(kind)
            gone = current - stored
            plan = BulkPlan(kind, rows=len(rows), held=len(current), deletions=len(gone), exact=True, new=len(stored - current), deletion_examples=_examples(gone))
            plan.replaced_by_skipped = len(current & set(rows)) - len(current & stored)
        else:
            total = held_total(client, kind)
            plan = BulkPlan(kind, rows=len(rows), held=total, deletions=max(0, total - len(stored)), exact=False)

        if plan.deletions and not approve_deletions and plan.deletions > max_delete_fraction * plan.held:
            bound = "" if plan.exact else "at least "
            also = " Their customer item mappings go with them." if kind != ChangeKind.MAPPING else ""
            if plan.replaced_by_skipped:
                also = f" That includes {plan.replaced_by_skipped} mapping(s) held under the key of a row that AIOTIC would skip."
            elif unknown and not plan.exact:
                also = f" That counts the {len(unknown)} row(s) AIOTIC would skip as absent."
            named = f" For example: {', '.join(plan.deletion_examples)}." if plan.deletion_examples else ""
            plan.problems.append(
                f"{PLURAL[kind]}: the data set holds {plan.rows} record(s) and AIOTIC holds {plan.held}; sending it would remove "
                f"{bound}{plan.deletions} of them ({plan.deletions / plan.held:.0%}, the limit is {max_delete_fraction:.0%}).{also}{named} "
                "If the export is complete and this clean-up is intended, approve the deletions."
            )

        if unknown:
            plan.unknown_targets = len(unknown)
            plan.unknown_target_examples = [
                f"{key[0]} / {key[1]} -> " + (f"customer {key[0]}" if (key[0],) not in customers else f"product {row['item_number']} ({row['language_code']})")
                for key, row in sorted(unknown)[:EXAMPLES]
            ]
            if not approve_skipped:
                plan.problems.append(
                    f"mappings: {len(unknown)} row(s) point at a customer or product that AIOTIC will not hold. AIOTIC would skip them "
                    f"and remove the mapping it holds under the same key. For example: {', '.join(plan.unknown_target_examples)}. "
                    "Send the missing customers or products first, or approve the skipped rows."
                )
        plans.append(plan)
    return plans


def run_bulk_sync(
    client: AioticClient,
    sources: dict[ChangeKind, Iterable[Any] | None],
    *,
    max_delete_fraction: float = 0.10,
    compare: str = "keys",
    approve_deletions: bool = False,
    approve_skipped: bool = False,
    dry_run: bool = False,
) -> BulkSyncReport:
    """Check and then send the data sets given (see :meth:`aiotic.sync.SyncEngine.bulk_sync`)."""
    sets = {kind: read_rows(kind, source) for kind, source in sources.items() if source is not None}
    if not sets:
        raise ValueError("pass at least one data set: customers, products or mappings")
    report = BulkSyncReport(dry_run=dry_run, skipped_approved=approve_skipped)
    report.plans = plan_bulk_sync(client, sets, max_delete_fraction=max_delete_fraction, compare=compare, approve_deletions=approve_deletions, approve_skipped=approve_skipped)
    for plan in report.plans:
        log.info("bulk sync plan: %s", plan)
    if any(plan.problems for plan in report.plans):
        raise BulkSyncRefused(report.plans)
    if dry_run:
        return report

    send = {
        ChangeKind.CUSTOMER: client.customers.bulk_sync,
        ChangeKind.PRODUCT: client.products.bulk_sync,
        ChangeKind.MAPPING: client.customer_products.bulk_sync,
    }
    for kind in ORDER:
        if kind in sets:
            report.results[kind] = send[kind](sets[kind].values())
    if report.skipped_total and not approve_skipped:
        log.error("bulk sync: AIOTIC skipped %d mapping row(s) whose customer or product it does not hold", report.skipped_total)
    log.info("bulk sync: %s", report)
    return report
