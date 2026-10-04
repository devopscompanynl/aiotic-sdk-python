"""Built-in business rules — *policy* checks that a functional ERP API would normally enforce."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, Protocol

from ..erp.ports import CustomerPort
from ..models import ErpPurchaseOrder
from . import Context, Issue, Severity


class SeenOrdersStore(Protocol):
    """Where the duplicate-order rule remembers what it has seen (see :mod:`aiotic.receive` for stores)."""

    def seen(self, key: str) -> bool: ...

    def remember(self, key: str, value: str) -> None: ...


class NoDuplicateOrder:
    """Reject a second order with the same (customer_id, order_number) — a common re-send mistake.

    The key is only *remembered* by the receiver after a successful ERP create (so a rejected
    order can be corrected and re-sent). ``on_duplicate`` can downgrade this to a warning.
    """

    name = "no_duplicate_order"

    def __init__(self, store: SeenOrdersStore, *, severity: Severity = Severity.ERROR):
        self.store = store
        self.severity = severity

    @staticmethod
    def key(o: ErpPurchaseOrder) -> str:
        return f"{o.customer.customer_id or '?'}::{o.order_number.upper()}"

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        if self.store.seen(self.key(o)):
            yield Issue("duplicate_order", f"Order {o.order_number} for customer {o.customer.customer_id} was already booked", path="order_number", severity=self.severity)


class CustomerNotBlocked:
    """Reject orders for customers on credit hold / blocked in the ERP (:meth:`CustomerPort.is_blocked`)."""

    name = "customer_not_blocked"

    def __init__(self, customers: CustomerPort):
        self.customers = customers

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        cid = o.customer.customer_id
        if cid and self.customers.is_blocked(cid):
            yield Issue("customer_blocked", f"Customer {cid} is blocked for new orders", path="customer.customer_id")


class OrderValueWithin:
    """Warn or reject when the order value is outside a band (typo detection, credit limits)."""

    name = "order_value_within"

    def __init__(self, *, min_total: float | None = None, max_total: float | None = None, severity: Severity = Severity.WARNING):
        self.min_total, self.max_total, self.severity = min_total, max_total, severity

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        total = o.total_price if o.total_price is not None else sum(i.line_total or 0 for i in o.items)
        if self.min_total is not None and total < self.min_total:
            yield Issue("order_value_low", f"Order value {total} is below {self.min_total}", path="total_price", severity=self.severity)
        if self.max_total is not None and total > self.max_total:
            yield Issue("order_value_high", f"Order value {total} exceeds {self.max_total}", path="total_price", severity=self.severity)


class ShipToAddressComplete:
    """A deliverable address needs street, postal code and city."""

    name = "ship_to_address_complete"

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        a = o.shipping_details.recipient.address
        for field in ("street", "postal_code", "city"):
            if not getattr(a, field):
                yield Issue("incomplete_ship_to", f"Shipping address is missing {field}", path=f"shipping_details.recipient.address.{field}")


class Custom:
    """Wrap any ``(order, ctx) -> Iterable[Issue]`` function as a rule."""

    def __init__(self, name: str, fn: Callable[[ErpPurchaseOrder, Context], Iterable[Issue]]):
        self.name = name
        self.fn = fn

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        return self.fn(o, ctx)


def rule(name: str) -> Callable[[Callable[[ErpPurchaseOrder, Context], Iterable[Issue]]], Custom]:
    """Decorator sugar::

        @rule("no_weekend_delivery")
        def no_weekend(order, ctx):
            ...
            yield Issue(...)
    """

    def deco(fn: Callable[[ErpPurchaseOrder, Context], Iterable[Issue]]) -> Custom:
        return Custom(name, fn)

    return deco


__all__: list[str] = [n for n in dir() if not n.startswith("_") and n not in {"Any", "Callable", "Iterable", "Protocol"}]
