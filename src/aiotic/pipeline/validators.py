"""Built-in validators — checks about the *shape and consistency* of the data."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date, datetime, timedelta

from ..erp.ports import CatalogPort, CustomerPort
from ..models import ErpPurchaseOrder
from . import Context, Issue, Severity


class RequiredFields:
    """Order number, order date, at least one line, a customer id, and a company + city on the customer."""

    name = "required_fields"

    def __init__(self, *, require_customer_id: bool = True, require_delivery_date: bool = False):
        self.require_customer_id = require_customer_id
        self.require_delivery_date = require_delivery_date

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        if not o.order_number:
            yield Issue("missing_order_number", "Order number is missing", path="order_number")
        if not o.order_date:
            yield Issue("missing_order_date", "Order date is missing", path="order_date")
        if self.require_delivery_date and not o.delivery_date:
            yield Issue("missing_delivery_date", "Delivery date is missing", path="delivery_date")
        if not o.items:
            yield Issue("no_lines", "The order has no line items", path="items")
        if self.require_customer_id and not o.customer.customer_id:
            yield Issue("missing_customer_id", "Customer is not identified (customer_id is empty)", path="customer.customer_id")
        if not o.customer.company:
            yield Issue("missing_customer_company", "Customer company name is missing", path="customer.company", severity=Severity.WARNING)


class CustomerResolved:
    """``customer_id`` must exist in your ERP (looked up through :class:`CustomerPort`)."""

    name = "customer_resolved"

    def __init__(self, customers: CustomerPort):
        self.customers = customers

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        cid = o.customer.customer_id
        if not cid:
            return
        rec = self.customers.get_customer(cid)
        if rec is None:
            yield Issue("unknown_customer", f"Customer {cid} does not exist in the ERP", path="customer.customer_id")
        else:
            ctx.set("erp_customer", rec)


class ArticlesInCatalog:
    """Every line needs an article number that exists in your ERP catalog (:class:`CatalogPort`)."""

    name = "articles_in_catalog"

    def __init__(self, catalog: CatalogPort):
        self.catalog = catalog

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        for idx, item in enumerate(o.items):
            if not item.article_number:
                yield Issue("missing_article", f"Line {idx + 1} has no article number", path=f"items[{idx}].article_number")
                continue
            if not self.catalog.product_exists(item.article_number):
                yield Issue(
                    "unknown_article",
                    f"Unknown article number: {item.article_number}",
                    path=f"items[{idx}].article_number",
                    data={"article_number": item.article_number},
                )


class PositiveQuantities:
    """Quantities must be whole numbers > 0 (AIOTIC drops blank/zero rows; ``Unrecognised`` rows arrive as ``null``)."""

    name = "positive_quantities"

    def __init__(self, max_quantity: int | None = 100_000):
        self.max_quantity = max_quantity

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        for idx, item in enumerate(o.items):
            q = item.quantity
            if q is None or q <= 0:
                yield Issue("invalid_quantity", f"Line {idx + 1} has no valid quantity", path=f"items[{idx}].quantity")
            elif self.max_quantity and q > self.max_quantity:
                yield Issue("suspicious_quantity", f"Line {idx + 1} quantity {q} exceeds {self.max_quantity}", path=f"items[{idx}].quantity", severity=Severity.WARNING)


class LineTotalsConsistent:
    """``quantity × price`` should equal ``line_total`` within a tolerance (warning by default)."""

    name = "line_totals_consistent"

    def __init__(self, tolerance: float = 0.05, severity: Severity = Severity.WARNING):
        self.tolerance = tolerance
        self.severity = severity

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        for idx, item in enumerate(o.items):
            if item.quantity is None or item.price is None or item.line_total is None:
                continue
            expected = round(item.quantity * item.price, 2)
            if abs(expected - item.line_total) > self.tolerance:
                yield Issue("line_total_mismatch", f"Line {idx + 1}: {item.quantity} × {item.price} = {expected}, but line_total is {item.line_total}", path=f"items[{idx}].line_total", severity=self.severity)


class OrderTotalConsistent:
    """The sum of line totals should match ``total_price`` (warning; gross totals incl. VAT legitimately differ)."""

    name = "order_total_consistent"

    def __init__(self, tolerance_ratio: float = 0.25, severity: Severity = Severity.WARNING):
        self.tolerance_ratio = tolerance_ratio
        self.severity = severity

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        if o.total_price is None:
            return
        lines = [i.line_total for i in o.items if i.line_total is not None]
        if not lines:
            return
        s = round(sum(lines), 2)
        if s and abs(s - o.total_price) / max(s, 0.01) > self.tolerance_ratio:
            yield Issue("order_total_mismatch", f"Lines sum to {s} but the document total is {o.total_price}", path="total_price", severity=self.severity)


class CurrencyAllowed:
    name = "currency_allowed"

    def __init__(self, allowed: Iterable[str] = ("EUR",)):
        self.allowed = {c.upper() for c in allowed}

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        for path, cur in [("currency", o.currency), *[(f"items[{i}].currency", it.currency) for i, it in enumerate(o.items)]]:
            if cur and cur.upper() not in self.allowed:
                yield Issue("currency_not_allowed", f"Currency {cur} is not accepted", path=path)


class DeliveryDateSane:
    """Delivery date must parse as ISO and lie within a plausible window."""

    name = "delivery_date_sane"

    def __init__(self, *, max_days_in_past: int = 30, max_days_ahead: int = 365, today: Callable[[], date] = date.today):
        self.max_past, self.max_ahead, self.today = max_days_in_past, max_days_ahead, today

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        if not o.delivery_date:
            return
        try:
            d = datetime.fromisoformat(o.delivery_date).date()
        except ValueError:
            yield Issue("invalid_delivery_date", f"Delivery date '{o.delivery_date}' is not an ISO date", path="delivery_date")
            return
        t = self.today()
        if d < t - timedelta(days=self.max_past):
            yield Issue("delivery_date_in_past", f"Delivery date {d} is in the past", path="delivery_date", severity=Severity.WARNING)
        if d > t + timedelta(days=self.max_ahead):
            yield Issue("delivery_date_far_future", f"Delivery date {d} is more than {self.max_ahead} days ahead", path="delivery_date", severity=Severity.WARNING)


class Custom:
    """Wrap any ``(order, ctx) -> Iterable[Issue]`` function as a validator / rule."""

    def __init__(self, name: str, fn: Callable[[ErpPurchaseOrder, Context], Iterable[Issue]]):
        self.name = name
        self.fn = fn

    def check(self, o: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]:
        return self.fn(o, ctx)
