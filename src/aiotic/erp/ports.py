"""Ports (interfaces) between the integration layer and your ERP."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ..models import ErpPurchaseOrder


@dataclass(slots=True)
class ErpCreateResult:
    """What creating a sales order in the ERP produced."""

    order_number: str  # the ERP's own reference — returned to AIOTIC and stored as erp_ref
    created: bool = True  # False when the order already existed (idempotent replay)
    details: dict[str, Any] = field(default_factory=dict)


class ErpRejected(Exception):
    """Raise from an adapter when the ERP refuses the order for a *business* reason.

    The message is returned to AIOTIC as ``{"success": false, "error": <message>}`` and shown to
    the operator. Use it for "unknown item", "customer blocked", "credit limit" — not for outages.

    ``retryable=True`` adds ``"retryable": true`` to the answer, which makes AIOTIC send the same order
    again (2 more times by default, a few seconds apart). Use :class:`ErpTemporaryRefusal` for that.
    """

    def __init__(self, message: str = "", *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


class ErpTemporaryRefusal(ErpRejected):
    """Raise from an adapter when the ERP refuses the order for a reason that passes by itself: a record that
    is in use for a moment, a deadlock, a number series that is busy.

    AIOTIC then sends the same order again with the same ``request_id``. Raise it **only** when a repeat cannot
    create the order twice: the refused attempt created nothing, or the ERP rejects a second order with the same
    external reference (see :class:`ErpPort`). An outage, a timeout or an error you cannot classify is not a
    temporary refusal: let that exception propagate, and the operator sends the order again.
    """

    def __init__(self, message: str = ""):
        super().__init__(message, retryable=True)


@runtime_checkable
class CatalogPort(Protocol):
    def product_exists(self, article_number: str) -> bool: ...


@runtime_checkable
class CustomerPort(Protocol):
    def get_customer(self, customer_id: str) -> dict[str, Any] | None: ...

    def is_blocked(self, customer_id: str) -> bool: ...


@runtime_checkable
class ErpPort(Protocol):
    """The single write operation the receive endpoint needs, and the idempotency contract that comes with it.

    AIOTIC sends an order again with the same ``request_id`` (after a refusal marked retryable, and when an
    operator sends again after a failed send), and several instances of your service may receive the same delivery. One receiver
    instance serialises deliveries with the same ``request_id`` and remembers results, but it cannot do that
    across instances or after a lost response. Therefore ``create_sales_order`` **must be safe to call
    twice with the same ``request_id``**: store the id as a unique external reference in the ERP (a unique index,
    or the ERP's own duplicate check) and, when the ERP reports that the reference already exists, return that
    order with ``created=False`` instead of booking again. Without that uniqueness in the ERP, "one ``request_id``
    → at most one sales order" cannot be guaranteed by any client-side code.
    """

    def find_order_by_request_id(self, request_id: str) -> ErpCreateResult | None:
        """Return the existing ERP order for an AIOTIC ``request_id`` (external reference), or ``None``."""
        ...

    def create_sales_order(self, request_id: str, order: ErpPurchaseOrder) -> ErpCreateResult:
        """Create the order, or return the one that already carries ``request_id`` (``created=False``).

        Raise :class:`ErpRejected` for business refusals; any other exception is a failure that AIOTIC rolls back.
        """
        ...
