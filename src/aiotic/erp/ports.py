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
    """


@runtime_checkable
class CatalogPort(Protocol):
    def product_exists(self, article_number: str) -> bool: ...


@runtime_checkable
class CustomerPort(Protocol):
    def get_customer(self, customer_id: str) -> dict[str, Any] | None: ...

    def is_blocked(self, customer_id: str) -> bool: ...


@runtime_checkable
class ErpPort(Protocol):
    """The single write operation the receive endpoint needs."""

    def find_order_by_request_id(self, request_id: str) -> ErpCreateResult | None:
        """Return the existing ERP order for an AIOTIC ``request_id`` (idempotency), or ``None``."""
        ...

    def create_sales_order(self, request_id: str, order: ErpPurchaseOrder) -> ErpCreateResult:
        """Create the order. Raise :class:`ErpRejected` for business refusals; any other exception is a failure."""
        ...
