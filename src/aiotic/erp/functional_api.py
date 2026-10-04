"""Template adapter for an ERP with a *functional* API (the ERP validates the order itself).

Copy this file, rename the class, and replace the three ``_map_*`` methods and the two HTTP calls
with your ERP's endpoints. Keep the contract: return :class:`ErpCreateResult`, raise
:class:`ErpRejected` for business refusals (the text goes back to the operator), let other
exceptions propagate (they become a generic failure and AIOTIC rolls the status back).

**Required ERP capability.** The external reference (``externalReference`` below) must be unique in
the ERP: a second create with the same value has to fail, which this template expects as HTTP 409.
The adapter then looks the order up and returns it with ``created=False``, so two deliveries of the
same ``request_id`` — even to different service instances — end in one sales order. If your ERP does
not enforce that uniqueness, add it (a unique index or a duplicate check inside one transaction)
before relying on this adapter; nothing on the client side can replace it.
"""

from __future__ import annotations

from typing import Any

import httpx

from ..models import ErpPurchaseOrder
from .ports import ErpCreateResult, ErpRejected


class FunctionalApiAdapter:
    """Example: a REST ERP with ``GET /salesOrders?externalRef=`` and ``POST /salesOrders``."""

    def __init__(self, base_url: str, token: str, *, timeout: float = 20.0, client: httpx.Client | None = None):
        self._http = client or httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout, headers={"Authorization": f"Bearer {token}"})

    # ---- mapping (adapt to your ERP) ---------------------------------------------------
    def _map_header(self, request_id: str, o: ErpPurchaseOrder) -> dict[str, Any]:
        return {
            "externalReference": request_id,  # AIOTIC request id → idempotency + traceability
            "customerNumber": o.customer.customer_id,
            "customerPurchaseOrderNumber": o.order_number,
            "orderDate": o.order_date,
            "requestedDeliveryDate": o.delivery_date,
            "currencyCode": o.currency,
            "shipTo": self._map_ship_to(o),
            "note": o.additional_information,
            "lines": [self._map_line(i, idx) for idx, i in enumerate(o.items, start=1)],
        }

    def _map_ship_to(self, o: ErpPurchaseOrder) -> dict[str, Any]:
        r = o.shipping_details.recipient
        return {"name": r.company, "contact": r.contact_person, "street": r.address.street, "postalCode": r.address.postal_code, "city": r.address.city, "countryCode": r.address.country}

    def _map_line(self, item: Any, line_no: int) -> dict[str, Any]:
        return {"lineNo": line_no, "itemNo": item.article_number, "description": item.description, "quantity": item.quantity, "unitOfMeasure": item.unit, "unitPrice": item.price}

    # ---- ErpPort -----------------------------------------------------------------------
    def find_order_by_request_id(self, request_id: str) -> ErpCreateResult | None:
        r = self._http.get("/salesOrders", params={"externalReference": request_id})
        r.raise_for_status()
        rows = r.json().get("value", []) if isinstance(r.json(), dict) else r.json()
        if rows:
            return ErpCreateResult(order_number=str(rows[0]["number"]), created=False)
        return None

    @staticmethod
    def _error_message(r: httpx.Response) -> str:
        try:
            return (r.json().get("error", {}).get("message") or r.text)[:500]
        except (ValueError, AttributeError):
            return r.text[:500]

    def create_sales_order(self, request_id: str, order: ErpPurchaseOrder) -> ErpCreateResult:
        r = self._http.post("/salesOrders", json=self._map_header(request_id, order))
        if r.status_code == 409:
            # The ERP refused a duplicate external reference: another delivery of this request_id won. Answer with it.
            existing = self.find_order_by_request_id(request_id)
            if existing:
                return existing
            raise ErpRejected(self._error_message(r))
        if r.status_code in (400, 422):
            # Functional ERPs return a business reason — pass it on to the operator verbatim.
            raise ErpRejected(self._error_message(r))
        r.raise_for_status()
        body = r.json()
        return ErpCreateResult(order_number=str(body["number"]), created=True, details=body)
