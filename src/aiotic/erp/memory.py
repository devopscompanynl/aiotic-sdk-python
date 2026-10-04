"""In-memory ERP fake — implements every port. Used by tests, the quick start and `aiotic serve --demo`."""

from __future__ import annotations

import itertools
import threading
from typing import Any

from ..models import ErpPurchaseOrder
from .ports import ErpCreateResult, ErpRejected


class InMemoryErp:
    def __init__(
        self,
        *,
        products: set[str] | None = None,
        customers: dict[str, dict[str, Any]] | None = None,
        blocked: set[str] | None = None,
        prefix: str = "SO-",
    ):
        self.products: set[str] = set(products or {"PROD-001", "PROD-002", "620206_01"})
        self.customers: dict[str, dict[str, Any]] = customers or {"58931": {"name": "LUMITECH INSTALLATIES", "city": "Apeldoorn"}}
        self.blocked: set[str] = set(blocked or ())
        self.orders: dict[str, dict[str, Any]] = {}
        self._by_request: dict[str, str] = {}
        self._seq = itertools.count(1000)
        self._lock = threading.Lock()
        self.prefix = prefix

    # CatalogPort / CustomerPort
    def product_exists(self, article_number: str) -> bool:
        return article_number in self.products

    def get_customer(self, customer_id: str) -> dict[str, Any] | None:
        return self.customers.get(customer_id)

    def is_blocked(self, customer_id: str) -> bool:
        return customer_id in self.blocked

    # ErpPort
    def find_order_by_request_id(self, request_id: str) -> ErpCreateResult | None:
        num = self._by_request.get(request_id)
        return ErpCreateResult(order_number=num, created=False) if num else None

    def create_sales_order(self, request_id: str, order: ErpPurchaseOrder) -> ErpCreateResult:
        with self._lock:
            if request_id in self._by_request:
                return ErpCreateResult(order_number=self._by_request[request_id], created=False)
            for item in order.items:  # a functional ERP would do this itself
                if item.article_number not in self.products:
                    raise ErpRejected(f"The Item does not exist. No.='{item.article_number}'")
            number = f"{self.prefix}{next(self._seq)}"
            self.orders[number] = {"request_id": request_id, "order": order.model_dump(mode="json")}
            self._by_request[request_id] = number
            return ErpCreateResult(order_number=number, created=True)
