"""Template adapter for an ERP with only a *data* API — you write straight into tables.

With a data API nothing validates the order for you: a wrong item number, a blocked customer or
a nonsense quantity lands in your ERP unvalidated and surfaces days later when the ERP tries to use it.
That is why this adapter is meant to run **behind the pipeline** (`aiotic.pipeline`) with at
least :class:`ArticlesInCatalog`, :class:`CustomerResolved`, :class:`PositiveQuantities` and
:class:`CustomerNotBlocked` configured — the adapter itself also implements :class:`CatalogPort`
and :class:`CustomerPort` by querying the same tables, so the pipeline can use it directly.

The example uses the DB-API 2.0 (``sqlite3`` in the quick start; your own database driver in real
life). All SQL lives in one place so you can swap table and column names.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from typing import Any

from ..models import ErpPurchaseOrder
from .ports import ErpCreateResult

Connection = Any  # any DB-API connection


class DataApiAdapter:
    """Example against three tables: ``items``, ``customers``, ``sales_orders`` (+ ``sales_order_lines``)."""

    SQL = {
        "item_exists": "SELECT 1 FROM items WHERE item_no = ? AND blocked = 0",
        "customer": "SELECT customer_no, name, blocked, credit_limit FROM customers WHERE customer_no = ?",
        "find_by_request": "SELECT order_no FROM sales_orders WHERE external_ref = ?",
        "next_number": "SELECT COALESCE(MAX(id), 0) + 1 FROM sales_orders",
        "insert_header": (
            "INSERT INTO sales_orders (id, order_no, external_ref, customer_no, customer_po_no, order_date, "
            "delivery_date, currency, ship_to_name, ship_to_street, ship_to_postal_code, ship_to_city, ship_to_country, note, payload)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        "insert_line": "INSERT INTO sales_order_lines (order_no, line_no, item_no, description, quantity, unit, unit_price) VALUES (?, ?, ?, ?, ?, ?, ?)",
    }

    def __init__(self, connect: Callable[[], Connection], *, number_prefix: str = "SO", paramstyle: str = "qmark"):
        self.connect = connect
        self.number_prefix = number_prefix
        self._q = (lambda s: s) if paramstyle == "qmark" else (lambda s: s.replace("?", "%s"))

    # ---- CatalogPort / CustomerPort — the validation the ERP cannot do for us --------------
    def product_exists(self, article_number: str) -> bool:
        with closing(self.connect()) as conn, closing(conn.cursor()) as cur:
            cur.execute(self._q(self.SQL["item_exists"]), (article_number,))
            return cur.fetchone() is not None

    def get_customer(self, customer_id: str) -> dict[str, Any] | None:
        with closing(self.connect()) as conn, closing(conn.cursor()) as cur:
            cur.execute(self._q(self.SQL["customer"]), (customer_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {"customer_no": row[0], "name": row[1], "blocked": bool(row[2]), "credit_limit": row[3]}

    def is_blocked(self, customer_id: str) -> bool:
        c = self.get_customer(customer_id)
        return bool(c and c["blocked"])

    # ---- ErpPort ------------------------------------------------------------------------
    def find_order_by_request_id(self, request_id: str) -> ErpCreateResult | None:
        with closing(self.connect()) as conn, closing(conn.cursor()) as cur:
            cur.execute(self._q(self.SQL["find_by_request"]), (request_id,))
            row = cur.fetchone()
            return ErpCreateResult(order_number=row[0], created=False) if row else None

    def create_sales_order(self, request_id: str, o: ErpPurchaseOrder) -> ErpCreateResult:
        """Header + lines in ONE transaction, keyed on the AIOTIC request id for idempotency."""
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(self._q(self.SQL["find_by_request"]), (request_id,))
            row = cur.fetchone()
            if row:
                return ErpCreateResult(order_number=row[0], created=False)
            cur.execute(self._q(self.SQL["next_number"]))
            next_id = int(cur.fetchone()[0])
            order_no = f"{self.number_prefix}-{next_id:06d}"
            r = o.shipping_details.recipient
            cur.execute(
                self._q(self.SQL["insert_header"]),
                (next_id, order_no, request_id, o.customer.customer_id, o.order_number, o.order_date, o.delivery_date, o.currency,
                 r.company, r.address.street, r.address.postal_code, r.address.city, r.address.country, o.additional_information,
                 json.dumps(o.model_dump(mode="json"))),
            )
            for n, item in enumerate(o.items, start=1):
                cur.execute(self._q(self.SQL["insert_line"]), (order_no, n, item.article_number, item.description, item.quantity, item.unit, item.price))
            conn.commit()
            return ErpCreateResult(order_number=order_no, created=True)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# --------------------------------------------------------------------------------------------
# Helper for the quick start / tests: an SQLite database with the three example tables.
# --------------------------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (item_no TEXT PRIMARY KEY, description TEXT, blocked INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS customers (customer_no TEXT PRIMARY KEY, name TEXT, blocked INTEGER DEFAULT 0, credit_limit REAL);
CREATE TABLE IF NOT EXISTS sales_orders (
  id INTEGER PRIMARY KEY, order_no TEXT UNIQUE, external_ref TEXT UNIQUE, customer_no TEXT, customer_po_no TEXT,
  order_date TEXT, delivery_date TEXT, currency TEXT, ship_to_name TEXT, ship_to_street TEXT, ship_to_postal_code TEXT,
  ship_to_city TEXT, ship_to_country TEXT, note TEXT, payload TEXT);
CREATE TABLE IF NOT EXISTS sales_order_lines (
  order_no TEXT, line_no INTEGER, item_no TEXT, description TEXT, quantity INTEGER, unit TEXT, unit_price REAL);
"""


def sqlite_demo(path: str = ":memory:", *, seed: bool = True) -> Callable[[], sqlite3.Connection]:
    """Return a ``connect()`` factory for a demo SQLite ERP (optionally seeded with a few rows)."""
    uri = path == ":memory:"
    shared = f"file:aiotic_demo_{id(path)}?mode=memory&cache=shared" if uri else path

    def connect() -> sqlite3.Connection:
        return sqlite3.connect(shared, uri=uri, check_same_thread=False)

    keep_alive = connect()  # keeps a shared in-memory DB alive
    keep_alive.executescript(SCHEMA)
    if seed:
        keep_alive.executemany("INSERT OR IGNORE INTO items VALUES (?, ?, 0)", [("PROD-001", "LED Driver 48V"), ("PROD-002", "LED Panel 60x60"), ("620206_01", "Cable 3x1.5")])
        keep_alive.executemany("INSERT OR IGNORE INTO customers VALUES (?, ?, ?, ?)", [("58931", "LUMITECH INSTALLATIES", 0, 10000.0), ("10577", "Example Handel GmbH", 1, 0.0)])
        keep_alive.commit()
    connect.keep_alive = keep_alive  # type: ignore[attr-defined]
    return connect
