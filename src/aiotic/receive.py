"""The ERP receive endpoint — the HTTP endpoint AIOTIC calls with a reviewed order.

Contract (see the guide → *Receiving orders*):

* ``POST <your URL>`` with ``X-API-KEY: <key you gave AIOTIC>`` and a JSON body
  ``{"request_id": "...", "purchase_order": {...}}``.
* Reply with JSON ``{"success": true, "order_number": "..."}`` or ``{"success": false, "error": "..."}``.
  The body is authoritative; the HTTP status is not.
* ``request_id`` is stable across retries → be idempotent.

:class:`ErpReceiver` implements all of that framework-independently; :func:`create_receive_router`
wraps it for FastAPI. Wire it into Flask/Django/anything by calling ``receiver.handle(...)``.
"""

import hmac
import logging
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import ValidationError

from .erp.ports import ErpCreateResult, ErpPort, ErpRejected
from .models import ErpReceiveRequest, ErpReceiveResponse
from .pipeline import Pipeline, Verdict

log = logging.getLogger("aiotic.receive")


class IdempotencyStore(Protocol):
    """Remembers ``request_id → ERP order number`` so a retried send never books twice."""

    def get(self, request_id: str) -> str | None: ...

    def put(self, request_id: str, order_number: str) -> None: ...

    # SeenOrdersStore for the duplicate-order rule
    def seen(self, key: str) -> bool: ...

    def remember(self, key: str, value: str) -> None: ...


class InMemoryStore:
    def __init__(self) -> None:
        self._d: dict[str, str] = {}
        self._lock = threading.Lock()

    def get(self, request_id: str) -> str | None:
        return self._d.get(f"req:{request_id}")

    def put(self, request_id: str, order_number: str) -> None:
        with self._lock:
            self._d[f"req:{request_id}"] = order_number

    def seen(self, key: str) -> bool:
        return f"key:{key}" in self._d

    def remember(self, key: str, value: str) -> None:
        with self._lock:
            self._d[f"key:{key}"] = value


class SqliteStore:
    """Durable store in a single SQLite file — enough for one service instance. Use your DB for more."""

    def __init__(self, path: str = "aiotic-integration.db"):
        self.path = path
        self._lock = threading.Lock()
        with sqlite3.connect(self.path) as c:
            c.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")

    def _get(self, k: str) -> str | None:
        with sqlite3.connect(self.path) as c:
            row = c.execute("SELECT v FROM kv WHERE k = ?", (k,)).fetchone()
            return row[0] if row else None

    def _put(self, k: str, v: str) -> None:
        with self._lock, sqlite3.connect(self.path) as c:
            c.execute("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", (k, v))

    def get(self, request_id: str) -> str | None:
        return self._get(f"req:{request_id}")

    def put(self, request_id: str, order_number: str) -> None:
        self._put(f"req:{request_id}", order_number)

    def seen(self, key: str) -> bool:
        return self._get(f"key:{key}") is not None

    def remember(self, key: str, value: str) -> None:
        self._put(f"key:{key}", value)


@dataclass(slots=True)
class ReceiveOutcome:
    response: ErpReceiveResponse
    verdict: Verdict | None
    result: ErpCreateResult | None
    http_status: int = 200


class ErpReceiver:
    """Framework-agnostic handler for the receive endpoint.

    ``handle()`` never raises for business problems; it returns a :class:`ReceiveOutcome` whose
    ``response`` is exactly what to send back to AIOTIC. Unexpected exceptions are converted to
    ``success: false`` with a generic message (the details go to your logs) so AIOTIC rolls back
    cleanly instead of timing out.
    """

    def __init__(
        self,
        erp: ErpPort,
        *,
        api_key: str,
        pipeline: Pipeline | None = None,
        store: IdempotencyStore | None = None,
        on_accepted: Callable[[ErpReceiveRequest, ErpCreateResult, Verdict | None], None] | None = None,
        on_rejected: Callable[[ErpReceiveRequest, ErpReceiveResponse, Verdict | None], None] | None = None,
        generic_error: str = "Order could not be booked in the ERP right now. Please try again or contact IT.",
    ):
        if not api_key:
            raise ValueError("api_key is required — it is the value AIOTIC sends in X-API-KEY")
        self.erp = erp
        self.api_key = api_key
        self.pipeline = pipeline
        self.store = store or InMemoryStore()
        self.on_accepted = on_accepted
        self.on_rejected = on_rejected
        self.generic_error = generic_error

    def verify_key(self, provided: str | None) -> bool:
        return bool(provided) and hmac.compare_digest(provided.encode(), self.api_key.encode())

    def handle(self, body: dict[str, Any] | bytes | str, *, api_key_header: str | None) -> ReceiveOutcome:
        if not self.verify_key(api_key_header):
            return ReceiveOutcome(ErpReceiveResponse.rejected("Unauthorized: invalid X-API-KEY"), None, None, http_status=401)
        try:
            req = ErpReceiveRequest.model_validate_json(body) if isinstance(body, (bytes, str)) else ErpReceiveRequest.model_validate(body)
        except ValidationError as exc:
            log.warning("receive: malformed payload: %s", exc)
            return ReceiveOutcome(ErpReceiveResponse.rejected(f"Malformed payload: {exc.errors()[0]['msg']}"), None, None, http_status=400)

        rid = str(req.request_id)
        # 1. Idempotency — same request id → same answer, no second booking.
        existing = self.store.get(rid) or (lambda r: r.order_number if r else None)(self.erp.find_order_by_request_id(rid))
        if existing:
            log.info("receive: %s already booked as %s (replay)", rid, existing)
            return ReceiveOutcome(ErpReceiveResponse.accepted(existing), None, ErpCreateResult(existing, created=False))

        # 2. Pipeline — the validation layer (essential for data-API ERPs).
        verdict: Verdict | None = None
        order = req.purchase_order
        if self.pipeline:
            verdict = self.pipeline.run(order, {"request_id": rid})
            order = verdict.order
            if not verdict.ok:
                resp = ErpReceiveResponse.rejected(verdict.error_message())
                if self.on_rejected:
                    self.on_rejected(req, resp, verdict)
                return ReceiveOutcome(resp, verdict, None)

        # 3. Create in the ERP.
        try:
            result = self.erp.create_sales_order(rid, order)
        except ErpRejected as exc:
            resp = ErpReceiveResponse.rejected(str(exc))
            if self.on_rejected:
                self.on_rejected(req, resp, verdict)
            return ReceiveOutcome(resp, verdict, None)
        except Exception:
            log.exception("receive: ERP create failed for %s", rid)
            return ReceiveOutcome(ErpReceiveResponse.rejected(self.generic_error), verdict, None, http_status=500)

        self.store.put(rid, result.order_number)
        self.store.remember(f"{order.customer.customer_id or '?'}::{order.order_number.upper()}", result.order_number)
        if self.on_accepted:
            self.on_accepted(req, result, verdict)
        log.info("receive: %s booked as %s", rid, result.order_number)
        return ReceiveOutcome(ErpReceiveResponse.accepted(result.order_number), verdict, result)


def create_receive_router(receiver: ErpReceiver, *, path: str = "/aiotic/orders") -> Any:
    """FastAPI router exposing the receive endpoint. Requires the ``server`` extra."""
    from fastapi import APIRouter, Header, Request, Response

    router = APIRouter()

    @router.post(path, summary="AIOTIC ERP receive endpoint")
    async def receive(request: Request, response: Response, x_api_key: str | None = Header(default=None, alias="X-API-KEY")) -> dict[str, Any]:
        body = await request.body()
        outcome = receiver.handle(body, api_key_header=x_api_key)
        response.status_code = outcome.http_status
        return outcome.response.model_dump(mode="json", exclude_none=True)

    return router
