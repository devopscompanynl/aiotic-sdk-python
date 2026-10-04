"""Inbound webhooks handled by your integration service.

1. **AIOTIC → you: processing webhook** (optional tenant feature). Fires when extraction finishes,
   *before* review. Authenticated with a static ``X-API-KEY``. Use :class:`ProcessingWebhookReceiver`.

2. **Your ERP → you: change events** that drive the sync engine. Any shape works; the helper
   :func:`parse_change_events` accepts a small generic JSON format so you can wire ERP webhooks,
   an outbox worker or a message queue consumer to the same code path::

       {"events": [
         {"kind": "product", "op": "upsert", "item_number": "620206_01", "language_code": "nl", "description": "…"},
         {"kind": "customer", "op": "delete", "number": "10577"}
       ]}

3. :func:`verify_hmac_signature` is provided for the *proposed* signed event webhooks
   (``X-AIOTIC-Signature``) so your endpoint is ready when they ship — it is not used by AIOTIC today.
"""

import hashlib
import hmac
import logging
import time
from collections.abc import Callable
from typing import Any

from pydantic import ValidationError

from .models import ProcessingWebhookRequest
from .sync.engine import ChangeEvent, ChangeKind, ChangeOp

log = logging.getLogger("aiotic.webhooks")


class ProcessingWebhookReceiver:
    def __init__(self, api_key: str, handler: Callable[[ProcessingWebhookRequest], None]):
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.handler = handler

    def verify_key(self, provided: str | None) -> bool:
        return bool(provided) and hmac.compare_digest(provided.encode(), self.api_key.encode())

    def handle(self, body: bytes | str | dict[str, Any], *, api_key_header: str | None) -> tuple[int, dict[str, Any]]:
        if not self.verify_key(api_key_header):
            return 401, {"detail": "invalid X-API-KEY"}
        try:
            req = ProcessingWebhookRequest.model_validate_json(body) if isinstance(body, (bytes, str)) else ProcessingWebhookRequest.model_validate(body)
        except ValidationError as exc:
            return 400, {"detail": f"malformed payload: {exc.errors()[0]['msg']}"}
        try:
            self.handler(req)
        except Exception:
            # Acknowledge anyway: AIOTIC does not retry, so failing here only loses the signal.
            log.exception("processing webhook handler failed for %s", req.request_id)
        return 200, {"received": str(req.request_id)}


def parse_change_events(payload: dict[str, Any]) -> list[ChangeEvent]:
    """Parse the generic change-event JSON described in the module docstring."""
    out: list[ChangeEvent] = []
    for raw in payload.get("events", []):
        kind, op = ChangeKind(raw["kind"]), ChangeOp(raw.get("op", "upsert"))
        if kind == ChangeKind.CUSTOMER:
            number = str(raw["number"])
            out.append(ChangeEvent.customer_delete(number) if op == ChangeOp.DELETE else ChangeEvent.customer_upsert(number, **{k: v for k, v in raw.items() if k not in {"kind", "op", "number"}}))
        elif kind == ChangeKind.PRODUCT:
            item, lang = str(raw["item_number"]), str(raw.get("language_code", "nl"))
            out.append(ChangeEvent.product_delete(item, lang) if op == ChangeOp.DELETE else ChangeEvent.product_upsert(item, lang, description=raw["description"], remark=raw.get("remark")))
        else:
            cn, cin = str(raw["customer_number"]), str(raw["customer_item_number"])
            out.append(ChangeEvent.mapping_delete(cn, cin) if op == ChangeOp.DELETE else ChangeEvent.mapping_upsert(cn, cin, item_number=str(raw["item_number"]), language_code=str(raw.get("language_code", "nl"))))
    return out


def verify_hmac_signature(secret: str, body: bytes, signature_header: str | None, *, tolerance: int = 300, now: Callable[[], float] = time.time) -> bool:
    """Verify ``t=<unix>,v1=<hex hmac-sha256(secret, f"{t}.{body}")>`` — the proposed signed-webhook scheme."""
    if not signature_header:
        return False
    parts = dict(p.split("=", 1) for p in signature_header.split(",") if "=" in p)
    try:
        ts = int(parts["t"])
    except (KeyError, ValueError):
        return False
    if abs(now() - ts) > tolerance:
        return False
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, parts.get("v1", ""))


def create_webhook_routers(*, processing: ProcessingWebhookReceiver | None = None, on_change_events: Callable[[list[ChangeEvent]], Any] | None = None, erp_events_key: str | None = None) -> Any:
    """FastAPI router with ``POST /aiotic/processing`` and ``POST /erp/events``. Requires the ``server`` extra.

    ``POST /erp/events`` writes to AIOTIC through the sync engine, so it is only mounted with a non-empty
    ``erp_events_key``; passing ``on_change_events`` without a key raises ``ValueError``.
    """
    from fastapi import APIRouter, Header, Request, Response

    if on_change_events is not None and not erp_events_key:
        raise ValueError("erp_events_key is required when on_change_events is set: POST /erp/events writes to AIOTIC and is never left open")

    router = APIRouter()

    if processing:

        @router.post("/aiotic/processing", summary="AIOTIC processing webhook")
        async def processing_hook(request: Request, response: Response, x_api_key: str | None = Header(default=None, alias="X-API-KEY")) -> dict[str, Any]:
            status, body = processing.handle(await request.body(), api_key_header=x_api_key)
            response.status_code = status
            return body

    if on_change_events:

        @router.post("/erp/events", summary="ERP change events → sync engine")
        async def erp_events(request: Request, response: Response, x_api_key: str | None = Header(default=None, alias="X-API-KEY")) -> dict[str, Any]:
            if not (x_api_key and hmac.compare_digest(x_api_key.encode(), erp_events_key.encode())):
                response.status_code = 401
                return {"detail": "invalid X-API-KEY"}
            events = parse_change_events(await request.json())
            result = on_change_events(events)
            return {"accepted": len(events), "result": str(result) if result is not None else None}

    return router
