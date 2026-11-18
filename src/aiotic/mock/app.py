"""FastAPI implementation of the mock AIOTIC tenant (see package docstring)."""

from __future__ import annotations

import asyncio
import email
import email.policy
import gzip
import json
import logging
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import JSONResponse

log = logging.getLogger("aiotic.mock")

INTEGRATION_KEY = "mock-integration-key"
SYNC_KEY = "mock-sync-key"
SENDABLE = {"PROCESSED", "MODIFIED", "ATTENTION"}
SAMPLE_ORDER_NUMBER = re.compile(r"(?:PO|EB|ORD)[-_ ]?\d{3,}", re.I)
NEWEST_SOURCE_CHANGE = "-source_updated_at"
RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})")
BULK_MAX_BYTES = 40 * 1024 * 1024
BULK_MAX_REPORTED_ERRORS = 100
BULK_MAX_REPORTED_SKIPS = 1000
CUSTOMER_FIELDS = ("name", "postal_code", "city", "address", "contact_person", "phone_number", "vat_number", "email", "coc_number", "home_page")
EMAIL_SOURCES = ("raw_email", "email_body")


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class MockState:
    def __init__(self) -> None:
        self.orders: dict[str, dict[str, Any]] = {}
        self.files: dict[str, dict[str, bytes]] = {}
        self.groups: dict[str, dict[str, Any]] = {}
        self.rejected: dict[str, dict[str, Any]] = {}
        self.customers: dict[str, dict[str, Any]] = {}
        self.products: dict[tuple[str, str], dict[str, Any]] = {}
        self.mappings: dict[tuple[str, str], dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.file_ids: dict[tuple[str, str], int] = {}
        self.seed()

    def file_id(self, request_id: str, name: str) -> int:
        """The identifier of a file stored with an order; the same on every listing."""
        return self.file_ids.setdefault((request_id, name), 801 + len(self.file_ids))

    def seed(self) -> None:
        self.customers["58931"] = {"number": "58931", "id": str(uuid.uuid4()), "name": "LUMITECH INSTALLATIES", "postal_code": "7327 AA", "city": "Apeldoorn", "address": "Ambachtsweg 12", "contact_person": "J. de Boer", "phone_number": "+31 55 123 4567", "vat_number": "NL001234567B01", "email": "info@lumitech.example", "coc_number": None, "home_page": None, "similarity": None, "source_updated_at": None}
        for item, desc in (("PROD-001", "LED Driver 48V 100W"), ("PROD-002", "LED Panel 60x60 40W"), ("CBL-3015", "Cable 3x1.5 mm² (100 m)")):
            self.products[(item, "nl")] = {"item_number": item, "language_code": "nl", "description": desc, "remark": None, "created_at": _now(), "source_updated_at": None}
        self.mappings[("58931", "LT-ART-001")] = {"customer_number": "58931", "customer_item_number": "LT-ART-001", "item_number": "PROD-001", "language_code": "nl", "created_at": _now(), "source_updated_at": None}


class Refused(Exception):
    """A record the API refuses: ``loc`` names the field, ``msg`` the reason (the shape of a 422 ``detail`` entry)."""

    def __init__(self, loc: tuple[Any, ...], msg: str, kind: str = "value_error"):
        super().__init__(msg)
        self.entry = {"type": kind, "loc": list(loc), "msg": msg}


def _source_updated_at(body: dict[str, Any]) -> str | None:
    """The ``source_updated_at`` of a write, in UTC. It must carry a zone; left out, the stored value is cleared."""
    value = body.get("source_updated_at")
    if value is None:
        return None
    if not isinstance(value, str) or not RFC3339.fullmatch(value):
        raise Refused(("body", "source_updated_at"), "Value error, must be an RFC 3339 timestamp with a zone, such as 2026-09-10T12:00:07+02:00")
    try:
        return datetime.fromisoformat(value.replace("z", "Z").replace("Z", "+00:00").replace(" ", "T").replace("t", "T")).astimezone(timezone.utc).isoformat()
    except ValueError:
        raise Refused(("body", "source_updated_at"), "Value error, not a valid date and time")


def _text(body: dict[str, Any], field: str, *, required: bool = False) -> str | None:
    value = body.get(field)
    if value is None:
        if required:
            raise Refused(("body", field), "Field required", "missing")
        return None
    if not isinstance(value, str):
        raise Refused(("body", field), "Input should be a valid string", "string_type")
    if "\x00" in value:
        raise Refused(("body", field), "Value error, contains a character that cannot be stored")
    return value


def _customer_record(number: str, body: dict[str, Any], existing: dict[str, Any] | None, ids: dict[str, str]) -> dict[str, Any]:
    """A customer as a write stores it. ``ids`` maps every id in use to its customer number."""
    name = _text(body, "name", required=True)
    if not name.strip():
        raise Refused(("body", "name"), "Value error, a customer needs a name: it may not be empty or only spaces")
    rec: dict[str, Any] = {"number": number, "similarity": None}
    for field in CUSTOMER_FIELDS:
        rec[field] = _text(body, field)
    if existing:
        rec["id"] = existing["id"]  # an id sent for a customer AIOTIC already holds is ignored
    else:
        sent = body.get("id")
        if sent and ids.get(str(sent), number) != number:
            raise IdTaken(f"Customer id {sent} already belongs to customer {ids[str(sent)]}; leave id out and one is assigned")
        rec["id"] = str(sent) if sent else str(uuid.uuid4())
    rec["source_updated_at"] = _source_updated_at(body)
    return rec


class IdTaken(Exception):
    """A new customer was sent with the id of another customer."""


def _product_record(item_number: str, language_code: str, body: dict[str, Any], existing: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "item_number": item_number,
        "language_code": language_code,
        "description": _text(body, "description", required=True),
        "remark": _text(body, "remark"),
        "created_at": (existing or {}).get("created_at") or body.get("created_at") or _now(),
        "source_updated_at": _source_updated_at(body),
    }


def _mapping_record(customer_number: str, customer_item_number: str, body: dict[str, Any], existing: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "customer_number": customer_number,
        "customer_item_number": customer_item_number,
        "item_number": _text(body, "item_number", required=True),
        "language_code": _text(body, "language_code", required=True),
        "created_at": (existing or {}).get("created_at") or body.get("created_at") or _now(),
        "source_updated_at": _source_updated_at(body),
    }


def _sorted(items: list[dict[str, Any]], sort: str | None) -> list[dict[str, Any]]:
    """The one sort the lists offer: newest source change first, records without one last."""
    if sort is None:
        return items
    if sort != NEWEST_SOURCE_CHANGE:
        raise HTTPException(422, [{"type": "literal_error", "loc": ["query", "sort"], "msg": "Input should be '-source_updated_at'"}])
    dated = sorted((i for i in items if i.get("source_updated_at")), key=lambda i: i["source_updated_at"], reverse=True)
    return dated + [i for i in items if not i.get("source_updated_at")]


def _email_body(raw: bytes) -> tuple[str, str, bytes]:
    """The body of a raw e-mail: file name, media type and bytes. HTML when the mail has no plain-text part."""
    message = email.message_from_bytes(raw, policy=email.policy.default)
    part = message.get_body(preferencelist=("plain", "html"))
    if part is None:
        return "body.txt", "text/plain", b""
    html = part.get_content_type() == "text/html"
    return ("body.html" if html else "body.txt"), part.get_content_type(), part.get_content().encode("utf-8")


def _sample_result(order_number: str, unknown_article: bool, filename: str) -> dict[str, Any]:
    items = [
        {"article_number": "PROD-001", "customer_item_number": "LT-ART-001", "description": "LED Driver 48V 100W", "quantity": 10, "quantity_state": "Valid", "unit": "ST", "price": 12.34, "currency": "EUR", "line_total": 123.4},
        {"article_number": "CBL-3015", "customer_item_number": None, "description": "Cable 3x1.5 mm² (100 m)", "quantity": 2, "quantity_state": "Valid", "unit": "ROL", "price": 45.0, "currency": "EUR", "line_total": 90.0},
    ]
    if unknown_article:
        items.append({"article_number": None, "customer_item_number": "LT-ART-999", "description": "Unknown widget", "quantity": 1, "quantity_state": "Valid", "unit": "ST", "price": 9.99, "currency": "EUR", "line_total": 9.99})
    return {
        "order_number": order_number,
        "order_date": datetime.now().date().isoformat(),
        "delivery_date": None, "delivery_date_from": None, "delivery_date_to": None,
        "supplier": {"company": "Acme Supplies BV", "contact_person": None, "email": "orders@acme.example", "address": {"street": "Industrieweg 5", "postal_code": "1234 AB", "city": "Amsterdam", "country": "NL"}},
        "customer": {"customer_id": "58931", "company": "LUMITECH INSTALLATIES", "contact_person": "J. de Boer", "email": "info@lumitech.example", "phone": "+31 55 123 4567", "branch": None, "vat_id": "NL001234567B01", "iban": None, "bic": None, "address": {"street": "Ambachtsweg 12", "postal_code": "7327 AA", "city": "Apeldoorn", "country": "NL"}},
        "shipping_details": {"recipient": {"company": "LUMITECH INSTALLATIES", "contact_person": "J. de Boer", "department": None, "email": None, "phone": None, "address": {"street": "Ambachtsweg 12", "postal_code": "7327 AA", "city": "Apeldoorn", "country": "NL"}}, "special_instructions": None},
        "items": items,
        "total_price": round(sum(i["line_total"] for i in items), 2),
        "currency": "EUR",
        "additional_information": f"Extracted from {filename}",
    }


def _erp_payload(order: dict[str, Any]) -> dict[str, Any]:
    r = order["result"]
    c, s = r.get("customer") or {}, (r.get("shipping_details") or {}).get("recipient") or {}
    ca, sa = c.get("address") or {}, s.get("address") or {}
    po = {
        "order_number": r.get("order_number") or order["request_id"],
        "order_date": r.get("order_date"), "delivery_date": r.get("delivery_date"), "currency": r.get("currency"), "total_price": r.get("total_price"),
        "additional_information": r.get("additional_information"), "supplier": r.get("supplier"),
        "customer": {k: c.get(k) for k in ("customer_id", "company", "contact_person", "email", "phone", "iban", "bic", "vat_id")} | {"address": {k: ca.get(k) for k in ("street", "postal_code", "city", "country")}},
        "shipping_details": {"recipient": {k: s.get(k) for k in ("company", "department", "contact_person", "email", "phone")} | {"address": {k: sa.get(k) for k in ("street", "postal_code", "city", "country")}}, "special_instructions": (r.get("shipping_details") or {}).get("special_instructions")},
        "items": [{k: i.get(k) for k in ("unit", "price", "currency", "quantity", "line_total", "description", "article_number")} for i in r.get("items") or []],
    }
    return {"request_id": order["request_id"], "purchase_order": po}


def create_mock_app(
    *,
    erp_url: str | None = None,
    erp_key: str | None = None,
    webhook_url: str | None = None,
    webhook_key: str | None = None,
    processing_seconds: float = 3.0,
    erp_retries: int = 2,
    erp_retry_delay: float = 1.0,
    direct_upload: bool = True,
    state: MockState | None = None,
) -> FastAPI:
    """``erp_retries`` / ``erp_retry_delay``: how often and how soon a send is repeated after a refusal your endpoint
    marks ``retryable``. ``direct_upload=False`` makes the mock behave like a tenant that takes its orders through
    its mailbox and the AIOTIC app only: both upload endpoints answer ``409``."""
    st = state or MockState()
    cfg = {
        "erp_url": erp_url, "erp_key": erp_key, "webhook_url": webhook_url, "webhook_key": webhook_key, "speed": processing_seconds,
        "erp_retries": erp_retries, "erp_retry_delay": erp_retry_delay, "direct_upload": direct_upload, "mail_unavailable": False,
    }
    bulk_lock = asyncio.Lock()  # one bulk sync at a time, of any data set
    app = FastAPI(title="Mock AIOTIC tenant", version="1.0.0", description="Local stand-in for an AIOTIC tenant. Keys: mock-integration-key / mock-sync-key.")
    app.state.mock = st
    app.state.cfg = cfg

    # ---- auth ----------------------------------------------------------------------------
    def auth(request: Request, x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
        path = request.url.path
        ok = x_api_key == INTEGRATION_KEY or (x_api_key == SYNC_KEY and path.startswith(("/customer", "/product", "/customer-product")))
        if not ok:
            raise HTTPException(401, "Invalid or missing API key")

    def uuid_or_400(value: str) -> str:
        try:
            return str(uuid.UUID(value))
        except ValueError:
            raise HTTPException(400, "Invalid document ID format")

    def direct_upload_or_409() -> None:
        if not cfg["direct_upload"]:
            raise HTTPException(409, "This tenant does not accept direct uploads. Send the document to the tenant's order mailbox or upload it in the AIOTIC app.")

    # ---- processing simulation -----------------------------------------------------------
    async def process_later(rid: str, filename: str, force_attention: bool) -> None:
        await asyncio.sleep(max(0.05, cfg["speed"] / 3))
        with st.lock:
            o = st.orders.get(rid)
            if not o:
                return
            o["status"], o["timestamp"] = "PROCESSING", _now()
        await asyncio.sleep(max(0.05, cfg["speed"] * 2 / 3))
        m = SAMPLE_ORDER_NUMBER.search(filename)
        order_number = (m.group(0) if m else f"PO-{rid[:8].upper()}").replace("/", "-")
        unknown = force_attention or "attention" in filename.lower()
        fail = "fail" in filename.lower()
        with st.lock:
            o = st.orders.get(rid)
            if not o:
                return
            if fail:
                o.update(status="FAILED", timestamp=_now(), last_error="Simulated extraction failure (filename contains 'fail')")
                return
            o["result"] = _sample_result(order_number, unknown, filename)
            o["order_label"] = order_number
            o["status"] = "ATTENTION" if unknown else "PROCESSED"
            o["timestamp"] = _now()
            payload = {"request_id": rid, "purchase_order": o["result"]}
        if cfg["webhook_url"]:
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.post(cfg["webhook_url"], json=payload, headers={"X-API-KEY": cfg["webhook_key"] or ""})
                    log.info("processing webhook → %s: %s", cfg["webhook_url"], r.status_code)
            except Exception as exc:  # best effort, exactly like the real thing
                log.warning("processing webhook failed: %s", exc)

    # ---- health ---------------------------------------------------------------------------
    @app.get("/healthcheck")
    def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/system-status")
    def system_status() -> dict[str, Any]:
        return {"status": "operational", "message": None, "updated_at": _now()}

    # ---- orders ---------------------------------------------------------------------------
    @app.post("/order/upload", dependencies=[Depends(auth)])
    async def upload(request: Request, files: list[UploadFile] = File(...), request_id: str | None = Form(default=None)) -> dict[str, Any]:
        direct_upload_or_409()
        form = await request.form()
        metadata = {k: v for k, v in form.items() if k not in {"files", "request_id"} and isinstance(v, str)}
        rid = uuid_or_400(request_id) if request_id else str(uuid.uuid4())
        # As the real API: an upload under an id that already exists replaces that order's files, puts it back
        # into QUEUED whatever its status was, and processes it again. A request_id is not an idempotency key.
        attachments: dict[str, Any] = {}
        blobs: dict[str, bytes] = {}
        for f in files:
            data = await f.read()
            name = f.filename or "upload.bin"
            if not name.lower().endswith((".pdf", ".jpg", ".jpeg", ".png", ".txt", ".md", ".eml")):
                raise HTTPException(415, f"Unsupported file type: {name}")
            attachments[name] = {"size": len(data), "mime_type": f.content_type or "application/octet-stream"}
            blobs[name] = data
        with st.lock:
            st.orders[rid] = {"request_id": rid, "status": "QUEUED", "timestamp": _now(), "attachments": attachments, "metadata": {"source": "api", **metadata}, "result": None, "state": None, "erp_ref": None, "email_group_id": None, "order_label": None, "retry_count": 0, "next_retry_at": None, "last_error": None}
            st.files[rid] = blobs
        asyncio.create_task(process_later(rid, next(iter(attachments)), False))
        return {"request_id": rid, "split": False}

    @app.post("/order/raw/upload", dependencies=[Depends(auth)])
    async def upload_raw(file: UploadFile = File(...), request_id: str | None = Form(default=None)) -> dict[str, Any]:
        direct_upload_or_409()
        name = file.filename or ""
        if not name.lower().endswith(".eml"):
            raise HTTPException(400, "Invalid file format: expected .eml")
        data = await file.read()
        text = data.decode("utf-8", "ignore")
        if "quotation" in text.lower() or "offerte" in text.lower():
            rid = str(uuid.uuid4())
            with st.lock:
                st.rejected[rid] = {"request_id": rid, "email_type": "quotation", "sender": "sales@example.com", "from_name": "Example", "subject": "Quotation", "timestamp": _now(), "metadata": {}, "message_id": None, "classification_reason": "Reads as a quotation, not an order", "rejection_status": "pending", "original_email_type": None}
            return JSONResponse(status_code=400, content={"detail": {"error": "not_a_purchase_order", "message": "Email classified as 'quotation', not a purchase order — nothing to process.", "request_id": rid, "category": "quotation", "subject": "Quotation", "language": "nl", "line_item_count": None}})
        split = text.count("Subject:") > 1 or "SPLIT" in text
        rid = uuid_or_400(request_id) if request_id else str(uuid.uuid4())
        if not split:
            with st.lock:
                st.orders[rid] = {"request_id": rid, "status": "QUEUED", "timestamp": _now(), "attachments": {name: {"size": len(data), "mime_type": "message/rfc822"}}, "metadata": {"source": "email"}, "result": None, "state": None, "erp_ref": None, "email_group_id": None, "order_label": None, "retry_count": 0, "next_retry_at": None, "last_error": None}
                st.files[rid] = {name: data}
            asyncio.create_task(process_later(rid, name, False))
            return {"request_id": rid, "split": False}
        gid = str(uuid.uuid4())
        children = []
        for n in (1, 2):
            cid = str(uuid.uuid4())
            with st.lock:
                st.orders[cid] = {"request_id": cid, "status": "QUEUED", "timestamp": _now(), "attachments": {name: {"size": len(data), "mime_type": "message/rfc822"}}, "metadata": {"source": "email"}, "result": None, "state": None, "erp_ref": None, "email_group_id": gid, "order_label": f"Order {n} of 2", "retry_count": 0, "next_retry_at": None, "last_error": None}
                st.files[cid] = {name: data}
            children.append({"request_id": cid, "status": "QUEUED", "order_label": f"Order {n} of 2"})
            asyncio.create_task(process_later(cid, f"PO-{4710 + n}.pdf", False))
        with st.lock:
            st.groups[gid] = {"email_group_id": gid, "message_id": None, "order_count": 2}
        return {"request_id": rid, "split": True, "email_group_id": gid, "orders": children}

    @app.post("/order/raw/classify", dependencies=[Depends(auth)])
    async def classify(file: UploadFile = File(...)) -> dict[str, str]:
        text = (await file.read()).decode("utf-8", "ignore").lower()
        return {"category": "quotation" if "quotation" in text or "offerte" in text else "purchase_order"}

    @app.get("/order_status/list", dependencies=[Depends(auth)])
    def list_orders(page: int = Query(1, ge=1), size: int = Query(100, ge=1, le=1000)) -> dict[str, Any]:
        with st.lock:
            items = sorted(st.orders.values(), key=lambda o: o["timestamp"], reverse=True)
        off = (page - 1) * size
        return {"items": items[off : off + size], "total": len(items), "limit": size, "offset": off}

    @app.get("/order_status/{request_id}", dependencies=[Depends(auth)])
    def get_order(request_id: str) -> dict[str, Any]:
        rid = uuid_or_400(request_id)
        o = st.orders.get(rid)
        if not o:
            raise HTTPException(404, f"Document with ID {rid} not found")
        return o

    @app.get("/order/group/{email_group_id}", dependencies=[Depends(auth)])
    def get_group(email_group_id: str) -> dict[str, Any]:
        gid = uuid_or_400(email_group_id)
        g = st.groups.get(gid)
        if not g:
            raise HTTPException(404, f"Email group {gid} not found")
        return {**g, "orders": [o for o in st.orders.values() if o["email_group_id"] == gid]}

    def order_or_404(request_id: str) -> dict[str, Any]:
        rid = uuid_or_400(request_id)
        o = st.orders.get(rid)
        if not o:
            raise HTTPException(404, f"Document with ID {rid} not found")
        return o

    def raw_email_of(o: dict[str, Any]) -> tuple[str, bytes] | None:
        """The original e-mail of an order that came from one (name and bytes), else None."""
        for name, blob in st.files.get(o["request_id"], {}).items():
            if o["attachments"].get(name, {}).get("mime_type") == "message/rfc822":
                return name, blob
        return None

    # Declared before the catch-all file route, so that the literal "files" and "email" segments match first.
    @app.get("/order/{request_id}/files", dependencies=[Depends(auth)])
    def list_files(request_id: str) -> list[dict[str, Any]]:
        """Uploaded files are stored with the order. The e-mail of an order that came from one is held by the mail
        intake in this mock (``id`` null); with ``mail_unavailable`` set those entries are missing from the list."""
        o = order_or_404(request_id)
        raw = raw_email_of(o)
        files = []
        for name, blob in st.files.get(o["request_id"], {}).items():
            if raw and name == raw[0]:
                continue
            files.append({"id": st.file_id(o["request_id"], name), "filename": name, "source": "upload", "content_type": o["attachments"][name]["mime_type"], "size_bytes": len(blob), "created_at": o["timestamp"]})
        if raw and not cfg["mail_unavailable"]:
            body_name, body_type, body = _email_body(raw[1])
            files.append({"id": None, "filename": raw[0], "source": "raw_email", "content_type": "message/rfc822", "size_bytes": len(raw[1]), "created_at": None})
            files.append({"id": None, "filename": body_name, "source": "email_body", "content_type": body_type, "size_bytes": len(body), "created_at": None})
        return files

    @app.get("/order/{request_id}/email/{source}", dependencies=[Depends(auth)])
    def get_email(request_id: str, source: str) -> Response:
        if source not in EMAIL_SOURCES:
            raise HTTPException(404, f"No email part called {source}")
        o = order_or_404(request_id)
        raw = raw_email_of(o)
        if raw is None:
            raise HTTPException(404, f"Order {o['request_id']} did not come from an email")
        if cfg["mail_unavailable"]:
            raise HTTPException(502, "The email could not be read right now; try again")
        headers = {"Content-Disposition": "attachment", "X-Content-Type-Options": "nosniff"}
        if source == "raw_email":
            return Response(raw[1], media_type="message/rfc822", headers=headers)
        _, body_type, body = _email_body(raw[1])
        return Response(body, media_type=body_type, headers=headers)

    @app.get("/order/{request_id}/{filename}/preview", dependencies=[Depends(auth)])
    @app.get("/order/{request_id}/{filename}", dependencies=[Depends(auth)])
    def get_file(request_id: str, filename: str) -> Response:
        rid = uuid_or_400(request_id)
        o = st.orders.get(rid)
        if not o:
            raise HTTPException(404, f"Document with ID {rid} not found")
        if filename == "latest_result.json":
            if not o["result"]:
                raise HTTPException(404, "No result yet")
            return Response(json.dumps(o["result"]), media_type="application/json")
        blob = st.files.get(rid, {}).get(filename)
        if blob is None:
            raise HTTPException(404, f"File {filename} not found")
        return Response(blob, media_type=o["attachments"][filename]["mime_type"])

    @app.post("/order/retry/{request_id}", dependencies=[Depends(auth)])
    async def retry(request_id: str) -> dict[str, Any]:
        rid = uuid_or_400(request_id)
        o = st.orders.get(rid)
        if not o:
            raise HTTPException(404, f"Document with ID {rid} not found")
        if o["status"] != "FAILED":
            raise HTTPException(400, f"Order {rid} is not in FAILED state (current: {o['status']})")
        new_id = str(uuid.uuid4())
        with st.lock:
            o["status"], o["timestamp"] = "REPROCESSED", _now()
            st.orders[new_id] = {**o, "request_id": new_id, "status": "QUEUED", "result": None, "erp_ref": None, "last_error": None, "timestamp": _now()}
            st.files[new_id] = st.files.get(rid, {})
        name = next(iter(o["attachments"]), "retry.pdf").replace("fail", "ok")
        asyncio.create_task(process_later(new_id, name, False))
        return {"request_id": new_id, "split": False}

    # ---- ERP send -------------------------------------------------------------------------
    @app.post("/erp/send/{request_id}", dependencies=[Depends(auth)])
    async def erp_send(request_id: str) -> dict[str, Any]:
        rid = uuid_or_400(request_id)
        if not cfg["erp_url"]:
            raise HTTPException(503, "ERP integration is not configured. Please set ERP_API_URL and ERP_API_KEY environment variables.")
        with st.lock:
            o = st.orders.get(rid)
            if not o:
                raise HTTPException(404, f"Order {rid} not found")
            if o["status"] not in SENDABLE:
                raise HTTPException(409, f"Order {rid} cannot be sent to ERP. Current status: {o['status']}. Must be one of: ATTENTION, MODIFIED, PROCESSED")
            previous = o["status"]
            o["status"], o["timestamp"] = "SENDING", _now()
            payload = _erp_payload(o)
        try:
            attempts = 1 + max(0, int(cfg["erp_retries"]))
            for attempt in range(1, attempts + 1):
                async with httpx.AsyncClient(timeout=30) as c:
                    r = await c.post(cfg["erp_url"], json=payload, headers={"X-API-KEY": cfg["erp_key"] or "", "Content-Type": "application/json"})
                try:
                    body = r.json()
                except ValueError:  # an HTML error page or an empty body: a failed send at any status
                    raise HTTPException(500, f"ERP server error for request {rid}")
                success = body.get("success") if isinstance(body, dict) else None
                if success is False:
                    # A refusal the endpoint marks retryable is sent again, with the same request_id.
                    if body.get("retryable") is True and attempt < attempts:
                        await asyncio.sleep(float(cfg["erp_retry_delay"]))
                        continue
                    raise HTTPException(422, f"ERP error: {body.get('error', 'unknown error')}")
                # Without a boolean success the status decides: only a 2xx counts as accepted.
                if success is not True and not 200 <= r.status_code < 300:
                    raise HTTPException(500, f"ERP server error for request {rid}")
                break
        except HTTPException:
            with st.lock:
                o["status"], o["timestamp"] = previous, _now()
            raise
        except Exception as exc:
            with st.lock:
                o["status"], o["timestamp"] = previous, _now()
            raise HTTPException(500, f"Unexpected error: {exc}")
        with st.lock:
            o["status"], o["erp_ref"], o["timestamp"] = "SENT", (body.get("order_number") if isinstance(body, dict) else None), _now()
        return {"success": True, "request_id": rid, "data": body}

    # ---- rejected --------------------------------------------------------------------------
    @app.get("/rejected/list", dependencies=[Depends(auth)])
    def rejected_list(page: int = 1, size: int = 100, status: str = "pending") -> dict[str, Any]:
        items = [r for r in st.rejected.values() if r["rejection_status"] == status]
        off = (page - 1) * size
        return {"items": items[off : off + size], "total": len(items), "limit": size, "offset": off}

    @app.get("/rejected/{request_id}", dependencies=[Depends(auth)])
    def rejected_get(request_id: str) -> dict[str, Any]:
        r = st.rejected.get(uuid_or_400(request_id))
        if not r:
            raise HTTPException(404, "No rejected email found")
        return r

    @app.post("/rejected/{request_id}/reprocess", dependencies=[Depends(auth)])
    async def rejected_reprocess(request_id: str) -> dict[str, Any]:
        rid = uuid_or_400(request_id)
        r = st.rejected.get(rid)
        if not r:
            raise HTTPException(404, "No rejected email found")
        if r["rejection_status"] == "overridden":
            raise HTTPException(409, "Email is already overridden")
        r["rejection_status"], r["original_email_type"], r["email_type"] = "overridden", r["email_type"], "purchase_order"
        with st.lock:
            st.orders[rid] = {"request_id": rid, "status": "QUEUED", "timestamp": _now(), "attachments": {}, "metadata": {"source": "email", "reprocessed": True}, "result": None, "state": None, "erp_ref": None, "email_group_id": None, "order_label": None, "retry_count": 0, "next_retry_at": None, "last_error": None}
        asyncio.create_task(process_later(rid, "PO-9001.pdf", False))
        return {"request_id": rid, "status": "reprocessing"}

    # ---- master data -------------------------------------------------------------------------
    def refused(exc: Refused) -> HTTPException:
        return HTTPException(422, [exc.entry])

    def customer_ids() -> dict[str, str]:
        return {c["id"]: c["number"] for c in st.customers.values()}

    def drop_mappings(where: Any) -> int:
        """Remove the mappings ``where`` selects: a customer or product that goes takes its mappings with it."""
        gone = [k for k, m in st.mappings.items() if where(m)]
        for k in gone:
            del st.mappings[k]
        return len(gone)

    async def bulk_rows(request: Request) -> list[dict[str, Any]]:
        """The rows of a bulk sync: a JSON array, gzip-compressed or not, with the refusals of the real API."""
        raw = await request.body()
        encoding = request.headers.get("content-encoding", "").strip().lower()
        if len(raw) > BULK_MAX_BYTES:
            raise HTTPException(413, "The body is larger than 40 MiB")
        if encoding == "gzip":
            try:
                raw = gzip.decompress(raw)
            except (OSError, EOFError):
                raise HTTPException(400, "Content-Encoding is gzip but the body is not valid gzip")
            if len(raw) > BULK_MAX_BYTES:
                raise HTTPException(413, "The body is larger than 40 MiB once decompressed")
        elif encoding:
            raise HTTPException(415, f"Unsupported Content-Encoding: {encoding}; send gzip or none")
        try:
            rows = json.loads(raw.decode("utf-8-sig"))
        except (UnicodeDecodeError, ValueError):
            rows = None
        if not isinstance(rows, list):
            raise HTTPException(422, [{"type": "list_type", "loc": ["body"], "msg": "The body must be a JSON array in UTF-8"}])
        if not rows:
            raise HTTPException(422, [{"type": "too_short", "loc": ["body"], "msg": "The body holds no rows; a bulk sync carries all of them. Remove records with DELETE."}])
        return rows

    def bulk_refused(problems: list[dict[str, Any]]) -> HTTPException:
        return HTTPException(422, problems[:BULK_MAX_REPORTED_ERRORS])

    def bulk_key(row: Any, position: int, fields: tuple[str, ...], problems: list[dict[str, Any]]) -> tuple[str, ...] | None:
        """The key of one bulk row, or None after recording why the row has none."""
        if not isinstance(row, dict):
            problems.append({"type": "dict_type", "loc": ["body", position], "msg": "Input should be an object"})
            return None
        key = []
        for field in fields:
            value = row.get(field)
            if not isinstance(value, str) or not value:
                problems.append({"type": "missing", "loc": ["body", position, field], "msg": "Field required"})
                return None
            key.append(value)
        return tuple(key)

    @app.get("/customer/list", dependencies=[Depends(auth)])
    def customers_list(page: int = Query(1, ge=1), size: int = Query(100, ge=1, le=1000), sort: str | None = None) -> dict[str, Any]:
        items = _sorted(list(st.customers.values()), sort)
        off = (page - 1) * size
        return {"items": items[off : off + size], "total": len(items), "limit": size, "offset": off}

    @app.get("/customer/search/{query}", dependencies=[Depends(auth)])
    def customers_search(query: str, top_k: int = 10) -> dict[str, Any]:
        q = query.lower()
        scored = []
        for c in st.customers.values():
            hay = " ".join(str(v) for v in c.values() if v).lower()
            score = sum(1 for tok in q.split() if tok in hay) / max(1, len(q.split()))
            if score > 0:
                scored.append({**c, "similarity": round(0.5 + score / 2, 2)})
        scored.sort(key=lambda c: -c["similarity"])
        return {"items": scored[:top_k], "total": len(st.customers), "limit": top_k}

    @app.post("/customer/bulk", dependencies=[Depends(auth)])
    async def customers_bulk(request: Request) -> dict[str, Any]:
        """The complete customer list: every row stored as the PUT would, every other customer removed with its mappings."""
        rows = await bulk_rows(request)
        async with bulk_lock:
            with st.lock:
                problems: list[dict[str, Any]] = []
                ids = customer_ids()
                new: dict[str, dict[str, Any]] = {}
                for position, row in enumerate(rows):
                    key = bulk_key(row, position, ("number",), problems)
                    if key is None:
                        continue
                    number = key[0]
                    if number in new:
                        problems.append({"type": "value_error", "loc": ["body", position, "number"], "msg": f"Customer {number} occurs twice"})
                        continue
                    try:
                        new[number] = _customer_record(number, row, st.customers.get(number), ids)
                    except Refused as exc:
                        problems.append({**exc.entry, "loc": ["body", position, *exc.entry["loc"][1:]]})
                    except IdTaken as exc:
                        problems.append({"type": "value_error", "loc": ["body", position, "id"], "msg": str(exc)})
                    else:
                        ids[new[number]["id"]] = number
                if problems:
                    raise bulk_refused(problems)
                result = {"received": len(rows), "inserted": 0, "updated": 0, "unchanged": 0}
                for number, rec in new.items():
                    old = st.customers.get(number)
                    result["inserted" if old is None else "updated" if any(old.get(f) != rec[f] for f in CUSTOMER_FIELDS) else "unchanged"] += 1
                removed = set(st.customers) - set(new)
                st.customers.clear()
                st.customers.update(new)
                result["deleted"] = len(removed)
                result["mappings_deleted"] = drop_mappings(lambda m: m["customer_number"] in removed)
                return result

    @app.get("/customer/{number}", dependencies=[Depends(auth)])
    def customer_get(number: str) -> dict[str, Any]:
        c = st.customers.get(number)
        if not c:
            raise HTTPException(404, f"Customer with unique number '{number}' not found")
        return c

    @app.put("/customer/{number}", status_code=202, dependencies=[Depends(auth)])
    def customer_put(number: str, body: dict[str, Any]) -> dict[str, Any]:
        with st.lock:
            try:
                rec = _customer_record(number, body, st.customers.get(number), customer_ids())
            except Refused as exc:
                raise refused(exc)
            except IdTaken as exc:
                raise HTTPException(409, str(exc))
            st.customers[number] = rec
        return rec

    @app.delete("/customer/{number}", status_code=204, dependencies=[Depends(auth)])
    def customer_delete(number: str) -> Response:
        with st.lock:
            if number not in st.customers:
                raise HTTPException(404, f"Customer with number '{number}' not found")
            del st.customers[number]
            drop_mappings(lambda m: m["customer_number"] == number)
        return Response(status_code=204)

    @app.get("/product/list", dependencies=[Depends(auth)])
    def products_list(page: int = Query(1, ge=1), size: int = Query(100, ge=1, le=1000), language_code: str | None = None, sort: str | None = None) -> dict[str, Any]:
        items = _sorted([p for p in st.products.values() if not language_code or p["language_code"] == language_code], sort)
        off = (page - 1) * size
        return {"items": items[off : off + size], "total": len(items), "limit": size, "offset": off}

    @app.post("/product/bulk", dependencies=[Depends(auth)])
    async def products_bulk(request: Request) -> dict[str, Any]:
        """The complete catalog, every language: every other product is removed with the mappings that point to it."""
        rows = await bulk_rows(request)
        async with bulk_lock:
            with st.lock:
                problems: list[dict[str, Any]] = []
                new: dict[tuple[str, str], dict[str, Any]] = {}
                for position, row in enumerate(rows):
                    key = bulk_key(row, position, ("item_number", "language_code"), problems)
                    if key is None:
                        continue
                    if key in new:
                        problems.append({"type": "value_error", "loc": ["body", position, "item_number"], "msg": f"Product {key[0]} ({key[1]}) occurs twice"})
                        continue
                    try:
                        new[key] = _product_record(key[0], key[1], row, st.products.get(key))
                    except Refused as exc:
                        problems.append({**exc.entry, "loc": ["body", position, *exc.entry["loc"][1:]]})
                if problems:
                    raise bulk_refused(problems)
                result = {"received": len(rows), "inserted": 0, "updated": 0, "unchanged": 0}
                for key, rec in new.items():
                    old = st.products.get(key)
                    result["inserted" if old is None else "updated" if (old["description"], old["remark"]) != (rec["description"], rec["remark"]) else "unchanged"] += 1
                removed = set(st.products) - set(new)
                st.products.clear()
                st.products.update(new)
                result["deleted"] = len(removed)
                result["mappings_deleted"] = drop_mappings(lambda m: (m["item_number"], m["language_code"]) in removed)
                return result

    # An item number may contain "/" (sent as %2F), so it is a path parameter that takes the rest of the path.
    @app.get("/product/{item_number:path}/{language_code}", dependencies=[Depends(auth)])
    def product_get(item_number: str, language_code: str) -> dict[str, Any]:
        p = st.products.get((item_number, language_code))
        if not p:
            raise HTTPException(404, f"Product with item number {item_number} and language code {language_code} not found")
        return p

    @app.put("/product/{item_number:path}/{language_code}", status_code=202, dependencies=[Depends(auth)])
    def product_put(item_number: str, language_code: str, body: dict[str, Any]) -> dict[str, Any]:
        with st.lock:
            try:
                rec = _product_record(item_number, language_code, body, st.products.get((item_number, language_code)))
            except Refused as exc:
                raise refused(exc)
            st.products[(item_number, language_code)] = rec
        return rec

    @app.delete("/product/{item_number:path}/{language_code}", status_code=204, dependencies=[Depends(auth)])
    def product_delete(item_number: str, language_code: str) -> Response:
        with st.lock:
            if (item_number, language_code) not in st.products:
                raise HTTPException(404, "Product not found")
            del st.products[(item_number, language_code)]
            drop_mappings(lambda m: (m["item_number"], m["language_code"]) == (item_number, language_code))
        return Response(status_code=204)

    @app.get("/customer-product/list", dependencies=[Depends(auth)])
    def mappings_list(page: int = Query(1, ge=1), size: int = Query(100, ge=1, le=1000), customer_number: str | None = None, customer_item_number: str | None = None, item_number: str | None = None, language_code: str | None = None, sort: str | None = None) -> dict[str, Any]:
        items = [m for m in st.mappings.values() if (not customer_number or m["customer_number"] == customer_number) and (not customer_item_number or m["customer_item_number"] == customer_item_number) and (not item_number or m["item_number"] == item_number) and (not language_code or m["language_code"] == language_code)]
        items = _sorted(items, sort)
        off = (page - 1) * size
        return {"items": items[off : off + size], "total": len(items), "limit": size, "offset": off}

    @app.post("/customer-product/bulk", dependencies=[Depends(auth)])
    async def mappings_bulk(request: Request) -> dict[str, Any]:
        """The complete mapping list. A row whose customer or product is unknown is skipped, not refused, and a
        mapping held under that row's key is removed with everything else the body did not store."""
        rows = await bulk_rows(request)
        async with bulk_lock:
            with st.lock:
                problems: list[dict[str, Any]] = []
                new: dict[tuple[str, str], dict[str, Any]] = {}
                skipped: list[dict[str, str]] = []
                seen: set[tuple[str, str]] = set()
                for position, row in enumerate(rows):
                    key = bulk_key(row, position, ("customer_number", "customer_item_number"), problems)
                    if key is None:
                        continue
                    if key in seen:
                        problems.append({"type": "value_error", "loc": ["body", position, "customer_item_number"], "msg": f"Mapping {key[0]} / {key[1]} occurs twice"})
                        continue
                    seen.add(key)
                    try:
                        rec = _mapping_record(key[0], key[1], row, st.mappings.get(key))
                    except Refused as exc:
                        problems.append({**exc.entry, "loc": ["body", position, *exc.entry["loc"][1:]]})
                        continue
                    if key[0] not in st.customers:
                        skipped.append({"customer_number": key[0], "customer_item_number": key[1], "reason": "customer_missing"})
                    elif (rec["item_number"], rec["language_code"]) not in st.products:
                        skipped.append({"customer_number": key[0], "customer_item_number": key[1], "reason": "product_missing"})
                    else:
                        new[key] = rec
                if problems:
                    raise bulk_refused(problems)
                result: dict[str, Any] = {"received": len(rows), "inserted": 0, "updated": 0, "unchanged": 0}
                for key, rec in new.items():
                    old = st.mappings.get(key)
                    result["inserted" if old is None else "updated" if (old["item_number"], old["language_code"]) != (rec["item_number"], rec["language_code"]) else "unchanged"] += 1
                result["deleted"] = len(set(st.mappings) - set(new))
                st.mappings.clear()
                st.mappings.update(new)
                result["skipped_total"] = len(skipped)
                result["skipped"] = skipped[:BULK_MAX_REPORTED_SKIPS]
                return result

    # A customer's own item number may contain "/" too.
    @app.get("/customer-product/{customer_number}/{customer_item_number:path}", dependencies=[Depends(auth)])
    def mapping_get(customer_number: str, customer_item_number: str) -> dict[str, Any]:
        m = st.mappings.get((customer_number, customer_item_number))
        if not m:
            raise HTTPException(404, f"Customer product mapping not found: customer={customer_number}, customer_item={customer_item_number}")
        return m

    @app.put("/customer-product/{customer_number}/{customer_item_number:path}", status_code=202, dependencies=[Depends(auth)])
    def mapping_put(customer_number: str, customer_item_number: str, body: dict[str, Any]) -> dict[str, Any]:
        with st.lock:
            try:
                rec = _mapping_record(customer_number, customer_item_number, body, st.mappings.get((customer_number, customer_item_number)))
            except Refused as exc:
                raise refused(exc)
            if customer_number not in st.customers:
                raise HTTPException(404, f"Customer {customer_number} does not exist")
            if (rec["item_number"], rec["language_code"]) not in st.products:
                raise HTTPException(404, f"Product number '{rec['item_number']}' with language '{rec['language_code']}' does not exist")
            st.mappings[(customer_number, customer_item_number)] = rec
        return rec

    @app.delete("/customer-product/{customer_number}/{customer_item_number:path}", status_code=204, dependencies=[Depends(auth)])
    def mapping_delete(customer_number: str, customer_item_number: str) -> Response:
        with st.lock:
            if (customer_number, customer_item_number) not in st.mappings:
                raise HTTPException(404, "Customer product mapping not found")
            del st.mappings[(customer_number, customer_item_number)]
        return Response(status_code=204)

    @app.post("/email-watcher/fetch-all", dependencies=[Depends(auth)])
    def fetch_all() -> dict[str, Any]:
        return {"status": "success", "emails_queued": 0, "emails_total": 0, "message": "Mock: nothing to fetch"}

    # ---- mock-only helpers (not part of the real API) -------------------------------------
    @app.post("/_mock/config")
    def mock_config(body: dict[str, Any]) -> dict[str, Any]:
        """Change the mock at runtime: ``erp_url``, ``erp_key``, ``webhook_url``, ``webhook_key``, ``erp_retries``,
        ``erp_retry_delay``, ``direct_upload`` (false: uploads answer 409) and ``mail_unavailable`` (true: the
        e-mail of an order cannot be read, as when the tenant's mail intake is unreachable)."""
        cfg.update({k: v for k, v in body.items() if k in cfg})
        return cfg

    @app.post("/_mock/orders/{request_id}/status")
    def mock_set_status(request_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Force a status (e.g. MODIFIED / CANCELED, which only the AIOTIC app can set today)."""
        o = st.orders.get(uuid_or_400(request_id))
        if not o:
            raise HTTPException(404, "not found")
        o["status"], o["timestamp"] = body["status"], _now()
        return o

    return app


_ = time
