"""FastAPI implementation of the mock AIOTIC tenant (see package docstring)."""

from __future__ import annotations

import asyncio
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
        self.seed()

    def seed(self) -> None:
        self.customers["58931"] = {"number": "58931", "id": str(uuid.uuid4()), "name": "LUMITECH INSTALLATIES", "postal_code": "7327 AA", "city": "Apeldoorn", "address": "Ambachtsweg 12", "contact_person": "J. de Boer", "phone_number": "+31 55 123 4567", "vat_number": "NL001234567B01", "email": "info@lumitech.example", "coc_number": None, "home_page": None, "similarity": None}
        for item, desc in (("PROD-001", "LED Driver 48V 100W"), ("PROD-002", "LED Panel 60x60 40W"), ("620206_01", "Cable 3x1.5 mm² (100 m)")):
            self.products[(item, "nl")] = {"item_number": item, "language_code": "nl", "description": desc, "remark": None, "created_at": _now()}
        self.mappings[("58931", "LT-ART-001")] = {"customer_number": "58931", "customer_item_number": "LT-ART-001", "item_number": "PROD-001", "language_code": "nl", "created_at": _now()}


def _sample_result(order_number: str, unknown_article: bool, filename: str) -> dict[str, Any]:
    items = [
        {"article_number": "PROD-001", "customer_item_number": "LT-ART-001", "description": "LED Driver 48V 100W", "quantity": 10, "quantity_state": "Valid", "unit": "ST", "price": 12.34, "currency": "EUR", "line_total": 123.4},
        {"article_number": "620206_01", "customer_item_number": None, "description": "Cable 3x1.5 mm² (100 m)", "quantity": 2, "quantity_state": "Valid", "unit": "ROL", "price": 45.0, "currency": "EUR", "line_total": 90.0},
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


def create_mock_app(*, erp_url: str | None = None, erp_key: str | None = None, webhook_url: str | None = None, webhook_key: str | None = None, processing_seconds: float = 3.0, state: MockState | None = None) -> FastAPI:
    st = state or MockState()
    cfg = {"erp_url": erp_url, "erp_key": erp_key, "webhook_url": webhook_url, "webhook_key": webhook_key, "speed": processing_seconds}
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
        form = await request.form()
        metadata = {k: v for k, v in form.items() if k not in {"files", "request_id"} and isinstance(v, str)}
        rid = uuid_or_400(request_id) if request_id else str(uuid.uuid4())
        if rid in st.orders:
            return {"request_id": rid, "split": False}
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
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.post(cfg["erp_url"], json=payload, headers={"X-API-KEY": cfg["erp_key"] or "", "Content-Type": "application/json"})
            try:
                body = r.json()
            except ValueError:
                raise HTTPException(500, f"ERP server error for request {rid}")
            if isinstance(body, dict) and body.get("success") is False:
                raise HTTPException(422, f"ERP error: {body.get('error', 'unknown error')}")
        except HTTPException:
            with st.lock:
                o["status"], o["timestamp"] = previous, _now()
            raise
        except Exception as exc:
            with st.lock:
                o["status"], o["timestamp"] = previous, _now()
            raise HTTPException(500, f"Unexpected error: {exc}")
        with st.lock:
            o["status"], o["erp_ref"], o["timestamp"] = "SENT", body.get("order_number"), _now()
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
    @app.get("/customer/list", dependencies=[Depends(auth)])
    def customers_list(page: int = Query(1, ge=1), size: int = Query(100, ge=1, le=1000)) -> dict[str, Any]:
        items = list(st.customers.values())
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

    @app.get("/customer/{number}", dependencies=[Depends(auth)])
    def customer_get(number: str) -> dict[str, Any]:
        c = st.customers.get(number)
        if not c:
            raise HTTPException(404, f"Customer with unique number '{number}' not found")
        return c

    @app.put("/customer/{number}", status_code=202, dependencies=[Depends(auth)])
    def customer_put(number: str, body: dict[str, Any]) -> dict[str, Any]:
        with st.lock:
            existing = st.customers.get(number, {})
            rec = {"number": number, "id": body.get("id") or existing.get("id") or str(uuid.uuid4()), "similarity": None}
            for k in ("name", "postal_code", "city", "address", "contact_person", "phone_number", "vat_number", "email", "coc_number", "home_page"):
                rec[k] = body.get(k)
            st.customers[number] = rec
        return rec

    @app.delete("/customer/{number}", status_code=204, dependencies=[Depends(auth)])
    def customer_delete(number: str) -> Response:
        with st.lock:
            if number not in st.customers:
                raise HTTPException(404, f"Customer with number '{number}' not found")
            del st.customers[number]
        return Response(status_code=204)

    @app.get("/product/list", dependencies=[Depends(auth)])
    def products_list(page: int = Query(1, ge=1), size: int = Query(100, ge=1, le=1000), language_code: str | None = None) -> dict[str, Any]:
        items = [p for p in st.products.values() if not language_code or p["language_code"] == language_code]
        off = (page - 1) * size
        return {"items": items[off : off + size], "total": len(items), "limit": size, "offset": off}

    @app.get("/product/{item_number}/{language_code}", dependencies=[Depends(auth)])
    def product_get(item_number: str, language_code: str) -> dict[str, Any]:
        p = st.products.get((item_number, language_code))
        if not p:
            raise HTTPException(404, f"Product with item number {item_number} and language code {language_code} not found")
        return p

    @app.put("/product/{item_number}/{language_code}", status_code=202, dependencies=[Depends(auth)])
    def product_put(item_number: str, language_code: str, body: dict[str, Any]) -> dict[str, Any]:
        if "description" not in body:
            raise HTTPException(422, "description is required")
        with st.lock:
            rec = {"item_number": item_number, "language_code": language_code, "description": body["description"], "remark": body.get("remark"), "created_at": st.products.get((item_number, language_code), {}).get("created_at") or _now()}
            st.products[(item_number, language_code)] = rec
        return rec

    @app.delete("/product/{item_number}/{language_code}", status_code=204, dependencies=[Depends(auth)])
    def product_delete(item_number: str, language_code: str) -> Response:
        with st.lock:
            if (item_number, language_code) not in st.products:
                raise HTTPException(404, "Product not found")
            del st.products[(item_number, language_code)]
        return Response(status_code=204)

    @app.get("/customer-product/list", dependencies=[Depends(auth)])
    def mappings_list(page: int = Query(1, ge=1), size: int = Query(100, ge=1, le=1000), customer_number: str | None = None, customer_item_number: str | None = None, item_number: str | None = None, language_code: str | None = None) -> dict[str, Any]:
        items = [m for m in st.mappings.values() if (not customer_number or m["customer_number"] == customer_number) and (not customer_item_number or m["customer_item_number"] == customer_item_number) and (not item_number or m["item_number"] == item_number) and (not language_code or m["language_code"] == language_code)]
        off = (page - 1) * size
        return {"items": items[off : off + size], "total": len(items), "limit": size, "offset": off}

    @app.get("/customer-product/{customer_number}/{customer_item_number}", dependencies=[Depends(auth)])
    def mapping_get(customer_number: str, customer_item_number: str) -> dict[str, Any]:
        m = st.mappings.get((customer_number, customer_item_number))
        if not m:
            raise HTTPException(404, f"Customer product mapping not found: customer={customer_number}, customer_item={customer_item_number}")
        return m

    @app.put("/customer-product/{customer_number}/{customer_item_number}", status_code=202, dependencies=[Depends(auth)])
    def mapping_put(customer_number: str, customer_item_number: str, body: dict[str, Any]) -> dict[str, Any]:
        if customer_number not in st.customers:
            raise HTTPException(404, f"Customer '{customer_number}' does not exist")
        key = (str(body.get("item_number")), str(body.get("language_code")))
        if key not in st.products:
            raise HTTPException(404, f"Product '{key[0]}' ({key[1]}) does not exist")
        with st.lock:
            rec = {"customer_number": customer_number, "customer_item_number": customer_item_number, "item_number": key[0], "language_code": key[1], "created_at": st.mappings.get((customer_number, customer_item_number), {}).get("created_at") or _now()}
            st.mappings[(customer_number, customer_item_number)] = rec
        return rec

    @app.delete("/customer-product/{customer_number}/{customer_item_number}", status_code=204, dependencies=[Depends(auth)])
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
        """Set ERP / webhook targets at runtime: {"erp_url": ..., "erp_key": ..., "webhook_url": ..., "webhook_key": ...}."""
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
