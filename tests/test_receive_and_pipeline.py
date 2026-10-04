"""End-to-end: mock AIOTIC → /erp/send → our receive endpoint (pipeline + ERP adapter) → back."""

from __future__ import annotations

import asyncio
import json
import threading
import uuid

import httpx
import pytest
import uvicorn
from fastapi import FastAPI

from aiotic import AioticClient, AioticErpRejectedError
from aiotic.erp.data_api import DataApiAdapter, sqlite_demo
from aiotic.erp.memory import InMemoryErp
from aiotic.models import ErpPurchaseOrder, ErpReceiveRequest
from aiotic.pipeline import Pipeline, Severity, rules as R, sanitizers as S, validators as V
from aiotic.receive import ErpReceiver, InMemoryStore, create_receive_router
from aiotic.service import default_pipeline

SAMPLE = {
    "order_number": "EB25/001",
    "order_date": "2026-01-14",
    "delivery_date": "2026-02-01",
    "currency": "€",
    "total_price": 213.4,
    "additional_information": None,
    "supplier": None,
    "customer": {"customer_id": "58931", "company": "  LUMITECH INSTALLATIES ", "contact_person": None, "email": None, "phone": None, "iban": None, "bic": None, "vat_id": None, "address": {"street": "Ambachtsweg 12", "postal_code": "7327aa", "city": "Apeldoorn", "country": "Nederland"}},
    "shipping_details": {"recipient": {"company": None, "department": None, "contact_person": None, "email": None, "phone": None, "address": {"street": None, "postal_code": None, "city": None, "country": None}}, "special_instructions": None},
    "items": [
        {"article_number": "PROD-001", "description": "LED Driver", "quantity": 10, "unit": "stuks", "price": 12.34, "currency": None, "line_total": 123.4},
        {"article_number": "620206_01", "description": "Cable", "quantity": 2, "unit": "rol", "price": 45.0, "currency": None, "line_total": 90.0},
    ],
}


def test_sanitizers_normalise() -> None:
    o = ErpPurchaseOrder.model_validate(SAMPLE)
    p = Pipeline(sanitizers=[S.StripWhitespace(), S.NormalizeCountryCodes(), S.NormalizeCurrency(), S.NormalizePostalCodes(), S.MapUnits(), S.FillShippingFromCustomer(), S.NormalizeOrderNumber()])
    v = p.run(o)
    assert v.ok
    assert v.order.customer.company == "LUMITECH INSTALLATIES"
    assert v.order.customer.address.country == "NL"
    assert v.order.customer.address.postal_code == "7327 AA"
    assert v.order.currency == "EUR" and v.order.items[0].currency == "EUR"
    assert [i.unit for i in v.order.items] == ["ST", "ROL"]
    assert v.order.shipping_details.recipient.company == "LUMITECH INSTALLATIES"
    assert v.order.order_number == "EB25-001"


def test_validators_reject_unknown_article_and_blocked_customer() -> None:
    erp = InMemoryErp(products={"PROD-001"}, blocked={"58931"})
    p = Pipeline(validators=[V.RequiredFields(), V.ArticlesInCatalog(erp), V.PositiveQuantities()], rules=[R.CustomerNotBlocked(erp)])
    v = p.run(ErpPurchaseOrder.model_validate(SAMPLE))
    codes = {i.code for i in v.errors}
    assert codes == {"unknown_article", "customer_blocked"}
    assert "620206_01" in v.error_message()


def test_data_api_adapter_with_pipeline() -> None:
    connect = sqlite_demo()
    erp = DataApiAdapter(connect)
    store = InMemoryStore()
    receiver = ErpReceiver(erp, api_key="k", pipeline=default_pipeline(erp, store), store=store)
    rid = str(uuid.uuid4())
    body = {"request_id": rid, "purchase_order": SAMPLE}
    out = receiver.handle(json.dumps(body), api_key_header="k")
    assert out.response.success and out.response.order_number.startswith("SO-")
    # idempotent replay → same number, nothing new written
    again = receiver.handle(body, api_key_header="k")
    assert again.response.order_number == out.response.order_number and again.result.created is False
    # duplicate customer PO with a NEW request id → business rejection
    dup = receiver.handle({"request_id": str(uuid.uuid4()), "purchase_order": SAMPLE}, api_key_header="k")
    assert dup.response.success is False and "already booked" in dup.response.error
    # wrong key → 401 with success:false
    assert receiver.handle(body, api_key_header="nope").http_status == 401
    with connect() as c:
        assert c.execute("SELECT COUNT(*) FROM sales_order_lines").fetchone()[0] == 2


def test_blocked_customer_in_data_api() -> None:
    erp = DataApiAdapter(sqlite_demo())
    receiver = ErpReceiver(erp, api_key="k", pipeline=default_pipeline(erp, InMemoryStore()))
    sample = {**SAMPLE, "customer": {**SAMPLE["customer"], "customer_id": "10577"}}
    out = receiver.handle({"request_id": str(uuid.uuid4()), "purchase_order": sample}, api_key_header="k")
    assert not out.response.success and "blocked" in out.response.error


def _run_receive_service(receiver: ErpReceiver) -> tuple[str, uvicorn.Server]:
    app = FastAPI()
    app.include_router(create_receive_router(receiver))
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning"))
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        asyncio.run(asyncio.sleep(0.05))
    port = server.servers[0].sockets[0].getsockname()[1]
    return f"http://127.0.0.1:{port}", server


def test_end_to_end_send(client: AioticClient, http: httpx.Client) -> None:
    erp = InMemoryErp()
    receiver = ErpReceiver(erp, api_key="erp-secret", pipeline=default_pipeline(erp, InMemoryStore()))
    url, server = _run_receive_service(receiver)
    try:
        http.post("/_mock/config", json={"erp_url": f"{url}/aiotic/orders", "erp_key": "erp-secret"})
        up = client.orders.upload([("PO-8001.pdf", b"x")])
        st = client.orders.wait(up.request_id, timeout=10, initial_interval=0.1)
        assert st.status.is_sendable
        res = client.erp.send(up.request_id)
        assert res.success and res.erp_order_number.startswith("SO-")
        assert client.orders.get(up.request_id).erp_ref == res.erp_order_number
        # ATTENTION order with an unknown article: the ERP (functional style) rejects it → 422 + rollback
        up2 = client.orders.upload([("PO-8002-attention.pdf", b"x")])
        client.orders.wait(up2.request_id, timeout=10, initial_interval=0.1)
        with pytest.raises(AioticErpRejectedError) as exc:
            client.erp.send(up2.request_id)
        assert exc.value.erp_error
        assert client.orders.get(up2.request_id).status.value == "ATTENTION"
    finally:
        http.post("/_mock/config", json={"erp_url": None})
        server.should_exit = True


def test_receive_request_model_roundtrip() -> None:
    req = ErpReceiveRequest.model_validate({"request_id": str(uuid.uuid4()), "purchase_order": SAMPLE})
    assert req.purchase_order.items[1].article_number == "620206_01"
    v = Pipeline(validators=[V.LineTotalsConsistent(severity=Severity.ERROR)]).run(req.purchase_order)
    assert v.ok
