"""Regression tests for the first-release review, service side: write endpoints fail closed without a configured
key, and the receive endpoint books one order per request_id, also under concurrent and repeated delivery."""

from __future__ import annotations

import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from aiotic.cli import app as cli
from aiotic.config import Settings
from aiotic.erp.data_api import DataApiAdapter, sqlite_demo
from aiotic.erp.functional_api import FunctionalApiAdapter
from aiotic.erp.memory import InMemoryErp
from aiotic.receive import ErpReceiver, InMemoryStore
from aiotic.service import build_app
from aiotic.sync import ChangeEvent, InMemoryStateStore, SyncEngine
from aiotic.webhooks import create_webhook_routers
from test_receive_and_pipeline import SAMPLE


class _Rows:
    """One master-data resource of a fake client: remembers rows and the order of calls."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, ...], dict] = {}
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def upsert(self, *key_and_data):
        *key, data = key_and_data
        self.calls.append(("upsert", tuple(key)))
        self.rows[tuple(key)] = data

    def delete(self, *key):
        self.calls.append(("delete", tuple(key)))
        self.rows.pop(tuple(key), None)


class FakeClient:
    def __init__(self) -> None:
        self.customers, self.products, self.customer_products = _Rows(), _Rows(), _Rows()


EVENT_DELETE_C1 = {"events": [{"kind": "customer", "op": "delete", "number": "C1"}]}


# ---- 2. no key, no write endpoint ------------------------------------------------------------------------------


def test_build_app_refuses_to_start_without_the_receive_key() -> None:
    with pytest.raises(ValueError, match="AIOTIC_ERP_RECEIVE_KEY"):
        build_app(settings=Settings(), store=InMemoryStore())
    with pytest.raises(ValueError, match="AIOTIC_ERP_RECEIVE_KEY"):
        build_app(settings=Settings(erp_receive_key=""), store=InMemoryStore())


def test_events_endpoint_rejects_missing_wrong_and_placeholder_keys_and_accepts_the_right_one() -> None:
    fake = FakeClient()
    engine = SyncEngine(fake, state=InMemoryStateStore())
    engine.apply(ChangeEvent.customer_upsert("C1", name="Example"))
    app = build_app(settings=Settings(erp_receive_key="receive-secret"), store=InMemoryStore(), sync_engine=engine)
    with TestClient(app) as http:
        for headers in ({}, {"X-API-KEY": "wrong"}, {"X-API-KEY": "change-me"}, {"X-API-KEY": ""}):
            assert http.post("/erp/events", json=EVENT_DELETE_C1, headers=headers).status_code == 401
        assert ("C1",) in fake.customers.rows and fake.customers.calls == [("upsert", ("C1",))]  # nothing was written
        assert http.post("/erp/events", json=EVENT_DELETE_C1, headers={"X-API-KEY": "receive-secret"}).status_code == 200
        assert ("C1",) not in fake.customers.rows


def test_events_endpoint_uses_its_own_key_when_configured() -> None:
    engine = SyncEngine(FakeClient(), state=InMemoryStateStore())
    app = build_app(settings=Settings(erp_receive_key="receive-secret", erp_events_key="events-secret"), store=InMemoryStore(), sync_engine=engine)
    with TestClient(app) as http:
        assert http.post("/erp/events", json={"events": []}, headers={"X-API-KEY": "receive-secret"}).status_code == 401
        assert http.post("/erp/events", json={"events": []}, headers={"X-API-KEY": "events-secret"}).status_code == 200
    app2 = build_app(settings=Settings(erp_receive_key="receive-secret"), store=InMemoryStore(), sync_engine=engine, erp_events_key="argument-wins")
    with TestClient(app2) as http:
        assert http.post("/erp/events", json={"events": []}, headers={"X-API-KEY": "argument-wins"}).status_code == 200


def test_events_endpoint_does_not_exist_without_a_sync_engine_and_the_router_refuses_an_empty_key() -> None:
    app = build_app(settings=Settings(erp_receive_key="k"), store=InMemoryStore())
    with TestClient(app) as http:
        assert http.post("/erp/events", json={"events": []}, headers={"X-API-KEY": "k"}).status_code == 404
    for key in (None, ""):
        with pytest.raises(ValueError, match="erp_events_key"):
            create_webhook_routers(on_change_events=lambda events: None, erp_events_key=key)


def test_receiver_only_accepts_the_configured_key() -> None:
    app = build_app(settings=Settings(erp_receive_key="real-key"), store=InMemoryStore())
    receiver = app.state.receiver
    assert receiver.verify_key("real-key") and not receiver.verify_key("change-me") and not receiver.verify_key(None)
    with pytest.raises(ValueError):
        ErpReceiver(InMemoryErp(), api_key="")


def test_the_init_path_still_produces_a_working_key(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    for name in [k for k in os.environ if k.startswith("AIOTIC_")]:
        monkeypatch.delenv(name)
    result = CliRunner().invoke(cli, ["init", "--base-url", "http://127.0.0.1:1", "--api-key", "test-only"])
    assert result.exit_code == 0, result.output
    env = dict(line.split("=", 1) for line in (tmp_path / ".env").read_text().splitlines() if "=" in line and not line.startswith("#"))
    assert len(env["AIOTIC_ERP_RECEIVE_KEY"]) >= 24
    app = build_app(store=InMemoryStore())  # Settings.from_env() reads the .env that init wrote
    assert app.state.receiver.verify_key(env["AIOTIC_ERP_RECEIVE_KEY"])


# ---- 7. one request_id, one sales order ------------------------------------------------------------------------


class FakeFunctionalErp:
    """A functional ERP behind httpx.MockTransport: GET /salesOrders?externalReference=, POST /salesOrders.

    ``unique`` makes the external reference unique (409 on a duplicate), which is the capability the adapter
    requires. ``hold_lookups`` lets a test park the first N lookups until all of them arrived, so every delivery
    misses the idempotency check before anyone creates. ``lose_first_create_response`` books the order but
    answers the first create with a transport error.
    """

    def __init__(self, *, unique: bool = True, hold_lookups: int = 0, lose_first_create_response: bool = False) -> None:
        self.booked: dict[str, dict] = {}
        self.unique = unique
        self.lose_first_create_response = lose_first_create_response
        self._lock = threading.Lock()
        self._lookups = 0
        self._barrier = threading.Barrier(hold_lookups) if hold_lookups else None
        self._creates = 0
        self.transport = httpx.MockTransport(self)
        self.http = httpx.Client(base_url="https://erp.invalid", transport=self.transport)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            with self._lock:
                self._lookups += 1
                park = self._barrier is not None and self._lookups <= self._barrier.parties
            if park:
                self._barrier.wait(timeout=5)
            ref = request.url.params["externalReference"]
            rows = [{"number": n} for n, b in self.booked.items() if b["externalReference"] == ref]
            return httpx.Response(200, json={"value": rows})
        body = json.loads(request.content)
        with self._lock:
            if self.unique and any(b["externalReference"] == body["externalReference"] for b in self.booked.values()):
                return httpx.Response(409, json={"error": {"message": "duplicate external reference"}})
            number = f"SO-{len(self.booked) + 1}"
            self.booked[number] = body
            self._creates += 1
            lost = self.lose_first_create_response and self._creates == 1
        if lost:
            raise httpx.ReadTimeout("ERP booked the order but the response was lost", request=request)
        return httpx.Response(201, json={"number": number})

    def adapter(self) -> FunctionalApiAdapter:
        return FunctionalApiAdapter("https://erp.invalid", "test-only", client=self.http)


def _body() -> dict:
    return {"request_id": str(uuid.uuid4()), "purchase_order": SAMPLE}


def test_concurrent_duplicate_delivery_to_one_receiver_books_once() -> None:
    erp = FakeFunctionalErp()
    receiver = ErpReceiver(erp.adapter(), api_key="k", store=InMemoryStore())
    body = _body()
    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(lambda _: receiver.handle(body, api_key_header="k"), range(4)))
    assert len(erp.booked) == 1
    assert all(o.response.success for o in outcomes) and {o.response.order_number for o in outcomes} == {"SO-1"}
    assert sorted(o.result.created for o in outcomes) == [False, False, False, True]


def test_two_service_instances_against_an_idempotent_erp_book_once() -> None:
    erp = FakeFunctionalErp(hold_lookups=2)  # both instances miss the lookup before either creates
    receivers = [ErpReceiver(erp.adapter(), api_key="k", store=InMemoryStore()) for _ in range(2)]
    body = _body()
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda r: r.handle(body, api_key_header="k"), receivers))
    assert len(erp.booked) == 1
    assert all(o.response.success for o in outcomes) and {o.response.order_number for o in outcomes} == {"SO-1"}


def test_replay_after_the_store_was_lost_finds_the_order_in_the_erp() -> None:
    erp = FakeFunctionalErp()
    body = _body()
    first = ErpReceiver(erp.adapter(), api_key="k", store=InMemoryStore()).handle(body, api_key_header="k")
    fresh = ErpReceiver(erp.adapter(), api_key="k", store=InMemoryStore())  # a restarted instance with an empty store
    again = fresh.handle(body, api_key_header="k")
    assert first.response.order_number == again.response.order_number == "SO-1" and again.result.created is False
    assert len(erp.booked) == 1


def test_an_accepted_create_whose_response_was_lost_is_recovered_on_retry() -> None:
    erp = FakeFunctionalErp(lose_first_create_response=True)
    receiver = ErpReceiver(erp.adapter(), api_key="k", store=InMemoryStore())
    body = _body()
    lost = receiver.handle(body, api_key_header="k")
    assert lost.response.success is False and lost.http_status == 500  # AIOTIC rolls back and the operator retries
    retry = receiver.handle(body, api_key_header="k")
    assert retry.response.success and retry.response.order_number == "SO-1" and retry.result.created is False
    assert len(erp.booked) == 1


def test_data_api_adapter_resolves_a_unique_conflict_to_the_existing_order(tmp_path) -> None:
    connect = sqlite_demo(str(tmp_path / "erp.db"))
    rid = str(uuid.uuid4())
    from aiotic.models import ErpPurchaseOrder

    order = ErpPurchaseOrder.model_validate(SAMPLE)
    first = DataApiAdapter(connect).create_sales_order(rid, order)

    class BlindAdapter(DataApiAdapter):
        """Its in-transaction lookup never sees the row, as if another instance inserted it a moment ago."""

        SQL = {**DataApiAdapter.SQL, "find_by_request": "SELECT order_no FROM sales_orders WHERE external_ref = ? AND 0"}

        def find_order_by_request_id(self, request_id: str):
            return DataApiAdapter(self.connect).find_order_by_request_id(request_id)

    second = BlindAdapter(connect).create_sales_order(rid, order)
    assert second.order_number == first.order_number and second.created is False
    with connect() as c:
        assert c.execute("SELECT COUNT(*) FROM sales_orders WHERE external_ref = ?", (rid,)).fetchone()[0] == 1
        assert c.execute("SELECT COUNT(*) FROM sales_order_lines").fetchone()[0] == len(SAMPLE["items"])


def test_functional_adapter_409_without_an_existing_order_is_a_business_rejection() -> None:
    from aiotic.erp.ports import ErpRejected
    from aiotic.models import ErpPurchaseOrder

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"value": []})
        return httpx.Response(409, json={"error": {"message": "credit limit exceeded"}})

    adapter = FunctionalApiAdapter("https://erp.invalid", "t", client=httpx.Client(base_url="https://erp.invalid", transport=httpx.MockTransport(handler)))
    with pytest.raises(ErpRejected, match="credit limit"):
        adapter.create_sales_order(str(uuid.uuid4()), ErpPurchaseOrder.model_validate(SAMPLE))
