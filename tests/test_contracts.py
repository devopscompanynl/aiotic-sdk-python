"""Contracts beyond the bulk sync: the moment a record last changed in the source and the sync that continues from
it, records the API refuses, a slash in item numbers, an order's files and e-mail, uploads that are not idempotent,
and what counts as an accepted send to the ERP. Every test gets its own mock tenant."""

from __future__ import annotations

import asyncio
import json
import threading
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
import uvicorn
from pydantic import ValidationError

from aiotic import AioticClient, AioticConflictError, AioticErpRejectedError, AioticNotFoundError, AioticServerError, AioticValidationError, AsyncAioticClient
from aiotic.erp import ErpCreateResult, ErpRejected, ErpTemporaryRefusal
from aiotic.mock import create_mock_app
from aiotic.models import Customer, CustomerUpsert, ErpReceiveResponse, OrderStatusValue, PurchaseOrder
from aiotic.receive import ErpReceiver
from aiotic.sync import ChangeEvent as E, ChangeKind, CursorChangeSource, SyncEngine

UTC = timezone.utc
ORDER_MAIL = b"From: buyer@lumitech.example\r\nTo: orders@acme.example\r\nSubject: Order PO-4711\r\nDate: Wed, 18 Nov 2026 09:00:00 +0000\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nPlease deliver 10 x PROD-001.\r\n"


class _Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:  # pragma: no cover
        pass


@pytest.fixture
def tenant() -> Iterator[str]:
    server = _Server(uvicorn.Config(create_mock_app(processing_seconds=0.2, erp_retry_delay=0.01), host="127.0.0.1", port=0, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        asyncio.run(asyncio.sleep(0.02))
    yield f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def client(tenant: str) -> Iterator[AioticClient]:
    with AioticClient(base_url=tenant, api_key="mock-integration-key", sync_api_key="mock-sync-key", rate_limit=0, max_retries=1) as c:
        yield c


@pytest.fixture
def http(tenant: str) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=tenant, headers={"X-API-Key": "mock-integration-key"}) as h:
        yield h


# ---- source_updated_at: stored in UTC, cleared when left out, and the one sort ------------------------------------


def test_source_updated_at_is_returned_in_utc_and_a_put_without_it_clears_it(client: AioticClient) -> None:
    stored = client.customers.upsert("58931", CustomerUpsert(name="LUMITECH INSTALLATIES", source_updated_at="2026-09-10T12:00:07+02:00"))
    assert stored.source_updated_at == datetime(2026, 9, 10, 10, 0, 7, tzinfo=UTC)
    assert client.customers.upsert("58931", {"name": "LUMITECH INSTALLATIES"}).source_updated_at is None  # a PUT replaces the record


def test_a_moment_without_a_zone_is_refused_by_the_models_and_by_the_api(client: AioticClient) -> None:
    with pytest.raises(ValidationError):
        CustomerUpsert(name="x", source_updated_at=datetime(2026, 9, 10, 12, 0, 7))
    with pytest.raises(ValidationError):
        E.product_upsert("PROD-001", "nl", description="x", source_updated_at="2026-09-10T12:00:07")
    with pytest.raises(AioticValidationError) as refused:
        client.products.upsert("PROD-001", "nl", {"description": "x", "source_updated_at": "2026-09-10T12:00:07"})
    assert refused.value.status == 422 and refused.value.detail[0]["loc"] == ["body", "source_updated_at"]


def test_the_newest_source_change_comes_first_and_records_without_one_last(client: AioticClient, http: httpx.Client) -> None:
    assert client.products.latest_source_update() is None  # nothing carries a moment yet
    client.products.upsert("PROD-001", "nl", {"description": "a", "source_updated_at": "2026-11-10T08:00:00+00:00"})
    client.products.upsert("PROD-002", "nl", {"description": "b", "source_updated_at": "2026-11-12T08:00:00+01:00"})
    listed = client.products.list(sort="-source_updated_at").items
    assert [p.item_number for p in listed] == ["PROD-002", "PROD-001", "CBL-3015"]
    assert client.products.latest_source_update() == datetime(2026, 11, 12, 7, 0, 0, tzinfo=UTC)
    assert client.products.latest_source_update(language_code="de") is None
    assert client.customers.latest_source_update() is None and client.customer_products.latest_source_update() is None
    assert http.get("/product/list", params={"sort": "name"}).status_code == 422  # any other sort is refused


# ---- a sync that continues from the cursor AIOTIC holds --------------------------------------------------------------


def _erp_changes(rows: list[tuple[str, datetime, str]]):
    """A stand-in for ``SELECT ... WHERE updated_at >= ? ORDER BY updated_at`` over product rows (item, moment, text)."""
    asked: list[datetime | None] = []

    def fetch(since: datetime | None):
        asked.append(since)
        return [E.product_upsert(item, "nl", description=text, source_updated_at=moment) for item, moment, text in sorted(rows, key=lambda r: r[1]) if since is None or moment >= since]

    return asked, fetch


def test_the_cursor_is_read_from_aiotic_and_the_restart_is_inclusive_with_a_margin(client: AioticClient) -> None:
    monday = datetime(2026, 11, 16, 9, 0, tzinfo=UTC)
    rows = [("PROD-001", monday, "one"), ("PROD-002", monday, "two"), ("CBL-3015", monday + timedelta(hours=1), "three")]
    asked, fetch = _erp_changes(rows)
    source = CursorChangeSource("product", fetch, SyncEngine(client))

    first = source.run_once()
    assert asked == [None] and first.sent == 3 and source.cursor() == monday + timedelta(hours=1)

    rows.append(("PROD-003", monday + timedelta(hours=2), "four"))
    second = SyncEngine(client)  # a new process: no local state at all
    again = CursorChangeSource(ChangeKind.PRODUCT, fetch, second).run_once()
    assert asked[-1] == monday + timedelta(hours=1) - timedelta(seconds=1)  # at or after the cursor, minus the margin
    assert again.sent == 2 and again.failed == 0  # the record at the cursor is sent again, which is harmless
    assert source.cursor() == monday + timedelta(hours=2)


def test_a_failed_change_stops_the_run_so_the_cursor_cannot_pass_it(client: AioticClient) -> None:
    tuesday, wednesday = datetime(2026, 11, 17, 9, 0, tzinfo=UTC), datetime(2026, 11, 18, 9, 0, tzinfo=UTC)

    def fetch(since: datetime | None):
        events = [
            E.mapping_upsert("58931", "LT-ART-002", item_number="NOT-YET-SENT", language_code="nl", source_updated_at=tuesday),  # its product is unknown
            E.mapping_upsert("58931", "LT-ART-003", item_number="PROD-002", language_code="nl", source_updated_at=wednesday),
        ]
        return [e for e in events if since is None or e.source_updated_at >= since]

    source = CursorChangeSource("mapping", fetch, SyncEngine(client))
    report = source.run_once()
    assert (report.sent, report.failed, report.not_attempted) == (0, 1, 1)
    assert source.cursor() is None  # Wednesday's change was not sent, so the next run starts before Tuesday's

    client.products.upsert("NOT-YET-SENT", "nl", {"description": "now it exists"})  # the parent arrives
    report = source.run_once()
    assert (report.sent, report.failed) == (2, 0) and source.cursor() == wednesday


def test_a_cursor_sync_refuses_changes_it_cannot_order(client: AioticClient) -> None:
    engine = SyncEngine(client)
    newer, older = datetime(2026, 11, 18, tzinfo=UTC), datetime(2026, 11, 17, tzinfo=UTC)
    with pytest.raises(ValueError, match="needs source_updated_at"):
        CursorChangeSource("product", lambda since: [E.product_upsert("PROD-001", "nl", description="x")], engine).run_once()
    with pytest.raises(ValueError, match="oldest first"):
        CursorChangeSource("product", lambda since: [E.product_upsert("PROD-001", "nl", description="x", source_updated_at=newer), E.product_upsert("PROD-002", "nl", description="y", source_updated_at=older)], engine).run_once()
    with pytest.raises(ValueError, match="one source per data set"):
        CursorChangeSource("product", lambda since: [E.customer_upsert("58931", name="x", source_updated_at=newer)], engine).run_once()


def test_the_same_moment_gives_the_same_fingerprint_whatever_the_offset() -> None:
    a = E.product_upsert("PROD-001", "nl", description="x", source_updated_at="2026-09-10T12:00:07+02:00")
    b = E.product_upsert("PROD-001", "nl", description="x", source_updated_at=datetime(2026, 9, 10, 10, 0, 7, tzinfo=UTC))
    assert a.fingerprint == b.fingerprint and a.source_updated_at == b.source_updated_at
    # An event without a moment keeps the body, and so the fingerprint, it always had.
    assert E.mapping_upsert("58931", "LT-ART-001", item_number="PROD-001", language_code="nl").data == {"item_number": "PROD-001", "language_code": "nl"}


# ---- records the API refuses ----------------------------------------------------------------------------------------


def test_a_customer_needs_a_name_that_is_not_blank(client: AioticClient) -> None:
    for blank in ({}, {"name": ""}, {"name": "   "}):
        with pytest.raises(ValidationError):
            CustomerUpsert(**blank)
        with pytest.raises(AioticValidationError) as refused:
            client.customers.upsert("C-1", blank)
        assert refused.value.status == 422 and refused.value.detail[0]["loc"] == ["body", "name"]
    with pytest.raises(AioticNotFoundError):
        client.customers.get("C-1")  # nothing was written
    assert Customer.model_validate({"number": "C-0", "name": None}).name is None  # a record stored without one still reads


def test_a_new_customer_with_the_id_of_another_is_a_conflict_and_an_existing_one_keeps_its_id(client: AioticClient) -> None:
    lumitech = client.customers.get("58931")
    with pytest.raises(AioticConflictError) as conflict:
        client.customers.upsert("C-NEW", {"name": "New", "id": str(lumitech.id)})
    assert conflict.value.status == 409 and "58931" in str(conflict.value.detail)
    assert client.customers.upsert("58931", {"name": "LUMITECH INSTALLATIES", "id": str(uuid.uuid4())}).id == lumitech.id  # ignored


def test_a_mapping_to_a_missing_customer_or_product_is_a_404_that_says_which(client: AioticClient) -> None:
    with pytest.raises(AioticNotFoundError, match="Customer 99999 does not exist"):
        client.customer_products.upsert("99999", "X", {"item_number": "PROD-001", "language_code": "nl"})
    with pytest.raises(AioticNotFoundError, match="Product number 'PROD-999'"):
        client.customer_products.upsert("58931", "X", {"item_number": "PROD-999", "language_code": "nl"})


def test_deleting_a_customer_or_a_product_takes_its_mappings_with_it(client: AioticClient) -> None:
    client.products.delete("PROD-001", "nl")
    assert client.customer_products.list().total == 0


# ---- a slash in item numbers ------------------------------------------------------------------------------------------


def test_item_numbers_and_customer_item_numbers_may_contain_a_slash(client: AioticClient) -> None:
    client.products.upsert("LED 48/100 W", "nl", {"description": "Hinge"})
    assert client.products.get("LED 48/100 W", "nl").description == "Hinge"
    client.customer_products.upsert("58931", "LT/ART/16", {"item_number": "LED 48/100 W", "language_code": "nl"})
    assert client.customer_products.get("58931", "LT/ART/16").item_number == "LED 48/100 W"
    client.customer_products.delete("58931", "LT/ART/16")
    client.products.delete("LED 48/100 W", "nl")
    with pytest.raises(AioticNotFoundError):
        client.products.get("LED 48/100 W", "nl")
    assert client.products.list().total == 3  # the list is still the list


# ---- an order's files and its e-mail ------------------------------------------------------------------------------------


def test_an_order_from_an_email_lists_the_message_and_its_body_and_serves_both(client: AioticClient, http: httpx.Client) -> None:
    up = client.orders.upload_raw_email(("order.eml", ORDER_MAIL))
    files = client.orders.files(up.request_id)
    assert [(f.source, f.id, f.content_type) for f in files] == [("raw_email", None, "message/rfc822"), ("email_body", None, "text/plain")]
    assert client.orders.email(up.request_id) == ORDER_MAIL
    assert client.orders.email(up.request_id, "email_body").decode().strip() == "Please deliver 10 x PROD-001."
    served = http.get(f"/order/{up.request_id}/email/raw_email")
    assert served.headers["content-disposition"] == "attachment" and served.headers["x-content-type-options"] == "nosniff"
    with pytest.raises(ValueError, match="raw_email, email_body"):
        client.orders.email(up.request_id, "attachment")


def test_an_uploaded_file_is_listed_as_an_upload_and_has_no_email(client: AioticClient) -> None:
    up = client.orders.upload([("PO-4711.pdf", b"%PDF-1.4 fixture")])
    (only,) = client.orders.files(up.request_id)
    assert (only.source, only.filename, only.size_bytes) == ("upload", "PO-4711.pdf", 16) and isinstance(only.id, int)
    assert client.orders.files(up.request_id)[0].id == only.id  # the same file keeps its id
    with pytest.raises(AioticNotFoundError):
        client.orders.email(up.request_id)
    with pytest.raises(AioticNotFoundError):
        client.orders.files(uuid.uuid4())


def test_a_mail_intake_that_cannot_be_reached_gives_a_partial_list_and_a_502_not_a_404(client: AioticClient, http: httpx.Client) -> None:
    up = client.orders.upload_raw_email(("order.eml", ORDER_MAIL))
    http.post("/_mock/config", json={"mail_unavailable": True})
    assert client.orders.files(up.request_id) == []  # still a 200: the list alone does not prove there is no e-mail
    with pytest.raises(AioticServerError) as unavailable:
        client.orders.email(up.request_id)
    assert unavailable.value.status == 502
    http.post("/_mock/config", json={"mail_unavailable": False})
    assert len(client.orders.files(up.request_id)) == 2


def test_a_file_list_with_a_source_the_sdk_does_not_know_is_still_read() -> None:
    listing = [
        {"id": 1042, "filename": "PO-4711.pdf", "source": "a_source_added_later", "content_type": "application/pdf", "size_bytes": 48213, "created_at": "2026-11-03T09:15:22+00:00"},
        {"id": None, "filename": "message.eml", "source": "raw_email", "content_type": "message/rfc822", "size_bytes": 71544, "created_at": None},
    ]
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=listing))
    with AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, transport=transport) as c:
        files = c.orders.files(uuid.uuid4())
    assert [f.source for f in files] == ["a_source_added_later", "raw_email"] and files[0].filename == "PO-4711.pdf"


def test_the_async_client_reads_files_and_email(tenant: str) -> None:
    async def run() -> tuple[list[str], bytes]:
        async with AsyncAioticClient(base_url=tenant, api_key="mock-integration-key", rate_limit=0) as c:
            up = await c.orders.upload_raw_email(("order.eml", ORDER_MAIL))
            return [f.source for f in await c.orders.files(up.request_id)], await c.orders.email(up.request_id, "raw_email")

    assert asyncio.run(run()) == (["raw_email", "email_body"], ORDER_MAIL)


# ---- uploads: a request_id is not an idempotency key ---------------------------------------------------------------------


def test_uploading_again_under_an_existing_id_puts_the_order_back_into_the_queue(client: AioticClient, http: httpx.Client) -> None:
    rid = uuid.uuid4()
    client.orders.upload([("PO-4711.pdf", b"x")], request_id=rid)
    assert client.orders.wait(rid, timeout=10, initial_interval=0.05).status == OrderStatusValue.PROCESSED
    http.post(f"/_mock/orders/{rid}/status", json={"status": "SENT"})  # the order went to the ERP in the meantime

    client.orders.upload([("PO-4711.pdf", b"x")], request_id=rid)  # the same upload, sent again

    assert client.orders.get(rid).status in (OrderStatusValue.QUEUED, OrderStatusValue.PROCESSING)  # no longer SENT
    assert client.orders.list().total == 1  # one order, processed twice


def test_a_tenant_without_direct_uploads_answers_409_on_both_upload_endpoints(client: AioticClient, http: httpx.Client) -> None:
    http.post("/_mock/config", json={"direct_upload": False})
    with pytest.raises(AioticConflictError):
        client.orders.upload([("PO-4711.pdf", b"x")])
    with pytest.raises(AioticConflictError):
        client.orders.upload_raw_email(("order.eml", ORDER_MAIL))
    assert client.orders.list().total == 0


# ---- what counts as an accepted send -------------------------------------------------------------------------------------


class _Erp:
    """A receive endpoint that answers from a script: (status, body) per delivery, the last one repeating."""

    def __init__(self, *answers: tuple[int, bytes]):
        self.answers, self.deliveries = list(answers), []
        erp = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                erp.deliveries.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
                status, body = erp.answers[min(len(erp.deliveries), len(erp.answers)) - 1]
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/orders"

    def close(self) -> None:
        self.server.shutdown()


def _processed_order(client: AioticClient) -> uuid.UUID:
    up = client.orders.upload([("PO-4711.pdf", b"x")])
    assert client.orders.wait(up.request_id, timeout=10, initial_interval=0.05).status == OrderStatusValue.PROCESSED
    return up.request_id


def _send(client: AioticClient, http: httpx.Client, *answers: tuple[int, bytes]) -> tuple[_Erp, uuid.UUID, Exception | None]:
    erp = _Erp(*answers)
    http.post("/_mock/config", json={"erp_url": erp.url, "erp_key": "k"})
    rid = _processed_order(client)
    try:
        client.erp.send(rid)
        return erp, rid, None
    except Exception as exc:  # the caller asserts on it
        return erp, rid, exc
    finally:
        erp.close()


def test_a_boolean_success_decides_at_any_status(client: AioticClient, http: httpx.Client) -> None:
    erp, rid, error = _send(client, http, (500, b'{"success": true, "order_number": "SO-1"}'))
    assert error is None and client.orders.get(rid).erp_ref == "SO-1"
    erp, rid, error = _send(client, http, (200, b'{"success": false, "error": "Unknown article number: PROD-999"}'))
    assert isinstance(error, AioticErpRejectedError) and error.erp_error == "Unknown article number: PROD-999"
    assert client.orders.get(rid).status == OrderStatusValue.PROCESSED and len(erp.deliveries) == 1


@pytest.mark.parametrize("body", [b'{"order_number": "SO-1"}', b'{"success": "true"}', b'{"success": null}', b'{"detail": "Invalid API key"}'])
def test_without_a_boolean_success_only_a_2xx_counts_as_accepted(client: AioticClient, http: httpx.Client, body: bytes) -> None:
    erp, rid, error = _send(client, http, (200, body))
    assert error is None and client.orders.get(rid).status == OrderStatusValue.SENT
    for status in (401, 404, 502):
        erp, rid, error = _send(client, http, (status, body))
        assert isinstance(error, AioticServerError) and error.status == 500
        assert client.orders.get(rid).status == OrderStatusValue.PROCESSED  # rolled back, never SENT without an order


@pytest.mark.parametrize("body", [b"", b"<html>Bad gateway</html>"])
def test_an_answer_that_is_not_json_is_a_failed_send_even_with_a_200(client: AioticClient, http: httpx.Client, body: bytes) -> None:
    erp, rid, error = _send(client, http, (200, body))
    assert isinstance(error, AioticServerError) and client.orders.get(rid).status == OrderStatusValue.PROCESSED


def test_a_retryable_refusal_is_sent_again_with_the_same_request_id(client: AioticClient, http: httpx.Client) -> None:
    locked = (500, b'{"success": false, "error": "The customer record is in use. Try again in a moment.", "retryable": true}')
    erp, rid, error = _send(client, http, locked, locked, (200, b'{"success": true, "order_number": "SO-7"}'))
    assert error is None and client.orders.get(rid).erp_ref == "SO-7"
    assert [d["request_id"] for d in erp.deliveries] == [str(rid)] * 3  # 2 more attempts by default, the same order

    erp, rid, error = _send(client, http, locked)  # refused every time: handled as any other refusal
    assert isinstance(error, AioticErpRejectedError) and "in use" in error.erp_error and len(erp.deliveries) == 3
    assert client.orders.get(rid).status == OrderStatusValue.PROCESSED

    erp, rid, error = _send(client, http, (200, b'{"success": false, "error": "Customer is blocked", "retryable": false}'))
    assert isinstance(error, AioticErpRejectedError) and len(erp.deliveries) == 1  # nothing else is sent again


# ---- the receive endpoint's side of it ---------------------------------------------------------------------------------------


class _RefusingErp:
    def __init__(self, refusal: Exception):
        self.refusal = refusal

    def find_order_by_request_id(self, request_id: str) -> ErpCreateResult | None:
        return None

    def create_sales_order(self, request_id: str, order: object) -> ErpCreateResult:
        raise self.refusal


def _delivery() -> dict:
    return {"request_id": str(uuid.uuid4()), "purchase_order": {"order_number": "PO-4711", "order_date": None, "items": []}}


def test_only_a_temporary_refusal_tells_aiotic_to_send_again() -> None:
    temporary = ErpReceiver(_RefusingErp(ErpTemporaryRefusal("The customer record is in use. Try again in a moment.")), api_key="k").handle(_delivery(), api_key_header="k")
    assert temporary.response.model_dump(exclude_none=True) == {"success": False, "error": "The customer record is in use. Try again in a moment.", "retryable": True}
    business = ErpReceiver(_RefusingErp(ErpRejected("Customer is blocked")), api_key="k").handle(_delivery(), api_key_header="k")
    assert business.response.model_dump(exclude_none=True) == {"success": False, "error": "Customer is blocked"}
    outage = ErpReceiver(_RefusingErp(RuntimeError("connection reset")), api_key="k").handle(_delivery(), api_key_header="k")
    assert outage.http_status == 500 and outage.response.retryable is None  # unknown whether anything was created


def test_response_helpers_and_an_order_without_a_date() -> None:
    assert ErpReceiveResponse.rejected("x").model_dump(exclude_none=True) == {"success": False, "error": "x"}
    assert ErpReceiveResponse.rejected("x", retryable=True).retryable is True
    assert ErpReceiveResponse.accepted("SO-1").retryable is None
    order = PurchaseOrder.model_validate({"order_number": "PO-1", "order_date": None, "supplier": {"company": "Acme", "address": {"street": "s", "postal_code": "p", "city": "c"}}, "items": []})
    assert order.order_date is None
