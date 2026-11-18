"""Regression tests for the first-release review, client side: settings isolation, identifier encoding,
the retry policy for uploads and other POSTs, and products without a description."""

from __future__ import annotations

import asyncio
import re
import uuid
from unittest.mock import patch

import httpx
import pytest
from pydantic import ValidationError

from aiotic import AioticClient, AioticIdentifierError, AioticOutcomeUnknownError, AioticServerError, AioticTransportError, AioticUnavailableError, AsyncAioticClient
from aiotic.config import Settings
from aiotic.models import Product, ProductListResponse, ProductUpsert
from aiotic.sync import InMemoryStateStore, SyncEngine

EMPTY_PAGE = {"items": [], "total": 0, "limit": 100, "offset": 0}
ANY_RECORD = {"number": "x", "item_number": "x", "language_code": "nl", "customer_number": "x", "customer_item_number": "x", "items": [], "total": 0, "limit": 100, "offset": 0}  # satisfies every response model used below
NO_SLEEP = {"aiotic.client.time.sleep": lambda _: None}


def _recorder(answer: dict | None = None, status: int = 200):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json=EMPTY_PAGE if answer is None else answer)

    return seen, httpx.MockTransport(handler)


def _client(transport: httpx.BaseTransport, **kw) -> AioticClient:
    return AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=transport, **kw)


# ---- 1. a shared Settings object never leaks another client's credentials ---------------------------------------


def test_shared_settings_do_not_leak_between_sync_clients() -> None:
    seen, transport = _recorder()
    shared = Settings(base_url="https://tenant-a.invalid", api_key="test-key-a", rate_limit=0)
    with AioticClient(settings=shared, transport=transport) as a, AioticClient(
        settings=shared, base_url="https://tenant-b.invalid", api_key="test-key-b", sync_api_key="test-sync-b", transport=transport
    ) as b:
        shared.api_key, shared.base_url = "mutated-later", "https://mutated.invalid"  # the caller keeps changing its object
        a.orders.list()
        b.orders.list()
        a.customers.list()
        b.customers.list()
    assert [(r.url.host, r.headers["x-api-key"]) for r in seen] == [
        ("tenant-a.invalid", "test-key-a"),
        ("tenant-b.invalid", "test-key-b"),
        ("tenant-a.invalid", "test-key-a"),  # no sync key configured for A: integration key
        ("tenant-b.invalid", "test-sync-b"),  # B routes master data to its own sync key
    ]
    assert shared.api_key == "mutated-later"  # the clients did not write into the caller's object either


def test_shared_settings_do_not_leak_between_async_clients() -> None:
    async def run() -> list[tuple[str, str]]:
        seen, transport = _recorder()
        shared = Settings(base_url="https://a.invalid", api_key="test-key-a", rate_limit=0)
        async with AsyncAioticClient(settings=shared, transport=transport) as a, AsyncAioticClient(
            settings=shared, base_url="https://b.invalid", api_key="test-key-b", transport=transport
        ) as b:
            shared.api_key = "mutated-later"
            await a.orders.list()
            await b.orders.list()
        return [(r.url.host, r.headers["x-api-key"]) for r in seen]

    assert asyncio.run(run()) == [("a.invalid", "test-key-a"), ("b.invalid", "test-key-b")]


# ---- 4. identifiers travel as one path segment or are refused -------------------------------------------------


@pytest.mark.parametrize(
    "raw,encoded",
    [("C#1", "C%231"), ("ITEM?2", "ITEM%3F2"), ("50%", "50%25"), ("A B", "A%20B"), ("Ærø", "%C3%86r%C3%B8"), ("C.1", "C.1"), ("a+b", "a%2Bb")],
)
def test_identifiers_are_encoded_in_every_path(raw: str, encoded: str) -> None:
    seen, transport = _recorder(ANY_RECORD)
    with _client(transport) as c:
        c.customers.get(raw)
        c.customers.upsert(raw, {"name": "x"})
        c.customers.delete(raw)
        c.products.get(raw, "nl")
        c.products.upsert("P1", raw, {"description": "d"})
        c.customer_products.delete("C1", raw)
        c.customers.search(raw)
        c.orders.download_file(uuid.uuid4(), raw)
    paths = [r.url.raw_path.decode() for r in seen]
    assert paths[:3] == [f"/customer/{encoded}"] * 3
    assert paths[3] == f"/product/{encoded}/nl" and paths[4] == f"/product/P1/{encoded}" and paths[5] == f"/customer-product/C1/{encoded}"
    assert paths[6].startswith(f"/customer/search/{encoded}?") and paths[7].endswith(f"/{encoded}")


@pytest.mark.parametrize("bad", ["", ".", "..", "C\\1", "C\n1", "C\x7f", "/C", "C/", "C//1", "C/../1"])
def test_unsafe_identifiers_are_refused_before_any_request(bad: str) -> None:
    seen, transport = _recorder(ANY_RECORD)
    with _client(transport) as c:
        for call in (
            lambda: c.customers.delete(bad),
            lambda: c.customers.get(bad),
            lambda: c.products.upsert(bad, "nl", {"description": "x"}),
            lambda: c.customer_products.get("C1", bad),
            lambda: c.orders.download_file(uuid.uuid4(), bad),
        ):
            with pytest.raises(AioticIdentifierError):
                call()
    assert seen == []


def test_a_slash_travels_only_in_item_numbers_and_customer_item_numbers() -> None:
    seen, transport = _recorder(ANY_RECORD)
    with _client(transport) as c:
        c.products.upsert("LED 48/100 W", "nl", {"description": "x"})
        c.products.get("CBL-3015/01", "nl")
        c.products.delete("CBL-3015/01", "nl")
        c.customer_products.get("C1", "LT/ART/001")
        c.customer_products.delete("C1", "LT/ART/001")
        for refused in (
            lambda: c.customers.get("C/1"),  # a customer number is one path segment in the API
            lambda: c.products.get("P1", "n/l"),
            lambda: c.customer_products.get("C/1", "X"),
            lambda: c.orders.download_file(uuid.uuid4(), "dir/file.pdf"),
        ):
            with pytest.raises(AioticIdentifierError):
                refused()
    assert [r.url.raw_path.decode() for r in seen] == [
        "/product/LED%2048%2F100%20W/nl",
        "/product/CBL-3015%2F01/nl",
        "/product/CBL-3015%2F01/nl",
        "/customer-product/C1/LT%2FART%2F001",
        "/customer-product/C1/LT%2FART%2F001",
    ]


def test_async_client_encodes_and_refuses_identifiers() -> None:
    async def run() -> list[str]:
        seen, transport = _recorder(ANY_RECORD)
        async with AsyncAioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, transport=transport) as c:
            await c.customers.delete("C#1")
            await c.customer_products.delete("C1", "ITEM?2")
            with pytest.raises(AioticIdentifierError):
                await c.customers.delete("C/1")
        return [r.url.raw_path.decode() for r in seen]

    assert asyncio.run(run()) == ["/customer/C%231", "/customer-product/C1/ITEM%3F2"]


def test_hash_in_a_customer_number_addresses_that_customer_on_the_mock(client: AioticClient) -> None:
    client.customers.upsert("C", {"name": "Plain"})
    client.customers.upsert("C#1", {"name": "Hash"})
    assert client.customers.get("C#1").name == "Hash"
    client.customers.delete("C#1")  # used to become DELETE /customer/C
    assert client.customers.get("C").name == "Plain"
    client.customers.delete("C")


# ---- 6. one logical upload, and no blind replay of other POSTs -------------------------------------------------


def _lost_then_ok():
    bodies: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        if len(bodies) == 1:
            raise httpx.ReadTimeout("response lost after the server accepted the request", request=request)
        return httpx.Response(200, json={"request_id": str(uuid.uuid4()), "split": False})

    return bodies, httpx.MockTransport(handler)


def _request_ids(bodies: list[bytes]) -> list[bytes]:
    return [re.search(rb'name="request_id"\r\n\r\n([^\r]+)', b).group(1) for b in bodies]  # type: ignore[union-attr]


def _upload_lost(*, appears_after: int | None):
    """An upload whose first answer is lost. The order is visible from the ``appears_after``-th lookup on
    (never when ``None``); a second upload is answered normally."""
    uploads: list[bytes] = []
    lookups: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            uploads.append(request.read())
            if len(uploads) == 1:
                raise httpx.ReadTimeout("response lost after the server accepted the request", request=request)
            return httpx.Response(200, json={"request_id": _request_ids(uploads)[0].decode(), "split": False})
        lookups.append(request.url.path)
        if appears_after is not None and len(lookups) >= appears_after:
            return httpx.Response(200, json={"request_id": request.url.path.rsplit("/", 1)[1], "status": "QUEUED", "timestamp": "2026-11-18T10:00:00"})
        return httpx.Response(404, json={"detail": "not found"})

    return uploads, lookups, httpx.MockTransport(handler)


def test_an_upload_whose_answer_was_lost_is_looked_up_and_not_sent_again() -> None:
    # The first lookup still answers 404: the request can be on its way. The order shows up on the third.
    uploads, lookups, transport = _upload_lost(appears_after=3)
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        up = c.orders.upload([("po.pdf", b"fixture")], confirm_timeout=60)
    assert len(uploads) == 1 and len(lookups) == 3
    assert str(up.request_id).encode() == _request_ids(uploads)[0] and uuid.UUID(str(up.request_id)).version == 4


def test_an_upload_that_never_shows_up_is_reported_with_its_request_id_and_not_repeated() -> None:
    uploads, lookups, transport = _upload_lost(appears_after=None)
    rid = uuid.uuid4()
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        with pytest.raises(AioticOutcomeUnknownError) as caught:
            c.orders.upload([("po.pdf", b"fixture")], request_id=rid, confirm_timeout=0)
    assert len(uploads) == 1 and len(lookups) >= 1  # looked up, never uploaded twice
    assert caught.value.request_id == str(rid) and "look it up" in str(caught.value)


def test_resend_if_missing_uploads_once_more_with_the_same_id_and_the_complete_body() -> None:
    import io

    uploads, lookups, transport = _upload_lost(appears_after=None)
    rid = uuid.uuid4()
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        up = c.orders.upload([("po.pdf", io.BytesIO(b"%PDF fixture body"))], request_id=rid, confirm_timeout=0, resend_if_missing=True)
    assert up.request_id == rid and lookups  # the lookup came first
    assert _request_ids(uploads) == [str(rid).encode()] * 2
    assert all(b"%PDF fixture body" in b for b in uploads)  # the second attempt is not an empty body


def test_an_upload_that_provably_did_not_arrive_is_simply_sent_again() -> None:
    uploads: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        uploads.append(request.read())
        if len(uploads) == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"request_id": _request_ids(uploads)[0].decode(), "split": False})

    with patch("aiotic.client.time.sleep", lambda _: None), _client(httpx.MockTransport(handler)) as c:
        c.orders.upload([("po.pdf", b"fixture")])
    assert len(uploads) == 2 and len(set(_request_ids(uploads))) == 1


def test_raw_upload_is_never_replayed_after_a_lost_response() -> None:
    bodies, transport = _lost_then_ok()
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        with pytest.raises(AioticTransportError):
            c.orders.upload_raw_email(("mail.eml", b"Subject: order"))
    assert len(bodies) == 1


def test_non_idempotent_posts_are_replayed_only_when_nothing_was_sent() -> None:
    calls: list[httpx.Request] = []

    def refused_once(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"request_id": str(uuid.uuid4()), "status": "reprocessing"})

    with patch("aiotic.client.time.sleep", lambda _: None), _client(httpx.MockTransport(refused_once)) as c:
        c.rejected.reprocess(uuid.uuid4())
    assert len(calls) == 2


@pytest.mark.parametrize(
    "call,status,expected_attempts",
    [
        (lambda c: c.erp.send(uuid.uuid4()), 502, 1),  # may have gone through: never replayed
        (lambda c: c.erp.send(uuid.uuid4()), 503, 1),  # integration not configured: surfaced, not retried
        (lambda c: c.orders.retry(uuid.uuid4()), 429, 3),  # the server did not process it: retried
        (lambda c: c.orders.get(uuid.uuid4()), 502, 3),  # GET is idempotent
        (lambda c: c.customers.upsert("C1", {"name": "x"}), 504, 3),  # PUT is idempotent
    ],
)
def test_http_retry_policy_is_operation_aware(call, status: int, expected_attempts: int) -> None:
    seen, transport = _recorder({"detail": "upstream"}, status=status)
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        with pytest.raises((AioticServerError, AioticUnavailableError, Exception)):
            call(c)
    assert len(seen) == expected_attempts


def test_async_upload_replay_and_raw_upload_policy() -> None:
    async def run() -> tuple[list[bytes], list[str], int]:
        uploads, lookups, transport = _upload_lost(appears_after=None)

        async def no_delay(_: float) -> None:
            return None

        with patch("aiotic.aio.asyncio.sleep", no_delay):
            async with AsyncAioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=transport) as c:
                with pytest.raises(AioticOutcomeUnknownError):
                    await c.orders.upload([("po.pdf", b"fixture")], confirm_timeout=0)
                assert len(uploads) == 1  # looked up, not sent again
                await c.orders.upload([("po.pdf", b"fixture")], confirm_timeout=0, resend_if_missing=True)
            raw_bodies, raw_transport = _lost_then_ok()
            async with AsyncAioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=raw_transport) as c:
                with pytest.raises(AioticTransportError):
                    await c.orders.upload_raw_email(("m.eml", b"Subject: x"))
        return uploads, lookups, len(raw_bodies)

    uploads, lookups, raw_attempts = asyncio.run(run())
    assert len(uploads) == 2 and lookups and raw_attempts == 1


# ---- 8. a product without a description is a valid response, never a valid write --------------------------------


def test_product_response_accepts_a_missing_description_but_upserts_still_require_one() -> None:
    p = Product.model_validate({"item_number": "P1", "language_code": "nl", "description": None, "created_at": "2026-10-04T00:00:00Z"})
    assert p.description is None and p.remark is None
    page = ProductListResponse.model_validate({"items": [{"item_number": "P1", "language_code": "nl"}], "total": 1, "limit": 100, "offset": 0})
    assert page.items[0].description is None
    with pytest.raises(ValidationError):
        ProductUpsert(description=None)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        ProductUpsert()  # type: ignore[call-arg]


def test_null_descriptions_parse_in_both_clients_and_bootstrap_skips_them() -> None:
    page = {
        "items": [
            {"item_number": "P1", "language_code": "nl", "description": None, "created_at": "2026-10-04T00:00:00Z"},
            {"item_number": "P2", "language_code": "nl", "description": "Known", "remark": None, "created_at": "2026-10-04T00:00:00Z"},
        ],
        "total": 2,
        "limit": 1000,
        "offset": 0,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/product/list":
            return httpx.Response(200, json=page)
        if request.url.path.startswith("/product/"):
            return httpx.Response(200, json=page["items"][0])
        return httpx.Response(200, json=EMPTY_PAGE)

    transport = httpx.MockTransport(handler)
    with _client(transport) as c:
        assert [p.description for p in c.products.iter_all()] == [None, "Known"]
        engine = SyncEngine(c, state=InMemoryStateStore())
        assert engine.bootstrap_state_from_aiotic() == 1  # P1 cannot be written back without a description: not seeded
        assert engine.state.get("product:P2|nl") and engine.state.get("product:P1|nl") is None

    async def run() -> str | None:
        async with AsyncAioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, transport=transport) as c:
            return (await c.products.get("P1", "nl")).description

    assert asyncio.run(run()) is None


# ---- answers of a gateway: no JSON, and a 503 that does not say why ------------------------------------------------


def _gateway(status: int):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, text=f"<html><body><h1>{status}</h1></body></html>", headers={"content-type": "text/html"})

    return seen, httpx.MockTransport(handler)


def test_a_too_large_upload_refused_by_a_gateway_is_a_validation_error_without_json() -> None:
    from aiotic import AioticValidationError

    seen, transport = _gateway(413)
    with _client(transport) as c, pytest.raises(AioticValidationError) as refused:
        c.orders.upload([("a.pdf", b"%PDF-1.4"), ("b.pdf", b"%PDF-1.4")], request_id=uuid.uuid4())
    assert refused.value.status == 413 and isinstance(refused.value.detail, str) and len(seen) == 1  # not sent again


def test_a_503_without_json_is_backed_off_on_a_read_and_surfaced_on_an_erp_send() -> None:
    seen, transport = _gateway(503)
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        with pytest.raises(AioticUnavailableError) as read:
            c.customers.list()
        attempts_of_the_read = len(seen)
        with pytest.raises(AioticUnavailableError):
            c.erp.send(uuid.uuid4())
    assert attempts_of_the_read == 3 and read.value.status == 503  # a read is safe to repeat: two more attempts
    assert len(seen) == attempts_of_the_read + 1  # the send is not repeated: a 503 does not say what happened
