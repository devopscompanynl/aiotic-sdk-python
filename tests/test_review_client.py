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

from aiotic import AioticClient, AioticIdentifierError, AioticServerError, AioticTransportError, AioticUnavailableError, AsyncAioticClient
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


@pytest.mark.parametrize("bad", ["", ".", "..", "C/1", "C\\1", "C\n1", "C\x7f"])
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


def test_upload_replay_carries_the_same_generated_request_id() -> None:
    bodies, transport = _lost_then_ok()
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        c.orders.upload([("po.pdf", b"fixture")])
    assert len(bodies) == 2
    ids = _request_ids(bodies)
    assert ids[0] == ids[1] and uuid.UUID(ids[0].decode()).version == 4  # the server sees one logical upload


def test_upload_keeps_the_callers_request_id_and_replays_file_objects_completely(tmp_path) -> None:
    import io

    bodies, transport = _lost_then_ok()
    rid = uuid.uuid4()
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        c.orders.upload([("po.pdf", io.BytesIO(b"%PDF fixture body"))], request_id=rid)
    assert _request_ids(bodies) == [str(rid).encode()] * 2
    assert all(b"%PDF fixture body" in b for b in bodies)  # the second attempt is not an empty body


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
    async def run() -> tuple[list[bytes], int]:
        bodies, transport = _lost_then_ok()

        async def no_delay(_: float) -> None:
            return None

        with patch("aiotic.aio.asyncio.sleep", no_delay):
            async with AsyncAioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=transport) as c:
                await c.orders.upload([("po.pdf", b"fixture")])
            raw_bodies, raw_transport = _lost_then_ok()
            async with AsyncAioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=raw_transport) as c:
                with pytest.raises(AioticTransportError):
                    await c.orders.upload_raw_email(("m.eml", b"Subject: x"))
        return bodies, len(raw_bodies)

    bodies, raw_attempts = asyncio.run(run())
    ids = _request_ids(bodies)
    assert len(bodies) == 2 and ids[0] == ids[1] and raw_attempts == 1


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
