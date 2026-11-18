"""Bulk sync: one call replaces a data set. What the API removes, what the client sends, and what the engine's
safeguard refuses before anything is sent. Every test gets its own mock tenant, because a bulk sync replaces all of it."""

from __future__ import annotations

import asyncio
import gzip
import json
import threading
from collections.abc import Iterator
from datetime import datetime, timezone
from unittest.mock import patch

import httpx
import pytest
import uvicorn
from typer.testing import CliRunner

from aiotic import AioticAuthError, AioticClient, AioticOutcomeUnknownError, AioticServerError, AioticTransportError, AioticValidationError, AsyncAioticClient
from aiotic.cli import app as cli
from aiotic.mock import create_mock_app
from aiotic.sync import BulkSyncRefused, ChangeEvent as E, ChangeKind, SyncEngine

LUMITECH = {"number": "58931", "name": "LUMITECH INSTALLATIES", "city": "Apeldoorn"}
JANSEN = {"number": "58932", "name": "Installatie Jansen", "city": "Utrecht"}
PRODUCTS = [
    {"item_number": "PROD-001", "language_code": "nl", "description": "LED Driver 48V 100W"},
    {"item_number": "PROD-002", "language_code": "nl", "description": "LED Panel 60x60 40W"},
    {"item_number": "CBL-3015", "language_code": "nl", "description": "Cable 3x1.5 mm² (100 m)"},
]
MAPPING = {"customer_number": "58931", "customer_item_number": "LT-ART-001", "item_number": "PROD-001", "language_code": "nl"}


class _Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:  # pragma: no cover
        pass


@pytest.fixture
def tenant() -> Iterator[str]:
    """A mock tenant of its own, seeded with one customer, three products and one mapping."""
    server = _Server(uvicorn.Config(create_mock_app(processing_seconds=0.2), host="127.0.0.1", port=0, log_level="warning"))
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


def _numbers(client: AioticClient) -> set[str]:
    return {c.number for c in client.customers.iter_all()}


def _mappings(client: AioticClient) -> set[tuple[str, str]]:
    return {(m.customer_number, m.customer_item_number) for m in client.customer_products.iter_all()}


# ---- the contract: what a bulk sync stores and what it removes --------------------------------------------------


def test_a_customer_bulk_sync_replaces_the_set_and_takes_the_mappings_of_removed_customers(client: AioticClient) -> None:
    client.customers.upsert("C-OLD", {"name": "Old customer"})
    client.customer_products.upsert("C-OLD", "X-1", {"item_number": "PROD-001", "language_code": "nl"})

    result = client.customers.bulk_sync([LUMITECH, JANSEN])

    # 58931 is sent with fewer fields than AIOTIC held, which is an update: a bulk row replaces the record.
    assert (result.received, result.inserted, result.updated, result.unchanged, result.deleted, result.mappings_deleted) == (2, 1, 1, 0, 1, 1)
    assert client.customers.get("58931").address is None
    assert _numbers(client) == {"58931", "58932"}
    assert _mappings(client) == {("58931", "LT-ART-001")}  # the kept customer keeps its mapping
    assert result.attempts == 1


def test_a_product_bulk_sync_covers_every_language_and_takes_the_mappings_of_removed_products(client: AioticClient) -> None:
    client.products.upsert("PROD-001", "de", {"description": "LED-Treiber 48V 100W"})

    # Only the Dutch records, and without PROD-001: its German record and its mapping go as well.
    result = client.products.bulk_sync([p for p in PRODUCTS if p["item_number"] != "PROD-001"])

    assert (result.inserted, result.unchanged, result.deleted, result.mappings_deleted) == (0, 2, 2, 1)
    assert {(p.item_number, p.language_code) for p in client.products.iter_all()} == {("PROD-002", "nl"), ("CBL-3015", "nl")}
    assert _mappings(client) == set()


def test_a_mapping_with_an_unknown_target_is_skipped_on_a_200_and_the_mapping_held_under_its_key_is_removed(client: AioticClient) -> None:
    client.customer_products.upsert("58931", "LT-ART-002", {"item_number": "PROD-002", "language_code": "nl"})

    result = client.customer_products.bulk_sync(
        [
            MAPPING,
            {"customer_number": "58931", "customer_item_number": "LT-ART-002", "item_number": "NOT-HELD", "language_code": "nl"},  # present, but its target is unknown
            {"customer_number": "99999", "customer_item_number": "X", "item_number": "PROD-001", "language_code": "nl"},
        ]
    )

    assert (result.received, result.unchanged, result.inserted, result.skipped_total, result.deleted) == (3, 1, 0, 2, 1)
    assert [(s.customer_item_number, s.reason) for s in result.skipped] == [("LT-ART-002", "product_missing"), ("X", "customer_missing")]
    assert _mappings(client) == {("58931", "LT-ART-001")}  # LT-ART-002 was in the body and is gone all the same


def test_sending_the_same_set_again_changes_nothing(client: AioticClient) -> None:
    client.customers.bulk_sync([LUMITECH, JANSEN])
    again = client.customers.bulk_sync([LUMITECH, JANSEN])
    assert (again.unchanged, again.inserted, again.updated, again.deleted) == (2, 0, 0, 0)


def test_the_api_refuses_a_broken_bulk_sync_as_a_whole(client: AioticClient, tenant: str) -> None:
    with pytest.raises(AioticValidationError) as twice:
        client.customers.bulk_sync([LUMITECH, {**JANSEN, "number": "58931"}])
    assert twice.value.status == 422 and twice.value.detail[0]["loc"] == ["body", 1, "number"]
    with httpx.Client(base_url=tenant, headers={"X-API-Key": "mock-sync-key"}) as http:
        assert http.post("/customer/bulk", json=[]).status_code == 422  # an empty set never empties a tenant
        assert http.post("/customer/bulk", json=[{"number": "1", "name": "  "}]).json()["detail"][0]["loc"] == ["body", 0, "name"]
        assert http.post("/product/bulk", content=b"not gzip", headers={"Content-Encoding": "gzip"}).status_code == 400
        assert http.post("/product/bulk", content=b"[]", headers={"Content-Encoding": "br"}).status_code == 415
        assert http.post("/customer-product/bulk", json={"items": []}).status_code == 422
    assert _numbers(client) == {"58931"}  # nothing changed


def test_the_client_refuses_an_empty_or_invalid_data_set_before_sending() -> None:
    seen: list[httpx.Request] = []
    transport = httpx.MockTransport(lambda r: seen.append(r) or httpx.Response(200, json={}))
    with AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, transport=transport) as c:
        with pytest.raises(ValueError, match="at least one row"):
            c.products.bulk_sync([])
        with pytest.raises(ValueError, match="row 1: name"):
            c.customers.bulk_sync([LUMITECH, {"number": "58932", "name": " "}])
        with pytest.raises(ValueError, match="row 0: source_updated_at"):
            c.customer_products.bulk_sync([{**MAPPING, "source_updated_at": "2026-11-10T02:00:07"}])  # no zone
    assert seen == []


def test_bulk_syncs_accept_the_sync_key_and_order_files_do_not(tenant: str) -> None:
    with AioticClient(base_url=tenant, api_key="mock-sync-key", rate_limit=0, max_retries=0) as sync_only:
        assert sync_only.customers.bulk_sync([LUMITECH]).received == 1
        assert sync_only.products.bulk_sync(PRODUCTS).unchanged == 3
        assert sync_only.customer_products.bulk_sync([MAPPING]).unchanged == 1
        with pytest.raises(AioticAuthError):
            sync_only.orders.files("550e8400-e29b-41d4-a716-446655440000")
        with pytest.raises(AioticAuthError):
            sync_only.orders.email("550e8400-e29b-41d4-a716-446655440000")


# ---- what the client puts on the wire ----------------------------------------------------------------------------


def _bulk_recorder(answers: list[httpx.Response | Exception]):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        request.read()
        requests.append(request)
        answer = answers[min(len(requests), len(answers)) - 1]
        if isinstance(answer, Exception):
            raise answer
        return answer

    return requests, httpx.MockTransport(handler)


OK = httpx.Response(200, json={"received": 2, "inserted": 0, "updated": 0, "unchanged": 2, "deleted": 0, "mappings_deleted": 0})


def test_the_body_is_one_gzip_compressed_json_array_with_a_longer_timeout() -> None:
    requests, transport = _bulk_recorder([OK])
    with AioticClient(base_url="https://example.invalid", api_key="test-only", sync_api_key="test-sync", rate_limit=0, transport=transport) as c:
        c.customers.bulk_sync([LUMITECH, {**JANSEN, "source_updated_at": "2026-11-10T02:00:07+02:00"}])
        c.customers.bulk_sync([LUMITECH, JANSEN], compress=False, timeout=5)
    first, plain = requests
    assert first.method == "POST" and first.url.path == "/customer/bulk" and first.headers["x-api-key"] == "test-sync"
    assert first.headers["content-encoding"] == "gzip" and first.headers["content-type"] == "application/json"
    rows = json.loads(gzip.decompress(first.content))
    assert [r["number"] for r in rows] == ["58931", "58932"] and rows[1]["source_updated_at"].startswith("2026-11-10T02:00:07")
    assert first.extensions["timeout"]["read"] == 120.0  # Settings.bulk_timeout, not the 30 s of other calls
    assert "content-encoding" not in plain.headers and json.loads(plain.content)[0]["number"] == "58931"
    assert plain.extensions["timeout"]["read"] == 5


def test_a_504_is_answered_with_the_same_body_and_the_result_says_it_was_sent_twice() -> None:
    requests, transport = _bulk_recorder([httpx.Response(504, text="gateway timeout"), OK])
    with patch("aiotic.client.time.sleep", lambda _: None), AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=transport) as c:
        result = c.customers.bulk_sync([LUMITECH, JANSEN])
    assert len(requests) == 2 and requests[0].content == requests[1].content  # never a different data set
    assert result.attempts == 2 and result.unchanged == 2


def test_a_bulk_sync_nobody_answered_is_reported_as_unknown_not_as_failed() -> None:
    requests, transport = _bulk_recorder([httpx.ReadTimeout("no answer")])
    with patch("aiotic.client.time.sleep", lambda _: None), AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=1, transport=transport) as c:
        with pytest.raises(AioticOutcomeUnknownError, match="may already be replaced"):
            c.products.bulk_sync(PRODUCTS)
    assert len(requests) == 2


def test_a_fault_on_the_server_is_not_repeated_and_a_too_large_body_is_a_validation_error() -> None:
    requests, transport = _bulk_recorder([httpx.Response(500, json={"detail": "the cause is in the server log."})])
    with AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=3, transport=transport) as c:
        with pytest.raises(AioticServerError):
            c.customers.bulk_sync([LUMITECH])
    assert len(requests) == 1
    requests, transport = _bulk_recorder([httpx.Response(413, text="Request Entity Too Large")])
    with AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, transport=transport) as c:
        with pytest.raises(AioticValidationError) as too_large:
            c.customers.bulk_sync([LUMITECH])
    assert too_large.value.status == 413


def test_the_async_client_sends_the_same_bulk_sync(tenant: str) -> None:
    async def run() -> tuple[int, int, int]:
        async with AsyncAioticClient(base_url=tenant, api_key="mock-integration-key", rate_limit=0) as c:
            customers = await c.customers.bulk_sync([LUMITECH, JANSEN])
            products = await c.products.bulk_sync(PRODUCTS)
            mappings = await c.customer_products.bulk_sync([MAPPING, {**MAPPING, "customer_item_number": "LT-ART-009", "item_number": "NOT-HELD"}])
            return customers.inserted, products.unchanged, mappings.skipped_total

    assert asyncio.run(run()) == (1, 3, 1)


# ---- the engine's safeguard: stop before sending -----------------------------------------------------------------


def _many_products(n: int) -> list[dict[str, str]]:
    return [{"item_number": f"P-{i:04d}", "language_code": "nl", "description": f"Article {i}"} for i in range(n)]


def test_a_truncated_export_is_refused_before_anything_is_sent(client: AioticClient) -> None:
    client.products.bulk_sync(_many_products(100))
    engine = SyncEngine(client)

    with pytest.raises(BulkSyncRefused) as refused:
        engine.bulk_sync(products=_many_products(10))  # the export stopped after 10 of 100 records

    plan = refused.value.plans[0]
    assert (plan.kind, plan.rows, plan.held, plan.deletions, plan.new, plan.exact) == (ChangeKind.PRODUCT, 10, 100, 90, 0, True)
    assert "would remove 90 of them (90%, the limit is 10%)" in str(refused.value) and "P-0010 / nl" in str(refused.value)
    assert client.products.list(size=1).total == 100  # nothing changed

    report = engine.bulk_sync(products=_many_products(10), approve_deletions=True)  # a clean-up that is intended
    assert report.deleted == 90 and client.products.list(size=1).total == 10


def test_an_export_of_the_right_size_with_the_wrong_records_is_refused_by_key_and_missed_by_count(client: AioticClient) -> None:
    client.products.bulk_sync(_many_products(50))
    wrong = [{**p, "item_number": "OTHER-" + p["item_number"]} for p in _many_products(50)]  # 50 rows, none of them held

    with pytest.raises(BulkSyncRefused, match="would remove 50 of them"):
        SyncEngine(client).bulk_sync(products=wrong)
    assert {p.item_number for p in client.products.iter_all()} == {p["item_number"] for p in _many_products(50)}

    # compare="count" is cheaper and blind to this case: say so by showing it.
    report = SyncEngine(client, dry_run=True).bulk_sync(products=wrong, compare="count")
    assert report.plans[0].deletions == 0 and report.plans[0].exact is False


def test_a_small_change_passes_and_the_report_shows_what_went(client: AioticClient) -> None:
    client.products.bulk_sync(_many_products(100))
    changed = _many_products(101)[3:]  # three articles discontinued, one new: 3 % removed
    report = SyncEngine(client).bulk_sync(products=changed)
    result = report.results[ChangeKind.PRODUCT]
    assert (result.inserted, result.unchanged, result.deleted) == (1, 97, 3)
    assert report.ok and report.deleted == 3 and "deleted=3" in str(report)


def test_mappings_with_an_unknown_target_stop_the_run_before_customers_and_products_are_sent(client: AioticClient) -> None:
    engine = SyncEngine(client)
    orphan = {"customer_number": "58931", "customer_item_number": "LT-ART-777", "item_number": "PROD-777", "language_code": "nl"}

    with pytest.raises(BulkSyncRefused) as refused:
        engine.bulk_sync(customers=[LUMITECH, JANSEN], products=PRODUCTS, mappings=[MAPPING, orphan])

    assert "58931 / LT-ART-777 -> product PROD-777 (nl)" in str(refused.value)
    assert _numbers(client) == {"58931"}  # the customers were not sent either: nothing is, unless every set passes

    # The same run with the product in the catalog set passes: targets are checked against what is being sent.
    report = engine.bulk_sync(customers=[LUMITECH, JANSEN], products=[*PRODUCTS, {"item_number": "PROD-777", "language_code": "nl", "description": "New"}], mappings=[MAPPING, orphan])
    assert report.ok and report.skipped_total == 0 and _mappings(client) == {("58931", "LT-ART-001"), ("58931", "LT-ART-777")}


def test_a_mapping_to_a_customer_that_this_run_removes_is_an_unknown_target(client: AioticClient) -> None:
    client.customers.upsert("58932", {"name": "Installatie Jansen"})
    client.customer_products.upsert("58932", "J-1", {"item_number": "PROD-001", "language_code": "nl"})
    # 58932 is no longer in the customer export, but its mapping still is.
    with pytest.raises(BulkSyncRefused, match="58932 / J-1 -> customer 58932"):
        SyncEngine(client).bulk_sync(customers=[LUMITECH], mappings=[MAPPING, {**MAPPING, "customer_number": "58932", "customer_item_number": "J-1"}], approve_deletions=True)


def test_approved_skips_are_sent_and_reported(client: AioticClient) -> None:
    orphan = {**MAPPING, "customer_item_number": "LT-ART-777", "item_number": "PROD-777"}
    report = SyncEngine(client).bulk_sync(mappings=[MAPPING, orphan], approve_skipped=True)
    assert report.skipped_total == 1 and report.ok and "skipped=1" in str(report)


def test_approving_skipped_rows_does_not_approve_the_mappings_they_remove(client: AioticClient) -> None:
    # The one mapping AIOTIC holds now points at a product it does not hold: AIOTIC skips the row and removes the mapping.
    redirected = {**MAPPING, "item_number": "PROD-777"}
    engine = SyncEngine(client)

    for compare in ("keys", "count"):
        with pytest.raises(BulkSyncRefused) as refused:
            engine.bulk_sync(mappings=[redirected], approve_skipped=True, compare=compare)
        plan = refused.value.plans[0]
        assert plan.deletions == 1 and plan.unknown_targets == 1 and "(100%, the limit is 10%)" in str(refused.value)
        assert "skip" in str(refused.value) and "approve the deletions" in str(refused.value)
        assert _mappings(client) == {("58931", "LT-ART-001")}  # nothing was sent

    report = engine.bulk_sync(mappings=[redirected], approve_skipped=True, approve_deletions=True)
    assert report.deleted == 1 and report.skipped_total == 1 and _mappings(client) == set()


def test_a_skipped_row_for_a_mapping_aiotic_does_not_hold_removes_nothing(client: AioticClient) -> None:
    orphan = {**MAPPING, "customer_item_number": "LT-ART-777", "item_number": "PROD-777"}
    plan = SyncEngine(client, dry_run=True).bulk_sync(mappings=[MAPPING, orphan], approve_skipped=True).plans[0]
    assert (plan.deletions, plan.replaced_by_skipped, plan.new, plan.unknown_targets) == (0, 0, 0, 1)


def test_the_engine_takes_the_events_reconcile_takes_and_leaves_delete_events_out(client: AioticClient) -> None:
    events = [E.customer_upsert("58931", name="LUMITECH INSTALLATIES", city="Apeldoorn"), E.customer_upsert("58932", name="Installatie Jansen"), E.customer_delete("40112")]
    report = SyncEngine(client).bulk_sync(customers=events)
    assert report.results[ChangeKind.CUSTOMER].received == 2 and _numbers(client) == {"58931", "58932"}


def test_a_dry_run_checks_and_sends_nothing(client: AioticClient) -> None:
    report = SyncEngine(client, dry_run=True).bulk_sync(customers=[LUMITECH, JANSEN])
    assert report.dry_run and not report.results and report.plans[0].new == 1
    assert _numbers(client) == {"58931"} and str(report).startswith("dry run, nothing sent")


def test_a_broken_data_set_is_a_value_error_not_a_refusal(client: AioticClient) -> None:
    engine = SyncEngine(client)
    with pytest.raises(ValueError, match="occurs twice"):
        engine.bulk_sync(customers=[LUMITECH, LUMITECH])
    with pytest.raises(ValueError, match="data set is empty"):
        engine.bulk_sync(products=[])
    with pytest.raises(ValueError, match="customers row 0: name"):
        engine.bulk_sync(customers=[{"number": "58931"}])


# ---- the CLI ------------------------------------------------------------------------------------------------------


def test_the_cli_stops_with_exit_code_2_and_explains_what_would_be_removed(tenant: str, client: AioticClient, tmp_path, monkeypatch) -> None:
    client.products.bulk_sync(_many_products(40))
    monkeypatch.setenv("AIOTIC_BASE_URL", tenant)
    monkeypatch.setenv("AIOTIC_API_KEY", "mock-integration-key")
    monkeypatch.setenv("AIOTIC_RATE_LIMIT", "0")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "products.csv").write_text("item_number,language_code,description\nP-0000,nl,Article 0\nP-0001,nl,Article 1\n", encoding="utf-8")

    refused = CliRunner().invoke(cli, ["sync", "bulk", "--products", "products.csv"])
    said = " ".join(refused.output.split())  # the terminal wraps long lines
    assert refused.exit_code == 2 and "would remove 38 of them" in said and "Nothing was sent" in said
    assert client.products.list(size=1).total == 40

    approved = CliRunner().invoke(cli, ["sync", "bulk", "--products", "products.csv", "--approve-deletions"])
    assert approved.exit_code == 0 and "deleted=38" in approved.output and client.products.list(size=1).total == 2


def _cli_tenant(tenant: str, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AIOTIC_BASE_URL", tenant)
    monkeypatch.setenv("AIOTIC_API_KEY", "mock-integration-key")
    monkeypatch.setenv("AIOTIC_RATE_LIMIT", "0")
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize(
    ("header", "record", "problem"),
    [
        ("item_number,language_code,description", ",nl,Article without a number", "item_number is blank"),
        ("item_number,language_code,description", "PROD-001,,Article without a language", "language_code is blank"),
        ("item_number,language_code,description", "   ,nl,Article with spaces for a number", "item_number is blank"),
        ("item_number,description", "PROD-001,Article in a file without languages", "language_code"),
    ],
)
def test_the_cli_never_fills_in_a_missing_key_of_a_bulk_sync(tenant: str, client: AioticClient, tmp_path, monkeypatch, header: str, record: str, problem: str) -> None:
    _cli_tenant(tenant, tmp_path, monkeypatch)
    before = {(p.item_number, p.language_code) for p in client.products.iter_all()}
    (tmp_path / "products.csv").write_text(f"{header}\nPROD-002,nl,LED Panel 60x60 40W\n{record}\n" if "language_code" in header else f"{header}\n{record}\n", encoding="utf-8")

    result = CliRunner().invoke(cli, ["sync", "bulk", "--products", "products.csv", "--approve-deletions"])

    assert result.exit_code == 2 and problem in " ".join(result.output.split())
    held = {(p.item_number, p.language_code) for p in client.products.iter_all()}
    assert held == before and not any(number == "None" for number, _ in held)  # nothing sent, nothing invented


def test_the_cli_refuses_a_mapping_without_one_of_its_key_values(tenant: str, client: AioticClient, tmp_path, monkeypatch) -> None:
    _cli_tenant(tenant, tmp_path, monkeypatch)
    before = client.customer_products.list(size=1).total
    (tmp_path / "mappings.csv").write_text("customer_number,customer_item_number,item_number,language_code\n58931,LT-ART-001,,nl\n", encoding="utf-8")

    result = CliRunner().invoke(cli, ["sync", "bulk", "--mappings", "mappings.csv", "--approve-deletions", "--approve-skipped"])

    assert result.exit_code == 2 and "mappings record 1: item_number is blank" in " ".join(result.output.split())
    assert client.customer_products.list(size=1).total == before


def _said(result) -> str:
    return " ".join(result.output.replace("│", " ").split())


@pytest.mark.parametrize(("value", "found"), [('["PROD-001"]', "a list"), ("true", "true/false"), ("4711", "a number"), ('{"no": "PROD-001"}', "an object")])
def test_the_cli_refuses_a_bulk_key_that_is_not_text(tenant: str, client: AioticClient, tmp_path, monkeypatch, value: str, found: str) -> None:
    _cli_tenant(tenant, tmp_path, monkeypatch)
    before = {(p.item_number, p.language_code) for p in client.products.iter_all()}
    (tmp_path / "products.json").write_text(
        '[{"item_number": "PROD-002", "language_code": "nl", "description": "LED Panel 60x60 40W"},'
        ' {"item_number": %s, "language_code": "nl", "description": "Article with a number that is not text"}]' % value,
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["sync", "bulk", "--products", "products.json", "--approve-deletions"])

    assert result.exit_code == 2 and "products record 2: item_number must be text" in _said(result) and f"found {found}" in _said(result)
    assert {(p.item_number, p.language_code) for p in client.products.iter_all()} == before  # nothing sent, nothing invented


def test_the_cli_refuses_a_bulk_language_or_customer_number_that_is_not_text(tenant: str, client: AioticClient, tmp_path, monkeypatch) -> None:
    _cli_tenant(tenant, tmp_path, monkeypatch)
    (tmp_path / "mappings.json").write_text('[{"customer_number": 58931, "customer_item_number": "LT-ART-001", "item_number": "PROD-001", "language_code": "nl"}]', encoding="utf-8")
    (tmp_path / "products.json").write_text('[{"item_number": "PROD-001", "language_code": ["nl", "de"], "description": "LED Driver 48V 100W"}]', encoding="utf-8")

    mappings = CliRunner().invoke(cli, ["sync", "bulk", "--mappings", "mappings.json", "--approve-deletions", "--approve-skipped"])
    products = CliRunner().invoke(cli, ["sync", "bulk", "--products", "products.json", "--approve-deletions"])

    assert mappings.exit_code == 2 and "mappings record 1: customer_number must be text" in _said(mappings)
    assert products.exit_code == 2 and "products record 1: language_code must be text" in _said(products)
    assert _mappings(client) == {("58931", "LT-ART-001")} and client.products.list(size=1).total == 3


def test_the_per_record_commands_refuse_a_key_that_is_not_text_and_read_a_whole_number_as_text(tenant: str, client: AioticClient, tmp_path, monkeypatch) -> None:
    _cli_tenant(tenant, tmp_path, monkeypatch)
    (tmp_path / "products.json").write_text('[{"item_number": ["PROD-900"], "language_code": "nl", "description": "Article with a list for a number"}]', encoding="utf-8")
    (tmp_path / "customers.json").write_text('[{"number": 58940, "name": "Example Handel GmbH"}, {"number": true, "name": "Not a number"}]', encoding="utf-8")
    (tmp_path / "one.json").write_text('[{"number": 58940, "name": "Example Handel GmbH"}]', encoding="utf-8")

    products = CliRunner().invoke(cli, ["sync", "products", "--from", "products.json", "--state", "state.db"])
    customers = CliRunner().invoke(cli, ["sync", "customers", "--from", "customers.json", "--state", "state.db"])
    one = CliRunner().invoke(cli, ["sync", "customers", "--from", "one.json", "--state", "state.db"])

    assert products.exit_code == 2 and "products record 1: item_number must be text" in _said(products)
    assert customers.exit_code == 2 and "customers record 2: number must be text" in _said(customers)
    assert one.exit_code == 0, one.output
    assert _numbers(client) == {"58931", "58940"} and client.products.list(size=1).total == 3


def test_the_cli_sends_the_columns_of_a_bulk_file_as_they_are(tenant: str, client: AioticClient, tmp_path, monkeypatch) -> None:
    _cli_tenant(tenant, tmp_path, monkeypatch)
    (tmp_path / "products.csv").write_text(
        "item_number,language_code,description,remark,created_at,source_updated_at,warehouse\n"
        "PROD-001,nl,LED Driver 48V 100W,,,,A\n"
        "PROD-001,de,LED-Treiber 48V 100W,,,,A\n"
        "PROD-100,de,LED-Band 5 m,Auf Rolle,2025-03-01T08:00:00+00:00,2026-11-02T09:30:00+01:00,B\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["sync", "bulk", "--products", "products.csv", "--approve-deletions"])

    assert result.exit_code == 0, result.output
    new = client.products.get("PROD-100", "de")  # the language of the file, not a default
    assert new.created_at == datetime(2025, 3, 1, 8, tzinfo=timezone.utc) and new.remark == "Auf Rolle"
    assert new.source_updated_at == datetime(2026, 11, 2, 8, 30, tzinfo=timezone.utc)
    assert not hasattr(new, "warehouse")  # a column the API does not know stays in the file
    assert {(p.item_number, p.language_code) for p in client.products.iter_all()} == {("PROD-001", "nl"), ("PROD-001", "de"), ("PROD-100", "de")}


def test_a_504_followed_by_a_connection_that_never_opens_is_still_an_unknown_outcome() -> None:
    requests, transport = _bulk_recorder([httpx.Response(504, text="gateway timeout"), httpx.ConnectError("connection refused")])
    with patch("aiotic.client.time.sleep", lambda _: None), AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=1, transport=transport) as c:
        with pytest.raises(AioticOutcomeUnknownError, match="may already be replaced"):  # the first attempt may have been stored
            c.customers.bulk_sync([LUMITECH, JANSEN])
    assert len(requests) == 2


def test_a_dropped_answer_followed_by_a_refusal_is_still_an_unknown_outcome() -> None:
    requests, transport = _bulk_recorder([httpx.ReadTimeout("no answer"), httpx.Response(401, json={"detail": "Invalid API key"})])
    with patch("aiotic.client.time.sleep", lambda _: None), AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=transport) as c:
        with pytest.raises(AioticOutcomeUnknownError) as unknown:
            c.products.bulk_sync(PRODUCTS)
    assert len(requests) == 2 and unknown.value.status == 401


def test_a_bulk_sync_that_was_never_sent_is_an_ordinary_transport_error() -> None:
    requests, transport = _bulk_recorder([httpx.ConnectError("connection refused")])
    with patch("aiotic.client.time.sleep", lambda _: None), AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=1, transport=transport) as c:
        with pytest.raises(AioticTransportError) as never_sent:
            c.customers.bulk_sync([LUMITECH, JANSEN])
    assert len(requests) == 2 and not isinstance(never_sent.value, AioticOutcomeUnknownError)


def test_the_async_client_reports_an_unanswered_earlier_attempt_too() -> None:
    requests, transport = _bulk_recorder([httpx.Response(504, text="gateway timeout"), httpx.ConnectError("connection refused")])

    async def run() -> None:
        async with AsyncAioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=1, transport=transport) as c:
            with pytest.raises(AioticOutcomeUnknownError):
                await c.customers.bulk_sync([LUMITECH, JANSEN])

    async def no_sleep(_: float) -> None:
        return None

    with patch("aiotic.aio.asyncio.sleep", no_sleep):
        asyncio.run(run())
    assert len(requests) == 2
