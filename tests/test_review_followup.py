"""Second review round: a 503 never replays a request the API does not declare idempotent, ambiguous CSV input never
reaches the tenant, and a batch that creates and removes dependent records succeeds in source order."""

from __future__ import annotations

import asyncio
import re
import uuid
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
import typer
from typer.testing import CliRunner

from aiotic import AioticClient, AioticUnavailableError, AsyncAioticClient
from aiotic.cli import _read_rows, app as cli
from aiotic.errors import AioticConflictError, AioticNotFoundError
from aiotic.sync import ChangeEvent as E, InMemoryStateStore, PollingChangeSource, SyncEngine

# ---- 503 is not proof that nothing happened ----------------------------------------------------------------------


def _accepts_then_503():
    """Every attempt is accepted by the backend; the first answer is a 503 from an intermediary, the second a 200."""
    accepted: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        accepted.append(request.read())
        if len(accepted) == 1:
            return httpx.Response(503, json={"detail": "upstream connection reset after the request was accepted"})
        return httpx.Response(200, json={"request_id": str(uuid.uuid4()), "split": False})

    return accepted, httpx.MockTransport(handler)


def _client(transport: httpx.BaseTransport) -> AioticClient:
    return AioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=transport)


def test_raw_upload_is_not_replayed_after_a_503() -> None:
    accepted, transport = _accepts_then_503()
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        with pytest.raises(AioticUnavailableError):
            c.orders.upload_raw_email(("mail.eml", b"Subject: order"))
    assert len(accepted) == 1


def test_async_raw_upload_is_not_replayed_after_a_503() -> None:
    async def run() -> int:
        accepted, transport = _accepts_then_503()

        async def no_delay(_: float) -> None:
            return None

        with patch("aiotic.aio.asyncio.sleep", no_delay):
            async with AsyncAioticClient(base_url="https://example.invalid", api_key="test-only", rate_limit=0, max_retries=2, transport=transport) as c:
                with pytest.raises(AioticUnavailableError):
                    await c.orders.upload_raw_email(("mail.eml", b"Subject: order"))
        return len(accepted)

    assert asyncio.run(run()) == 1


def test_other_non_idempotent_posts_surface_a_503_while_keyed_uploads_and_gets_retry() -> None:
    for call in (lambda c: c.orders.retry(uuid.uuid4()), lambda c: c.rejected.reprocess(uuid.uuid4()), lambda c: c.mailbox.fetch_all()):
        accepted, transport = _accepts_then_503()
        with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
            with pytest.raises(AioticUnavailableError):
                call(c)
        assert len(accepted) == 1
    accepted, transport = _accepts_then_503()  # a document upload carries its request_id: the replay is the same order
    with patch("aiotic.client.time.sleep", lambda _: None), _client(transport) as c:
        c.orders.upload([("po.pdf", b"fixture")])
    ids = [re.search(rb'name="request_id"\r\n\r\n([^\r]+)', b).group(1) for b in accepted]  # type: ignore[union-attr]
    assert len(accepted) == 2 and ids[0] == ids[1]
    seen: list[httpx.Request] = []

    def get_503_then_ok(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(503) if len(seen) == 1 else httpx.Response(200, json={"items": [], "total": 0, "limit": 100, "offset": 0})

    with patch("aiotic.client.time.sleep", lambda _: None), _client(httpx.MockTransport(get_503_then_ok)) as c:
        c.orders.list()
    assert len(seen) == 2


# ---- ambiguous CSV input never reaches the tenant ---------------------------------------------------------------


@pytest.mark.parametrize(
    "text,message",
    [
        ("number,number,name\nCSV-C1,CSV-C2,Changed\n", "duplicate column"),
        ("Number, number ,name\nCSV-C1,CSV-C2,Changed\n", "duplicate column"),  # duplicates after trimming, ignoring case
        ("number,name\nCSV-C1,Example,extra\n", "has 3 field"),  # an overflow value must not be dropped silently
        ("number,name\nCSV-C1\n", "has 1 field"),
    ],
)
def test_ambiguous_csv_is_a_usage_error(tmp_path: Path, text: str, message: str) -> None:
    f = tmp_path / "f.csv"
    f.write_text(text, encoding="utf-8")
    with pytest.raises(typer.BadParameter, match=message):
        _read_rows(f)


@pytest.mark.parametrize(
    "text,message",
    [
        ('number,name\nQUOTED-C1,"First\nQUOTED-C2,Second\n', "unexpected end of data"),  # an unterminated quote would swallow the next row
        ('number,name\nQUOTED-C1,"First"garbage\n', "expected after"),  # characters after a closing quote
    ],
)
def test_malformed_quoting_is_a_usage_error(tmp_path: Path, text: str, message: str) -> None:
    f = tmp_path / "f.csv"
    f.write_text(text, encoding="utf-8")
    with pytest.raises(typer.BadParameter, match=message):
        _read_rows(f)


def test_correct_quoting_still_works(tmp_path: Path) -> None:
    f = tmp_path / "f.csv"
    f.write_text('number,name\nQ1,"First\nline two"\nQ2,"Doe, John"\nQ3,"Say ""hi"""\n', encoding="utf-8")
    rows, _ = _read_rows(f)
    assert [r["name"] for r in rows] == ["First\nline two", "Doe, John", 'Say "hi"']
    g = tmp_path / "g.csv"
    g.write_text('number;name\nQ1;"a;b"\n', encoding="utf-8-sig")  # semicolon file with a byte order mark
    assert _read_rows(g)[0] == [{"number": "Q1", "name": "a;b"}]


def test_duplicate_json_keys_are_a_usage_error(tmp_path: Path) -> None:
    f = tmp_path / "f.json"
    f.write_text('[{"number": "CSV-C1", "number": "CSV-C2", "name": "x"}]', encoding="utf-8")
    with pytest.raises(typer.BadParameter, match="duplicate key"):
        _read_rows(f)
    f.write_text("number,name\nCSV-C1,Example\n\nCSV-C2,Second\n", encoding="utf-8")  # blank lines are fine
    f2 = tmp_path / "ok.csv"
    f2.write_text("number,name\nCSV-C1,Example\n\nCSV-C2,Second\n", encoding="utf-8")
    rows, _ = _read_rows(f2)
    assert [r["number"] for r in rows] == ["CSV-C1", "CSV-C2"]


def test_cli_reconcile_with_a_duplicate_identity_header_changes_nothing(tmp_path: Path, monkeypatch, mock_url: str, client: AioticClient) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AIOTIC_BASE_URL", mock_url)
    monkeypatch.setenv("AIOTIC_API_KEY", "mock-integration-key")
    monkeypatch.setenv("AIOTIC_RATE_LIMIT", "0")
    runner = CliRunner(env={"COLUMNS": "250"})
    state = str(tmp_path / "state.db")
    good = tmp_path / "good.csv"
    good.write_text("number,name\nCSV-C1,First\nCSV-C2,Second\n")
    assert runner.invoke(cli, ["sync", "customers", "--from", str(good), "--state", state]).exit_code == 0
    bad = tmp_path / "bad.csv"
    bad.write_text("number,number,name\nCSV-C1,CSV-C2,Changed\n")
    result = runner.invoke(cli, ["sync", "reconcile", "--customers", str(bad), "--state", state])
    assert result.exit_code == 2 and "duplicate column" in " ".join(result.output.split())
    assert client.customers.get("CSV-C1").name == "First" and client.customers.get("CSV-C2").name == "Second"
    for number in ("CSV-C1", "CSV-C2"):
        client.customers.delete(number)


@pytest.mark.parametrize("text", ['number,name\nQUOTED-C1,"First\nQUOTED-C2,Second\n', 'number,name\nQUOTED-C1,"First"garbage\n'])
def test_cli_reconcile_with_malformed_quoting_changes_nothing(tmp_path: Path, monkeypatch, mock_url: str, client: AioticClient, text: str) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AIOTIC_BASE_URL", mock_url)
    monkeypatch.setenv("AIOTIC_API_KEY", "mock-integration-key")
    monkeypatch.setenv("AIOTIC_RATE_LIMIT", "0")
    runner = CliRunner(env={"COLUMNS": "250"})
    state = str(tmp_path / "state.db")
    good = tmp_path / "good.csv"
    good.write_text("number,name\nQUOTED-C1,First\nQUOTED-C2,Second\n")
    assert runner.invoke(cli, ["sync", "customers", "--from", str(good), "--state", state]).exit_code == 0
    bad = tmp_path / "bad.csv"
    bad.write_text(text)
    result = runner.invoke(cli, ["sync", "reconcile", "--customers", str(bad), "--state", state])
    assert result.exit_code == 2 and "malformed CSV" in " ".join(result.output.split())
    assert client.customers.get("QUOTED-C1").name == "First" and client.customers.get("QUOTED-C2").name == "Second"
    for number in ("QUOTED-C1", "QUOTED-C2"):
        client.customers.delete(number)


# ---- mixed lifecycles respect dependencies in source order ------------------------------------------------------


class _StrictRows:
    """A master-data resource that refuses children without parents and parents with children, as a strict tenant would."""

    def __init__(self, parent_of=None) -> None:
        self.rows: dict[tuple[str, ...], dict] = {}
        self.calls: list[tuple[str, tuple[str, ...]]] = []
        self.parent_of = parent_of  # callable(key, data) -> list of (resource, key) that must exist
        self.children: list["_StrictRows"] = []  # resources whose rows reference this one

    def upsert(self, *key_and_data):
        *key, data = key_and_data
        key = tuple(key)
        for resource, parent in (self.parent_of(key, data) if self.parent_of else []):
            if parent not in resource.rows:
                raise AioticNotFoundError(f"{parent} does not exist", status=404)
        self.calls.append(("upsert", key))
        self.rows[key] = data

    def delete(self, *key):
        key = tuple(key)
        for child in self.children:
            if any(key in child.parent_keys(row_key, row) for row_key, row in child.rows.items()):
                raise AioticConflictError(f"{key} is still referenced", status=409)
        self.calls.append(("delete", key))
        self.rows.pop(key, None)

    def parent_keys(self, key, data):
        return [parent for _, parent in (self.parent_of(key, data) if self.parent_of else [])]


class StrictFakeClient:
    def __init__(self) -> None:
        self.customers = _StrictRows()
        self.products = _StrictRows()
        self.customer_products = _StrictRows(lambda key, data: [(self.customers, (key[0],)), (self.products, (data["item_number"], data["language_code"]))])
        self.customers.children.append(self.customer_products)
        self.products.children.append(self.customer_products)

    def timeline(self) -> list[tuple[str, str]]:
        return [(name, op) for res, name in ((self.customers, "customer"), (self.products, "product"), (self.customer_products, "mapping")) for op, _ in res.calls]


LIFECYCLE = [
    E.customer_upsert("EPHEMERAL", name="Temporary"),
    E.product_upsert("EPHEMERAL", "nl", description="Temporary"),
    E.mapping_upsert("EPHEMERAL", "X", item_number="EPHEMERAL", language_code="nl"),
    E.mapping_delete("EPHEMERAL", "X"),
    E.product_delete("EPHEMERAL", "nl"),
    E.customer_delete("EPHEMERAL"),
]


def test_mixed_lifecycle_batch_completes_against_an_enforcing_tenant_and_advances_the_watermark() -> None:
    fake = StrictFakeClient()
    engine = SyncEngine(fake, state=InMemoryStateStore(), concurrency=4)
    source = PollingChangeSource("review", lambda wm: (LIFECYCLE, "next"), engine)
    reports = [source.run_once() for _ in range(2)]
    assert all(r.failed == 0 for r in reports), [r.errors for r in reports]
    assert engine.state.get_watermark("review") == "next"
    assert not fake.customers.rows and not fake.products.rows and not fake.customer_products.rows
    calls = [(res, op) for res, op in fake.timeline()]
    assert calls.count(("mapping", "upsert")) == 2 and calls.count(("customer", "delete")) == 2  # both rounds ran fully


def test_mixed_lifecycle_batch_completes_against_the_mock_tenant(client: AioticClient) -> None:
    engine = SyncEngine(client, state=InMemoryStateStore(), concurrency=1)
    source = PollingChangeSource("review", lambda wm: (LIFECYCLE, "next"), engine)
    reports = [source.run_once() for _ in range(2)]
    assert all(r.failed == 0 for r in reports), [r.errors for r in reports]
    assert engine.state.get_watermark("review") == "next"
    with pytest.raises(AioticNotFoundError):
        client.customers.get("EPHEMERAL")


def test_source_order_wins_for_dependent_events_and_phases_order_the_rest() -> None:
    fake = StrictFakeClient()
    engine = SyncEngine(fake, state=InMemoryStateStore(), concurrency=4)
    # parents arrive after the child in the batch: the upsert phases still send customer and product first
    rep = engine.apply_many([E.mapping_upsert("C9", "X", item_number="P9", language_code="nl"), E.product_upsert("P9", "nl", description="Nine"), E.customer_upsert("C9", name="Nine BV")])
    assert rep.failed == 0 and ("C9", "X") in fake.customer_products.rows
    # deletes given parent-first: the delete phases still remove the mapping before its parents
    rep = engine.apply_many([E.customer_delete("C9"), E.product_delete("P9", "nl"), E.mapping_delete("C9", "X")])
    assert rep.failed == 0 and not fake.customers.rows and not fake.products.rows
    # a record's own events keep their order even when a parent is involved: create, mapping, delete mapping, re-create mapping
    rep = engine.apply_many(
        [E.customer_upsert("C1", name="One"), E.product_upsert("P1", "nl", description="One"), E.mapping_upsert("C1", "A", item_number="P1", language_code="nl"), E.mapping_delete("C1", "A"), E.mapping_upsert("C1", "A", item_number="P1", language_code="nl")]
    )
    assert rep.failed == 0 and ("C1", "A") in fake.customer_products.rows
    assert [op for op, key in fake.customer_products.calls if key == ("C1", "A")] == ["upsert", "delete", "upsert"]
