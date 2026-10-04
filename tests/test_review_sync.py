"""Regression tests for the first-release review, sync side: CSV/JSON input validation, reconciliation that never
deletes from an invalid or unexpectedly empty data set, and per-record ordering in batches."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from aiotic import AioticClient
from aiotic.cli import _check_columns, _load, _read_rows, app as cli
from aiotic.errors import AioticError
from aiotic.sync import ChangeEvent, InMemoryStateStore, PollingChangeSource, SyncEngine


class _Rows:
    def __init__(self, delays: dict | None = None, fail: set | None = None) -> None:
        self.rows: dict[tuple[str, ...], dict] = {}
        self.calls: list[tuple[str, tuple[str, ...], dict | None]] = []
        self.delays = delays or {}
        self.fail = fail or set()
        self._lock = threading.Lock()

    def upsert(self, *key_and_data):
        *key, data = key_and_data
        key = tuple(key)
        time.sleep(self.delays.get(data.get("name") or data.get("description"), 0))
        if key in self.fail:
            raise AioticError("simulated failure", status=500)
        with self._lock:
            self.calls.append(("upsert", key, data))
            self.rows[key] = data

    def delete(self, *key):
        key = tuple(key)
        with self._lock:
            self.calls.append(("delete", key, None))
            self.rows.pop(key, None)


class FakeClient:
    def __init__(self, **kw) -> None:
        self.customers, self.products, self.customer_products = _Rows(**kw), _Rows(), _Rows()


# ---- 3a. reading files ------------------------------------------------------------------------------------------


def _write(tmp_path: Path, name: str, text: str, encoding: str = "utf-8") -> Path:
    p = tmp_path / name
    p.write_text(text, encoding=encoding)
    return p


@pytest.mark.parametrize(
    "text,encoding,expected",
    [
        ("number,name\nC1,Example\n", "utf-8", [{"number": "C1", "name": "Example"}]),
        ("number,name\nC1,Example\nC2,Second\n", "utf-8", [{"number": "C1", "name": "Example"}, {"number": "C2", "name": "Second"}]),
        ("number;name\nC1;Example\n", "utf-8", [{"number": "C1", "name": "Example"}]),
        ("number,name\nC1,Example\n", "utf-8-sig", [{"number": "C1", "name": "Example"}]),  # byte order mark
        ("number,name\n", "utf-8", []),  # header only: a valid, empty data set
    ],
)
def test_csv_rows_and_columns(tmp_path, text, encoding, expected) -> None:
    rows, columns = _read_rows(_write(tmp_path, "f.csv", text, encoding))
    assert rows == expected and columns == ["number", "name"]


def test_json_shapes(tmp_path) -> None:
    rows, cols = _read_rows(_write(tmp_path, "a.json", '[{"number": "C1", "name": "A"}]'))
    assert rows == [{"number": "C1", "name": "A"}] and cols == ["name", "number"]
    rows, _ = _read_rows(_write(tmp_path, "b.json", '{"items": [{"number": "C2"}]}'))
    assert rows == [{"number": "C2"}]
    for bad in ('{"number": "C1"', '{"foo": 1}', "[1, 2]", '"text"'):
        with pytest.raises(typer.BadParameter):
            _read_rows(_write(tmp_path, "bad.json", bad))


def test_invalid_files_and_missing_columns_are_usage_errors(tmp_path) -> None:
    with pytest.raises(typer.BadParameter, match="empty file"):
        _read_rows(_write(tmp_path, "empty.csv", ""))
    with pytest.raises(typer.BadParameter, match="malformed header"):
        _read_rows(_write(tmp_path, "header.csv", "number,,name\nC1,x,y\n"))
    with pytest.raises(typer.BadParameter, match="needs the column"):
        _check_columns("customers", ["name", "city"], Path("c.csv"))
    with pytest.raises(typer.BadParameter, match="needs the column"):
        _check_columns("products", ["item_number"], Path("p.csv"))
    _check_columns("customers", ["customer_number", "name"], Path("c.csv"))
    events = _load("customers", _write(tmp_path, "ok.csv", "number,name\nC1,Example\n"))
    assert len(events) == 1 and events[0].key == ("C1",)


# ---- 3b. the CLI-to-reconcile path, against the mock tenant ------------------------------------------------------


def test_cli_reconcile_never_deletes_from_invalid_or_empty_input(tmp_path, monkeypatch, mock_url: str, client: AioticClient) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AIOTIC_BASE_URL", mock_url)
    monkeypatch.setenv("AIOTIC_API_KEY", "mock-integration-key")
    monkeypatch.setenv("AIOTIC_RATE_LIMIT", "0")
    state = str(tmp_path / "state.db")
    runner = CliRunner(env={"COLUMNS": "250"})  # keep Typer's boxed error messages on one line

    seeded = _write(tmp_path, "customers.csv", "number,name\nRV-C1,Review BV\n")  # one row, comma separated
    result = runner.invoke(cli, ["sync", "customers", "--from", str(seeded), "--state", state])
    assert result.exit_code == 0, result.output
    assert client.customers.get("RV-C1").name == "Review BV"

    broken = _write(tmp_path, "broken.csv", "name,city\nReview BV,Utrecht\n")  # no key column
    result = runner.invoke(cli, ["sync", "reconcile", "--customers", str(broken), "--state", state])
    assert result.exit_code == 2 and "needs the column" in " ".join(result.output.split())
    assert client.customers.get("RV-C1").name == "Review BV"

    empty = _write(tmp_path, "empty.csv", "number,name\n")
    result = runner.invoke(cli, ["sync", "reconcile", "--customers", str(empty), "--state", state])
    assert result.exit_code == 1 and "refusing to delete" in " ".join(result.output.split())
    assert client.customers.get("RV-C1").name == "Review BV"

    result = runner.invoke(cli, ["sync", "reconcile", "--customers", str(empty), "--state", state, "--allow-empty"])
    assert result.exit_code == 0 and "deleted=1" in result.output, result.output
    with pytest.raises(Exception):
        client.customers.get("RV-C1")


# ---- 5. ordering per record, dependency order across kinds, watermark -----------------------------------------


def _engine(fake: FakeClient, **kw) -> SyncEngine:
    return SyncEngine(fake, state=InMemoryStateStore(), **kw)


def test_delete_then_upsert_ends_present_and_upsert_then_delete_ends_absent() -> None:
    fake = FakeClient()
    engine = _engine(fake, concurrency=4)
    engine.apply(ChangeEvent.customer_upsert("C1", name="Old"))
    fake.customers.calls.clear()
    rep = engine.apply_many([ChangeEvent.customer_delete("C1"), ChangeEvent.customer_upsert("C1", name="Recreated")])
    assert rep.failed == 0 and [c[0] for c in fake.customers.calls] == ["delete", "upsert"]
    assert fake.customers.rows[("C1",)]["name"] == "Recreated"
    rep = engine.apply_many([ChangeEvent.customer_upsert("C1", name="Updated"), ChangeEvent.customer_delete("C1")])
    assert rep.failed == 0 and ("C1",) not in fake.customers.rows


def test_repeated_updates_to_one_record_keep_their_order_even_when_the_first_is_slow() -> None:
    fake = FakeClient(delays={"v1": 0.3})  # the first update would finish last if both ran concurrently
    engine = _engine(fake, concurrency=8)
    rep = engine.apply_many([ChangeEvent.customer_upsert("C1", name="v1"), ChangeEvent.customer_upsert("C1", name="v2"), ChangeEvent.customer_upsert("C1", name="v3")])
    assert rep.sent == 3 and [c[2]["name"] for c in fake.customers.calls] == ["v1", "v2", "v3"]
    assert fake.customers.rows[("C1",)]["name"] == "v3"
    assert engine.state.get("customer:C1") == ChangeEvent.customer_upsert("C1", name="v3").fingerprint


def test_dependency_order_holds_for_upserts_and_deletes_in_one_batch() -> None:
    fake = FakeClient()
    engine = _engine(fake, concurrency=1)
    engine.apply_many(
        [
            ChangeEvent.mapping_upsert("C9", "THEIRS", item_number="P9", language_code="nl"),
            ChangeEvent.product_upsert("P9", "nl", description="Nine"),
            ChangeEvent.customer_upsert("C9", name="Nine BV"),
        ]
    )
    order = [c for c in [*fake.customers.calls, *fake.products.calls, *fake.customer_products.calls]]
    assert [fake.customers.calls[0][1], fake.products.calls[0][1], fake.customer_products.calls[0][1]] == [("C9",), ("P9", "nl"), ("C9", "THEIRS")]
    timeline: list[str] = []
    for res, name in ((fake.customers, "customer"), (fake.products, "product"), (fake.customer_products, "mapping")):
        res.calls.clear()
        original = res.delete

        def record(*key, _res=res, _name=name, _original=original):
            timeline.append(_name)
            return _original(*key)

        res.delete = record  # type: ignore[method-assign]
    engine.apply_many([ChangeEvent.customer_delete("C9"), ChangeEvent.product_delete("P9", "nl"), ChangeEvent.mapping_delete("C9", "THEIRS")])
    assert timeline == ["mapping", "product", "customer"]
    assert order  # the upsert phase did run


def test_batch_counters_are_exact_under_concurrency_and_watermark_waits_for_a_clean_batch() -> None:
    fake = FakeClient(fail={("C-FAIL",)})
    engine = _engine(fake, concurrency=8)
    rep = engine.apply_many([ChangeEvent.customer_upsert(f"C{i}", name=str(i)) for i in range(60)])
    assert (rep.sent, rep.failed) == (60, 0)

    batches = [[ChangeEvent.customer_upsert("C-FAIL", name="x"), ChangeEvent.customer_upsert("C-OK", name="y")], [ChangeEvent.customer_upsert("C-OK", name="z")]]

    def fetch(watermark: str | None):
        return batches.pop(0), f"wm-{len(batches)}"

    source = PollingChangeSource("customers", fetch, engine)
    first = source.run_once()
    assert first.failed == 1 and engine.state.get_watermark("customers") is None  # one failure: the watermark stays
    second = source.run_once()
    assert second.failed == 0 and engine.state.get_watermark("customers") == "wm-0"


def test_reconcile_refuses_an_empty_snapshot_unless_allowed() -> None:
    fake = FakeClient()
    engine = _engine(fake)
    engine.apply(ChangeEvent.customer_upsert("C1", name="Example"))
    rep = engine.reconcile(customers=[])
    assert rep.deleted == 0 and rep.failed == 1 and "refusing to delete" in rep.errors[0] and ("C1",) in fake.customers.rows
    rep = engine.reconcile(customers=[], allow_empty=True)
    assert rep.deleted == 1 and rep.failed == 0 and ("C1",) not in fake.customers.rows
    rep = engine.reconcile(customers=[])  # nothing left in the state: an empty set is simply empty
    assert rep.failed == 0 and rep.deleted == 0
