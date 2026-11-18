"""`aiotic` command line — bootstrapping and day-to-day operations.

  aiotic init                 write .env + a service skeleton in the current directory
  aiotic doctor               check base URL, keys, health, and that master data is present
  aiotic orders list|get|watch|send
  aiotic sync customers|products|mappings --from file.csv   (upsert only what changed)
  aiotic sync reconcile --customers a.csv --products b.csv --mappings c.csv
  aiotic serve                run the integration service (receive endpoint + webhooks)
  aiotic mock                 run a local mock AIOTIC tenant on :8080
"""

from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path
from typing import Any

try:
    import typer
    from rich import print as rprint
    from rich.table import Table
except ImportError:  # pragma: no cover
    print("The CLI needs the 'cli' extra: pip install 'aiotic-sdk[cli]'", file=sys.stderr)
    raise SystemExit(2)

from pydantic import ValidationError

from . import __version__
from .client import AioticClient
from .config import Settings
from .errors import AioticError

app = typer.Typer(help="AIOTIC integration toolkit", no_args_is_help=True, add_completion=False)
orders_app = typer.Typer(help="Work with orders")
sync_app = typer.Typer(help="Sync master data (only changed records are sent)")
app.add_typer(orders_app, name="orders")
app.add_typer(sync_app, name="sync")


def _client() -> AioticClient:
    try:
        return AioticClient(settings=Settings.from_env())
    except ValueError as exc:
        rprint(f"[red]{exc}[/red]")
        raise typer.Exit(2)


ENV_TEMPLATE = """# AIOTIC integration service — configuration
AIOTIC_BASE_URL={base_url}
AIOTIC_API_KEY={api_key}
# Optional: a separate key for master-data sync jobs (customers/products/mappings only)
AIOTIC_SYNC_API_KEY=
# The key AIOTIC sends in X-API-KEY when calling YOUR receive endpoint (give this value to AIOTIC)
AIOTIC_ERP_RECEIVE_KEY={receive_key}
# The key AIOTIC sends to your processing webhook (optional feature)
AIOTIC_WEBHOOK_KEY=
# The key YOUR ERP sends when it posts change events to /erp/events (optional; defaults to the receive key)
AIOTIC_ERP_EVENTS_KEY=
AIOTIC_RATE_LIMIT=10
"""

SERVICE_TEMPLATE = '''"""Your AIOTIC integration service. Run: uvicorn service:app --port 9000"""
from aiotic.service import build_app
from aiotic.erp.memory import InMemoryErp          # replace with your adapter (see aiotic.erp.functional_api / data_api)

erp = InMemoryErp()
app = build_app(erp=erp)
'''


@app.command()
def version() -> None:
    rprint(f"aiotic-sdk {__version__}")


@app.command()
def init(
    base_url: str = typer.Option("http://localhost:8080", prompt="AIOTIC tenant base URL"),
    api_key: str = typer.Option("mock-integration-key", prompt="Integration API key"),
    directory: Path = typer.Option(Path("."), help="Where to write .env and service.py"),
) -> None:
    """Bootstrap a new integration service (.env + service.py)."""
    import secrets

    directory.mkdir(parents=True, exist_ok=True)
    env_path = directory / ".env"
    if env_path.exists():
        rprint(f"[yellow]{env_path} exists — not overwriting[/yellow]")
    else:
        env_path.write_text(ENV_TEMPLATE.format(base_url=base_url.rstrip("/"), api_key=api_key, receive_key=secrets.token_urlsafe(24)))
        rprint(f"[green]wrote {env_path}[/green]")
    svc = directory / "service.py"
    if not svc.exists():
        svc.write_text(SERVICE_TEMPLATE)
        rprint(f"[green]wrote {svc}[/green]")
    rprint("\nNext: [bold]aiotic doctor[/bold], then [bold]aiotic serve[/bold] (or: uvicorn service:app --port 9000)")


@app.command()
def doctor() -> None:
    """Check connectivity, keys and master-data presence."""
    s = Settings.from_env()
    ok = True
    t = Table(title="aiotic doctor", show_lines=False)
    t.add_column("check")
    t.add_column("result")

    def row(name: str, good: bool, detail: str) -> None:
        nonlocal ok
        ok &= good
        t.add_row(name, ("[green]OK[/green] " if good else "[red]FAIL[/red] ") + detail)

    row("AIOTIC_BASE_URL", bool(s.base_url), s.base_url or "missing")
    row("AIOTIC_API_KEY", bool(s.api_key), "set" if s.api_key else "missing")
    row("AIOTIC_ERP_RECEIVE_KEY", bool(s.erp_receive_key), "set" if s.erp_receive_key else "missing (the service refuses to start without it)")
    if s.base_url and s.api_key:
        try:
            c = AioticClient(settings=s)
            h = c.health()
            row("GET /healthcheck", h.status == "ok", h.status)
            st = c.system_status()
            row("GET /system-status", st.status == "operational", f"{st.status} {st.message or ''}".strip())
            try:
                lst = c.orders.list(size=1)
                row("GET /order_status/list (key valid)", True, f"{lst.total} orders")
            except AioticError as exc:
                row("GET /order_status/list (key valid)", False, str(exc))
            for name, fn in (("customers", lambda: c.customers.list(size=1).total), ("products", lambda: c.products.list(size=1).total), ("mappings", lambda: c.customer_products.list(size=1).total)):
                try:
                    n = fn()
                    row(f"master data: {name}", n > 0, f"{n} records" if n else "0 records — sync before go-live")
                except AioticError as exc:
                    row(f"master data: {name}", False, str(exc))
        except AioticError as exc:
            row("connection", False, str(exc))
    rprint(t)
    raise typer.Exit(0 if ok else 1)


@orders_app.command("list")
def orders_list(size: int = 20, page: int = 1) -> None:
    c = _client()
    res = c.orders.list(page=page, size=size)
    t = Table(title=f"orders ({res.total} total)")
    for col in ("request_id", "status", "order_number", "customer_id", "erp_ref", "timestamp"):
        t.add_column(col)
    for o in res.items:
        r = o.result
        t.add_row(str(o.request_id), o.status, r.order_number if r else "", (r.customer.customer_id if r and r.customer else "") or "", o.erp_ref or "", o.timestamp.isoformat(timespec="minutes"))
    rprint(t)


@orders_app.command("get")
def orders_get(request_id: str) -> None:
    c = _client()
    o = c.orders.get(request_id)
    rprint(json.dumps(o.model_dump(mode="json"), indent=2, ensure_ascii=False))


@orders_app.command("upload")
def orders_upload(files: list[Path], request_id: str | None = None, wait: bool = True) -> None:
    """Upload one order (1..n files) and optionally wait for it to land."""
    c = _client()
    up = c.orders.upload([str(f) for f in files], request_id=request_id)
    rprint(f"uploaded → request_id [bold]{up.request_id}[/bold]" + (" (split)" if up.split else ""))
    if wait:
        for rid in up.request_ids:
            st = c.orders.wait(rid)
            rprint(f"{rid}: [bold]{st.status}[/bold]" + (f" — {st.result.order_number}, {len(st.result.items)} lines" if st.result else ""))


@orders_app.command("send")
def orders_send(request_id: str) -> None:
    c = _client()
    try:
        res = c.erp.send(request_id)
        rprint(f"[green]sent[/green] — ERP reference {res.erp_order_number}")
    except AioticError as exc:
        rprint(f"[red]{exc}[/red]")
        raise typer.Exit(1)


@orders_app.command("watch")
def orders_watch(interval: float = 15.0, state: str = "aiotic-watch-state.db") -> None:
    """Print status transitions as they happen (the polling-based notification mechanism)."""
    from .watch import OrderWatcher, SqliteWatchState, transition_to_json

    c = _client()
    rprint(f"watching {c.settings.base_url} every {interval}s … (Ctrl-C to stop)")
    OrderWatcher(c, on_transition=lambda t: rprint(transition_to_json(t)), state=SqliteWatchState(state), interval=interval).run_forever()


REQUIRED_COLUMNS: dict[str, tuple[set[str], ...]] = {
    "customers": ({"number", "name"}, {"customer_number", "name"}),
    "products": ({"item_number", "description"},),
    "mappings": ({"customer_number", "customer_item_number", "item_number"},),
}


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: set[str] = set()
    for key, _ in pairs:
        if key in seen:
            raise ValueError(f"duplicate key {key!r} in one record")
        seen.add(key)
    return dict(pairs)


def _read_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Rows and column names from a CSV (comma or semicolon separated, UTF-8 with or without BOM) or a JSON file
    (a list of records, or ``{"items": [...]}``).

    Anything ambiguous is a usage error, never an empty or reshaped data set: a header with duplicate names (after
    trimming, ignoring case), a row with more or fewer fields than the header, malformed quoting (an unterminated
    quoted field would swallow the rows after it), a JSON record with a duplicate key. Each of those could change
    which record a value belongs to, which matters when the result drives deletions.
    """
    if path.suffix.lower() == ".json":
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"), object_pairs_hook=_no_duplicate_keys)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"{path}: not valid JSON ({exc.msg} at line {exc.lineno})")
        except ValueError as exc:
            raise typer.BadParameter(f"{path}: {exc}")
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            data = data["items"]
        if not isinstance(data, list) or not all(isinstance(r, dict) for r in data):
            raise typer.BadParameter(f'{path}: expected a JSON list of records or {{"items": [...]}}')
        return data, sorted({k for r in data for k in r})
    with path.open(newline="", encoding="utf-8-sig") as fh:
        head = fh.readline()
        if not head.strip():
            raise typer.BadParameter(f"{path}: empty file, no header row")
        delimiter = ";" if head.count(";") > head.count(",") else ","
        fh.seek(0)
        reader = csv.reader(fh, delimiter=delimiter, strict=True)  # strict: bad quoting is an error, not a repair
        rows: list[dict[str, Any]] = []
        try:
            columns = [c.strip() for c in next(reader)]
            if not columns or any(not c for c in columns):
                raise typer.BadParameter(f"{path}: malformed header row: {head.strip()!r}")
            normalized = [c.casefold() for c in columns]
            duplicates = sorted({c for c in normalized if normalized.count(c) > 1})
            if duplicates:
                raise typer.BadParameter(f"{path}: duplicate column(s) in the header: {', '.join(duplicates)}")
            for values in reader:
                if not values or (len(values) == 1 and not values[0].strip()):
                    continue  # a blank line
                if len(values) != len(columns):
                    raise typer.BadParameter(f"{path}: line {reader.line_num} has {len(values)} field(s), the header has {len(columns)}")
                rows.append(dict(zip(columns, values)))
        except csv.Error as exc:
            raise typer.BadParameter(f"{path}: malformed CSV near line {reader.line_num}: {exc}")
        return rows, columns


# A bulk sync replaces the data set, so nothing about a record's key is filled in for the file: the language too
# must be in it.
BULK_REQUIRED_COLUMNS: dict[str, tuple[set[str], ...]] = {
    "customers": REQUIRED_COLUMNS["customers"],
    "products": ({"item_number", "language_code", "description"},),
    "mappings": ({"customer_number", "customer_item_number", "item_number", "language_code"},),
}
KEY_COLUMNS = {
    "customers": ("number",),
    "products": ("item_number", "language_code"),
    "mappings": ("customer_number", "customer_item_number", "item_number", "language_code"),
}


def _check_columns(kind: str, columns: list[str], source: Path, required: dict[str, tuple[set[str], ...]] = REQUIRED_COLUMNS) -> None:
    options = required[kind]
    if not any(req <= set(columns) for req in options):
        wanted = " or ".join(", ".join(sorted(o)) for o in options)
        raise typer.BadParameter(f"{source}: a {kind} file needs the column(s) {wanted}; found: {', '.join(columns) or 'none'}")


def _load(kind: str, source: Path) -> list[Any]:
    rows, columns = _read_rows(source)
    _check_columns(kind, columns, source)
    return _events(kind, rows)


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _found(value: Any) -> str:
    if isinstance(value, bool):
        return "true/false"
    return {int: "a number", float: "a number", list: "a list", dict: "an object"}.get(type(value), type(value).__name__)


def _key(record: dict[str, Any], column: str, kind: str, position: int, *, text_only: bool = False) -> str:
    """A value that identifies a record. A blank one is an error, and so is one that is not text: neither may be
    sent as some other text. A whole number has one way to write it and is accepted, except in a bulk sync
    (``text_only``), where a file is sent as it is."""
    value = record.get(column)
    if _blank(value):
        raise typer.BadParameter(f"{kind} record {position}: {column} is blank")
    if isinstance(value, str):
        return value
    if text_only or isinstance(value, bool) or not isinstance(value, int):
        raise typer.BadParameter(f'{kind} record {position}: {column} must be text (in a JSON file: "…" in quotes), found {_found(value)}')
    return str(value)


def _bulk_rows(kind: str, source: Path) -> list[dict[str, Any]]:
    """The records of a file as bulk sync rows: every column the API knows, as it is in the file. Nothing is
    filled in, and a record without one of its key values stops the run."""
    from . import models as m

    model = {"customers": m.CustomerBulkRow, "products": m.ProductBulkRow, "mappings": m.CustomerProductBulkRow}[kind]
    rows, columns = _read_rows(source)
    _check_columns(kind, columns, source, BULK_REQUIRED_COLUMNS)
    out = []
    for position, record in enumerate(rows, start=1):
        if kind == "customers" and _blank(record.get("number")):
            record = {**record, "number": record.get("customer_number")}
        row = {column: value for column, value in record.items() if column in model.model_fields and not _blank(value)}
        for column in KEY_COLUMNS[kind]:
            row[column] = _key(record, column, kind, position, text_only=True)
        out.append(row)
    return out


def _events(kind: str, rows: list[dict[str, Any]]) -> list[Any]:
    from .sync import ChangeEvent

    out = []
    for position, r in enumerate(rows, start=1):
        r = {k: (v if v != "" else None) for k, v in r.items()}
        for column in ("item_number",) if kind == "products" else ("customer_number", "customer_item_number", "item_number") if kind == "mappings" else ():
            _key(r, column, kind, position)
        if kind != "customers" and not _blank(r.get("language_code")):
            _key(r, "language_code", kind, position)
        try:
            if kind == "customers":
                number = _key(r, "number" if not _blank(r.get("number")) or "customer_number" not in r else "customer_number", kind, position)
                out.append(ChangeEvent.customer_upsert(number, **{k: v for k, v in r.items() if k in {"name", "postal_code", "city", "address", "contact_person", "phone_number", "vat_number", "email", "coc_number", "home_page", "source_updated_at"}}))
            elif kind == "products":
                out.append(ChangeEvent.product_upsert(str(r["item_number"]), str(r.get("language_code") or "nl"), description=r.get("description") or "", remark=r.get("remark"), source_updated_at=r.get("source_updated_at")))
            else:
                out.append(ChangeEvent.mapping_upsert(str(r["customer_number"]), str(r["customer_item_number"]), item_number=str(r["item_number"]), language_code=str(r.get("language_code") or "nl"), source_updated_at=r.get("source_updated_at")))
        except ValidationError as exc:
            first = exc.errors()[0]
            raise typer.BadParameter(f"{kind} record {position}: {'.'.join(str(p) for p in first['loc'])}: {first['msg']}")
    return out


def _engine(state: str, dry_run: bool, concurrency: int) -> Any:
    from .sync import HashStateStore, SyncEngine

    return SyncEngine(_client(), state=HashStateStore(state), dry_run=dry_run, concurrency=concurrency)


for _kind in ("customers", "products", "mappings"):

    def _make(kind: str) -> Any:
        def cmd(source: Path = typer.Option(..., "--from", help="CSV or JSON file"), state: str = "aiotic-sync-state.db", dry_run: bool = False, concurrency: int = 4, force: bool = False) -> None:
            events = _load(kind, source)
            rep = _engine(state, dry_run, concurrency).apply_many(events, force=force)
            rprint(f"{kind}: {rep}")
            for e in rep.errors[:20]:
                rprint(f"  [red]{e}[/red]")
            raise typer.Exit(1 if rep.failed else 0)

        cmd.__name__ = kind
        cmd.__doc__ = f"Upsert {kind} from a CSV/JSON file (unchanged records are skipped)."
        return cmd

    sync_app.command(_kind)(_make(_kind))


@sync_app.command("reconcile")
def sync_reconcile(
    customers: Path | None = None,
    products: Path | None = None,
    mappings: Path | None = None,
    state: str = "aiotic-sync-state.db",
    delete_missing: bool = True,
    dry_run: bool = False,
    allow_empty: bool = typer.Option(False, "--allow-empty", help="An empty file really means: delete everything of that kind that was sent earlier"),
) -> None:
    """Safety net: send only differences vs. the last known state; delete records that disappeared.

    Files are validated first (header, required columns, JSON shape); an empty data set never deletes anything
    unless --allow-empty is given.
    """
    sources = {
        "customers": _load("customers", customers) if customers else None,
        "products": _load("products", products) if products else None,
        "mappings": _load("mappings", mappings) if mappings else None,
    }
    eng = _engine(state, dry_run, 4)
    rep = eng.reconcile(**sources, delete_missing=delete_missing, allow_empty=allow_empty)
    rprint(f"reconcile: {rep}")
    for e in rep.errors[:20]:
        rprint(f"  [red]{e}[/red]")
    raise typer.Exit(1 if rep.failed else 0)


@sync_app.command("bulk")
def sync_bulk(
    customers: Path | None = None,
    products: Path | None = None,
    mappings: Path | None = None,
    max_delete_fraction: float = typer.Option(0.10, help="Refuse when a data set would remove more than this share of what AIOTIC holds"),
    compare: str = typer.Option("keys", help="'keys' compares every record with AIOTIC; 'count' only the totals"),
    approve_deletions: bool = typer.Option(False, "--approve-deletions", help="The files are complete and the records missing from them must go"),
    approve_skipped: bool = typer.Option(False, "--approve-skipped", help="Send mappings even when some point at an unknown customer or product. The mappings held under their keys are removed and count as deletions"),
    dry_run: bool = False,
) -> None:
    """Replace complete data sets, one call each: AIOTIC removes every record a file does not contain.

    Each file must hold the complete data set (products: every language) and name every record in full: a
    products or mappings file needs the language_code column, and a key value that is blank or not text stops the
    run. Before
    anything is sent, the files are compared with what AIOTIC holds; a file that would remove more than
    --max-delete-fraction of it, or mappings that point at an unknown customer or product, stop the run with
    exit code 2 and nothing is changed.
    """
    from .sync import BulkSyncRefused, SyncEngine

    sources = {
        "customers": _bulk_rows("customers", customers) if customers else None,
        "products": _bulk_rows("products", products) if products else None,
        "mappings": _bulk_rows("mappings", mappings) if mappings else None,
    }
    if not any(v is not None for v in sources.values()):
        raise typer.BadParameter("give at least one of --customers, --products, --mappings")
    engine = SyncEngine(_client(), dry_run=dry_run)
    try:
        rep = engine.bulk_sync(**sources, max_delete_fraction=max_delete_fraction, compare=compare, approve_deletions=approve_deletions, approve_skipped=approve_skipped)
    except BulkSyncRefused as exc:
        rprint(f"[red]{exc}[/red]")
        rprint("Nothing was sent. Fix the export, or repeat with --approve-deletions / --approve-skipped when this is what you intend.")
        raise typer.Exit(2)
    except ValueError as exc:
        raise typer.BadParameter(str(exc))
    rprint(f"bulk: {rep}")
    for kind, result in rep.results.items():
        for s in getattr(result, "skipped", [])[:20]:
            rprint(f"  [red]skipped {s.customer_number} / {s.customer_item_number}: {s.reason}[/red]")
    raise typer.Exit(0 if rep.ok else 1)


@sync_app.command("bootstrap-state")
def sync_bootstrap(state: str = "aiotic-sync-state.db") -> None:
    """Seed the local state from what AIOTIC already holds (so the first reconcile does not re-send everything)."""
    n = _engine(state, False, 4).bootstrap_state_from_aiotic()
    rprint(f"seeded {n} fingerprints into {state}")


@app.command()
def serve(host: str = "0.0.0.0", port: int = 9000, reload: bool = False) -> None:
    """Run the integration service (receive endpoint + webhooks) from service.py or the built-in default."""
    import uvicorn

    target = "service:app" if Path("service.py").exists() else "aiotic.service:build_app"
    if target.endswith("build_app"):
        rprint("[yellow]no service.py found — running the built-in demo service with the in-memory ERP[/yellow]")
        uvicorn.run(target, host=host, port=port, factory=True)
    else:
        sys.path.insert(0, os.getcwd())
        uvicorn.run(target, host=host, port=port, reload=reload)


@app.command()
def mock(host: str = "0.0.0.0", port: int = 8080, erp_url: str | None = None, erp_key: str | None = None, processing_speed: float = 3.0) -> None:
    """Run a local mock AIOTIC tenant (keys: mock-integration-key / mock-sync-key)."""
    import uvicorn

    from .mock import create_mock_app

    uvicorn.run(create_mock_app(erp_url=erp_url, erp_key=erp_key, processing_seconds=processing_speed), host=host, port=port)


def main() -> None:
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
