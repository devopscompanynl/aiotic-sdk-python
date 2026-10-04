from __future__ import annotations

from aiotic import AioticClient
from aiotic.models import OrderStatusValue
from aiotic.sync import ChangeEvent, InMemoryStateStore, SyncEngine
from aiotic.watch import OrderWatcher
from aiotic.webhooks import parse_change_events, verify_hmac_signature
import hashlib
import hmac
import time


def test_sync_engine_skips_unchanged(client: AioticClient) -> None:
    eng = SyncEngine(client, state=InMemoryStateStore(), concurrency=2)
    ev = ChangeEvent.product_upsert("SYNC-1", "nl", description="Sync test")
    assert eng.apply(ev) is True
    assert eng.apply(ev) is False  # fingerprint unchanged → no request
    ev2 = ChangeEvent.product_upsert("SYNC-1", "nl", description="Sync test v2")
    assert eng.apply(ev2) is True
    rep = eng.apply_many([ChangeEvent.customer_upsert("SYNC-C", name="Sync BV"), ChangeEvent.mapping_upsert("SYNC-C", "THEIR-1", item_number="SYNC-1", language_code="nl")])
    assert rep.sent == 2 and rep.failed == 0
    assert client.customer_products.get("SYNC-C", "THEIR-1").item_number == "SYNC-1"
    # reconcile with an EMPTY product set: refused (a broken export must not delete), customer and mapping unchanged → skipped
    rep2 = eng.reconcile(customers=[ChangeEvent.customer_upsert("SYNC-C", name="Sync BV")], products=[], mappings=[ChangeEvent.mapping_upsert("SYNC-C", "THEIR-1", item_number="SYNC-1", language_code="nl")])
    assert rep2.skipped_unchanged == 2 and rep2.deleted == 0 and rep2.failed == 1 and "refusing to delete" in rep2.errors[0]
    assert client.products.get("SYNC-1", "nl").description == "Sync test v2"
    # the same run with the explicit opt-in: the product that disappeared from the ERP is deleted in AIOTIC
    rep3 = eng.reconcile(products=[], allow_empty=True)
    assert rep3.deleted == 1 and rep3.failed == 0
    eng.apply_many([ChangeEvent.mapping_delete("SYNC-C", "THEIR-1"), ChangeEvent.customer_delete("SYNC-C")])


def test_fk_order_and_error_report(client: AioticClient) -> None:
    eng = SyncEngine(client, state=InMemoryStateStore())
    rep = eng.apply_many([ChangeEvent.mapping_upsert("NOPE", "X", item_number="NOPE", language_code="nl")])
    assert rep.failed == 1 and "upsert those first" in rep.errors[0]


def test_parse_change_events() -> None:
    evs = parse_change_events({"events": [{"kind": "product", "op": "upsert", "item_number": "A", "language_code": "de", "description": "x"}, {"kind": "customer", "op": "delete", "number": "1"}]})
    assert evs[0].key == ("A", "de") and evs[1].op == "delete"


def test_watcher_emits_transitions(client: AioticClient) -> None:
    seen: list[tuple[str, str | None, str]] = []
    w = OrderWatcher(client, on_transition=lambda t: seen.append((t.request_id, t.from_status, t.to)), interval=0.1, seed_silently=True)
    w.run_once()  # learn current state silently
    up = client.orders.upload([("PO-9100.pdf", b"x")])
    client.orders.wait(up.request_id, timeout=10, initial_interval=0.1)
    w.run_once()
    mine = [s for s in seen if s[0] == str(up.request_id)]
    assert mine and mine[-1][2] == OrderStatusValue.PROCESSED


def test_hmac_signature() -> None:
    body = b'{"x":1}'
    ts = int(time.time())
    sig = hmac.new(b"s", f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    assert verify_hmac_signature("s", body, f"t={ts},v1={sig}")
    assert not verify_hmac_signature("s", body, f"t={ts - 999},v1={sig}")
    assert not verify_hmac_signature("s", body, None)
