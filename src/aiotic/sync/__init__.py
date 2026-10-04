"""Master-data sync: customers, products and customer item mappings — driven by change events,
with a hash-based reconciler as safety net. Never a full re-upload.

    from aiotic.sync import SyncEngine, ChangeEvent, HashStateStore

    engine = SyncEngine(client, state=HashStateStore("sync-state.db"))
    engine.apply(ChangeEvent.product_upsert("620206_01", "nl", description="Cable 3x1.5"))
    engine.apply(ChangeEvent.customer_delete("10577"))

    # Nightly safety net: compare everything you have with what was last sent; push only differences.
    report = engine.reconcile(customers=iter_erp_customers(), products=iter_erp_products(), mappings=iter_erp_mappings())
"""

from .engine import ChangeEvent, ChangeKind, ChangeOp, HashStateStore, InMemoryStateStore, PollingChangeSource, SyncEngine, SyncReport, StateStore

__all__ = [
    "ChangeEvent",
    "ChangeKind",
    "ChangeOp",
    "SyncEngine",
    "SyncReport",
    "StateStore",
    "HashStateStore",
    "InMemoryStateStore",
    "PollingChangeSource",
]
