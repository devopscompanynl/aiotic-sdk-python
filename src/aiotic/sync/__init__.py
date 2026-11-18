"""Master-data sync: customers, products and customer item mappings — driven by change events,
with a hash-based reconciler as safety net, or one bulk sync per data set when your ERP can only export everything.

    from aiotic.sync import SyncEngine, ChangeEvent, HashStateStore

    engine = SyncEngine(client, state=HashStateStore("sync-state.db"))
    engine.apply(ChangeEvent.product_upsert("CBL-3015", "nl", description="Cable 3x1.5"))
    engine.apply(ChangeEvent.customer_delete("40112"))

    # Nightly safety net: compare everything you have with what was last sent; push only differences.
    report = engine.reconcile(customers=iter_erp_customers(), products=iter_erp_products(), mappings=iter_erp_mappings())

    # Or replace the complete data sets in one call each. AIOTIC removes what a data set does not contain, so the
    # engine first checks what that would remove and raises BulkSyncRefused instead of sending a truncated export.
    report = engine.bulk_sync(customers=iter_erp_customers(), products=iter_erp_products(), mappings=iter_erp_mappings())
"""

from .bulk import BulkPlan, BulkSyncRefused, BulkSyncReport
from .engine import ChangeEvent, ChangeKind, ChangeOp, CursorChangeSource, HashStateStore, InMemoryStateStore, PollingChangeSource, SyncEngine, SyncReport, StateStore

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
    "CursorChangeSource",
    "BulkPlan",
    "BulkSyncReport",
    "BulkSyncRefused",
]
