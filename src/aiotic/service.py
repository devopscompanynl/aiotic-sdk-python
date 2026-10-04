"""A complete integration service in one call — what `aiotic init` scaffolds and `aiotic serve` runs.

    from aiotic.service import build_app
    app = build_app()          # reads AIOTIC_* env vars; uses the demo in-memory ERP unless you pass one

Endpoints:
  POST /aiotic/orders       ERP receive endpoint (AIOTIC → you)
  POST /aiotic/processing   processing webhook   (AIOTIC → you), when AIOTIC_WEBHOOK_KEY is set
  POST /erp/events          change events        (your ERP → you) → sync engine
  GET  /healthz
"""

from __future__ import annotations

import logging
from typing import Any

from .config import Settings
from .erp.memory import InMemoryErp
from .erp.ports import CatalogPort, CustomerPort, ErpPort
from .pipeline import Pipeline, rules as R, sanitizers as S, validators as V
from .receive import ErpReceiver, IdempotencyStore, SqliteStore, create_receive_router
from .webhooks import ProcessingWebhookReceiver, create_webhook_routers

log = logging.getLogger("aiotic.service")


def default_pipeline(erp: Any, store: IdempotencyStore, *, allowed_currencies: tuple[str, ...] = ("EUR",)) -> Pipeline:
    """A sensible default: everything a functional ERP API would check, applied on our side."""
    validators: list[Any] = [V.RequiredFields(), V.PositiveQuantities(), V.LineTotalsConsistent(), V.OrderTotalConsistent(), V.CurrencyAllowed(allowed_currencies), V.DeliveryDateSane()]
    rules: list[Any] = [R.NoDuplicateOrder(store), R.ShipToAddressComplete()]
    if isinstance(erp, CatalogPort):
        validators.insert(1, V.ArticlesInCatalog(erp))
    if isinstance(erp, CustomerPort):
        validators.insert(1, V.CustomerResolved(erp))
        rules.append(R.CustomerNotBlocked(erp))
    return Pipeline(
        sanitizers=[S.StripWhitespace(), S.NormalizeCountryCodes(), S.NormalizeCurrency(), S.NormalizePostalCodes(), S.MapUnits(), S.FillShippingFromCustomer(), S.DropEmptyLines()],
        validators=validators,
        rules=rules,
    )


def build_app(
    *,
    settings: Settings | None = None,
    erp: ErpPort | None = None,
    pipeline: Pipeline | None = None,
    store: IdempotencyStore | None = None,
    receive_path: str = "/aiotic/orders",
    sync_engine: Any | None = None,
) -> Any:
    """Build the FastAPI app. Requires the ``server`` extra (``pip install "aiotic-sdk[server]"``)."""
    from fastapi import FastAPI

    s = settings or Settings.from_env()
    erp = erp or InMemoryErp()
    store = store or SqliteStore()
    receiver = ErpReceiver(erp, api_key=s.erp_receive_key or "change-me", pipeline=pipeline or default_pipeline(erp, store), store=store)
    if not s.erp_receive_key:
        log.warning("AIOTIC_ERP_RECEIVE_KEY is not set — the receive endpoint uses the placeholder key 'change-me'")

    app = FastAPI(title="AIOTIC integration service", version="0.1.0", docs_url="/docs")
    app.include_router(create_receive_router(receiver, path=receive_path))

    processing = None
    if s.webhook_key:
        processing = ProcessingWebhookReceiver(s.webhook_key, lambda req: log.info("processing webhook: %s %s", req.request_id, req.purchase_order.order_number))

    def on_change_events(events: list[Any]) -> Any:
        if sync_engine is None:
            log.warning("received %d change events but no sync engine is configured", len(events))
            return None
        return sync_engine.apply_many(events)

    app.include_router(create_webhook_routers(processing=processing, on_change_events=on_change_events, erp_events_key=s.erp_receive_key))

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    app.state.receiver = receiver
    app.state.erp = erp
    return app
