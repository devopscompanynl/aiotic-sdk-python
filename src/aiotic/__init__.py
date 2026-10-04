"""AIOTIC integration SDK.

Typed client for the AIOTIC API, building blocks for the ERP receive endpoint, a validation /
business-rule pipeline for ERPs that cannot validate on their own, an event-driven master-data
sync engine, an order watcher and a mock AIOTIC server for local development.

Quick start::

    from aiotic import AioticClient

    client = AioticClient(base_url="https://acme.aiotic.ai", api_key="…")
    order = client.orders.get("550e8400-e29b-41d4-a716-446655440000")
    print(order.status, order.result.order_number if order.result else None)
"""

from ._version import __version__
from .client import AioticClient
from .aio import AsyncAioticClient
from .config import Settings
from .errors import (
    AioticAuthError,
    AioticConflictError,
    AioticError,
    AioticErpRejectedError,
    AioticNotFoundError,
    AioticServerError,
    AioticUnavailableError,
    AioticValidationError,
)
from . import models

__all__ = [
    "__version__",
    "AioticClient",
    "AsyncAioticClient",
    "Settings",
    "AioticError",
    "AioticAuthError",
    "AioticNotFoundError",
    "AioticConflictError",
    "AioticValidationError",
    "AioticErpRejectedError",
    "AioticServerError",
    "AioticUnavailableError",
    "models",
]
