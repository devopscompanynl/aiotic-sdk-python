"""Mock AIOTIC tenant for local development and CI.

    python -m aiotic.mock            # or: aiotic mock
    → http://localhost:8080, keys: mock-integration-key (all endpoints), mock-sync-key (master data)

Implements the public endpoints with realistic behaviour: uploads move QUEUED → PROCESSING →
PROCESSED/ATTENTION after a few seconds (ATTENTION when a line's article is not in /product),
``/erp/send`` really POSTs to the configured ERP URL with the documented contract, the master-data
endpoints enforce the same keys and reference checks as the real API, and the processing webhook can
be enabled with ``MOCK_WEBHOOK_URL`` / ``MOCK_WEBHOOK_KEY``.
"""

from .app import create_mock_app

__all__ = ["create_mock_app"]
