"""Tests run the mock AIOTIC tenant in-process (ASGI transport) — no network, no real tenant."""

from __future__ import annotations

import asyncio
import threading

import httpx
import pytest
import uvicorn

from aiotic import AioticClient
from aiotic.mock import create_mock_app


class _Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:  # pragma: no cover
        pass


@pytest.fixture(scope="session")
def mock_url() -> str:
    """A real HTTP mock server (needed because the mock runs background tasks + the ERP callback)."""
    app = create_mock_app(processing_seconds=0.3)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = _Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        asyncio.run(asyncio.sleep(0.05))
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def client(mock_url: str) -> AioticClient:
    with AioticClient(base_url=mock_url, api_key="mock-integration-key", sync_api_key="mock-sync-key", rate_limit=0, max_retries=1) as c:
        yield c


@pytest.fixture
def http(mock_url: str) -> httpx.Client:
    with httpx.Client(base_url=mock_url) as h:
        yield h
