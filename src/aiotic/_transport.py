"""HTTP plumbing shared by the sync and async clients: auth headers, retries with backoff,
client-side rate limiting and error mapping."""

from __future__ import annotations

import asyncio
import random
import threading
import time
from typing import Any

import httpx

from ._version import __version__
from .config import Settings
from .errors import AioticTransportError, error_for_status

RETRY_STATUSES = frozenset({408, 425, 429, 502, 503, 504})
SYNC_KEY_PREFIXES = ("/customer/", "/product/", "/customer-product/")


class TokenBucket:
    """Simple token bucket so a sync job never floods the tenant (the API has no server-side limit)."""

    def __init__(self, rate_per_sec: float, burst: int | None = None):
        self.rate = rate_per_sec
        self.capacity = burst or max(1, int(rate_per_sec))
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
        self.updated = now

    def wait_time(self) -> float:
        with self._lock:
            self._refill()
            if self.tokens >= 1:
                self.tokens -= 1
                return 0.0
            return (1 - self.tokens) / self.rate

    def acquire(self) -> None:
        if self.rate <= 0:
            return
        while (w := self.wait_time()) > 0:
            time.sleep(w)

    async def acquire_async(self) -> None:
        if self.rate <= 0:
            return
        while (w := self.wait_time()) > 0:
            await asyncio.sleep(w)


def backoff_delay(attempt: int, retry_after: str | None = None) -> float:
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return min(30.0, (2**attempt) * 0.5) + random.uniform(0, 0.25)


def build_headers(settings: Settings, path: str, *, use_sync_key: bool | None = None) -> dict[str, str]:
    """Pick the sync key for master-data paths when one is configured; the integration key otherwise."""
    if use_sync_key is None:
        use_sync_key = settings.sync_api_key is not None and path.startswith(SYNC_KEY_PREFIXES)
    key = settings.sync_api_key if (use_sync_key and settings.sync_api_key) else settings.api_key
    return {"X-API-Key": key, "User-Agent": f"{settings.user_agent}/{__version__}", "Accept": "application/json"}


def parse_error_detail(response: httpx.Response) -> Any:
    try:
        body = response.json()
    except ValueError:
        return response.text[:500] or f"HTTP {response.status_code}"
    return body.get("detail", body) if isinstance(body, dict) else body


def raise_for_status(response: httpx.Response, path: str) -> None:
    if 200 <= response.status_code < 300:
        return
    detail = parse_error_detail(response)
    request_id = None
    if isinstance(detail, dict):
        request_id = detail.get("request_id")
    raise error_for_status(response.status_code, detail, path=path, request_id=request_id)


def should_retry(response: httpx.Response | None, method: str) -> bool:
    if response is None:
        return True  # transport error
    if response.status_code not in RETRY_STATUSES:
        return False
    # Never blindly retry an ERP send on 503 (integration not configured) — surface it.
    return not (method == "POST" and response.request.url.path.startswith("/erp/send") and response.status_code == 503)


def transport_error(exc: Exception, path: str) -> AioticTransportError:
    return AioticTransportError(f"{path}: {exc.__class__.__name__}: {exc}", detail=str(exc))
