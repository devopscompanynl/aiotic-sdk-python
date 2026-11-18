"""HTTP plumbing shared by the sync and async clients: auth headers, retries with backoff,
client-side rate limiting and error mapping."""

from __future__ import annotations

import asyncio
import gzip
import json
import random
import threading
import time
from collections.abc import Iterable
from typing import Any
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ValidationError

from ._version import __version__
from .config import Settings
from .errors import AioticError, AioticIdentifierError, AioticOutcomeUnknownError, AioticTransportError, error_for_status

RETRY_STATUSES = frozenset({408, 425, 429, 502, 503, 504})
# With these statuses the server did not process the request, so even a request that is not idempotent can be sent
# again: 408 (the request never arrived completely), 425 (not accepted yet), 429 (rejected before processing).
# 503 is deliberately absent: an intermediary can answer 503 after the upstream connection broke, that is, after the
# backend may already have applied the request. 502 and 504 are ambiguous for the same reason.
NOT_PROCESSED_STATUSES = frozenset({408, 425, 429})
# Transport failures that happen before anything was sent: safe to repeat for any operation.
NOT_SENT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE"})
SYNC_KEY_PREFIXES = ("/customer/", "/product/", "/customer-product/")
# Answers an intermediary can give after the backend already accepted the request: the outcome is unknown.
AMBIGUOUS_STATUSES = frozenset({502, 503, 504})
# The one sort the list endpoints of the master data offer: newest source change first, records without one last.
NEWEST_SOURCE_CHANGE = "-source_updated_at"
# The parts of the e-mail an order came from, as ``orders.email()`` names them.
EMAIL_SOURCES = ("raw_email", "email_body")


class TokenBucket:
    """Simple token bucket so a sync job keeps a steady pace. AIOTIC can rate-limit requests per client IP address
    and refuse the ones above the limit, with a 503; staying below it is cheaper than backing off."""

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


def should_retry(response: httpx.Response, method: str, *, idempotent: bool = True) -> bool:
    """Whether a response may be answered with another attempt.

    Idempotent requests (GET, PUT, DELETE, and uploads that carry a ``request_id``) are retried on every status in
    :data:`RETRY_STATUSES`. Other POSTs are retried only when the status proves the request was not processed
    (:data:`NOT_PROCESSED_STATUSES`); a 502, 503 or 504 after an order retry, a raw e-mail upload or an ERP send may
    have gone through and is surfaced instead.
    """
    if response.status_code not in RETRY_STATUSES:
        return False
    # Never blindly retry an ERP send on 503 (integration not configured, or a refusal by a gateway such as the
    # rate limit: the status does not say which) — surface it.
    if method == "POST" and response.request.url.path.startswith("/erp/send") and response.status_code == 503:
        return False
    return idempotent or response.status_code in NOT_PROCESSED_STATUSES


def replay_allowed(exc: Exception, *, idempotent: bool) -> bool:
    """Whether a request that failed with a transport error may be sent again: always for idempotent requests,
    otherwise only when the failure happened before the request was sent."""
    return idempotent or isinstance(exc, NOT_SENT_ERRORS)


def path_segment(value: Any, *, what: str = "identifier", allow_slash: bool = False) -> str:
    """Encode one value as a single URL path segment (``#``, ``?``, ``%``, spaces and non-ASCII are percent-encoded).

    Values that cannot travel as one segment are refused before any request: empty strings, the dot segments
    ``.`` and ``..``, and anything containing a slash, a backslash or a control character. Servers decode and
    normalise those differently, so encoding them could address another record.

    ``allow_slash`` is for the two identifiers the API accepts with a slash, a product's item number and a
    customer's own item number: ``LED 48/100 W`` travels as ``LED%2048%2F100%20W``. The parts between the
    slashes still may not be empty or a dot segment, so the value cannot start or end with a slash or hold two in
    a row.
    """
    text = str(value)
    parts = text.split("/") if allow_slash else [text]
    bad_part = any(part in ("", ".", "..") for part in parts)
    if bad_part or (not allow_slash and "/" in text) or "\\" in text or any(ord(ch) < 32 or ch == "\x7f" for ch in text):
        raise AioticIdentifierError(f"{what} {text!r} cannot be sent as a URL path segment")
    return quote(text, safe="")


def transport_error(exc: Exception, path: str) -> AioticTransportError:
    return AioticTransportError(f"{path}: {exc.__class__.__name__}: {exc}", detail=str(exc))


def email_source(source: str) -> str:
    """Check the ``source`` of ``orders.email()`` before it becomes part of a URL."""
    if source not in EMAIL_SOURCES:
        raise ValueError(f"source must be one of {', '.join(EMAIL_SOURCES)}, not {source!r}")
    return source


def outcome_unknown(exc: AioticError) -> bool:
    """Whether a failed request may nevertheless have been carried out: a transport failure after the request
    was sent, or an answer an intermediary gives when the connection to the backend broke."""
    if exc.earlier_attempt_unanswered:
        return True  # the last attempt may have sent nothing, but an earlier one did and nobody answered it
    if isinstance(exc, AioticTransportError):
        return not isinstance(exc.__cause__, NOT_SENT_ERRORS)  # a connection that never opened sent nothing
    return exc.status in AMBIGUOUS_STATUSES


def unanswered_attempt(failure: Exception | httpx.Response) -> bool:
    """Whether an attempt that is about to be repeated may have been carried out: it was sent and the connection
    broke, or an intermediary answered for a backend it lost."""
    if isinstance(failure, httpx.Response):
        return failure.status_code in AMBIGUOUS_STATUSES
    return not isinstance(failure, NOT_SENT_ERRORS)


def upload_outcome_unknown(request_id: str, waited: float, cause: AioticError) -> AioticOutcomeUnknownError:
    return AioticOutcomeUnknownError(
        f"/order/upload: no answer for request_id {request_id}, and the order did not appear within {waited:g}s. "
        "The upload may still arrive: look it up with orders.get() before you upload again. Uploading again under "
        "the same request_id puts that order back into QUEUED and processes it again",
        status=cause.status,
        detail=cause.detail,
        request_id=request_id,
    )


def bulk_outcome_unknown(path: str, cause: AioticError) -> AioticOutcomeUnknownError:
    return AioticOutcomeUnknownError(
        f"{path}: an attempt was sent and nobody answered it. AIOTIC finishes a bulk sync it has started, so the data set may "
        "already be replaced: send the same, current data set again later to see the result. Do not send an older one",
        status=cause.status,
        detail=cause.detail,
    )


def bulk_body(rows: Iterable[Any], model: type[BaseModel], *, compress: bool = True) -> tuple[bytes, dict[str, str]]:
    """Validate the rows of a bulk sync against ``model`` and serialise them as one JSON array, gzip-compressed
    unless ``compress`` is off. Returns the body and the headers that describe it.

    A row the API would refuse is refused here, before anything is sent, with its position in the data set.
    """
    out: list[dict[str, Any]] = []
    for position, row in enumerate(rows):
        try:
            validated = row if isinstance(row, model) else model.model_validate(row.model_dump() if isinstance(row, BaseModel) else row)
        except ValidationError as exc:
            first = exc.errors()[0]
            field = ".".join(str(part) for part in first["loc"]) or "row"
            raise ValueError(f"bulk sync row {position}: {field}: {first['msg']}") from exc
        out.append(validated.model_dump(mode="json", exclude_none=True))
    if not out:
        raise ValueError("a bulk sync needs at least one row: it replaces the whole data set, and AIOTIC refuses an empty one")
    data = json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if compress:
        data = gzip.compress(data, compresslevel=6)
        headers["Content-Encoding"] = "gzip"
    return data, headers
