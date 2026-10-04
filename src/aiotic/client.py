"""Synchronous AIOTIC API client.

    from aiotic import AioticClient

    with AioticClient(base_url="https://acme.aiotic.ai", api_key="…") as client:
        up = client.orders.upload(["po.pdf"], metadata={"my_ref": "TICKET-1"})
        status = client.orders.wait(up.request_id, timeout=300)
        if status.status.is_sendable and not status.result.unresolved_items:
            client.erp.send(up.request_id)

Every method maps 1:1 to an endpoint of the public OpenAPI document; see the API reference.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import replace
from pathlib import Path
from typing import IO, Any, BinaryIO
from uuid import UUID

import httpx

from . import models as m
from ._transport import IDEMPOTENT_METHODS, TokenBucket, backoff_delay, build_headers, path_segment, raise_for_status, replay_allowed, should_retry, transport_error
from .config import Settings
from .errors import AioticError

FileInput = str | Path | tuple[str, bytes] | tuple[str, BinaryIO] | tuple[str, bytes, str]


def _file_tuple(f: FileInput) -> tuple[str, bytes, str | None]:
    """Normalise a file input to ``(name, bytes, content_type)``. File objects are read once here, so a request
    that has to be sent again carries the complete body."""
    if isinstance(f, (str, Path)):
        p = Path(f)
        return (p.name, p.read_bytes(), None)
    name, data, *rest = f
    if hasattr(data, "read"):
        data = data.read()
    return (name, data, rest[0] if rest else None)


class _Resource:
    def __init__(self, client: "AioticClient"):
        self._c = client


class Orders(_Resource):
    """``/order/*`` and ``/order_status/*``."""

    def upload(
        self,
        files: Sequence[FileInput],
        *,
        request_id: UUID | str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> m.OrderUploadResponse:
        """Upload one or more files that together form ONE purchase order.

        ``request_id`` (UUID v4) makes the upload idempotent and lets you correlate the order. When you do not
        pass one, the client generates it once per call, so a request that has to be sent again after a lost
        response creates no second order. Extra ``metadata`` form fields are stored on the order and echoed
        back by the status endpoints.
        """
        data: dict[str, str] = dict(metadata or {})
        data["request_id"] = str(request_id) if request_id else str(uuid.uuid4())
        multipart = [("files", _file_tuple(f)) for f in files]
        r = self._c._request("POST", "/order/upload", data=data, files=multipart, idempotent=True)
        return m.OrderUploadResponse.model_validate(r.json())

    def upload_raw_email(self, eml: FileInput, *, request_id: UUID | str | None = None) -> m.OrderUploadResponse:
        """Upload a raw ``.eml``. Branch on ``response.split`` and poll ``response.request_ids``.

        Raises :class:`aiotic.AioticValidationError` with a structured ``detail`` when the e-mail is
        not a purchase order (``detail["error"] == "not_a_purchase_order"``). The API does not promise
        idempotency for this endpoint, so the client never sends it again once it may have been received.
        """
        data = {"request_id": str(request_id)} if request_id else {}
        r = self._c._request("POST", "/order/raw/upload", data=data, files=[("file", _file_tuple(eml))])
        return m.OrderUploadResponse.model_validate(r.json())

    def classify_raw_email(self, eml: FileInput) -> m.EmailClassification:
        r = self._c._request("POST", "/order/raw/classify", files=[("file", _file_tuple(eml))], idempotent=True)
        return m.EmailClassification.model_validate(r.json())

    def get(self, request_id: UUID | str) -> m.OrderStatus:
        r = self._c._request("GET", f"/order_status/{path_segment(request_id, what='request_id')}")
        return m.OrderStatus.model_validate(r.json())

    def list(self, *, page: int = 1, size: int = 100) -> m.OrderListResponse:
        r = self._c._request("GET", "/order_status/list", params={"page": page, "size": size})
        return m.OrderListResponse.model_validate(r.json())

    def iter_all(self, *, size: int = 200, max_pages: int | None = None) -> Iterator[m.OrderStatus]:
        """Iterate over all orders, newest first, page by page."""
        page = 1
        while True:
            batch = self.list(page=page, size=size)
            yield from batch.items
            if len(batch.items) < size or (max_pages and page >= max_pages):
                return
            page += 1

    def group(self, email_group_id: UUID | str) -> m.OrderGroup:
        r = self._c._request("GET", f"/order/group/{path_segment(email_group_id, what='email_group_id')}")
        return m.OrderGroup.model_validate(r.json())

    def download_file(self, request_id: UUID | str, filename: str, *, preview: bool = False) -> bytes:
        """Download an original upload or a generated artifact such as ``latest_result.json``."""
        suffix = "/preview" if preview else ""
        r = self._c._request("GET", f"/order/{path_segment(request_id, what='request_id')}/{path_segment(filename, what='filename')}{suffix}", stream=True)
        return r.content

    def retry(self, request_id: UUID | str) -> m.OrderUploadResponse:
        """Retry a FAILED order. Returns the NEW request id; the old order becomes REPROCESSED."""
        r = self._c._request("POST", f"/order/retry/{path_segment(request_id, what='request_id')}")
        return m.OrderUploadResponse.model_validate(r.json())

    def wait(
        self,
        request_id: UUID | str,
        *,
        until: Iterable[m.OrderStatusValue] = m.LANDED_STATUSES,
        timeout: float = 600,
        initial_interval: float = 2.0,
        max_interval: float = 15.0,
    ) -> m.OrderStatus:
        """Poll with exponential backoff until the order reaches one of ``until`` (default: landed states)."""
        targets = set(until)
        deadline = time.monotonic() + timeout
        interval = initial_interval
        while True:
            status = self.get(request_id)
            if status.status in targets or status.status.is_terminal:
                return status
            if time.monotonic() >= deadline:
                raise TimeoutError(f"order {request_id} still {status.status} after {timeout}s")
            time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
            interval = min(max_interval, interval * 1.6)


class Erp(_Resource):
    """``/erp/*``."""

    def send(self, request_id: UUID | str) -> m.ErpSendResponse:
        """Hand a sendable order to your ERP receive endpoint. Raises ``AioticErpRejectedError`` on ``success: false``."""
        r = self._c._request("POST", f"/erp/send/{path_segment(request_id, what='request_id')}")
        return m.ErpSendResponse.model_validate(r.json())


class Rejected(_Resource):
    """``/rejected/*``."""

    def list(self, *, page: int = 1, size: int = 100, status: str = "pending") -> m.RejectedEmailListResponse:
        r = self._c._request("GET", "/rejected/list", params={"page": page, "size": size, "status": status})
        return m.RejectedEmailListResponse.model_validate(r.json())

    def get(self, request_id: UUID | str) -> m.ClassifiedEmail:
        r = self._c._request("GET", f"/rejected/{path_segment(request_id, what='request_id')}")
        return m.ClassifiedEmail.model_validate(r.json())

    def reprocess(self, request_id: UUID | str) -> m.ReprocessResponse:
        r = self._c._request("POST", f"/rejected/{path_segment(request_id, what='request_id')}/reprocess")
        return m.ReprocessResponse.model_validate(r.json())


class Customers(_Resource):
    """``/customer/*`` (accepts the sync key)."""

    def list(self, *, page: int = 1, size: int = 100) -> m.CustomerListResponse:
        r = self._c._request("GET", "/customer/list", params={"page": page, "size": size})
        return m.CustomerListResponse.model_validate(r.json())

    def iter_all(self, *, size: int = 500) -> Iterator[m.Customer]:
        page = 1
        while True:
            batch = self.list(page=page, size=size)
            yield from batch.items
            if len(batch.items) < size:
                return
            page += 1

    def search(self, query: str, *, top_k: int = 10) -> m.CustomerSearchResponse:
        r = self._c._request("GET", f"/customer/search/{path_segment(query, what='query')}", params={"top_k": top_k})
        return m.CustomerSearchResponse.model_validate(r.json())

    def get(self, number: str) -> m.Customer:
        r = self._c._request("GET", f"/customer/{path_segment(number, what='customer number')}")
        return m.Customer.model_validate(r.json())

    def upsert(self, number: str, data: m.CustomerUpsert | dict[str, Any]) -> m.Customer:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = self._c._request("PUT", f"/customer/{path_segment(number, what='customer number')}", json=body)
        return m.Customer.model_validate(r.json())

    def delete(self, number: str) -> None:
        self._c._request("DELETE", f"/customer/{path_segment(number, what='customer number')}")


class Products(_Resource):
    """``/product/*`` (accepts the sync key)."""

    def list(self, *, page: int = 1, size: int = 100, language_code: str | None = None) -> m.ProductListResponse:
        params: dict[str, Any] = {"page": page, "size": size}
        if language_code:
            params["language_code"] = language_code
        r = self._c._request("GET", "/product/list", params=params)
        return m.ProductListResponse.model_validate(r.json())

    def iter_all(self, *, size: int = 1000, language_code: str | None = None) -> Iterator[m.Product]:
        page = 1
        while True:
            batch = self.list(page=page, size=size, language_code=language_code)
            yield from batch.items
            if len(batch.items) < size:
                return
            page += 1

    def get(self, item_number: str, language_code: str) -> m.Product:
        r = self._c._request("GET", f"/product/{path_segment(item_number, what='item number')}/{path_segment(language_code, what='language code')}")
        return m.Product.model_validate(r.json())

    def upsert(self, item_number: str, language_code: str, data: m.ProductUpsert | dict[str, Any]) -> m.Product:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = self._c._request("PUT", f"/product/{path_segment(item_number, what='item number')}/{path_segment(language_code, what='language code')}", json=body)
        return m.Product.model_validate(r.json())

    def delete(self, item_number: str, language_code: str) -> None:
        self._c._request("DELETE", f"/product/{path_segment(item_number, what='item number')}/{path_segment(language_code, what='language code')}")


class CustomerProducts(_Resource):
    """``/customer-product/*`` (accepts the sync key)."""

    def list(
        self,
        *,
        page: int = 1,
        size: int = 100,
        customer_number: str | None = None,
        customer_item_number: str | None = None,
        item_number: str | None = None,
        language_code: str | None = None,
    ) -> m.CustomerProductListResponse:
        params = {
            k: v
            for k, v in {
                "page": page,
                "size": size,
                "customer_number": customer_number,
                "customer_item_number": customer_item_number,
                "item_number": item_number,
                "language_code": language_code,
            }.items()
            if v is not None
        }
        r = self._c._request("GET", "/customer-product/list", params=params)
        return m.CustomerProductListResponse.model_validate(r.json())

    def iter_all(self, *, size: int = 1000, **filters: Any) -> Iterator[m.CustomerProduct]:
        page = 1
        while True:
            batch = self.list(page=page, size=size, **filters)
            yield from batch.items
            if len(batch.items) < size:
                return
            page += 1

    def get(self, customer_number: str, customer_item_number: str) -> m.CustomerProduct:
        r = self._c._request("GET", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number')}")
        return m.CustomerProduct.model_validate(r.json())

    def upsert(
        self, customer_number: str, customer_item_number: str, data: m.CustomerProductUpsert | dict[str, Any]
    ) -> m.CustomerProduct:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = self._c._request("PUT", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number')}", json=body)
        return m.CustomerProduct.model_validate(r.json())

    def delete(self, customer_number: str, customer_item_number: str) -> None:
        self._c._request("DELETE", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number')}")


class Mailbox(_Resource):
    def fetch_all(self) -> m.FetchAllEmailsResponse:
        r = self._c._request("POST", "/email-watcher/fetch-all")
        return m.FetchAllEmailsResponse.model_validate(r.json())


class AioticClient:
    """Synchronous client. Thread-safe for concurrent use (httpx connection pool + token bucket)."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        *,
        sync_api_key: str | None = None,
        settings: Settings | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        rate_limit: float | None = None,
        transport: httpx.BaseTransport | None = None,
    ):
        # Work on a private copy: two clients may share one Settings object, and the caller may keep changing it.
        s = replace(settings) if settings is not None else Settings.from_env()
        if base_url:
            s.base_url = base_url.rstrip("/")
        if api_key:
            s.api_key = api_key
        if sync_api_key is not None:
            s.sync_api_key = sync_api_key
        if timeout is not None:
            s.timeout = timeout
        if max_retries is not None:
            s.max_retries = max_retries
        if rate_limit is not None:
            s.rate_limit = rate_limit
        self.settings = s.require()
        self._bucket = TokenBucket(self.settings.rate_limit)
        self._http = httpx.Client(base_url=self.settings.base_url, timeout=self.settings.timeout, transport=transport)
        self.orders = Orders(self)
        self.erp = Erp(self)
        self.rejected = Rejected(self)
        self.customers = Customers(self)
        self.products = Products(self)
        self.customer_products = CustomerProducts(self)
        self.mailbox = Mailbox(self)

    # -- lifecycle -----------------------------------------------------------------------
    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "AioticClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- health --------------------------------------------------------------------------
    def health(self) -> m.HealthCheck:
        return m.HealthCheck.model_validate(self._request("GET", "/healthcheck", auth=False).json())

    def system_status(self) -> m.SystemStatus:
        return m.SystemStatus.model_validate(self._request("GET", "/system-status", auth=False).json())

    # -- core ----------------------------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        data: dict[str, str] | None = None,
        files: list[tuple[str, tuple[str, Any, str | None]]] | None = None,
        auth: bool = True,
        stream: bool = False,
        idempotent: bool | None = None,
    ) -> httpx.Response:
        """One API call with retries. ``idempotent`` decides whether a request may be sent a second time when the
        outcome of the first one is unknown; by default only GET, PUT and DELETE are (see :mod:`aiotic._transport`)."""
        if idempotent is None:
            idempotent = method in IDEMPOTENT_METHODS
        headers = build_headers(self.settings, path) if auth else {}
        last_exc: Exception | None = None
        for attempt in range(self.settings.max_retries + 1):
            self._bucket.acquire()
            try:
                response = self._http.request(method, path, params=params, json=json, data=data, files=files, headers=headers)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_exc = exc
                if attempt >= self.settings.max_retries or not replay_allowed(exc, idempotent=idempotent):
                    raise transport_error(exc, path) from exc
                time.sleep(backoff_delay(attempt))
                continue
            if should_retry(response, method, idempotent=idempotent) and attempt < self.settings.max_retries:
                time.sleep(backoff_delay(attempt, response.headers.get("Retry-After")))
                continue
            raise_for_status(response, path)
            return response
        raise AioticError(f"{path}: giving up after {self.settings.max_retries} retries", detail=str(last_exc))
