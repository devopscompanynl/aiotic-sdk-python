"""Asynchronous AIOTIC API client — the same surface as :class:`aiotic.AioticClient`, ``await``-able.

    async with AsyncAioticClient(base_url=..., api_key=...) as client:
        status = await client.orders.get(request_id)
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any
from uuid import UUID

import httpx

from . import models as m
from ._transport import (
    IDEMPOTENT_METHODS,
    NEWEST_SOURCE_CHANGE,
    TokenBucket,
    backoff_delay,
    build_headers,
    bulk_body,
    bulk_outcome_unknown,
    email_source,
    outcome_unknown,
    unanswered_attempt,
    path_segment,
    raise_for_status,
    replay_allowed,
    should_retry,
    transport_error,
    upload_outcome_unknown,
)
from .client import FileInput, _file_tuple
from .config import Settings
from .errors import AioticError


class _AResource:
    def __init__(self, client: "AsyncAioticClient"):
        self._c = client


class AsyncOrders(_AResource):
    async def upload(
        self,
        files: Sequence[FileInput],
        *,
        request_id: UUID | str | None = None,
        metadata: dict[str, str] | None = None,
        confirm_timeout: float = 30.0,
        resend_if_missing: bool = False,
    ) -> m.OrderUploadResponse:
        """Upload files as one order. An upload is never blindly repeated: see ``AioticClient.orders.upload``."""
        rid = str(request_id) if request_id else str(uuid.uuid4())
        data: dict[str, str] = {**(metadata or {}), "request_id": rid}
        multipart = [("files", _file_tuple(f)) for f in files]
        resends = 1 if resend_if_missing else 0
        while True:
            try:
                r = await self._c._request("POST", "/order/upload", data=data, files=multipart)
                return m.OrderUploadResponse.model_validate(r.json())
            except AioticError as exc:
                if not outcome_unknown(exc):
                    raise
                if await self._appeared(rid, confirm_timeout):
                    return m.OrderUploadResponse(request_id=UUID(rid), split=False)
                if resends == 0:
                    raise upload_outcome_unknown(rid, confirm_timeout, exc) from exc
                resends -= 1

    async def _appeared(self, request_id: str, timeout: float) -> bool:
        """Look an order up until it exists or ``timeout`` seconds have passed (at least once)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        delay = 1.0
        while True:
            try:
                await self.get(request_id)
                return True
            except AioticError:
                pass  # not there yet, or the lookup itself failed: both mean "not confirmed"
            if loop.time() + delay > deadline:
                return False
            await asyncio.sleep(delay)
            delay = min(delay * 2, 8.0)

    async def upload_raw_email(self, eml: FileInput, *, request_id: UUID | str | None = None) -> m.OrderUploadResponse:
        data = {"request_id": str(request_id)} if request_id else {}
        r = await self._c._request("POST", "/order/raw/upload", data=data, files=[("file", _file_tuple(eml))])
        return m.OrderUploadResponse.model_validate(r.json())

    async def classify_raw_email(self, eml: FileInput) -> m.EmailClassification:
        r = await self._c._request("POST", "/order/raw/classify", files=[("file", _file_tuple(eml))], idempotent=True)
        return m.EmailClassification.model_validate(r.json())

    async def get(self, request_id: UUID | str) -> m.OrderStatus:
        r = await self._c._request("GET", f"/order_status/{path_segment(request_id, what='request_id')}")
        return m.OrderStatus.model_validate(r.json())

    async def list(self, *, page: int = 1, size: int = 100) -> m.OrderListResponse:
        r = await self._c._request("GET", "/order_status/list", params={"page": page, "size": size})
        return m.OrderListResponse.model_validate(r.json())

    async def iter_all(self, *, size: int = 200, max_pages: int | None = None) -> AsyncIterator[m.OrderStatus]:
        page = 1
        while True:
            batch = await self.list(page=page, size=size)
            for item in batch.items:
                yield item
            if len(batch.items) < size or (max_pages and page >= max_pages):
                return
            page += 1

    async def group(self, email_group_id: UUID | str) -> m.OrderGroup:
        r = await self._c._request("GET", f"/order/group/{path_segment(email_group_id, what='email_group_id')}")
        return m.OrderGroup.model_validate(r.json())

    async def files(self, request_id: UUID | str) -> list[m.OrderFile]:
        """List the order's files; see ``AioticClient.orders.files``."""
        r = await self._c._request("GET", f"/order/{path_segment(request_id, what='request_id')}/files")
        return [m.OrderFile.model_validate(item) for item in r.json()]

    async def email(self, request_id: UUID | str, source: str = "raw_email") -> bytes:
        """Download the e-mail an order came from (``raw_email`` or ``email_body``); see ``AioticClient.orders.email``."""
        r = await self._c._request("GET", f"/order/{path_segment(request_id, what='request_id')}/email/{email_source(source)}")
        return r.content

    async def download_file(self, request_id: UUID | str, filename: str, *, preview: bool = False) -> bytes:
        r = await self._c._request("GET", f"/order/{path_segment(request_id, what='request_id')}/{path_segment(filename, what='filename')}{'/preview' if preview else ''}")
        return r.content

    async def retry(self, request_id: UUID | str) -> m.OrderUploadResponse:
        r = await self._c._request("POST", f"/order/retry/{path_segment(request_id, what='request_id')}")
        return m.OrderUploadResponse.model_validate(r.json())

    async def wait(
        self,
        request_id: UUID | str,
        *,
        until: Iterable[m.OrderStatusValue] = m.LANDED_STATUSES,
        timeout: float = 600,
        initial_interval: float = 2.0,
        max_interval: float = 15.0,
    ) -> m.OrderStatus:
        targets = set(until)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        interval = initial_interval
        while True:
            status = await self.get(request_id)
            if status.status in targets or status.status.is_terminal:
                return status
            if loop.time() >= deadline:
                raise TimeoutError(f"order {request_id} still {status.status} after {timeout}s")
            await asyncio.sleep(min(interval, max(0.0, deadline - loop.time())))
            interval = min(max_interval, interval * 1.6)


class AsyncErp(_AResource):
    async def send(self, request_id: UUID | str) -> m.ErpSendResponse:
        r = await self._c._request("POST", f"/erp/send/{path_segment(request_id, what='request_id')}")
        return m.ErpSendResponse.model_validate(r.json())


class AsyncRejected(_AResource):
    async def list(self, *, page: int = 1, size: int = 100, status: str = "pending") -> m.RejectedEmailListResponse:
        r = await self._c._request("GET", "/rejected/list", params={"page": page, "size": size, "status": status})
        return m.RejectedEmailListResponse.model_validate(r.json())

    async def get(self, request_id: UUID | str) -> m.ClassifiedEmail:
        return m.ClassifiedEmail.model_validate((await self._c._request("GET", f"/rejected/{path_segment(request_id, what='request_id')}")).json())

    async def reprocess(self, request_id: UUID | str) -> m.ReprocessResponse:
        return m.ReprocessResponse.model_validate((await self._c._request("POST", f"/rejected/{path_segment(request_id, what='request_id')}/reprocess")).json())


class AsyncCustomers(_AResource):
    async def list(self, *, page: int = 1, size: int = 100, sort: str | None = None) -> m.CustomerListResponse:
        params: dict[str, Any] = {"page": page, "size": size}
        if sort:
            params["sort"] = sort
        r = await self._c._request("GET", "/customer/list", params=params)
        return m.CustomerListResponse.model_validate(r.json())

    async def latest_source_update(self) -> datetime | None:
        """The newest ``source_updated_at`` AIOTIC holds for a customer (UTC), or ``None``."""
        items = (await self.list(size=1, sort=NEWEST_SOURCE_CHANGE)).items
        return items[0].source_updated_at if items else None

    async def bulk_sync(self, rows: Iterable[m.CustomerBulkRow | dict[str, Any]], *, compress: bool = True, timeout: float | None = None) -> m.CustomerBulkResult:
        """Replace all customers with ``rows`` in one call; see ``AioticClient.customers.bulk_sync``."""
        return m.CustomerBulkResult.model_validate(await self._c._bulk("/customer/bulk", rows, m.CustomerBulkRow, compress, timeout))

    async def iter_all(self, *, size: int = 500, sort: str | None = None) -> AsyncIterator[m.Customer]:
        page = 1
        while True:
            batch = await self.list(page=page, size=size, sort=sort)
            for c in batch.items:
                yield c
            if len(batch.items) < size:
                return
            page += 1

    async def search(self, query: str, *, top_k: int = 10) -> m.CustomerSearchResponse:
        r = await self._c._request("GET", f"/customer/search/{path_segment(query, what='query')}", params={"top_k": top_k})
        return m.CustomerSearchResponse.model_validate(r.json())

    async def get(self, number: str) -> m.Customer:
        return m.Customer.model_validate((await self._c._request("GET", f"/customer/{path_segment(number, what='customer number')}")).json())

    async def upsert(self, number: str, data: m.CustomerUpsert | dict[str, Any]) -> m.Customer:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        return m.Customer.model_validate((await self._c._request("PUT", f"/customer/{path_segment(number, what='customer number')}", json=body)).json())

    async def delete(self, number: str) -> None:
        await self._c._request("DELETE", f"/customer/{path_segment(number, what='customer number')}")


class AsyncProducts(_AResource):
    async def list(self, *, page: int = 1, size: int = 100, language_code: str | None = None, sort: str | None = None) -> m.ProductListResponse:
        params: dict[str, Any] = {"page": page, "size": size}
        if language_code:
            params["language_code"] = language_code
        if sort:
            params["sort"] = sort
        return m.ProductListResponse.model_validate((await self._c._request("GET", "/product/list", params=params)).json())

    async def latest_source_update(self, *, language_code: str | None = None) -> datetime | None:
        """The newest ``source_updated_at`` AIOTIC holds for a product (UTC), or ``None``."""
        items = (await self.list(size=1, language_code=language_code, sort=NEWEST_SOURCE_CHANGE)).items
        return items[0].source_updated_at if items else None

    async def bulk_sync(self, rows: Iterable[m.ProductBulkRow | dict[str, Any]], *, compress: bool = True, timeout: float | None = None) -> m.ProductBulkResult:
        """Replace all products, in every language, with ``rows``; see ``AioticClient.products.bulk_sync``."""
        return m.ProductBulkResult.model_validate(await self._c._bulk("/product/bulk", rows, m.ProductBulkRow, compress, timeout))

    async def iter_all(self, *, size: int = 1000, language_code: str | None = None, sort: str | None = None) -> AsyncIterator[m.Product]:
        page = 1
        while True:
            batch = await self.list(page=page, size=size, language_code=language_code, sort=sort)
            for p in batch.items:
                yield p
            if len(batch.items) < size:
                return
            page += 1

    async def get(self, item_number: str, language_code: str) -> m.Product:
        return m.Product.model_validate((await self._c._request("GET", f"/product/{path_segment(item_number, what='item number', allow_slash=True)}/{path_segment(language_code, what='language code')}")).json())

    async def upsert(self, item_number: str, language_code: str, data: m.ProductUpsert | dict[str, Any]) -> m.Product:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = await self._c._request("PUT", f"/product/{path_segment(item_number, what='item number', allow_slash=True)}/{path_segment(language_code, what='language code')}", json=body)
        return m.Product.model_validate(r.json())

    async def delete(self, item_number: str, language_code: str) -> None:
        await self._c._request("DELETE", f"/product/{path_segment(item_number, what='item number', allow_slash=True)}/{path_segment(language_code, what='language code')}")


class AsyncCustomerProducts(_AResource):
    async def list(self, *, page: int = 1, size: int = 100, **filters: Any) -> m.CustomerProductListResponse:
        params = {"page": page, "size": size, **{k: v for k, v in filters.items() if v is not None}}
        r = await self._c._request("GET", "/customer-product/list", params=params)
        return m.CustomerProductListResponse.model_validate(r.json())

    async def latest_source_update(self) -> datetime | None:
        """The newest ``source_updated_at`` AIOTIC holds for a mapping (UTC), or ``None``."""
        items = (await self.list(size=1, sort=NEWEST_SOURCE_CHANGE)).items
        return items[0].source_updated_at if items else None

    async def bulk_sync(self, rows: Iterable[m.CustomerProductBulkRow | dict[str, Any]], *, compress: bool = True, timeout: float | None = None) -> m.CustomerProductBulkResult:
        """Replace all customer item mappings with ``rows``; see ``AioticClient.customer_products.bulk_sync``."""
        return m.CustomerProductBulkResult.model_validate(await self._c._bulk("/customer-product/bulk", rows, m.CustomerProductBulkRow, compress, timeout))

    async def iter_all(self, *, size: int = 1000, **filters: Any) -> AsyncIterator[m.CustomerProduct]:
        page = 1
        while True:
            batch = await self.list(page=page, size=size, **filters)
            for cp in batch.items:
                yield cp
            if len(batch.items) < size:
                return
            page += 1

    async def get(self, customer_number: str, customer_item_number: str) -> m.CustomerProduct:
        r = await self._c._request("GET", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number', allow_slash=True)}")
        return m.CustomerProduct.model_validate(r.json())

    async def upsert(
        self, customer_number: str, customer_item_number: str, data: m.CustomerProductUpsert | dict[str, Any]
    ) -> m.CustomerProduct:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = await self._c._request("PUT", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number', allow_slash=True)}", json=body)
        return m.CustomerProduct.model_validate(r.json())

    async def delete(self, customer_number: str, customer_item_number: str) -> None:
        await self._c._request("DELETE", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number', allow_slash=True)}")


class AsyncMailbox(_AResource):
    async def fetch_all(self) -> m.FetchAllEmailsResponse:
        return m.FetchAllEmailsResponse.model_validate((await self._c._request("POST", "/email-watcher/fetch-all")).json())


class AsyncAioticClient:
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
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        s = replace(settings) if settings is not None else Settings.from_env()  # a private copy, see AioticClient
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
        self._http = httpx.AsyncClient(base_url=self.settings.base_url, timeout=self.settings.timeout, transport=transport)
        self.orders = AsyncOrders(self)
        self.erp = AsyncErp(self)
        self.rejected = AsyncRejected(self)
        self.customers = AsyncCustomers(self)
        self.products = AsyncProducts(self)
        self.customer_products = AsyncCustomerProducts(self)
        self.mailbox = AsyncMailbox(self)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "AsyncAioticClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def health(self) -> m.HealthCheck:
        return m.HealthCheck.model_validate((await self._request("GET", "/healthcheck", auth=False)).json())

    async def system_status(self) -> m.SystemStatus:
        return m.SystemStatus.model_validate((await self._request("GET", "/system-status", auth=False)).json())

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        data: dict[str, str] | None = None,
        files: list[tuple[str, tuple[str, Any, str | None]]] | None = None,
        auth: bool = True,
        idempotent: bool | None = None,
        content: bytes | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        if idempotent is None:
            idempotent = method in IDEMPOTENT_METHODS
        headers = {**(build_headers(self.settings, path) if auth else {}), **(extra_headers or {})}
        last_exc: Exception | None = None
        unanswered = False  # an earlier attempt was sent and nobody said what became of it
        for attempt in range(self.settings.max_retries + 1):
            await self._bucket.acquire_async()
            try:
                response = await self._http.request(method, path, params=params, json=json, data=data, files=files, content=content, headers=headers, timeout=timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_exc = exc
                if attempt >= self.settings.max_retries or not replay_allowed(exc, idempotent=idempotent):
                    error = transport_error(exc, path)
                    error.earlier_attempt_unanswered = unanswered
                    raise error from exc
                unanswered = unanswered or unanswered_attempt(exc)
                await asyncio.sleep(backoff_delay(attempt))
                continue
            if should_retry(response, method, idempotent=idempotent) and attempt < self.settings.max_retries:
                unanswered = unanswered or unanswered_attempt(response)
                await asyncio.sleep(backoff_delay(attempt, response.headers.get("Retry-After")))
                continue
            try:
                raise_for_status(response, path)
            except AioticError as error:
                error.earlier_attempt_unanswered = unanswered
                raise
            response.extensions["aiotic_attempts"] = attempt + 1
            return response
        raise AioticError(f"{path}: giving up after {self.settings.max_retries} retries", detail=str(last_exc))

    async def _bulk(self, path: str, rows: Iterable[Any], model: type[m.BaseModel], compress: bool, timeout: float | None) -> dict[str, Any]:
        """Send one bulk sync. The body is built once, so every attempt carries the same data set."""
        body, headers = bulk_body(rows, model, compress=compress)
        try:
            r = await self._request("POST", path, content=body, extra_headers=headers, timeout=timeout if timeout is not None else self.settings.bulk_timeout, idempotent=True)
        except AioticError as exc:
            if outcome_unknown(exc):
                raise bulk_outcome_unknown(path, exc) from exc
            raise
        return {**r.json(), "attempts": r.extensions.get("aiotic_attempts", 1)}
