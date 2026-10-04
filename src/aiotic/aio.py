"""Asynchronous AIOTIC API client — the same surface as :class:`aiotic.AioticClient`, ``await``-able.

    async with AsyncAioticClient(base_url=..., api_key=...) as client:
        status = await client.orders.get(request_id)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable, Sequence
from typing import Any
from uuid import UUID

import httpx

from . import models as m
from ._transport import TokenBucket, backoff_delay, build_headers, raise_for_status, should_retry, transport_error
from .client import FileInput, _file_tuple
from .config import Settings
from .errors import AioticError


class _AResource:
    def __init__(self, client: "AsyncAioticClient"):
        self._c = client


class AsyncOrders(_AResource):
    async def upload(
        self, files: Sequence[FileInput], *, request_id: UUID | str | None = None, metadata: dict[str, str] | None = None
    ) -> m.OrderUploadResponse:
        data: dict[str, str] = dict(metadata or {})
        if request_id:
            data["request_id"] = str(request_id)
        r = await self._c._request("POST", "/order/upload", data=data, files=[("files", _file_tuple(f)) for f in files])
        return m.OrderUploadResponse.model_validate(r.json())

    async def upload_raw_email(self, eml: FileInput, *, request_id: UUID | str | None = None) -> m.OrderUploadResponse:
        data = {"request_id": str(request_id)} if request_id else {}
        r = await self._c._request("POST", "/order/raw/upload", data=data, files=[("file", _file_tuple(eml))])
        return m.OrderUploadResponse.model_validate(r.json())

    async def classify_raw_email(self, eml: FileInput) -> m.EmailClassification:
        r = await self._c._request("POST", "/order/raw/classify", files=[("file", _file_tuple(eml))])
        return m.EmailClassification.model_validate(r.json())

    async def get(self, request_id: UUID | str) -> m.OrderStatus:
        r = await self._c._request("GET", f"/order_status/{request_id}")
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
        r = await self._c._request("GET", f"/order/group/{email_group_id}")
        return m.OrderGroup.model_validate(r.json())

    async def download_file(self, request_id: UUID | str, filename: str, *, preview: bool = False) -> bytes:
        r = await self._c._request("GET", f"/order/{request_id}/{filename}{'/preview' if preview else ''}")
        return r.content

    async def retry(self, request_id: UUID | str) -> m.OrderUploadResponse:
        r = await self._c._request("POST", f"/order/retry/{request_id}")
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
        r = await self._c._request("POST", f"/erp/send/{request_id}")
        return m.ErpSendResponse.model_validate(r.json())


class AsyncRejected(_AResource):
    async def list(self, *, page: int = 1, size: int = 100, status: str = "pending") -> m.RejectedEmailListResponse:
        r = await self._c._request("GET", "/rejected/list", params={"page": page, "size": size, "status": status})
        return m.RejectedEmailListResponse.model_validate(r.json())

    async def get(self, request_id: UUID | str) -> m.ClassifiedEmail:
        return m.ClassifiedEmail.model_validate((await self._c._request("GET", f"/rejected/{request_id}")).json())

    async def reprocess(self, request_id: UUID | str) -> m.ReprocessResponse:
        return m.ReprocessResponse.model_validate((await self._c._request("POST", f"/rejected/{request_id}/reprocess")).json())


class AsyncCustomers(_AResource):
    async def list(self, *, page: int = 1, size: int = 100) -> m.CustomerListResponse:
        r = await self._c._request("GET", "/customer/list", params={"page": page, "size": size})
        return m.CustomerListResponse.model_validate(r.json())

    async def iter_all(self, *, size: int = 500) -> AsyncIterator[m.Customer]:
        page = 1
        while True:
            batch = await self.list(page=page, size=size)
            for c in batch.items:
                yield c
            if len(batch.items) < size:
                return
            page += 1

    async def search(self, query: str, *, top_k: int = 10) -> m.CustomerSearchResponse:
        r = await self._c._request("GET", f"/customer/search/{httpx.URL(path=query).path.lstrip('/')}", params={"top_k": top_k})
        return m.CustomerSearchResponse.model_validate(r.json())

    async def get(self, number: str) -> m.Customer:
        return m.Customer.model_validate((await self._c._request("GET", f"/customer/{number}")).json())

    async def upsert(self, number: str, data: m.CustomerUpsert | dict[str, Any]) -> m.Customer:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        return m.Customer.model_validate((await self._c._request("PUT", f"/customer/{number}", json=body)).json())

    async def delete(self, number: str) -> None:
        await self._c._request("DELETE", f"/customer/{number}")


class AsyncProducts(_AResource):
    async def list(self, *, page: int = 1, size: int = 100, language_code: str | None = None) -> m.ProductListResponse:
        params: dict[str, Any] = {"page": page, "size": size}
        if language_code:
            params["language_code"] = language_code
        return m.ProductListResponse.model_validate((await self._c._request("GET", "/product/list", params=params)).json())

    async def iter_all(self, *, size: int = 1000, language_code: str | None = None) -> AsyncIterator[m.Product]:
        page = 1
        while True:
            batch = await self.list(page=page, size=size, language_code=language_code)
            for p in batch.items:
                yield p
            if len(batch.items) < size:
                return
            page += 1

    async def get(self, item_number: str, language_code: str) -> m.Product:
        return m.Product.model_validate((await self._c._request("GET", f"/product/{item_number}/{language_code}")).json())

    async def upsert(self, item_number: str, language_code: str, data: m.ProductUpsert | dict[str, Any]) -> m.Product:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = await self._c._request("PUT", f"/product/{item_number}/{language_code}", json=body)
        return m.Product.model_validate(r.json())

    async def delete(self, item_number: str, language_code: str) -> None:
        await self._c._request("DELETE", f"/product/{item_number}/{language_code}")


class AsyncCustomerProducts(_AResource):
    async def list(self, *, page: int = 1, size: int = 100, **filters: Any) -> m.CustomerProductListResponse:
        params = {"page": page, "size": size, **{k: v for k, v in filters.items() if v is not None}}
        r = await self._c._request("GET", "/customer-product/list", params=params)
        return m.CustomerProductListResponse.model_validate(r.json())

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
        r = await self._c._request("GET", f"/customer-product/{customer_number}/{customer_item_number}")
        return m.CustomerProduct.model_validate(r.json())

    async def upsert(
        self, customer_number: str, customer_item_number: str, data: m.CustomerProductUpsert | dict[str, Any]
    ) -> m.CustomerProduct:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = await self._c._request("PUT", f"/customer-product/{customer_number}/{customer_item_number}", json=body)
        return m.CustomerProduct.model_validate(r.json())

    async def delete(self, customer_number: str, customer_item_number: str) -> None:
        await self._c._request("DELETE", f"/customer-product/{customer_number}/{customer_item_number}")


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
        s = settings or Settings.from_env()
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
    ) -> httpx.Response:
        headers = build_headers(self.settings, path) if auth else {}
        last_exc: Exception | None = None
        for attempt in range(self.settings.max_retries + 1):
            await self._bucket.acquire_async()
            try:
                response = await self._http.request(method, path, params=params, json=json, data=data, files=files, headers=headers)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_exc = exc
                if attempt >= self.settings.max_retries or (files and attempt > 0):
                    raise transport_error(exc, path) from exc
                await asyncio.sleep(backoff_delay(attempt))
                continue
            if should_retry(response, method) and attempt < self.settings.max_retries:
                await asyncio.sleep(backoff_delay(attempt, response.headers.get("Retry-After")))
                continue
            raise_for_status(response, path)
            return response
        raise AioticError(f"{path}: giving up after {self.settings.max_retries} retries", detail=str(last_exc))
