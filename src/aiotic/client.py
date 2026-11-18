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
from datetime import datetime
from pathlib import Path
from typing import IO, Any, BinaryIO
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
        confirm_timeout: float = 30.0,
        resend_if_missing: bool = False,
    ) -> m.OrderUploadResponse:
        """Upload one or more files that together form ONE purchase order.

        ``request_id`` (UUID v4) lets you correlate the order; the client generates one when you pass none. It is
        **not** an idempotency key: AIOTIC processes an upload again when it arrives a second time under the same
        id, and puts the order back into ``QUEUED`` whatever its status was. So the client never blindly repeats
        an upload. It sends it again only when the first attempt provably did not arrive. When the outcome is
        unknown (the connection broke after sending, or a 502, 503 or 504 came back), it looks the ``request_id``
        up for ``confirm_timeout`` seconds, because the first request can still be on its way:

        * the order appears: the upload arrived, and this call returns normally;
        * it does not appear: ``AioticOutcomeUnknownError`` with the ``request_id``. Look it up again before you
          upload again. With ``resend_if_missing=True`` the client uploads once more by itself at that point,
          accepting that the order is processed twice if the first request arrives after all.

        Extra ``metadata`` form fields are stored on the order and echoed back by the status endpoints. A tenant
        that takes its orders through its mailbox and the AIOTIC app only answers ``409`` (``AioticConflictError``).
        """
        rid = str(request_id) if request_id else str(uuid.uuid4())
        data: dict[str, str] = {**(metadata or {}), "request_id": rid}
        multipart = [("files", _file_tuple(f)) for f in files]
        resends = 1 if resend_if_missing else 0
        while True:
            try:
                r = self._c._request("POST", "/order/upload", data=data, files=multipart)
                return m.OrderUploadResponse.model_validate(r.json())
            except AioticError as exc:
                if not outcome_unknown(exc):
                    raise
                if self._appeared(rid, confirm_timeout):
                    return m.OrderUploadResponse(request_id=UUID(rid), split=False)
                if resends == 0:
                    raise upload_outcome_unknown(rid, confirm_timeout, exc) from exc
                resends -= 1

    def _appeared(self, request_id: str, timeout: float) -> bool:
        """Look an order up until it exists or ``timeout`` seconds have passed (at least once)."""
        deadline = time.monotonic() + timeout
        delay = 1.0
        while True:
            try:
                self.get(request_id)
                return True
            except AioticError:
                pass  # not there yet, or the lookup itself failed: both mean "not confirmed"
            if time.monotonic() + delay > deadline:
                return False
            time.sleep(delay)
            delay = min(delay * 2, 8.0)

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

    def files(self, request_id: UUID | str) -> list[m.OrderFile]:
        """List the order's files. ``source`` says what each one is: ``raw_email`` and ``email_body`` are
        read with :meth:`email`, every other value by file name with :meth:`download_file`. The e-mail parts of some orders are held by the tenant's mail intake; when
        that cannot be reached the list holds only the files stored with the order, so a list without
        ``raw_email`` is no proof that the order has no e-mail."""
        r = self._c._request("GET", f"/order/{path_segment(request_id, what='request_id')}/files")
        return [m.OrderFile.model_validate(item) for item in r.json()]

    def email(self, request_id: UUID | str, source: str = "raw_email") -> bytes:
        """Download the e-mail an order came from: ``raw_email`` (the original message) or ``email_body``.

        ``AioticNotFoundError`` means the order has no such part (an uploaded file has no e-mail);
        ``AioticServerError`` with status 502 means the tenant's mail intake could not be reached and the e-mail
        may well exist. The bytes are what a sender wrote: sanitise them before you show them in a browser.
        """
        r = self._c._request("GET", f"/order/{path_segment(request_id, what='request_id')}/email/{email_source(source)}", stream=True)
        return r.content

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

    def list(self, *, page: int = 1, size: int = 100, sort: str | None = None) -> m.CustomerListResponse:
        """``sort="-source_updated_at"`` lists the newest source change first, customers without one last."""
        params: dict[str, Any] = {"page": page, "size": size}
        if sort:
            params["sort"] = sort
        r = self._c._request("GET", "/customer/list", params=params)
        return m.CustomerListResponse.model_validate(r.json())

    def latest_source_update(self) -> datetime | None:
        """The newest ``source_updated_at`` AIOTIC holds for a customer (UTC), or ``None`` when no customer has
        one: the moment an incremental sync continues from, inclusively."""
        items = self.list(size=1, sort=NEWEST_SOURCE_CHANGE).items
        return items[0].source_updated_at if items else None

    def bulk_sync(self, rows: Iterable[m.CustomerBulkRow | dict[str, Any]], *, compress: bool = True, timeout: float | None = None) -> m.CustomerBulkResult:
        """Replace **all** customers with ``rows`` in one call (``POST /customer/bulk``).

        AIOTIC stores every row as an upsert would and removes every customer that is not in ``rows``, together
        with the customer item mappings of the removed customers. This method sends what it is given: use
        ``SyncEngine.bulk_sync()`` to have the data set checked against what AIOTIC holds before it is sent.

        ``timeout`` defaults to ``Settings.bulk_timeout`` (120 s). The same body is sent again when an attempt
        ends without an answer; AIOTIC lets a bulk sync it has started run to its end and makes the next one wait,
        so the repeat reports the final state (``result.attempts`` says how often the request was sent). When no
        attempt is answered, ``AioticOutcomeUnknownError`` is raised.
        """
        return m.CustomerBulkResult.model_validate(self._c._bulk("/customer/bulk", rows, m.CustomerBulkRow, compress, timeout))

    def iter_all(self, *, size: int = 500, sort: str | None = None) -> Iterator[m.Customer]:
        page = 1
        while True:
            batch = self.list(page=page, size=size, sort=sort)
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

    def list(self, *, page: int = 1, size: int = 100, language_code: str | None = None, sort: str | None = None) -> m.ProductListResponse:
        """``sort="-source_updated_at"`` lists the newest source change first, products without one last."""
        params: dict[str, Any] = {"page": page, "size": size}
        if language_code:
            params["language_code"] = language_code
        if sort:
            params["sort"] = sort
        r = self._c._request("GET", "/product/list", params=params)
        return m.ProductListResponse.model_validate(r.json())

    def latest_source_update(self, *, language_code: str | None = None) -> datetime | None:
        """The newest ``source_updated_at`` AIOTIC holds for a product (UTC), or ``None`` when no product has one."""
        items = self.list(size=1, language_code=language_code, sort=NEWEST_SOURCE_CHANGE).items
        return items[0].source_updated_at if items else None

    def bulk_sync(self, rows: Iterable[m.ProductBulkRow | dict[str, Any]], *, compress: bool = True, timeout: float | None = None) -> m.ProductBulkResult:
        """Replace **all** products, in every language, with ``rows`` in one call (``POST /product/bulk``).

        A product that is not in ``rows`` is removed together with the customer item mappings that point to it,
        so ``rows`` must hold every language you sell in. See ``Customers.bulk_sync`` for timeouts and repeats, and
        ``SyncEngine.bulk_sync()`` for the checks before sending.
        """
        return m.ProductBulkResult.model_validate(self._c._bulk("/product/bulk", rows, m.ProductBulkRow, compress, timeout))

    def iter_all(self, *, size: int = 1000, language_code: str | None = None, sort: str | None = None) -> Iterator[m.Product]:
        page = 1
        while True:
            batch = self.list(page=page, size=size, language_code=language_code, sort=sort)
            yield from batch.items
            if len(batch.items) < size:
                return
            page += 1

    def get(self, item_number: str, language_code: str) -> m.Product:
        r = self._c._request("GET", f"/product/{path_segment(item_number, what='item number', allow_slash=True)}/{path_segment(language_code, what='language code')}")
        return m.Product.model_validate(r.json())

    def upsert(self, item_number: str, language_code: str, data: m.ProductUpsert | dict[str, Any]) -> m.Product:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = self._c._request("PUT", f"/product/{path_segment(item_number, what='item number', allow_slash=True)}/{path_segment(language_code, what='language code')}", json=body)
        return m.Product.model_validate(r.json())

    def delete(self, item_number: str, language_code: str) -> None:
        self._c._request("DELETE", f"/product/{path_segment(item_number, what='item number', allow_slash=True)}/{path_segment(language_code, what='language code')}")


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
        sort: str | None = None,
    ) -> m.CustomerProductListResponse:
        """``sort="-source_updated_at"`` lists the newest source change first, mappings without one last."""
        params = {
            k: v
            for k, v in {
                "page": page,
                "size": size,
                "customer_number": customer_number,
                "customer_item_number": customer_item_number,
                "item_number": item_number,
                "language_code": language_code,
                "sort": sort,
            }.items()
            if v is not None
        }
        r = self._c._request("GET", "/customer-product/list", params=params)
        return m.CustomerProductListResponse.model_validate(r.json())

    def latest_source_update(self) -> datetime | None:
        """The newest ``source_updated_at`` AIOTIC holds for a mapping (UTC), or ``None`` when no mapping has one."""
        items = self.list(size=1, sort=NEWEST_SOURCE_CHANGE).items
        return items[0].source_updated_at if items else None

    def bulk_sync(self, rows: Iterable[m.CustomerProductBulkRow | dict[str, Any]], *, compress: bool = True, timeout: float | None = None) -> m.CustomerProductBulkResult:
        """Replace **all** customer item mappings with ``rows`` in one call (``POST /customer-product/bulk``).

        A row whose customer or product AIOTIC does not hold is skipped, not refused: the call still succeeds,
        the row is counted in ``result.skipped_total`` and listed in ``result.skipped``, and a mapping AIOTIC held
        under that key is removed. Always read ``skipped_total``. See ``Customers.bulk_sync`` for timeouts and
        repeats, and ``SyncEngine.bulk_sync()`` for the checks before sending.
        """
        return m.CustomerProductBulkResult.model_validate(self._c._bulk("/customer-product/bulk", rows, m.CustomerProductBulkRow, compress, timeout))

    def iter_all(self, *, size: int = 1000, **filters: Any) -> Iterator[m.CustomerProduct]:
        page = 1
        while True:
            batch = self.list(page=page, size=size, **filters)
            yield from batch.items
            if len(batch.items) < size:
                return
            page += 1

    def get(self, customer_number: str, customer_item_number: str) -> m.CustomerProduct:
        r = self._c._request("GET", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number', allow_slash=True)}")
        return m.CustomerProduct.model_validate(r.json())

    def upsert(
        self, customer_number: str, customer_item_number: str, data: m.CustomerProductUpsert | dict[str, Any]
    ) -> m.CustomerProduct:
        body = data if isinstance(data, dict) else data.model_dump(mode="json", exclude_none=True)
        r = self._c._request("PUT", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number', allow_slash=True)}", json=body)
        return m.CustomerProduct.model_validate(r.json())

    def delete(self, customer_number: str, customer_item_number: str) -> None:
        self._c._request("DELETE", f"/customer-product/{path_segment(customer_number, what='customer number')}/{path_segment(customer_item_number, what='customer item number', allow_slash=True)}")


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
        content: bytes | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        """One API call with retries. ``idempotent`` decides whether a request may be sent a second time when the
        outcome of the first one is unknown; by default only GET, PUT and DELETE are (see :mod:`aiotic._transport`).
        The response records how often the request was sent in ``extensions["aiotic_attempts"]``."""
        if idempotent is None:
            idempotent = method in IDEMPOTENT_METHODS
        headers = {**(build_headers(self.settings, path) if auth else {}), **(extra_headers or {})}
        last_exc: Exception | None = None
        unanswered = False  # an earlier attempt was sent and nobody said what became of it
        for attempt in range(self.settings.max_retries + 1):
            self._bucket.acquire()
            try:
                response = self._http.request(method, path, params=params, json=json, data=data, files=files, content=content, headers=headers, timeout=timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_exc = exc
                if attempt >= self.settings.max_retries or not replay_allowed(exc, idempotent=idempotent):
                    error = transport_error(exc, path)
                    error.earlier_attempt_unanswered = unanswered
                    raise error from exc
                unanswered = unanswered or unanswered_attempt(exc)
                time.sleep(backoff_delay(attempt))
                continue
            if should_retry(response, method, idempotent=idempotent) and attempt < self.settings.max_retries:
                unanswered = unanswered or unanswered_attempt(response)
                time.sleep(backoff_delay(attempt, response.headers.get("Retry-After")))
                continue
            try:
                raise_for_status(response, path)
            except AioticError as error:
                error.earlier_attempt_unanswered = unanswered
                raise
            response.extensions["aiotic_attempts"] = attempt + 1
            return response
        raise AioticError(f"{path}: giving up after {self.settings.max_retries} retries", detail=str(last_exc))

    def _bulk(self, path: str, rows: Iterable[Any], model: type[m.BaseModel], compress: bool, timeout: float | None) -> dict[str, Any]:
        """Send one bulk sync. The body is built once, so every attempt carries the same data set."""
        body, headers = bulk_body(rows, model, compress=compress)
        try:
            # Sending the same complete data set again is safe: AIOTIC runs one bulk sync at a time and the repeat
            # finds the data as the first attempt left it.
            r = self._request("POST", path, content=body, extra_headers=headers, timeout=timeout if timeout is not None else self.settings.bulk_timeout, idempotent=True)
        except AioticError as exc:
            if outcome_unknown(exc):
                raise bulk_outcome_unknown(path, exc) from exc
            raise
        return {**r.json(), "attempts": r.extensions.get("aiotic_attempts", 1)}
