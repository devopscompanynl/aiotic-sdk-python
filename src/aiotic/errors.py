"""Exception hierarchy. Every error carries the HTTP status and the server's ``detail`` (string or object)."""

from __future__ import annotations

from typing import Any


class AioticError(Exception):
    """Base class for all SDK errors."""

    def __init__(self, message: str, *, status: int | None = None, detail: Any = None, request_id: str | None = None):
        super().__init__(message)
        self.message = message
        self.status = status
        self.detail = detail
        self.request_id = request_id

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.message}" + (f" (HTTP {self.status})" if self.status else "")


class AioticAuthError(AioticError):
    """401 — missing or invalid API key (or the key is not accepted on this endpoint)."""


class AioticNotFoundError(AioticError):
    """404 — order, customer, product or mapping does not exist."""


class AioticConflictError(AioticError):
    """409 — the order is not in a sendable status, is already being sent, or a reprocess is already claimed."""


class AioticValidationError(AioticError):
    """400 / 415 / 422 — the request was rejected (malformed id, unsupported file, not a purchase order, …).

    For ``/order/raw/upload`` the ``detail`` attribute is the structured rejection object
    (``error``, ``category``, ``subject``, …) when the e-mail was not a purchase order.
    """


class AioticErpRejectedError(AioticError):
    """422 on ``POST /erp/send`` — your ERP receive endpoint answered ``success: false``.

    ``erp_error`` holds the ``error`` text your endpoint returned (also shown to operators).
    """

    def __init__(self, message: str, *, erp_error: str | None, **kw: Any):
        super().__init__(message, **kw)
        self.erp_error = erp_error


class AioticUnavailableError(AioticError):
    """503 — tenant not initialised / classification or ERP integration not configured."""


class AioticServerError(AioticError):
    """5xx — server-side failure (also used when the ERP endpoint returned a non-JSON body)."""


class AioticTransportError(AioticError):
    """Network problem after all retries were exhausted."""


class AioticIdentifierError(AioticError, ValueError):
    """A customer number, item number, file name or search text cannot be sent safely as one URL path segment
    (it is empty, a dot segment, or contains a slash or a control character). Raised before any request is made."""


def error_for_status(status: int, detail: Any, *, path: str, request_id: str | None = None) -> AioticError:
    """Map an HTTP status + body to the right exception."""
    text = detail if isinstance(detail, str) else (detail.get("message") if isinstance(detail, dict) else None)
    msg = f"{path}: {text or detail or 'request failed'}"
    kw = {"status": status, "detail": detail, "request_id": request_id}
    if status == 401:
        return AioticAuthError(msg, **kw)
    if status == 404:
        return AioticNotFoundError(msg, **kw)
    if status == 409:
        return AioticConflictError(msg, **kw)
    if status == 422 and path.startswith("/erp/send"):
        erp_error = text.removeprefix("ERP error: ") if isinstance(text, str) else None
        return AioticErpRejectedError(msg, erp_error=erp_error, **kw)
    if status in (400, 415, 422):
        return AioticValidationError(msg, **kw)
    if status == 503:
        return AioticUnavailableError(msg, **kw)
    return AioticServerError(msg, **kw)
