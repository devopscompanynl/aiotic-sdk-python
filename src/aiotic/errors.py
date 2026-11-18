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
        #: True when an earlier attempt of the same call ended without an answer that settles it (a dropped
        #: connection after sending, a 502, 503 or 504). The request may have been carried out by that attempt,
        #: whatever this error says about the last one.
        self.earlier_attempt_unanswered = False

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.message}" + (f" (HTTP {self.status})" if self.status else "")


class AioticAuthError(AioticError):
    """401 — missing or invalid API key (or the key is not accepted on this endpoint)."""


class AioticNotFoundError(AioticError):
    """404 — order, customer, product or mapping does not exist."""


class AioticConflictError(AioticError):
    """409 — the order is not in a sendable status, is already being sent, a reprocess is already claimed, the
    tenant does not accept direct uploads, or a new customer carries the ``id`` of another customer."""


class AioticValidationError(AioticError):
    """400 / 413 / 415 / 422 — the request was rejected (malformed id, unsupported file, not a purchase order, a
    record AIOTIC refuses, a bulk sync that is too large, …). For a refused record or bulk sync ``detail`` is the
    list of problems, each with the field (and for a bulk sync the row's position) in ``loc``.

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
    """503 — tenant not initialised, classification or ERP integration not configured, or the request was refused by
    a gateway in front of AIOTIC (for example the rate limit per client IP address). The status alone does not say
    which, and a gateway's answer carries no JSON ``detail``."""


class AioticServerError(AioticError):
    """5xx — server-side failure (also used when the ERP endpoint returned a non-JSON body)."""


class AioticTransportError(AioticError):
    """Network problem after all retries were exhausted."""


class AioticOutcomeUnknownError(AioticError):
    """The request was sent, no usable answer came back, and the SDK could not establish what happened.

    Raised by ``orders.upload()`` when the upload may or may not have been stored (``request_id`` holds the id to
    look up before you decide to upload again), and by the bulk syncs when an attempt ended without an answer and no
    later attempt settled it (AIOTIC finishes a bulk sync it has started, so the data set may already be replaced).
    """


class AioticIdentifierError(AioticError, ValueError):
    """A customer number, item number, file name or search text cannot be sent safely as one URL path segment
    (it is empty, a dot segment, or contains a control character, a backslash or, except in a product's item number
    and a customer's own item number, a slash). Raised before any request is made."""


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
    if status in (400, 413, 415, 422):
        return AioticValidationError(msg, **kw)
    if status == 503:
        return AioticUnavailableError(msg, **kw)
    return AioticServerError(msg, **kw)
