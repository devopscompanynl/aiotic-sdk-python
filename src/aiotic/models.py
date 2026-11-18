"""Pydantic models mirroring the public AIOTIC OpenAPI document (spec/openapi.yaml).

Field names and nullability follow the API exactly; optional fields are ``None`` when the API
sends ``null``. Unknown fields are preserved (``extra="allow"``) so a newer server never breaks
an older SDK.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


# --------------------------------------------------------------------------------------------
# Order lifecycle
# --------------------------------------------------------------------------------------------


class OrderStatusValue(StrEnum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    ATTENTION = "ATTENTION"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"
    RETRY_PENDING = "RETRY_PENDING"
    MODIFIED = "MODIFIED"
    REPROCESSED = "REPROCESSED"
    SENDING = "SENDING"
    SENT = "SENT"
    CANCELED = "CANCELED"

    @property
    def is_terminal(self) -> bool:
        return self in {OrderStatusValue.SENT, OrderStatusValue.CANCELED, OrderStatusValue.REPROCESSED}

    @property
    def is_sendable(self) -> bool:
        return self in SENDABLE_STATUSES

    @property
    def needs_human(self) -> bool:
        return self in {OrderStatusValue.ATTENTION, OrderStatusValue.FAILED}


SENDABLE_STATUSES = frozenset({OrderStatusValue.PROCESSED, OrderStatusValue.MODIFIED, OrderStatusValue.ATTENTION})
LANDED_STATUSES = frozenset(
    {OrderStatusValue.PROCESSED, OrderStatusValue.ATTENTION, OrderStatusValue.FAILED, OrderStatusValue.MODIFIED}
)


class QuantityState(StrEnum):
    VALID = "Valid"
    EMPTY = "Empty"
    ZERO = "Zero"
    UNRECOGNISED = "Unrecognised"


# --------------------------------------------------------------------------------------------
# Purchase order (as stored on the order: OrderStatus.result)
# --------------------------------------------------------------------------------------------


class Address(_Model):
    street: str
    postal_code: str
    city: str
    country: str | None = None


class Supplier(_Model):
    company: str
    contact_person: str | None = None
    email: str | None = None
    address: Address


class OrderCustomer(_Model):
    customer_id: str | None = None
    company: str
    contact_person: str | None = None
    email: str | None = None
    phone: str | None = None
    branch: str | None = None
    vat_id: str | None = None
    iban: str | None = None
    bic: str | None = None
    address: Address


class ShippingRecipient(_Model):
    company: str
    contact_person: str | None = None
    department: str | None = None
    email: str | None = None
    phone: str | None = None
    address: Address


class ShippingDetails(_Model):
    recipient: ShippingRecipient
    special_instructions: str | None = None


class OrderItem(_Model):
    article_number: str | None = None
    customer_item_number: str | None = None
    description: str | None = None
    quantity: int | None = None
    quantity_state: QuantityState | None = None
    unit: str | None = None
    price: float | None = None
    currency: str | None = None
    line_total: float | None = None


class PurchaseOrder(_Model):
    """The extracted purchase order. ``order_date`` is the date printed on the document; when the document shows
    none, AIOTIC fills in the date the e-mail was received or else the date of processing (UTC). It is ``None``
    only in the rare case that this could not be applied."""

    order_number: str
    order_date: str | None = None
    delivery_date: str | None = None
    delivery_date_from: str | None = None
    delivery_date_to: str | None = None
    supplier: Supplier
    customer: OrderCustomer | None = None
    shipping_details: ShippingDetails | None = None
    items: list[OrderItem] = Field(default_factory=list)
    total_price: float | None = None
    currency: str | None = None
    additional_information: str | None = None

    @property
    def unresolved_items(self) -> list[OrderItem]:
        """Lines without an article number — these put an order into ATTENTION."""
        return [i for i in self.items if not i.article_number]


class OrderStatus(_Model):
    request_id: UUID
    status: OrderStatusValue
    timestamp: datetime
    attachments: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    result: PurchaseOrder | None = None
    state: dict[str, Any] | None = None
    erp_ref: str | None = None
    email_group_id: UUID | None = None
    order_label: str | None = None
    retry_count: int = 0
    next_retry_at: datetime | None = None
    last_error: str | None = None


class OrderRef(_Model):
    request_id: UUID
    status: OrderStatusValue
    order_label: str | None = None


class OrderUploadResponse(_Model):
    request_id: UUID
    split: bool = False
    email_group_id: UUID | None = None
    orders: list[OrderRef] | None = None

    @property
    def request_ids(self) -> list[UUID]:
        """The ids to poll: the children when split, otherwise the single request id."""
        return [o.request_id for o in self.orders] if self.split and self.orders else [self.request_id]


class OrderListResponse(_Model):
    items: list[OrderStatus]
    total: int
    limit: int
    offset: int


class OrderGroup(_Model):
    email_group_id: UUID
    message_id: str | None = None
    order_count: int
    orders: list[OrderStatus]


class OrderFile(_Model):
    """One file of an order (``GET /order/{request_id}/files``). ``source`` says what it is. ``raw_email`` and
    ``email_body`` are the original e-mail and its body: read those with ``orders.email()``. Every other value
    (``upload``, ``email_attachment`` and values added later) is a file to read by name with
    ``orders.download_file()``. ``id`` and ``created_at`` are ``None`` for an e-mail part that the tenant's mail
    intake holds."""

    id: int | None = None
    filename: str | None = None
    source: str
    content_type: str | None = None
    size_bytes: int | None = None
    created_at: datetime | None = None


class EmailClassification(_Model):
    category: str


class RawUploadRejection(_Model):
    """Structured ``detail`` returned by ``/order/raw/upload`` for non-purchase-order e-mails."""

    error: str
    message: str
    request_id: UUID | None = None
    category: str | None = None
    subject: str | None = None
    language: str | None = None
    line_item_count: int | None = None


# --------------------------------------------------------------------------------------------
# ERP hand-off (what AIOTIC sends to your receive endpoint, and what you answer)
# --------------------------------------------------------------------------------------------


class ErpAddress(_Model):
    street: str | None = None
    postal_code: str | None = None
    city: str | None = None
    country: str | None = None


class ErpCustomer(_Model):
    customer_id: str | None = None
    company: str | None = None
    contact_person: str | None = None
    email: str | None = None
    phone: str | None = None
    iban: str | None = None
    bic: str | None = None
    vat_id: str | None = None
    address: ErpAddress = Field(default_factory=ErpAddress)


class ErpRecipient(_Model):
    company: str | None = None
    department: str | None = None
    contact_person: str | None = None
    email: str | None = None
    phone: str | None = None
    address: ErpAddress = Field(default_factory=ErpAddress)


class ErpShippingDetails(_Model):
    recipient: ErpRecipient = Field(default_factory=ErpRecipient)
    special_instructions: str | None = None


class ErpOrderItem(_Model):
    article_number: str | None = None
    description: str | None = None
    quantity: int | None = None
    unit: str | None = None
    price: float | None = None
    currency: str | None = None
    line_total: float | None = None


class ErpPurchaseOrder(_Model):
    order_number: str
    order_date: str | None = None
    delivery_date: str | None = None
    currency: str | None = None
    total_price: float | None = None
    additional_information: str | None = None
    supplier: Supplier | None = None
    customer: ErpCustomer = Field(default_factory=ErpCustomer)
    shipping_details: ErpShippingDetails = Field(default_factory=ErpShippingDetails)
    items: list[ErpOrderItem] = Field(default_factory=list)


class ErpReceiveRequest(_Model):
    """Body AIOTIC POSTs to your ERP receive endpoint."""

    request_id: UUID
    purchase_order: ErpPurchaseOrder


class ErpReceiveResponse(_Model):
    """Body your receive endpoint must return. A boolean ``success`` is authoritative at any HTTP status.

    ``retryable`` belongs to a refusal only: ``True`` tells AIOTIC that the refusal is temporary and that sending
    the same order again cannot create it twice. AIOTIC then sends it again (2 more times by default, a few
    seconds apart, with the same ``request_id``) before the send counts as refused.
    """

    success: bool
    order_number: str | None = None
    error: str | None = None
    retryable: bool | None = None

    @classmethod
    def accepted(cls, order_number: str) -> "ErpReceiveResponse":
        return cls(success=True, order_number=order_number)

    @classmethod
    def rejected(cls, error: str, *, retryable: bool = False) -> "ErpReceiveResponse":
        """A refusal. Pass ``retryable=True`` only for a temporary reason (a record that is in use for a moment, a
        deadlock) **and** only when a repeat is safe: the refused attempt created nothing, or your ERP rejects a
        second order with the same ``request_id``."""
        return cls(success=False, error=error, retryable=True if retryable else None)


class ErpSendResponse(_Model):
    """Response of ``POST /erp/send/{request_id}``."""

    success: bool
    request_id: UUID
    data: ErpReceiveResponse | dict[str, Any]

    @property
    def erp_order_number(self) -> str | None:
        d = self.data
        return d.order_number if isinstance(d, ErpReceiveResponse) else (d.get("order_number") if isinstance(d, dict) else None)


class ProcessingWebhookRequest(_Model):
    """Body of the optional processing webhook (sent before human review)."""

    request_id: UUID
    purchase_order: PurchaseOrder


# --------------------------------------------------------------------------------------------
# Rejected e-mails
# --------------------------------------------------------------------------------------------


class ClassifiedEmail(_Model):
    request_id: UUID
    email_type: str
    sender: str
    from_name: str
    subject: str
    timestamp: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)
    message_id: str | None = None
    classification_reason: str | None = None
    rejection_status: str | None = None
    original_email_type: str | None = None


class RejectedEmailListResponse(_Model):
    items: list[ClassifiedEmail]
    total: int
    limit: int
    offset: int


class ReprocessResponse(_Model):
    request_id: UUID
    status: str


# --------------------------------------------------------------------------------------------
# Master data
# --------------------------------------------------------------------------------------------


class CustomerUpsert(_Model):
    """What you send when creating or updating a customer. ``name`` is required and may not be blank: the API
    refuses a customer without one. ``source_updated_at`` is when your ERP last changed the record; it needs a
    zone or offset, and a write without it clears the value AIOTIC held."""

    id: UUID | None = None
    name: str
    postal_code: str | None = None
    city: str | None = None
    address: str | None = None
    contact_person: str | None = None
    phone_number: str | None = None
    vat_number: str | None = None
    email: str | None = None
    coc_number: str | None = None
    home_page: str | None = None
    source_updated_at: AwareDatetime | None = None

    @field_validator("name")
    @classmethod
    def _name_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("a customer needs a name: it may not be empty or only spaces")
        return value


class Customer(_Model):
    """A customer as AIOTIC returns it. ``name`` can be absent on records stored before the API required one;
    writing a customer (:class:`CustomerUpsert`) requires it. ``source_updated_at`` is in UTC."""

    number: str
    id: UUID | None = None
    name: str | None = None
    postal_code: str | None = None
    city: str | None = None
    address: str | None = None
    contact_person: str | None = None
    phone_number: str | None = None
    vat_number: str | None = None
    email: str | None = None
    coc_number: str | None = None
    home_page: str | None = None
    similarity: float | None = None
    archived_at: datetime | None = None
    superseded_by: str | None = None
    source_updated_at: datetime | None = None


class CustomerListResponse(_Model):
    items: list[Customer]
    total: int
    limit: int
    offset: int


class CustomerSearchResponse(_Model):
    items: list[Customer]
    total: int
    limit: int


class ProductUpsert(_Model):
    description: str
    remark: str | None = None
    source_updated_at: AwareDatetime | None = None


class Product(_Model):
    """A product as AIOTIC returns it. ``description`` can be absent on records that were created without one;
    writing a product (:class:`ProductUpsert`) still requires it, as the API does."""

    item_number: str
    language_code: str
    description: str | None = None
    remark: str | None = None
    created_at: datetime | None = None
    source_updated_at: datetime | None = None


class ProductListResponse(_Model):
    items: list[Product]
    total: int
    limit: int
    offset: int


class CustomerProductUpsert(_Model):
    item_number: str
    language_code: str
    source_updated_at: AwareDatetime | None = None


class CustomerProduct(_Model):
    customer_number: str
    customer_item_number: str
    item_number: str
    language_code: str
    created_at: datetime | None = None
    source_updated_at: datetime | None = None


class CustomerProductListResponse(_Model):
    items: list[CustomerProduct]
    total: int
    limit: int
    offset: int


# --------------------------------------------------------------------------------------------
# Bulk sync: one call carries a data set's complete content and replaces what AIOTIC holds
# --------------------------------------------------------------------------------------------


class CustomerBulkRow(CustomerUpsert):
    """One customer in ``customers.bulk_sync()``: the number plus the fields of an upsert."""

    number: str = Field(min_length=1)


class ProductBulkRow(ProductUpsert):
    """One product record in ``products.bulk_sync()``. Send every language you sell in: a bulk sync replaces the
    products of all languages."""

    item_number: str = Field(min_length=1)
    language_code: str = Field(min_length=1)
    created_at: datetime | None = None


class CustomerProductBulkRow(CustomerProductUpsert):
    """One customer item mapping in ``customer_products.bulk_sync()``."""

    customer_number: str = Field(min_length=1)
    customer_item_number: str = Field(min_length=1)
    created_at: datetime | None = None


class BulkResult(_Model):
    """What a bulk sync did. Every row sent is counted once in ``inserted``, ``updated``, ``unchanged`` or (for
    mappings) ``skipped_total``; ``deleted`` counts what AIOTIC held and the call removed.

    ``attempts`` is set by the SDK, not by the API: how often the request was sent. Above 1 the counts are those
    of the last attempt; an earlier attempt that ended without an answer may already have applied the changes,
    which then show up here as ``unchanged``.
    """

    received: int
    inserted: int
    updated: int
    unchanged: int
    deleted: int
    attempts: int = 1


class CustomerBulkResult(BulkResult):
    mappings_deleted: int = 0


class ProductBulkResult(BulkResult):
    mappings_deleted: int = 0


class SkippedMapping(_Model):
    """A mapping in a bulk sync that was not stored. ``reason`` is ``customer_missing`` or ``product_missing``."""

    customer_number: str
    customer_item_number: str
    reason: str


class CustomerProductBulkResult(BulkResult):
    """``skipped`` lists the first 1000 rows that were not stored because AIOTIC holds no such customer or product.
    A mapping AIOTIC held under a skipped row's key is removed and counted in ``deleted``."""

    skipped_total: int = 0
    skipped: list[SkippedMapping] = Field(default_factory=list)


# --------------------------------------------------------------------------------------------
# Health
# --------------------------------------------------------------------------------------------


class HealthCheck(_Model):
    status: str


class SystemStatus(_Model):
    status: str
    message: str | None = None
    updated_at: datetime | None = None


class FetchAllEmailsResponse(_Model):
    status: str
    emails_queued: int
    emails_total: int
    message: str
