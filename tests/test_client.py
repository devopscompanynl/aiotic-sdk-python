import uuid

import pytest

from aiotic import AioticAuthError, AioticClient, AioticConflictError, AioticNotFoundError, AioticValidationError
from aiotic.models import OrderStatusValue


def test_health_and_status(client: AioticClient) -> None:
    assert client.health().status == "ok"
    assert client.system_status().status == "operational"


def test_upload_and_wait(client: AioticClient) -> None:
    rid = uuid.uuid4()
    up = client.orders.upload([("PO-4711.pdf", b"%PDF-1.4 fake")], request_id=rid, metadata={"my_ref": "T-1"})
    assert up.request_id == rid and not up.split
    st = client.orders.wait(rid, timeout=10, initial_interval=0.1)
    assert st.status == OrderStatusValue.PROCESSED
    assert st.result and st.result.order_number == "PO-4711"
    assert st.metadata["my_ref"] == "T-1"
    assert st.result.unresolved_items == []


def test_attention_when_article_unknown(client: AioticClient) -> None:
    up = client.orders.upload([("PO-4712-attention.pdf", b"x")])
    st = client.orders.wait(up.request_id, timeout=10, initial_interval=0.1)
    assert st.status == OrderStatusValue.ATTENTION
    assert len(st.result.unresolved_items) == 1


def test_failed_then_retry(client: AioticClient) -> None:
    up = client.orders.upload([("PO-4713-fail.pdf", b"x")])
    st = client.orders.wait(up.request_id, timeout=10, initial_interval=0.1)
    assert st.status == OrderStatusValue.FAILED and st.last_error
    new = client.orders.retry(up.request_id)
    assert new.request_id != up.request_id
    assert client.orders.get(up.request_id).status == OrderStatusValue.REPROCESSED
    assert client.orders.wait(new.request_id, timeout=10, initial_interval=0.1).status == OrderStatusValue.PROCESSED


def test_raw_email_rejection_is_structured(client: AioticClient) -> None:
    with pytest.raises(AioticValidationError) as exc:
        client.orders.upload_raw_email(("quote.eml", b"Subject: Quotation\n\nour quotation"))
    assert exc.value.detail["error"] == "not_a_purchase_order"
    assert exc.value.request_id
    assert client.rejected.list().total >= 1


def test_raw_email_split(client: AioticClient) -> None:
    up = client.orders.upload_raw_email(("two.eml", b"Subject: a\nSubject: b\nSPLIT"))
    assert up.split and up.email_group_id and len(up.request_ids) == 2
    group = client.orders.group(up.email_group_id)
    assert group.order_count == 2


def test_errors(client: AioticClient, mock_url: str) -> None:
    with pytest.raises(AioticNotFoundError):
        client.orders.get(uuid.uuid4())
    with pytest.raises(AioticValidationError):
        client.orders.get("not-a-uuid")
    with AioticClient(base_url=mock_url, api_key="wrong", rate_limit=0, max_retries=0) as bad:
        with pytest.raises(AioticAuthError):
            bad.orders.list()


def test_sync_key_scope(mock_url: str) -> None:
    with AioticClient(base_url=mock_url, api_key="mock-sync-key", rate_limit=0, max_retries=0) as sync_only:
        assert sync_only.products.list().total >= 3
        with pytest.raises(AioticAuthError):
            sync_only.orders.list()


def test_master_data_roundtrip(client: AioticClient) -> None:
    client.customers.upsert("C-1", {"name": "Test BV", "city": "Utrecht"})
    assert client.customers.get("C-1").name == "Test BV"
    client.products.upsert("X-1", "nl", {"description": "Thing"})
    with pytest.raises(AioticNotFoundError):  # FK: product must exist
        client.customer_products.upsert("C-1", "CUST-X", {"item_number": "NOPE", "language_code": "nl"})
    m = client.customer_products.upsert("C-1", "CUST-X", {"item_number": "X-1", "language_code": "nl"})
    assert m.item_number == "X-1"
    assert client.customer_products.list(customer_number="C-1").total == 1
    assert client.customers.search("Test Utrecht").items[0].number == "C-1"
    client.customer_products.delete("C-1", "CUST-X")
    client.products.delete("X-1", "nl")
    client.customers.delete("C-1")
    with pytest.raises(AioticNotFoundError):
        client.customers.get("C-1")


def test_send_requires_sendable_status(client: AioticClient, http) -> None:
    http.post("/_mock/config", json={"erp_url": "http://127.0.0.1:1/nowhere", "erp_key": "k"})
    up = client.orders.upload([("PO-1.pdf", b"x")])
    with pytest.raises(AioticConflictError):
        client.erp.send(up.request_id)  # still QUEUED
    http.post("/_mock/config", json={"erp_url": None})
