import pytest
from pydantic import ValidationError

from app.schemas.enums import JobType
from app.schemas.payloads import (
    MAX_BATCH_ITEMS,
    BatchPayload,
    EmailPayload,
    ReportPayload,
    WebhookPayload,
    validate_payload,
)


def test_batch_payload_accepts_heterogeneous_items():
    p = validate_payload(
        JobType.batch,
        {
            "items": [
                {"type": "email", "to": "a@b.com", "subject": "Hi"},
                {"type": "webhook", "url": "https://x.test"},
                {"type": "report", "report_type": "sales"},
            ]
        },
    )
    assert isinstance(p, BatchPayload)
    assert isinstance(p.items[0], EmailPayload)
    assert isinstance(p.items[1], WebhookPayload)
    assert isinstance(p.items[2], ReportPayload)


def test_batch_rejects_item_with_unknown_type():
    with pytest.raises(ValidationError):
        BatchPayload(items=[{"type": "sms", "to": "+1"}])


def test_batch_rejects_item_missing_required_field():
    with pytest.raises(ValidationError):
        BatchPayload(items=[{"type": "webhook"}])  # missing required 'url'


def test_batch_rejects_nested_batch_item():
    with pytest.raises(ValidationError):
        BatchPayload(items=[{"type": "batch", "items": []}])


def test_batch_rejects_too_many_items():
    item = {"type": "email", "to": "a@b.com", "subject": "Hi"}
    with pytest.raises(ValidationError):
        BatchPayload(items=[item for _ in range(MAX_BATCH_ITEMS + 1)])
