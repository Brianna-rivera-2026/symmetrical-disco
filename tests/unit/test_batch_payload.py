import pytest
from pydantic import ValidationError

from app.schemas.enums import JobType
from app.schemas.payloads import MAX_BATCH_ITEMS, BatchPayload, validate_payload


def test_batch_payload_defaults():
    p = validate_payload(JobType.batch, {"items": [{"x": 1}]})
    assert isinstance(p, BatchPayload)
    assert p.item_delay_ms == 50
    assert p.items == [{"x": 1}]


def test_batch_rejects_too_many_items():
    with pytest.raises(ValidationError):
        BatchPayload(items=[{} for _ in range(MAX_BATCH_ITEMS + 1)])


def test_budget_validator_rejects_doomed_batch():
    ctx = {"handler_timeout_s": 45.0, "safety_factor": 0.8}
    # 1000 items * 50ms = 50s >= 45*0.8 = 36s -> reject
    with pytest.raises(ValueError):
        validate_payload(
            JobType.batch,
            {"items": [{} for _ in range(1000)], "item_delay_ms": 50},
            context=ctx,
        )


def test_budget_validator_accepts_under_budget():
    ctx = {"handler_timeout_s": 45.0, "safety_factor": 0.8}
    p = validate_payload(
        JobType.batch,
        {"items": [{} for _ in range(10)], "item_delay_ms": 50},
        context=ctx,
    )
    assert len(p.items) == 10


def test_budget_check_skipped_without_context():
    # 1000 * 50ms would be doomed, but no context -> only the size cap applies
    p = validate_payload(
        JobType.batch, {"items": [{} for _ in range(100)], "item_delay_ms": 50}
    )
    assert len(p.items) == 100
