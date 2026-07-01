from app.idempotency import canonical_hash
from app.schemas.enums import JobType


def test_hash_is_stable_across_key_order():
    a = canonical_hash(JobType.email, {"to": "x", "subject": "y"})
    b = canonical_hash(JobType.email, {"subject": "y", "to": "x"})
    assert a == b


def test_hash_differs_for_different_payloads():
    a = canonical_hash(JobType.email, {"to": "x", "subject": "y"})
    b = canonical_hash(JobType.email, {"to": "z", "subject": "y"})
    assert a != b


def test_hash_differs_for_different_type():
    a = canonical_hash(JobType.email, {"k": 1})
    b = canonical_hash(JobType.webhook, {"k": 1})
    assert a != b
