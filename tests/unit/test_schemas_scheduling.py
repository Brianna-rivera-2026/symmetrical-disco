from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.schemas.api import JobSubmission

_PAYLOAD = {"to": "a@b.com", "subject": "Hi"}


def test_naive_scheduled_at_becomes_utc():
    future_utc = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=30)
    naive = future_utc.replace(tzinfo=None)
    s = JobSubmission(type="email", payload=_PAYLOAD, scheduled_at=naive)
    assert s.scheduled_at == future_utc


def test_aware_scheduled_at_converted_to_utc():
    tz = timezone(timedelta(hours=2))
    future_utc = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=30)
    future_local = future_utc.astimezone(tz)
    s = JobSubmission(type="email", payload=_PAYLOAD, scheduled_at=future_local)
    assert s.scheduled_at == future_utc


def test_scheduled_at_is_optional():
    s = JobSubmission(type="email", payload=_PAYLOAD)
    assert s.scheduled_at is None


def test_submission_rejects_far_future_schedule():
    far = datetime.now(timezone.utc) + timedelta(days=366)
    with pytest.raises(ValidationError):
        JobSubmission(type="email", payload={}, scheduled_at=far)


def test_submission_rejects_empty_idempotency_key():
    with pytest.raises(ValidationError):
        JobSubmission(type="email", payload={}, idempotency_key="")


def test_submission_rejects_unknown_keys():
    with pytest.raises(ValidationError):
        JobSubmission(type="email", payload={}, surprise=True)
