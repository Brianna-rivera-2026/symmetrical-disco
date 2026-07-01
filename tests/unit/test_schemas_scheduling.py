from datetime import datetime, timedelta, timezone

from app.schemas.api import JobSubmission

_PAYLOAD = {"to": "a@b.com", "subject": "Hi"}


def test_naive_scheduled_at_becomes_utc():
    s = JobSubmission(
        type="email", payload=_PAYLOAD, scheduled_at=datetime(2030, 1, 1, 12, 0, 0)
    )
    assert s.scheduled_at == datetime(2030, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_aware_scheduled_at_converted_to_utc():
    tz = timezone(timedelta(hours=2))
    s = JobSubmission(
        type="email",
        payload=_PAYLOAD,
        scheduled_at=datetime(2030, 1, 1, 12, 0, 0, tzinfo=tz),
    )
    assert s.scheduled_at == datetime(2030, 1, 1, 10, 0, 0, tzinfo=timezone.utc)


def test_scheduled_at_is_optional():
    s = JobSubmission(type="email", payload=_PAYLOAD)
    assert s.scheduled_at is None
