import pytest

from app import repository as repo
from app.jobs import handlers
from app.schemas.enums import JobStatus, JobType
from app.worker.runner import process_job


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def test_process_job_completes_email(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    process_job(db_session, job.id)
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert "message_id" in job.result


def test_process_job_marks_failure(db_session, monkeypatch):
    monkeypatch.setattr(
        handlers.random, "random", lambda: 0.05
    )  # force webhook failure
    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    process_job(db_session, job.id)
    db_session.refresh(job)
    assert job.status is JobStatus.failed
    assert job.error["type"] == "WebhookFailedError"


def test_duplicate_delivery_is_noop(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    process_job(db_session, job.id)
    db_session.refresh(job)
    first_result = job.result
    # Second delivery: already completed, claim guard fails → no change.
    process_job(db_session, job.id)
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.result == first_result


def test_invalid_payload_fails_job(db_session):
    job = repo.create_job(db_session, JobType.email, {"missing": "recipient"})
    process_job(db_session, job.id)
    db_session.refresh(job)
    assert job.status is JobStatus.failed


def test_run_forever_processes_one_then_stops(test_settings, redis_client, pg_engine):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        job = repo.create_job(s, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    enqueue(redis_client, test_settings.stream_normal, str(job.id))

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    run_forever(test_settings, stop=stop)

    with factory() as s:
        refreshed = repo.get_job(s, job.id)
    assert refreshed.status is JobStatus.completed


def test_run_forever_drains_high_before_low(test_settings, redis_client, pg_engine):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.schemas.enums import JobPriority
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        low = repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            priority=JobPriority.low,
        )
        high = repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            priority=JobPriority.high,
        )
    # Low is enqueued first, but high must be processed first.
    enqueue(redis_client, test_settings.stream_low, str(low.id))
    enqueue(redis_client, test_settings.stream_high, str(high.id))

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    run_forever(test_settings, stop=stop)  # exactly one processing pass

    with factory() as s:
        assert repo.get_job(s, high.id).status is JobStatus.completed
        assert repo.get_job(s, low.id).status is JobStatus.pending  # untouched
    # High was acked on its own stream; low is still queued.
    assert (
        redis_client.xpending(test_settings.stream_high, test_settings.consumer_group)[
            "pending"
        ]
        == 0
    )
    assert redis_client.xlen(test_settings.stream_low) == 1
