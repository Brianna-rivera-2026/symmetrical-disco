import time

import pytest

from app import repository as repo
from app.jobs import handlers
from app.schemas.enums import JobPriority, JobStatus, JobType
from app.worker.runner import process_job

# handlers.time is the real stdlib `time` module (not a wrapper), so patching
# handlers.time.sleep patches time.sleep globally. Capture the genuine
# implementation now, before any fixture monkeypatches it, so tests that need
# a real (slow) sleep don't recurse into their own patched no-op.
_real_sleep = time.sleep


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def test_process_job_completes_email(db_session, redis_client, test_settings):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "completed"
    assert outcome.ack is True
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1


def test_process_job_retries_on_handler_failure(
    db_session, redis_client, test_settings, monkeypatch
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # force webhook fail
    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "retried"
    db_session.refresh(job)
    assert job.status is JobStatus.pending  # immediate retry re-enqueued
    assert job.attempts == 1
    assert redis_client.xlen(test_settings.stream_normal) == 1


def test_process_job_permanent_fail_when_attempts_exhausted(
    db_session, redis_client, test_settings, monkeypatch
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)
    job = repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, max_attempts=1
    )
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.ack is True
    db_session.refresh(job)
    assert job.status is JobStatus.failed


def test_process_job_skips_unclaimable(db_session, redis_client, test_settings):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)
    repo.complete_job(db_session, job.id, {"message_id": "m1"})  # already terminal
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "skipped"
    assert outcome.ack is True


def test_process_job_timeout_recycles(
    db_session, redis_client, test_settings, monkeypatch
):
    settings = test_settings.model_copy(update={"job_handler_timeout_s": 0.05})
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: _real_sleep(0.5))
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    outcome = process_job(db_session, redis_client, settings, job.id)
    assert outcome.recycle is True
    db_session.refresh(job)
    assert job.status is JobStatus.pending  # timeout → immediate retry
    assert job.attempts == 1


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

    assert run_forever(test_settings, stop=stop) == 0
    with factory() as s:
        assert repo.get_job(s, job.id).status is JobStatus.completed


def test_run_forever_drains_high_before_low(test_settings, redis_client, pg_engine):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
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
    enqueue(redis_client, test_settings.stream_low, str(low.id))
    enqueue(redis_client, test_settings.stream_high, str(high.id))

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    run_forever(test_settings, stop=stop)
    with factory() as s:
        assert repo.get_job(s, high.id).status is JobStatus.completed
        assert repo.get_job(s, low.id).status is JobStatus.pending
    assert (
        redis_client.xpending(test_settings.stream_high, test_settings.consumer_group)[
            "pending"
        ]
        == 0
    )
    assert redis_client.xlen(test_settings.stream_low) == 1
