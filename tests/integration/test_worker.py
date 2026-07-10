import time

import pytest
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from app import repository as repo
from app.core.db import make_session_factory
from app.jobs import handlers
from app.queue.consumer import ensure_group, read_priority
from app.queue.producer import enqueue
from app.schemas.enums import JobPriority, JobStatus, JobType
from app.worker.runner import cpu_utilization_observations, handle_message, process_job

# handlers.time is the real stdlib `time` module (not a wrapper), so patching
# handlers.time.sleep patches time.sleep globally. Capture the genuine
# implementation now, before any fixture monkeypatches it, so tests that need
# a real (slow) sleep don't recurse into their own patched no-op.
_real_sleep = time.sleep


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def test_process_job_completes_email(db_session, redis_client, test_settings, owner_id):
    job = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "completed"
    assert outcome.ack is True
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1


def test_process_job_retries_on_handler_failure(
    db_session, redis_client, test_settings, monkeypatch, owner_id
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # force webhook fail
    job = repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, user_id=owner_id
    )
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "retried"
    db_session.refresh(job)
    assert job.status is JobStatus.pending  # immediate retry re-enqueued
    assert job.attempts == 1
    assert redis_client.xlen(test_settings.stream_normal) == 1


def test_process_job_permanent_fail_when_attempts_exhausted(
    db_session, redis_client, test_settings, monkeypatch, owner_id
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)
    job = repo.create_job(
        db_session,
        JobType.webhook,
        {"url": "https://x.test"},
        max_attempts=1,
        user_id=owner_id,
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
    db_session, redis_client, test_settings, monkeypatch, owner_id
):
    settings = test_settings.model_copy(update={"job_handler_timeout_s": 0.05})
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: _real_sleep(0.5))
    job = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    outcome = process_job(db_session, redis_client, settings, job.id)
    assert outcome.recycle is True
    db_session.refresh(job)
    assert job.status is JobStatus.pending  # timeout → immediate retry
    assert job.attempts == 1


def test_run_forever_processes_one_then_stops(
    test_settings, redis_client, pg_engine, owner_id
):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        job = repo.create_job(
            s, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
        )
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


def test_run_forever_drains_high_before_low(
    test_settings, redis_client, pg_engine, owner_id
):
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
            user_id=owner_id,
        )
        high = repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            priority=JobPriority.high,
            user_id=owner_id,
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


def _read_one(redis_client, test_settings):
    batch = read_priority(
        redis_client,
        test_settings.ordered_streams,
        test_settings.consumer_group,
        "test-consumer",
        block_ms=100,
    )
    assert batch, "expected one message"
    return batch[0]


def test_consumer_span_joins_producer_trace(
    db_session, redis_client, test_settings, pg_engine, span_exporter, owner_id
):
    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    job = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("submit") as submit_span:
        enqueue(redis_client, test_settings.stream_normal, str(job.id))
    stream, message_id, fields = _read_one(redis_client, test_settings)

    outcome = handle_message(
        make_session_factory(pg_engine),
        redis_client,
        test_settings,
        stream,
        message_id,
        fields,
    )

    assert outcome.label == "completed"
    consumer = next(
        s for s in span_exporter.get_finished_spans() if s.name == "process job"
    )
    expected_trace = format(submit_span.get_span_context().trace_id, "032x")
    assert format(consumer.context.trace_id, "032x") == expected_trace
    assert consumer.kind is SpanKind.CONSUMER
    assert consumer.attributes["job.outcome"] == "completed"
    assert consumer.attributes["job.type"] == "email"
    assert consumer.attributes["job.attempt"] == 1
    # message acked: no pending entries left for the group
    pending = redis_client.xpending(stream, test_settings.consumer_group)
    assert pending["pending"] == 0


def test_consumer_span_without_traceparent_starts_new_trace(
    db_session, redis_client, test_settings, pg_engine, span_exporter, owner_id
):
    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    job = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    redis_client.xadd(
        test_settings.stream_normal, {"job_id": str(job.id)}
    )  # legacy shape
    stream, message_id, fields = _read_one(redis_client, test_settings)
    outcome = handle_message(
        make_session_factory(pg_engine),
        redis_client,
        test_settings,
        stream,
        message_id,
        fields,
    )
    assert outcome.label == "completed"


def test_consumer_span_records_handler_error(
    db_session,
    redis_client,
    test_settings,
    pg_engine,
    span_exporter,
    monkeypatch,
    owner_id,
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # force webhook fail
    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    job = repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, user_id=owner_id
    )
    enqueue(redis_client, test_settings.stream_normal, str(job.id))
    stream, message_id, fields = _read_one(redis_client, test_settings)
    handle_message(
        make_session_factory(pg_engine),
        redis_client,
        test_settings,
        stream,
        message_id,
        fields,
    )
    consumer = next(
        s for s in span_exporter.get_finished_spans() if s.name == "process job"
    )
    assert consumer.attributes["job.outcome"] == "retried"
    assert consumer.status.status_code is StatusCode.ERROR
    assert consumer.events  # exception recorded


def test_cpu_utilization_observations_returns_one_point():
    observations = cpu_utilization_observations()
    assert len(observations) == 1
    assert isinstance(observations[0].value, float)
    assert observations[0].value >= 0.0


def test_ownerless_job_is_dropped_not_executed(
    db_session, redis_client, test_settings, owner_id
):
    from app.schemas.enums import JobStatus, JobType
    from app.worker.runner import process_job

    job = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "s"}
    )  # no user_id -> ownerless

    outcome = process_job(db_session, redis_client, test_settings, job.id)

    assert outcome.label == "dropped_ownerless"
    assert outcome.ack is True
    db_session.expire_all()
    refreshed = repo.get_job(db_session, job.id)
    assert refreshed.status is JobStatus.failed
    assert refreshed.error == {"reason": "ownerless job dropped"}
    assert refreshed.result is None  # handler never ran


def test_owned_job_still_processes(db_session, redis_client, test_settings, owner_id):
    from app.schemas.enums import JobType
    from app.worker.runner import process_job

    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "s"},
        user_id=owner_id,
    )
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "completed"
