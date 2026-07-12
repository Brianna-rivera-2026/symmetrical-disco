import asyncio

import pytest
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from app import repository as repo
from app.core.db import make_session_factory
from app.jobs import handlers
import app.jobs.registry as registry
from app.queue.consumer import ensure_group, read_priority
from app.queue.producer import enqueue
from app.schemas.enums import JobPriority, JobStatus, JobType
from app.worker.runner import cpu_utilization_observations, handle_message, process_job

# Capture the genuine asyncio.sleep now, before any fixture monkeypatches
# handlers.asyncio.sleep, so tests that need a real (slow) sleep don't
# recurse into their own patched no-op.
_real_sleep = asyncio.sleep


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    async def _instant(*_a, **_kw):
        return None

    monkeypatch.setattr(handlers.asyncio, "sleep", _instant)


async def test_process_job_completes_email(
    db_session, redis_client, test_settings, owner_id
):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    outcome = await process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "completed"
    assert outcome.ack is True
    await db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1


async def test_process_job_retries_on_handler_failure(
    db_session, redis_client, test_settings, monkeypatch, owner_id
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # force webhook fail
    job = await repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, user_id=owner_id
    )
    outcome = await process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "retried"
    await db_session.refresh(job)
    assert job.status is JobStatus.pending  # immediate retry re-enqueued
    assert job.attempts == 1
    assert await redis_client.xlen(test_settings.stream_normal) == 1


async def test_process_job_permanent_fail_when_attempts_exhausted(
    db_session, redis_client, test_settings, monkeypatch, owner_id
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)
    job = await repo.create_job(
        db_session,
        JobType.webhook,
        {"url": "https://x.test"},
        max_attempts=1,
        user_id=owner_id,
    )
    outcome = await process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.ack is True
    await db_session.refresh(job)
    assert job.status is JobStatus.failed


async def test_process_job_skips_unclaimable(
    db_session, redis_client, test_settings, owner_id
):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    await repo.claim_job(db_session, job.id)
    await repo.complete_job(
        db_session, job.id, {"message_id": "m1"}
    )  # already terminal
    outcome = await process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "skipped"
    assert outcome.ack is True


async def test_process_job_timeout_then_worker_continues(
    db_session, redis_client, test_settings, monkeypatch, owner_id
):
    settings = test_settings.model_copy(update={"job_handler_timeout_s": 0.05})

    async def _slow(*_a, **_kw):
        await _real_sleep(0.5)

    monkeypatch.setattr(handlers.asyncio, "sleep", _slow)
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    outcome = await process_job(db_session, redis_client, settings, job.id)
    assert outcome.label == "timeout"
    await db_session.refresh(job)
    assert job.status is JobStatus.pending  # timeout → immediate retry
    assert job.attempts == 1

    # Worker keeps running after a timeout: process the retried job (no sleep
    # patched back in, so this attempt completes normally).
    async def _instant(*_a, **_kw):
        return None

    monkeypatch.setattr(handlers.asyncio, "sleep", _instant)
    outcome2 = await process_job(db_session, redis_client, settings, job.id)
    assert outcome2.label == "completed"


async def test_process_job_timeout_survives_full_expiration_on_rollback(
    db_session, redis_client, test_settings, monkeypatch, owner_id
):
    """Regression test for a MissingGreenlet crash found in Docker verification.

    session.rollback() expires every ORM attribute on objects loaded in the
    session (independent of expire_on_commit). The HandlerTimeout branch of
    process_job used to keep referencing the pre-rollback `job` ORM object
    afterward; `schedule_retry_or_fail`'s plain (un-awaited) `job.attempts`
    access then tried to lazy-load the expired attribute, which requires an
    active SQLAlchemy async-greenlet bridge that a bare attribute access
    doesn't have — raising `sqlalchemy.exc.MissingGreenlet` and crashing the
    whole worker TaskGroup. Whether the ORM happens to still have unexpired
    attributes cached at that point is timing/connection-pool sensitive,
    which is why the plain existing timeout test didn't reliably catch this.
    This test forces the worst case directly with session.expire_all()
    monkeypatched onto rollback, so it fails deterministically without the
    fix and passes deterministically with it.
    """
    settings = test_settings.model_copy(update={"job_handler_timeout_s": 0.05})

    async def _slow(*_a, **_kw):
        await _real_sleep(0.5)

    monkeypatch.setattr(handlers.asyncio, "sleep", _slow)

    real_rollback = db_session.rollback

    async def _rollback_and_expire_everything():
        await real_rollback()
        db_session.expire_all()  # force worst-case expiration of every object

    monkeypatch.setattr(db_session, "rollback", _rollback_and_expire_everything)

    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )

    # Must not raise sqlalchemy.exc.MissingGreenlet (or anything else).
    outcome = await process_job(db_session, redis_client, settings, job.id)

    assert outcome.label == "timeout"
    assert outcome.ack is True
    await db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 1


async def test_run_forever_processes_one_then_stops(
    test_settings, redis_client, pg_engine, owner_id
):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    async with factory() as s:
        job = await repo.create_job(
            s, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
        )
    await enqueue(redis_client, test_settings.stream_normal, str(job.id))

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    assert await run_forever(test_settings, stop=stop) == 0
    async with factory() as s:
        assert (await repo.get_job(s, job.id)).status is JobStatus.completed


async def test_run_forever_drains_high_before_low(
    test_settings, redis_client, pg_engine, owner_id
):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    async with factory() as s:
        low = await repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            priority=JobPriority.low,
            user_id=owner_id,
        )
        high = await repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            priority=JobPriority.high,
            user_id=owner_id,
        )
    await enqueue(redis_client, test_settings.stream_low, str(low.id))
    await enqueue(redis_client, test_settings.stream_high, str(high.id))

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    await run_forever(test_settings, stop=stop)
    async with factory() as s:
        assert (await repo.get_job(s, high.id)).status is JobStatus.completed
        assert (await repo.get_job(s, low.id)).status is JobStatus.pending
    assert (
        await redis_client.xpending(
            test_settings.stream_high, test_settings.consumer_group
        )
    )["pending"] == 0
    assert await redis_client.xlen(test_settings.stream_low) == 1


async def test_worker_exits_zero_when_memory_threshold_breached(
    test_settings, redis_client, pg_engine
):
    """A threshold of 1 MB is always exceeded by a real running process, so
    the loop must notice on its first iteration, drain (no in-flight jobs),
    and return 0 without any stop() ever being called."""
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    settings = test_settings.model_copy(update={"worker_max_rss_mb": 1})

    exit_code = await asyncio.wait_for(run_forever(settings), timeout=30)
    assert exit_code == 0


async def test_jobs_run_concurrently(
    test_settings, pg_engine, redis_client, owner_id, monkeypatch
):
    """With concurrency N, two slow jobs overlap instead of running serially."""
    running = 0
    peak = 0

    async def slow_handler(payload, ctx):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await _real_sleep(
            0.3
        )  # real sleep: the autouse fixture patches asyncio.sleep to a no-op
        running -= 1
        return {"ok": True}

    monkeypatch.setitem(registry.HANDLERS, JobType.email, slow_handler)
    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    async with factory() as session:
        jobs = [
            await repo.create_job(
                session,
                JobType.email,
                {"to": "a@b.com", "subject": "s", "body": "b"},
                user_id=owner_id,
            )
            for _ in range(2)
        ]
    for job in jobs:
        await enqueue(
            redis_client, test_settings.stream_for_priority(job.priority), str(job.id)
        )

    settings = test_settings.model_copy(
        update={"worker_concurrency": 2, "block_ms": 100}
    )
    processed = {"n": 0}

    async def _poll_done() -> bool:
        async with factory() as session:
            done = [
                (await repo.get_job(session, j.id)).status is JobStatus.completed
                for j in jobs
            ]
        processed["n"] = sum(done)
        return all(done)

    stop_flag = {"stop": False}
    from app.worker.runner import run_forever

    worker = asyncio.create_task(run_forever(settings, stop=lambda: stop_flag["stop"]))
    try:
        async with asyncio.timeout(15):
            while not await _poll_done():
                await _real_sleep(0.05)
    finally:
        stop_flag["stop"] = True
        await worker
    assert peak == 2


def _read_one(redis_client, test_settings):
    return read_priority(
        redis_client,
        test_settings.ordered_streams,
        test_settings.consumer_group,
        "test-consumer",
        block_ms=100,
    )


async def test_consumer_span_joins_producer_trace(
    db_session, redis_client, test_settings, pg_engine, span_exporter, owner_id
):
    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("submit") as submit_span:
        await enqueue(redis_client, test_settings.stream_normal, str(job.id))
    batch = await _read_one(redis_client, test_settings)
    assert batch, "expected one message"
    stream, message_id, fields = batch[0]

    outcome = await handle_message(
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
    pending = await redis_client.xpending(stream, test_settings.consumer_group)
    assert pending["pending"] == 0


async def test_consumer_span_without_traceparent_starts_new_trace(
    db_session, redis_client, test_settings, pg_engine, span_exporter, owner_id
):
    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    await redis_client.xadd(
        test_settings.stream_normal, {"job_id": str(job.id)}
    )  # legacy shape
    batch = await _read_one(redis_client, test_settings)
    assert batch, "expected one message"
    stream, message_id, fields = batch[0]
    outcome = await handle_message(
        make_session_factory(pg_engine),
        redis_client,
        test_settings,
        stream,
        message_id,
        fields,
    )
    assert outcome.label == "completed"


async def test_consumer_span_records_handler_error(
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
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    job = await repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, user_id=owner_id
    )
    await enqueue(redis_client, test_settings.stream_normal, str(job.id))
    batch = await _read_one(redis_client, test_settings)
    assert batch, "expected one message"
    stream, message_id, fields = batch[0]
    await handle_message(
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


async def test_ownerless_job_is_dropped_not_executed(
    db_session, redis_client, test_settings
):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "s"}
    )  # no user_id -> ownerless
    job_id = job.id

    outcome = await process_job(db_session, redis_client, test_settings, job_id)

    assert outcome.label == "dropped_ownerless"
    assert outcome.ack is True
    db_session.expire_all()
    refreshed = await repo.get_job(db_session, job_id)
    assert refreshed.status is JobStatus.failed
    assert refreshed.error == {"reason": "ownerless job dropped"}
    assert refreshed.result is None  # handler never ran


async def test_owned_job_still_processes(
    db_session, redis_client, test_settings, owner_id
):
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "s"},
        user_id=owner_id,
    )
    outcome = await process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "completed"


async def test_policy_violation_fails_without_retry(
    db_session, redis_client, test_settings, owner_id
):
    job = await repo.create_job(
        db_session,
        JobType.webhook,
        {"url": "https://evil.test/x"},  # not in test_settings.webhook_allowed_hosts
        user_id=owner_id,
    )
    outcome = await process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "policy_rejected"
    assert outcome.ack is True
    await db_session.refresh(job)
    assert job.status is JobStatus.failed
    assert job.attempts == 1  # fail_job increments once; no retry ladder beyond it
    assert job.error["type"] == "PayloadPolicyError"
