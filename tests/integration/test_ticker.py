from datetime import datetime, timedelta, timezone

import sqlalchemy as sa
from sqlalchemy import text

from app import repository as repo
from app.core.db import make_session_factory
from app.queue import delayed
from app.queue.consumer import ensure_group
from app.schemas.enums import JobStatus, JobType
from app.ticker.runner import (
    job_status_observations,
    promote_due,
    queue_depth_observations,
    queue_scheduled_observations,
    reconcile_orphans,
    run_forever,
)


async def test_promote_due_moves_mature_job(db_session, redis_client, test_settings):
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    await delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )

    promoted = await promote_due(db_session, redis_client, test_settings)

    assert promoted == 1
    assert await redis_client.xlen(test_settings.stream_normal) == 1
    assert await redis_client.zcard(test_settings.delayed_zset) == 0
    await db_session.refresh(job)
    assert job.status is JobStatus.pending


async def test_promote_due_skips_future(db_session, redis_client, test_settings):
    future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=future,
    )
    await delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), future.timestamp()
    )

    promoted = await promote_due(db_session, redis_client, test_settings)

    assert promoted == 0
    assert await redis_client.xlen(test_settings.stream_normal) == 0
    assert await redis_client.zcard(test_settings.delayed_zset) == 1


async def _backdate(db_session, job_id):
    await db_session.execute(
        text("UPDATE jobs SET created_at = now() - interval '1 hour' WHERE id = :id"),
        {"id": str(job_id)},
    )
    await db_session.commit()


async def test_reconcile_reenqueues_pending_orphan(db_session, redis_client, test_settings):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await _backdate(db_session, job.id)

    recovered = await reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 1
    assert await redis_client.xlen(test_settings.stream_normal) == 1
    await db_session.refresh(job)
    assert job.is_synced_to_redis is True


async def test_reconcile_readds_scheduled_orphan(db_session, redis_client, test_settings):
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    await _backdate(db_session, job.id)

    recovered = await reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 1
    assert await redis_client.zcard(test_settings.delayed_zset) == 1
    await db_session.refresh(job)
    assert job.is_synced_to_redis is True


async def test_reconcile_noop_when_synced(db_session, redis_client, test_settings):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.mark_synced(db_session, job.id)
    await _backdate(db_session, job.id)

    recovered = await reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 0
    assert await redis_client.xlen(test_settings.stream_normal) == 0


async def test_reconcile_respects_grace_for_recent_jobs(
    db_session, redis_client, test_settings
):
    await repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    recovered = await reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 0
    assert await redis_client.xlen(test_settings.stream_normal) == 0


async def test_run_forever_promotes_then_stops(redis_client, test_settings, pg_engine):
    settings = test_settings.model_copy(
        update={"ticker_interval_s": 0.01, "reconcile_interval_s": 0.01}
    )
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    factory = make_session_factory(pg_engine)
    async with factory() as s:
        job = await repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            status=JobStatus.scheduled,
            scheduled_at=when,
        )
    await delayed.schedule(
        redis_client, settings.delayed_zset, str(job.id), when.timestamp()
    )

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    await run_forever(settings, stop=stop)

    async with factory() as s:
        refreshed = await repo.get_job(s, job.id)
    assert refreshed.status is JobStatus.pending
    assert await redis_client.xlen(settings.stream_normal) == 1


async def test_end_to_end_scheduled_job_completes(
    db_session, redis_client, test_settings, pg_engine, owner_id
):
    """Submit a scheduled job, promote it, then process it — asserts completed status."""
    from app.queue.consumer import ensure_group

    from app.worker.runner import process_job

    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
        user_id=owner_id,
    )
    await delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )

    await promote_due(db_session, redis_client, test_settings)

    factory = make_session_factory(pg_engine)
    async with factory() as s:
        await process_job(s, redis_client, test_settings, job.id)
        refreshed = await repo.get_job(s, job.id)
    assert refreshed.status is JobStatus.completed


async def test_duplicate_promotion_second_claim_is_noop(
    db_session, redis_client, test_settings, pg_engine, owner_id
):
    """Promote the same job twice; second worker claim must be a no-op."""
    from app.queue.consumer import ensure_group

    from app.worker.runner import process_job

    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
        user_id=owner_id,
    )
    # Manually add the job to the ZSET twice to simulate a crash-recovery re-promotion.
    await delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )
    await promote_due(db_session, redis_client, test_settings)

    # Re-add to ZSET and promote again (simulates ticker crash between XADD and ZREM).
    await delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )
    await promote_due(db_session, redis_client, test_settings)

    # Stream has 2 entries for the same job; process both.
    factory = make_session_factory(pg_engine)
    async with factory() as s:
        await process_job(
            s, redis_client, test_settings, job.id
        )  # First delivery: claim succeeds, job completes.
        first_result = (await repo.get_job(s, job.id)).result

    async with factory() as s:
        await process_job(
            s, redis_client, test_settings, job.id
        )  # Second delivery: claim guard rejects, no-op.
        refreshed = await repo.get_job(s, job.id)

    assert refreshed.status is JobStatus.completed
    assert refreshed.result == first_result  # Result unchanged by duplicate.


async def test_promote_due_routes_by_priority(db_session, redis_client, test_settings):
    from app.schemas.enums import JobPriority

    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    high = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
        priority=JobPriority.high,
    )
    await delayed.schedule(
        redis_client, test_settings.delayed_zset, str(high.id), when.timestamp()
    )

    promoted = await promote_due(db_session, redis_client, test_settings)

    assert promoted == 1
    assert await redis_client.xlen(test_settings.stream_high) == 1
    assert await redis_client.xlen(test_settings.stream_normal) == 0
    await db_session.refresh(high)
    assert high.status is JobStatus.pending


async def test_promote_reinjects_stored_trace_context(
    db_session, redis_client, test_settings
):
    stored = {"traceparent": f"00-{'ab' * 16}-{'cd' * 8}-01"}
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=past,
        trace_context=stored,
    )
    await delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), past.timestamp()
    )

    await promote_due(db_session, redis_client, test_settings)

    entries = await redis_client.xrange(test_settings.stream_normal)
    assert len(entries) == 1
    fields = entries[0][1]
    assert fields["job_id"] == str(job.id)
    assert "ab" * 16 in fields["traceparent"]  # original trace id restored


async def test_promote_without_stored_context_still_enqueues(
    db_session, redis_client, test_settings
):
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=past,
    )
    await delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), past.timestamp()
    )
    await promote_due(db_session, redis_client, test_settings)
    entries = await redis_client.xrange(test_settings.stream_normal)
    assert entries[0][1]["job_id"] == str(job.id)


async def test_reconcile_reinjects_stored_trace_context(
    db_session, redis_client, test_settings
):
    stored = {"traceparent": f"00-{'12' * 16}-{'34' * 8}-01"}
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        trace_context=stored,
    )
    # pending + unsynced + old enough → reconcile re-enqueues it
    await db_session.execute(
        sa.text(
            "UPDATE jobs SET created_at = now() - interval '1 hour' WHERE id = :id"
        ),
        {"id": str(job.id)},
    )
    await db_session.commit()
    await reconcile_orphans(db_session, redis_client, test_settings)
    entries = await redis_client.xrange(test_settings.stream_normal)
    assert "12" * 16 in entries[0][1]["traceparent"]


async def test_queue_depth_observations(redis_client, test_settings, sync_redis_client):
    for stream in test_settings.ordered_streams:
        await ensure_group(redis_client, stream, test_settings.consumer_group)
    await redis_client.xadd(test_settings.stream_high, {"job_id": "x"})
    observations = queue_depth_observations(sync_redis_client, test_settings)
    by_stream = {o.attributes["stream"]: o.value for o in observations}
    assert by_stream["high"] == 1
    assert by_stream["normal"] == 0


async def test_queue_scheduled_observations(redis_client, test_settings, sync_redis_client):
    await redis_client.zadd(test_settings.delayed_zset, {"a": 1.0, "b": 2.0})
    observations = queue_scheduled_observations(sync_redis_client, test_settings)
    assert observations[0].value == 2


def test_observations_swallow_redis_errors(test_settings):
    import redis as redis_lib

    dead = redis_lib.Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.2)
    assert queue_depth_observations(dead, test_settings) == []
    assert queue_scheduled_observations(dead, test_settings) == []


async def test_job_status_observations_counts_pending_and_processing(
    db_session, test_settings, sync_session_factory
):
    await repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.claim_job(db_session, job.id)  # -> processing
    await db_session.commit()

    observations = job_status_observations(sync_session_factory, test_settings)
    by_status = {o.attributes["status"]: o.value for o in observations}
    assert by_status["pending"] == 1
    assert by_status["processing"] == 1
    assert by_status["completed"] == 0  # zero-filled, not just omitted


def test_job_status_observations_swallow_db_errors(test_settings):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    dead_factory = sessionmaker(
        bind=create_engine("postgresql+psycopg://u:p@127.0.0.1:1/x")
    )
    assert job_status_observations(dead_factory, test_settings) == []
