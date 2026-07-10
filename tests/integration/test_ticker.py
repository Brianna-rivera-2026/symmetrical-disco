from datetime import datetime, timedelta, timezone

import sqlalchemy as sa
from sqlalchemy import text

from app import repository as repo
from app.core.db import make_session_factory
from app.queue import delayed
from app.schemas.enums import JobStatus, JobType
from app.ticker.runner import promote_due, reconcile_orphans, run_forever


def test_promote_due_moves_mature_job(db_session, redis_client, test_settings):
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )

    promoted = promote_due(db_session, redis_client, test_settings)

    assert promoted == 1
    assert redis_client.xlen(test_settings.stream_normal) == 1
    assert redis_client.zcard(test_settings.delayed_zset) == 0
    db_session.refresh(job)
    assert job.status is JobStatus.pending


def test_promote_due_skips_future(db_session, redis_client, test_settings):
    future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=future,
    )
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), future.timestamp()
    )

    promoted = promote_due(db_session, redis_client, test_settings)

    assert promoted == 0
    assert redis_client.xlen(test_settings.stream_normal) == 0
    assert redis_client.zcard(test_settings.delayed_zset) == 1


def _backdate(db_session, job_id):
    db_session.execute(
        text("UPDATE jobs SET created_at = now() - interval '1 hour' WHERE id = :id"),
        {"id": str(job_id)},
    )
    db_session.commit()


def test_reconcile_reenqueues_pending_orphan(db_session, redis_client, test_settings):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    _backdate(db_session, job.id)

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 1
    assert redis_client.xlen(test_settings.stream_normal) == 1
    db_session.refresh(job)
    assert job.is_synced_to_redis is True


def test_reconcile_readds_scheduled_orphan(db_session, redis_client, test_settings):
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    _backdate(db_session, job.id)

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 1
    assert redis_client.zcard(test_settings.delayed_zset) == 1
    db_session.refresh(job)
    assert job.is_synced_to_redis is True


def test_reconcile_noop_when_synced(db_session, redis_client, test_settings):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.mark_synced(db_session, job.id)
    _backdate(db_session, job.id)

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 0
    assert redis_client.xlen(test_settings.stream_normal) == 0


def test_reconcile_respects_grace_for_recent_jobs(
    db_session, redis_client, test_settings
):
    repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 0
    assert redis_client.xlen(test_settings.stream_normal) == 0


def test_run_forever_promotes_then_stops(redis_client, test_settings, pg_engine):
    settings = test_settings.model_copy(
        update={"ticker_interval_s": 0.01, "reconcile_interval_s": 0.01}
    )
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        job = repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            status=JobStatus.scheduled,
            scheduled_at=when,
        )
    delayed.schedule(redis_client, settings.delayed_zset, str(job.id), when.timestamp())

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    run_forever(settings, stop=stop)

    with factory() as s:
        refreshed = repo.get_job(s, job.id)
    assert refreshed.status is JobStatus.pending
    assert redis_client.xlen(settings.stream_normal) == 1


def test_end_to_end_scheduled_job_completes(
    db_session, redis_client, test_settings, pg_engine
):
    """Submit a scheduled job, promote it, then process it — asserts completed status."""
    from app.queue.consumer import ensure_group

    from app.worker.runner import process_job

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )

    promote_due(db_session, redis_client, test_settings)

    factory = make_session_factory(pg_engine)
    with factory() as s:
        process_job(s, redis_client, test_settings, job.id)
        refreshed = repo.get_job(s, job.id)
    assert refreshed.status is JobStatus.completed


def test_duplicate_promotion_second_claim_is_noop(
    db_session, redis_client, test_settings, pg_engine
):
    """Promote the same job twice; second worker claim must be a no-op."""
    from app.queue.consumer import ensure_group

    from app.worker.runner import process_job

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    # Manually add the job to the ZSET twice to simulate a crash-recovery re-promotion.
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )
    promote_due(db_session, redis_client, test_settings)

    # Re-add to ZSET and promote again (simulates ticker crash between XADD and ZREM).
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )
    promote_due(db_session, redis_client, test_settings)

    # Stream has 2 entries for the same job; process both.
    factory = make_session_factory(pg_engine)
    with factory() as s:
        process_job(
            s, redis_client, test_settings, job.id
        )  # First delivery: claim succeeds, job completes.
        first_result = repo.get_job(s, job.id).result

    with factory() as s:
        process_job(
            s, redis_client, test_settings, job.id
        )  # Second delivery: claim guard rejects, no-op.
        refreshed = repo.get_job(s, job.id)

    assert refreshed.status is JobStatus.completed
    assert refreshed.result == first_result  # Result unchanged by duplicate.


def test_promote_due_routes_by_priority(db_session, redis_client, test_settings):
    from app.schemas.enums import JobPriority

    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    high = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
        priority=JobPriority.high,
    )
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(high.id), when.timestamp()
    )

    promoted = promote_due(db_session, redis_client, test_settings)

    assert promoted == 1
    assert redis_client.xlen(test_settings.stream_high) == 1
    assert redis_client.xlen(test_settings.stream_normal) == 0
    db_session.refresh(high)
    assert high.status is JobStatus.pending


def test_promote_reinjects_stored_trace_context(db_session, redis_client, test_settings):
    stored = {"traceparent": f"00-{'ab' * 16}-{'cd' * 8}-01"}
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=past,
        trace_context=stored,
    )
    delayed.schedule(redis_client, test_settings.delayed_zset, str(job.id), past.timestamp())

    promote_due(db_session, redis_client, test_settings)

    entries = redis_client.xrange(test_settings.stream_normal)
    assert len(entries) == 1
    fields = entries[0][1]
    assert fields["job_id"] == str(job.id)
    assert "ab" * 16 in fields["traceparent"]  # original trace id restored


def test_promote_without_stored_context_still_enqueues(
    db_session, redis_client, test_settings
):
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=past,
    )
    delayed.schedule(redis_client, test_settings.delayed_zset, str(job.id), past.timestamp())
    promote_due(db_session, redis_client, test_settings)
    entries = redis_client.xrange(test_settings.stream_normal)
    assert entries[0][1]["job_id"] == str(job.id)


def test_reconcile_reinjects_stored_trace_context(
    db_session, redis_client, test_settings
):
    stored = {"traceparent": f"00-{'12' * 16}-{'34' * 8}-01"}
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        trace_context=stored,
    )
    # pending + unsynced + old enough → reconcile re-enqueues it
    db_session.execute(
        sa.text("UPDATE jobs SET created_at = now() - interval '1 hour' WHERE id = :id"),
        {"id": str(job.id)},
    )
    db_session.commit()
    reconcile_orphans(db_session, redis_client, test_settings)
    entries = redis_client.xrange(test_settings.stream_normal)
    assert "12" * 16 in entries[0][1]["traceparent"]
