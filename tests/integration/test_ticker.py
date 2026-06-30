from datetime import datetime, timezone

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
    assert redis_client.xlen(test_settings.jobs_stream) == 1
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
    assert redis_client.xlen(test_settings.jobs_stream) == 0
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
    assert redis_client.xlen(test_settings.jobs_stream) == 1
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
    assert redis_client.xlen(test_settings.jobs_stream) == 0


def test_reconcile_respects_grace_for_recent_jobs(
    db_session, redis_client, test_settings
):
    repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 0
    assert redis_client.xlen(test_settings.jobs_stream) == 0


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
    assert redis_client.xlen(settings.jobs_stream) == 1
