from datetime import datetime, timezone

import pytest
from sqlalchemy import update

from app import repository as repo
from app.core.db import make_session_factory
from app.jobs import handlers
from app.models.job import Job
from app.schemas.enums import JobStatus, JobType
from app.worker.context import PgJobContext
from app.worker.runner import process_job


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    async def _instant(*_a, **_kw):
        return None

    monkeypatch.setattr(handlers.asyncio, "sleep", _instant)


async def test_pg_context_writes_progress_and_reads_cancel(db_session, pg_engine):
    sf = make_session_factory(pg_engine)
    job = await repo.create_job(db_session, JobType.batch, {"items": []})
    await repo.claim_job(db_session, job.id)  # -> processing

    ctx = PgJobContext(job.id, sf, poll_interval_s=0.0)
    ctx.set_progress(42)
    assert await ctx.cancelled() is False
    await db_session.refresh(job)
    assert job.progress == 42

    await repo.request_cancel(db_session, job.id)
    ctx.set_progress(43)  # change so the next poll writes + re-reads the flag
    assert await ctx.cancelled() is True


async def test_batch_completes_with_progress_100(
    db_session, redis_client, test_settings, pg_engine, owner_id
):
    sf = make_session_factory(pg_engine)
    job = await repo.create_job(
        db_session,
        JobType.batch,
        {
            "items": [
                {"type": "email", "to": "a@b.com", "subject": "Hi"},
                {"type": "report", "report_type": "sales"},
            ]
        },
        user_id=owner_id,
    )
    outcome = await process_job(db_session, redis_client, test_settings, job.id, sf)
    assert outcome.label == "completed"
    await db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.progress == 100
    assert job.result["total"] == 2
    assert job.result["succeeded"] == 2


async def test_tiny_batch_reaches_progress_100_without_polling(
    db_session, redis_client, test_settings, pg_engine, owner_id
):
    # poll interval is huge, so the poll loop never writes progress; completion must
    # still land it on 100 (type-driven, not IS NOT NULL).
    settings = test_settings.model_copy(update={"cancel_poll_interval_s": 999.0})
    sf = make_session_factory(pg_engine)
    job = await repo.create_job(
        db_session,
        JobType.batch,
        {"items": [{"type": "email", "to": "a@b.com", "subject": "Hi"}]},
        user_id=owner_id,
    )
    await process_job(db_session, redis_client, settings, job.id, sf)
    await db_session.refresh(job)
    assert job.progress == 100


async def test_batch_cooperative_cancel(
    db_session, redis_client, test_settings, pg_engine, owner_id
):
    sf = make_session_factory(pg_engine)
    job = await repo.create_job(
        db_session,
        JobType.batch,
        {
            "items": [
                {"type": "email", "to": "a@b.com", "subject": "1"},
                {"type": "email", "to": "a@b.com", "subject": "2"},
                {"type": "email", "to": "a@b.com", "subject": "3"},
            ]
        },
        user_id=owner_id,
    )
    # Simulate "cancel arrived just as processing begins": stamp the flag on the row
    # so the handler's first poll sees it. (claim_job leaves cancel_requested_at intact.)
    await db_session.execute(
        update(Job)
        .where(Job.id == job.id)
        .values(cancel_requested_at=datetime.now(timezone.utc))
    )
    await db_session.commit()
    outcome = await process_job(db_session, redis_client, test_settings, job.id, sf)
    assert outcome.label == "cancelled"
    await db_session.refresh(job)
    assert job.status is JobStatus.cancelled
    assert job.result == {
        "total": 3,
        "succeeded": 0,
        "failed": 0,
        "results": [],
        "errors": [],
    }
