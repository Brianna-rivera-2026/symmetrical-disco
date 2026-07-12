import pytest

from app import repository as repo
from app.retry import schedule_retry_or_fail
from app.schemas.enums import JobStatus, JobType


@pytest.mark.asyncio
async def test_retry_immediate_reenqueues_to_priority_stream(
    db_session, redis_client, test_settings
):
    job = await repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    await repo.claim_job(db_session, job.id)  # → processing
    await db_session.refresh(job)

    won = await schedule_retry_or_fail(
        db_session, redis_client, test_settings, job, {"type": "E", "message": "boom"}
    )

    assert won is True
    await db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 1
    assert job.is_synced_to_redis is True
    assert await redis_client.xlen(test_settings.stream_normal) == 1


@pytest.mark.asyncio
async def test_retry_delayed_parks_in_zset(db_session, redis_client, test_settings):
    settings = test_settings.model_copy(update={"retry_backoff_schedule": [30, 30, 30]})
    job = await repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    await repo.claim_job(db_session, job.id)
    await db_session.refresh(job)

    won = await schedule_retry_or_fail(
        db_session, redis_client, settings, job, {"type": "E", "message": "boom"}
    )

    assert won is True
    await db_session.refresh(job)
    assert job.status is JobStatus.scheduled
    assert await redis_client.zcard(settings.delayed_zset) == 1
    assert await redis_client.xlen(settings.stream_normal) == 0


@pytest.mark.asyncio
async def test_retry_permanent_fail_at_max_attempts(
    db_session, redis_client, test_settings
):
    job = await repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, max_attempts=1
    )
    await repo.claim_job(db_session, job.id)
    await db_session.refresh(job)

    won = await schedule_retry_or_fail(
        db_session, redis_client, test_settings, job, {"type": "E", "message": "boom"}
    )

    assert won is True
    await db_session.refresh(job)
    assert job.status is JobStatus.failed
    assert job.attempts == 1
    assert await redis_client.xlen(test_settings.stream_normal) == 0
