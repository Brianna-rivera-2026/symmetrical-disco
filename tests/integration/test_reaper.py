from app import repository as repo
from app.queue.consumer import ensure_group
from app.schemas.enums import JobStatus, JobType
from app.ticker.runner import reap_stale


async def _plant_pel(client, group, stream, job_id):
    """Add a message and read it into a dead consumer's PEL without acking."""
    await ensure_group(client, stream, group)
    await client.xadd(stream, {"job_id": str(job_id)})
    await client.xreadgroup(
        groupname=group, consumername="deadworker", streams={stream: ">"}, count=10
    )


async def test_reaper_requeues_abandoned_processing_job(
    db_session, redis_client, test_settings
):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)
    await repo.claim_job(db_session, job.id)  # → processing (worker "died")

    handled = await reap_stale(db_session, redis_client, settings)

    assert handled == 1
    await db_session.refresh(job)
    assert job.status is JobStatus.pending  # immediate retry
    assert job.attempts == 1
    # Original PEL entry cleared; a fresh message was enqueued.
    assert (
        await redis_client.xpending(settings.stream_normal, settings.consumer_group)
    )["pending"] == 0
    assert await redis_client.xlen(settings.stream_normal) == 2  # planted + re-enqueued


async def test_reaper_finishes_handoff_for_unsynced_pending(
    db_session, redis_client, test_settings
):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    # pending + is_synced_to_redis=False (create_job leaves it False), worker
    # "died" after winning the guard but before XADD.
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)

    handled = await reap_stale(db_session, redis_client, settings)

    assert handled == 1
    await db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 0  # handoff finished, NOT a new retry
    assert job.is_synced_to_redis is True
    assert await redis_client.xlen(settings.stream_normal) == 2  # planted + reaper's re-add


async def test_reaper_only_acks_completed_ghost(db_session, redis_client, test_settings):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.claim_job(db_session, job.id)
    await repo.complete_job(db_session, job.id, {"message_id": "m1"})  # terminal
    await repo.mark_synced(db_session, job.id)
    await _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)

    handled = await reap_stale(db_session, redis_client, settings)

    assert handled == 1
    await db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1  # unchanged
    assert (
        await redis_client.xpending(settings.stream_normal, settings.consumer_group)
    )["pending"] == 0
    assert await redis_client.xlen(settings.stream_normal) == 1  # no re-enqueue


async def test_reaper_treats_cancelled_unsynced_as_ghost_no_resurrection(
    db_session, redis_client, test_settings
):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    # pending + is_synced_to_redis=False (create_job leaves it False) — the
    # exact row state the gap targets: cancelled while pending, in the narrow
    # window before the Redis handoff completed.
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)
    assert await repo.cancel_pending_or_scheduled(db_session, job.id) is True

    handled = await reap_stale(db_session, redis_client, settings)

    assert handled == 1
    await db_session.refresh(job)
    assert job.status is JobStatus.cancelled
    assert (
        await redis_client.xpending(settings.stream_normal, settings.consumer_group)
    )["pending"] == 0
    assert await redis_client.xlen(settings.stream_normal) == 1  # no resurrection XADD
