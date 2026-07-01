from app import repository as repo
from app.queue.consumer import ensure_group
from app.schemas.enums import JobStatus, JobType
from app.ticker.runner import reap_stale


def _plant_pel(client, group, stream, job_id):
    """Add a message and read it into a dead consumer's PEL without acking."""
    ensure_group(client, stream, group)
    client.xadd(stream, {"job_id": str(job_id)})
    client.xreadgroup(
        groupname=group, consumername="deadworker", streams={stream: ">"}, count=10
    )


def test_reaper_requeues_abandoned_processing_job(
    db_session, redis_client, test_settings
):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)
    repo.claim_job(db_session, job.id)  # → processing (worker "died")

    handled = reap_stale(db_session, redis_client, settings)

    assert handled == 1
    db_session.refresh(job)
    assert job.status is JobStatus.pending  # immediate retry
    assert job.attempts == 1
    # Original PEL entry cleared; a fresh message was enqueued.
    assert (
        redis_client.xpending(settings.stream_normal, settings.consumer_group)[
            "pending"
        ]
        == 0
    )
    assert redis_client.xlen(settings.stream_normal) == 2  # planted + re-enqueued


def test_reaper_finishes_handoff_for_unsynced_pending(
    db_session, redis_client, test_settings
):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    # pending + is_synced_to_redis=False (create_job leaves it False), worker
    # "died" after winning the guard but before XADD.
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)

    handled = reap_stale(db_session, redis_client, settings)

    assert handled == 1
    db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 0  # handoff finished, NOT a new retry
    assert job.is_synced_to_redis is True
    assert redis_client.xlen(settings.stream_normal) == 2  # planted + reaper's re-add


def test_reaper_only_acks_completed_ghost(db_session, redis_client, test_settings):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)
    repo.complete_job(db_session, job.id, {"message_id": "m1"})  # terminal
    repo.mark_synced(db_session, job.id)
    _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)

    handled = reap_stale(db_session, redis_client, settings)

    assert handled == 1
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1  # unchanged
    assert (
        redis_client.xpending(settings.stream_normal, settings.consumer_group)[
            "pending"
        ]
        == 0
    )
    assert redis_client.xlen(settings.stream_normal) == 1  # no re-enqueue
