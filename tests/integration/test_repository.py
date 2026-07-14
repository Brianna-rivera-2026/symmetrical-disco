import uuid

from app import repository as repo
from app.schemas.enums import JobStatus, JobType


async def test_create_and_get(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    assert job.status is JobStatus.pending
    fetched = await repo.get_job(db_session, job.id)
    assert fetched.id == job.id


async def test_get_missing_returns_none(db_session):
    assert await repo.get_job(db_session, uuid.uuid4()) is None


async def test_claim_guard_only_succeeds_once(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    assert await repo.claim_job(db_session, job.id) is True
    assert await repo.claim_job(db_session, job.id) is False  # already processing
    await db_session.refresh(job)
    assert job.status is JobStatus.processing
    assert job.started_at is not None


async def test_complete_and_fail(db_session):
    j1 = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.claim_job(db_session, j1.id)
    await repo.complete_job(db_session, j1.id, {"message_id": "m-1"})
    await db_session.refresh(j1)
    assert j1.status is JobStatus.completed
    assert j1.result == {"message_id": "m-1"}
    assert j1.completed_at is not None

    j2 = await repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    await repo.claim_job(db_session, j2.id)
    await repo.fail_job(
        db_session, j2.id, {"type": "WebhookFailedError", "message": "boom"}
    )
    await db_session.refresh(j2)
    assert j2.status is JobStatus.failed
    assert j2.error["type"] == "WebhookFailedError"


async def test_list_filters_and_cursor(db_session):
    for _ in range(3):
        await repo.create_job(
            db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
        )
    await repo.create_job(db_session, JobType.report, {"report_type": "sales"})

    emails, _ = await repo.list_jobs(db_session, job_type=JobType.email)
    assert len(emails) == 3

    page1, cursor = await repo.list_jobs(db_session, limit=2)
    assert len(page1) == 2
    assert cursor is not None
    page2, cursor2 = await repo.list_jobs(db_session, limit=2, cursor=cursor)
    assert len(page2) == 2
    assert cursor2 is None
    ids = {j.id for j in page1} | {j.id for j in page2}
    assert len(ids) == 4


async def test_create_scheduled_job_sets_fields(db_session):
    from datetime import datetime, timezone

    when = datetime(2030, 1, 1, tzinfo=timezone.utc)
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    assert job.status is JobStatus.scheduled
    assert job.scheduled_at == when
    assert job.is_synced_to_redis is False


async def test_mark_synced_sets_flag(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.mark_synced(db_session, job.id)
    await db_session.refresh(job)
    assert job.is_synced_to_redis is True


async def test_claim_accepts_scheduled_state(db_session):
    from datetime import datetime, timezone

    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    assert await repo.claim_job(db_session, job.id) is True
    await db_session.refresh(job)
    assert job.status is JobStatus.processing
    assert job.started_at is not None


async def test_job_has_scheduling_columns(db_session):
    from datetime import datetime, timezone

    from app.models.job import Job

    when = datetime(2030, 1, 1, tzinfo=timezone.utc)
    job = Job(
        type=JobType.email,
        payload={"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    assert job.scheduled_at == when
    assert job.is_synced_to_redis is False


async def test_promote_scheduled_to_pending_only_scheduled(db_session):
    from datetime import datetime, timezone

    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    scheduled = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    already_pending = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    changed = await repo.promote_scheduled_to_pending(
        db_session, [scheduled.id, already_pending.id]
    )
    assert changed == 1
    await db_session.refresh(scheduled)
    assert scheduled.status is JobStatus.pending


async def test_list_unsynced_filters_synced_and_grace(db_session):
    from datetime import datetime, timedelta, timezone

    synced = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.mark_synced(db_session, synced.id)
    orphan = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    now = datetime.now(timezone.utc)

    rows = await repo.list_unsynced(
        db_session, older_than=now + timedelta(seconds=1), limit=100
    )
    ids = {r.id for r in rows}
    assert orphan.id in ids
    assert synced.id not in ids

    # Grace window: nothing is old enough when the cutoff is in the past.
    none_rows = await repo.list_unsynced(
        db_session, older_than=now - timedelta(seconds=1000), limit=100
    )
    assert none_rows == []


async def test_create_job_defaults_priority_normal(db_session):
    from app.schemas.enums import JobPriority

    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    assert job.priority is JobPriority.normal


async def test_create_job_sets_priority(db_session):
    from app.schemas.enums import JobPriority

    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    await db_session.refresh(job)
    assert job.priority is JobPriority.high


async def test_list_filters_by_priority(db_session):
    from app.schemas.enums import JobPriority

    await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    await repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    highs, _ = await repo.list_jobs(db_session, priority=JobPriority.high)
    assert len(highs) == 1
    assert highs[0].priority is JobPriority.high


async def test_get_promotion_info_batched(db_session):
    from app.schemas.enums import JobPriority

    a = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    b = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )

    result = await repo.get_promotion_info(db_session, [a.id, b.id])
    assert result == {
        a.id: (JobPriority.high, None),
        b.id: (JobPriority.normal, None),
    }


async def test_get_promotion_info_empty_returns_empty(db_session):
    assert await repo.get_promotion_info(db_session, []) == {}


async def test_create_job_defaults_attempts(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await db_session.refresh(job)
    assert job.attempts == 0
    assert job.max_attempts == 4


async def test_create_job_sets_max_attempts(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, max_attempts=2
    )
    await db_session.refresh(job)
    assert job.max_attempts == 2


async def test_complete_job_guarded_increments_attempts(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.claim_job(db_session, job.id)
    assert await repo.complete_job(db_session, job.id, {"message_id": "m1"}) is True
    await db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1


async def test_complete_job_loses_when_not_processing(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.claim_job(db_session, job.id)
    await repo.retry_to_pending(
        db_session, job.id
    )  # someone re-queued it → now pending
    assert await repo.complete_job(db_session, job.id, {"message_id": "m1"}) is False
    await db_session.refresh(job)
    assert job.status is JobStatus.pending


async def test_fail_job_guarded_increments_attempts(db_session):
    job = await repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    await repo.claim_job(db_session, job.id)
    assert (
        await repo.fail_job(db_session, job.id, {"type": "E", "message": "boom"})
        is True
    )
    await db_session.refresh(job)
    assert job.status is JobStatus.failed
    assert job.attempts == 1


async def test_retry_to_pending_resets_sync_and_counts(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.mark_synced(db_session, job.id)
    await repo.claim_job(db_session, job.id)
    assert await repo.retry_to_pending(db_session, job.id) is True
    await db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 1
    assert job.is_synced_to_redis is False
    assert job.started_at is None


async def test_retry_to_scheduled_sets_when(db_session):
    from datetime import datetime, timezone

    when = datetime(2030, 1, 1, tzinfo=timezone.utc)
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.claim_job(db_session, job.id)
    assert await repo.retry_to_scheduled(db_session, job.id, when) is True
    await db_session.refresh(job)
    assert job.status is JobStatus.scheduled
    assert job.scheduled_at == when
    assert job.attempts == 1
    assert job.is_synced_to_redis is False


async def test_reset_failed_to_pending_only_from_failed(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    await repo.claim_job(db_session, job.id)
    await repo.fail_job(db_session, job.id, {"type": "E", "message": "x"})
    assert await repo.reset_failed_to_pending(db_session, job.id) is True
    await db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 0
    assert job.error is None
    assert job.completed_at is None
    assert job.is_synced_to_redis is False
    # A second reset finds it already pending → guard fails.
    assert await repo.reset_failed_to_pending(db_session, job.id) is False


async def test_init_progress_only_when_processing(db_session):
    job = await repo.create_job(db_session, JobType.batch, {"items": []})
    assert (
        await repo.init_progress(db_session, job.id) is False
    )  # pending, not processing
    await repo.claim_job(db_session, job.id)
    assert await repo.init_progress(db_session, job.id) is True
    await db_session.refresh(job)
    assert job.progress == 0


async def test_complete_job_sets_progress_for_batch(db_session):
    job = await repo.create_job(db_session, JobType.batch, {"items": []})
    await repo.claim_job(db_session, job.id)
    assert (
        await repo.complete_job(db_session, job.id, {"total": 0}, progress=100) is True
    )
    await db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.progress == 100


async def test_complete_job_leaves_progress_null_for_non_batch(db_session):
    job = await repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    await repo.claim_job(db_session, job.id)
    await repo.complete_job(db_session, job.id, {"message_id": "m"})
    await db_session.refresh(job)
    assert job.progress is None


async def test_cancel_pending_or_scheduled_guard(db_session):
    job = await repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    assert await repo.cancel_pending_or_scheduled(db_session, job.id) is True
    await db_session.refresh(job)
    assert job.status is JobStatus.cancelled
    # second call is a no-op (already cancelled)
    assert await repo.cancel_pending_or_scheduled(db_session, job.id) is False


async def test_cancel_pending_or_scheduled_rejects_processing(db_session):
    job = await repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    await repo.claim_job(db_session, job.id)
    assert await repo.cancel_pending_or_scheduled(db_session, job.id) is False


async def test_request_cancel_only_when_processing(db_session):
    job = await repo.create_job(db_session, JobType.batch, {"items": []})
    assert await repo.request_cancel(db_session, job.id) is False  # pending
    await repo.claim_job(db_session, job.id)
    assert await repo.request_cancel(db_session, job.id) is True
    await db_session.refresh(job)
    assert job.cancel_requested_at is not None


async def test_cancel_job_guarded_terminal(db_session):
    job = await repo.create_job(db_session, JobType.batch, {"items": []})
    await repo.claim_job(db_session, job.id)
    assert (
        await repo.cancel_job(db_session, job.id, {"total": 3, "succeeded": 1}) is True
    )
    await db_session.refresh(job)
    assert job.status is JobStatus.cancelled
    assert job.result == {"total": 3, "succeeded": 1}


async def test_get_by_idempotency_key(db_session):
    owner = uuid.uuid4()
    key = "abc"
    assert await repo.get_by_idempotency_key(db_session, key, owner) is None
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a", "subject": "b"},
        user_id=owner,
        idempotency_key=key,
        idempotency_hash="h1",
    )
    found = await repo.get_by_idempotency_key(db_session, key, owner)
    assert found is not None and found.id == job.id
    assert found.idempotency_hash == "h1"


async def test_create_job_persists_trace_context(db_session):
    carrier = {"traceparent": f"00-{'ab' * 16}-{'cd' * 8}-01"}
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        trace_context=carrier,
    )
    await db_session.refresh(job)
    assert job.trace_context == carrier


async def test_create_job_trace_context_defaults_to_none(db_session):
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    assert job.trace_context is None


async def test_get_job_scoped_to_owner(db_session):
    from app.schemas.enums import JobType

    owner = uuid.uuid4()
    other = uuid.uuid4()
    job = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "s"}, user_id=owner
    )

    assert await repo.get_job(db_session, job.id, user_id=owner) is not None
    assert await repo.get_job(db_session, job.id, user_id=other) is None
    assert await repo.get_job(db_session, job.id) is not None  # unscoped (internal)


async def test_list_jobs_scoped_to_owner(db_session):
    from app.schemas.enums import JobType

    owner = uuid.uuid4()
    other = uuid.uuid4()
    mine = await repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "s"}, user_id=owner
    )
    await repo.create_job(
        db_session, JobType.email, {"to": "c@d.com", "subject": "s"}, user_id=other
    )

    rows, _ = await repo.list_jobs(db_session, user_id=owner)
    assert [j.id for j in rows] == [mine.id]


async def test_idempotency_lookup_scoped_per_user(db_session):
    from app.schemas.enums import JobType

    owner = uuid.uuid4()
    other = uuid.uuid4()
    job = await repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "s"},
        user_id=owner,
        idempotency_key="k1",
        idempotency_hash="x",
    )

    assert (await repo.get_by_idempotency_key(db_session, "k1", owner)).id == job.id
    assert await repo.get_by_idempotency_key(db_session, "k1", other) is None
