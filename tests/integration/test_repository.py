import uuid

from app import repository as repo
from app.schemas.enums import JobStatus, JobType


def test_create_and_get(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert job.status is JobStatus.pending
    fetched = repo.get_job(db_session, job.id)
    assert fetched.id == job.id


def test_get_missing_returns_none(db_session):
    assert repo.get_job(db_session, uuid.uuid4()) is None


def test_claim_guard_only_succeeds_once(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert repo.claim_job(db_session, job.id) is True
    assert repo.claim_job(db_session, job.id) is False  # already processing
    db_session.refresh(job)
    assert job.status is JobStatus.processing
    assert job.started_at is not None


def test_complete_and_fail(db_session):
    j1 = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, j1.id)
    repo.complete_job(db_session, j1.id, {"message_id": "m-1"})
    db_session.refresh(j1)
    assert j1.status is JobStatus.completed
    assert j1.result == {"message_id": "m-1"}
    assert j1.completed_at is not None

    j2 = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    repo.claim_job(db_session, j2.id)
    repo.fail_job(db_session, j2.id, {"type": "WebhookFailedError", "message": "boom"})
    db_session.refresh(j2)
    assert j2.status is JobStatus.failed
    assert j2.error["type"] == "WebhookFailedError"


def test_list_filters_and_cursor(db_session):
    for _ in range(3):
        repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.create_job(db_session, JobType.report, {"report_type": "sales"})

    emails, _ = repo.list_jobs(db_session, job_type=JobType.email)
    assert len(emails) == 3

    page1, cursor = repo.list_jobs(db_session, limit=2)
    assert len(page1) == 2
    assert cursor is not None
    page2, cursor2 = repo.list_jobs(db_session, limit=2, cursor=cursor)
    assert len(page2) == 2
    assert cursor2 is None
    ids = {j.id for j in page1} | {j.id for j in page2}
    assert len(ids) == 4


def test_create_scheduled_job_sets_fields(db_session):
    from datetime import datetime, timezone

    when = datetime(2030, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    assert job.status is JobStatus.scheduled
    assert job.scheduled_at == when
    assert job.is_synced_to_redis is False


def test_mark_synced_sets_flag(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.mark_synced(db_session, job.id)
    db_session.refresh(job)
    assert job.is_synced_to_redis is True


def test_claim_accepts_scheduled_state(db_session):
    from datetime import datetime, timezone

    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    assert repo.claim_job(db_session, job.id) is True
    db_session.refresh(job)
    assert job.status is JobStatus.processing
    assert job.started_at is not None


def test_job_has_scheduling_columns(db_session):
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
    db_session.commit()
    db_session.refresh(job)
    assert job.scheduled_at == when
    assert job.is_synced_to_redis is False


def test_promote_scheduled_to_pending_only_scheduled(db_session):
    from datetime import datetime, timezone

    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    scheduled = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    already_pending = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    changed = repo.promote_scheduled_to_pending(
        db_session, [scheduled.id, already_pending.id]
    )
    assert changed == 1
    db_session.refresh(scheduled)
    assert scheduled.status is JobStatus.pending


def test_list_unsynced_filters_synced_and_grace(db_session):
    from datetime import datetime, timedelta, timezone

    synced = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    repo.mark_synced(db_session, synced.id)
    orphan = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    now = datetime.now(timezone.utc)

    rows = repo.list_unsynced(
        db_session, older_than=now + timedelta(seconds=1), limit=100
    )
    ids = {r.id for r in rows}
    assert orphan.id in ids
    assert synced.id not in ids

    # Grace window: nothing is old enough when the cutoff is in the past.
    none_rows = repo.list_unsynced(
        db_session, older_than=now - timedelta(seconds=1000), limit=100
    )
    assert none_rows == []


def test_create_job_defaults_priority_normal(db_session):
    from app.schemas.enums import JobPriority

    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert job.priority is JobPriority.normal


def test_create_job_sets_priority(db_session):
    from app.schemas.enums import JobPriority

    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    db_session.refresh(job)
    assert job.priority is JobPriority.high


def test_list_filters_by_priority(db_session):
    from app.schemas.enums import JobPriority

    repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    highs, _ = repo.list_jobs(db_session, priority=JobPriority.high)
    assert len(highs) == 1
    assert highs[0].priority is JobPriority.high


def test_get_priorities_batched(db_session):
    from app.schemas.enums import JobPriority

    a = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    b = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    result = repo.get_priorities(db_session, [a.id, b.id])
    assert result == {a.id: JobPriority.high, b.id: JobPriority.normal}


def test_get_priorities_empty_returns_empty(db_session):
    assert repo.get_priorities(db_session, []) == {}
