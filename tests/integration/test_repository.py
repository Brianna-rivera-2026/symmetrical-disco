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
