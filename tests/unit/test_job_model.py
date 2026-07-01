import uuid

from app.models.job import Job
from app.schemas.enums import JobStatus, JobType


def test_job_table_and_columns():
    assert Job.__tablename__ == "jobs"
    cols = set(Job.__table__.columns.keys())
    assert cols == {
        "id",
        "type",
        "payload",
        "status",
        "result",
        "error",
        "created_at",
        "started_at",
        "completed_at",
        "scheduled_at",
        "is_synced_to_redis",
        "priority",
    }


def test_job_defaults_when_instantiated():
    j = Job(type=JobType.email, payload={"to": "a@b.com", "subject": "Hi"})
    assert isinstance(j.id, uuid.UUID)
    assert j.status is JobStatus.pending


def test_job_defaults_priority_normal():
    from app.schemas.enums import JobPriority

    j = Job(type=JobType.email, payload={"to": "a@b.com", "subject": "Hi"})
    assert j.priority is JobPriority.normal
