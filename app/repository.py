from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select, tuple_, update
from sqlalchemy.orm import Session

from app.cursor import decode_cursor, encode_cursor
from app.models.job import Job
from app.schemas.enums import JobStatus, JobType


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_job(session: Session, job_type: JobType, payload: dict) -> Job:
    job = Job(type=job_type, payload=payload, status=JobStatus.pending)
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def get_job(session: Session, job_id: UUID) -> Job | None:
    return session.get(Job, job_id)


def list_jobs(
    session: Session,
    *,
    status: JobStatus | None = None,
    job_type: JobType | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> tuple[list[Job], str | None]:
    stmt = select(Job)
    if status is not None:
        stmt = stmt.where(Job.status == status)
    if job_type is not None:
        stmt = stmt.where(Job.type == job_type)
    if cursor is not None:
        c_created, c_id = decode_cursor(cursor)
        stmt = stmt.where(tuple_(Job.created_at, Job.id) < (c_created, c_id))
    stmt = stmt.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit + 1)

    rows = list(session.execute(stmt).scalars())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = encode_cursor(last.created_at, last.id)
    return rows, next_cursor


def claim_job(session: Session, job_id: UUID) -> bool:
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.pending)
        .values(status=JobStatus.processing, started_at=_now())
    )
    result = session.execute(stmt)
    session.commit()
    return result.rowcount == 1


def complete_job(session: Session, job_id: UUID, result: dict) -> None:
    session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(status=JobStatus.completed, result=result, completed_at=_now())
    )
    session.commit()


def fail_job(session: Session, job_id: UUID, error: dict) -> None:
    session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(status=JobStatus.failed, error=error, completed_at=_now())
    )
    session.commit()
