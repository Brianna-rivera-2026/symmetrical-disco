from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select, tuple_, update
from sqlalchemy.orm import Session

from app.cursor import decode_cursor, encode_cursor
from app.models.job import Job
from app.schemas.enums import JobPriority, JobStatus, JobType


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_job(
    session: Session,
    job_type: JobType,
    payload: dict,
    *,
    status: JobStatus = JobStatus.pending,
    scheduled_at: datetime | None = None,
    priority: JobPriority = JobPriority.normal,
    max_attempts: int = 4,
    idempotency_key: str | None = None,
    idempotency_hash: str | None = None,
    trace_context: dict | None = None,
) -> Job:
    job = Job(
        type=job_type,
        payload=payload,
        status=status,
        scheduled_at=scheduled_at,
        priority=priority,
        max_attempts=max_attempts,
        idempotency_key=idempotency_key,
        idempotency_hash=idempotency_hash,
        trace_context=trace_context,
    )
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
    priority: JobPriority | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> tuple[list[Job], str | None]:
    stmt = select(Job)
    if status is not None:
        stmt = stmt.where(Job.status == status)
    if job_type is not None:
        stmt = stmt.where(Job.type == job_type)
    if priority is not None:
        stmt = stmt.where(Job.priority == priority)
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


def mark_synced(session: Session, job_id: UUID) -> None:
    session.execute(update(Job).where(Job.id == job_id).values(is_synced_to_redis=True))
    session.commit()


def claim_job(session: Session, job_id: UUID) -> bool:
    stmt = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
        )
        .values(status=JobStatus.processing, started_at=_now())
    )
    result = session.execute(stmt)
    session.commit()
    return result.rowcount == 1


def complete_job(
    session: Session, job_id: UUID, result: dict, progress: int | None = None
) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.completed,
            result=result,
            completed_at=_now(),
            attempts=Job.attempts + 1,
            progress=progress if progress is not None else Job.progress,
        )
    )
    session.commit()
    return res.rowcount == 1


def fail_job(session: Session, job_id: UUID, error: dict) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.failed,
            error=error,
            completed_at=_now(),
            attempts=Job.attempts + 1,
        )
    )
    session.commit()
    return res.rowcount == 1


def retry_to_pending(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.pending,
            attempts=Job.attempts + 1,
            is_synced_to_redis=False,
            started_at=None,
        )
    )
    session.commit()
    return res.rowcount == 1


def retry_to_scheduled(session: Session, job_id: UUID, scheduled_at: datetime) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.scheduled,
            scheduled_at=scheduled_at,
            attempts=Job.attempts + 1,
            is_synced_to_redis=False,
            started_at=None,
        )
    )
    session.commit()
    return res.rowcount == 1


def reset_failed_to_pending(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.failed)
        .values(
            status=JobStatus.pending,
            attempts=0,
            error=None,
            started_at=None,
            completed_at=None,
            is_synced_to_redis=False,
        )
    )
    session.commit()
    return res.rowcount == 1


def promote_scheduled_to_pending(session: Session, job_ids: list[UUID]) -> int:
    if not job_ids:
        return 0
    result = session.execute(
        update(Job)
        .where(Job.id.in_(job_ids), Job.status == JobStatus.scheduled)
        .values(status=JobStatus.pending)
    )
    session.commit()
    return result.rowcount


def list_unsynced(session: Session, *, older_than: datetime, limit: int) -> list[Job]:
    stmt = (
        select(Job)
        .where(
            Job.is_synced_to_redis.is_(False),
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
            Job.created_at < older_than,
        )
        .order_by(Job.created_at, Job.id)
        .limit(limit)
    )
    return list(session.execute(stmt).scalars())


def get_promotion_info(
    session: Session, job_ids: list[UUID]
) -> dict[UUID, tuple[JobPriority, dict | None]]:
    if not job_ids:
        return {}
    rows = session.execute(
        select(Job.id, Job.priority, Job.trace_context).where(Job.id.in_(job_ids))
    ).all()
    return {row.id: (row.priority, row.trace_context) for row in rows}


def init_progress(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(progress=0)
    )
    session.commit()
    return res.rowcount == 1


def cancel_pending_or_scheduled(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
        )
        .values(status=JobStatus.cancelled, completed_at=_now())
    )
    session.commit()
    return res.rowcount == 1


def request_cancel(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(cancel_requested_at=_now())
    )
    session.commit()
    return res.rowcount == 1


def cancel_job(session: Session, job_id: UUID, summary: dict) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(status=JobStatus.cancelled, result=summary, completed_at=_now())
    )
    session.commit()
    return res.rowcount == 1


def get_by_idempotency_key(session: Session, key: str) -> Job | None:
    return session.execute(
        select(Job).where(Job.idempotency_key == key)
    ).scalar_one_or_none()
