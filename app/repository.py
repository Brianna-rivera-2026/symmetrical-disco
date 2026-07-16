from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.cursor import decode_cursor, encode_cursor
from app.models.job import Job
from app.schemas.enums import JobPriority, JobStatus, JobType


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def create_job(
    session: AsyncSession,
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
    user_id: UUID | None = None,
    user_name: str | None = None,
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
        user_id=user_id,
        user_name=user_name,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def get_job(
    session: AsyncSession, job_id: UUID, *, user_id: UUID | None = None
) -> Job | None:
    """user_id=None is the unscoped internal form (worker, post-ownership
    re-reads). API routes must always pass the caller's id."""
    if user_id is None:
        return await session.get(Job, job_id)
    return (
        await session.execute(
            select(Job).where(Job.id == job_id, Job.user_id == user_id)
        )
    ).scalar_one_or_none()


async def list_jobs(
    session: AsyncSession,
    *,
    status: JobStatus | None = None,
    job_type: JobType | None = None,
    priority: JobPriority | None = None,
    user_id: UUID | None = None,
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
    if user_id is not None:
        stmt = stmt.where(Job.user_id == user_id)
    if cursor is not None:
        c_created, c_id = decode_cursor(cursor)
        stmt = stmt.where(tuple_(Job.created_at, Job.id) < (c_created, c_id))
    stmt = stmt.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit + 1)

    rows = list((await session.execute(stmt)).scalars())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = encode_cursor(last.created_at, last.id)
    return rows, next_cursor


async def mark_synced(session: AsyncSession, job_id: UUID) -> None:
    await session.execute(
        update(Job).where(Job.id == job_id).values(is_synced_to_redis=True)
    )
    await session.commit()


async def claim_job(session: AsyncSession, job_id: UUID) -> bool:
    stmt = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
        )
        .values(status=JobStatus.processing, started_at=_now())
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount == 1


async def complete_job(
    session: AsyncSession, job_id: UUID, result: dict, progress: int | None = None
) -> bool:
    res = await session.execute(
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
    await session.commit()
    return res.rowcount == 1


async def fail_job(session: AsyncSession, job_id: UUID, error: dict) -> bool:
    res = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.failed,
            error=error,
            completed_at=_now(),
            attempts=Job.attempts + 1,
        )
    )
    await session.commit()
    return res.rowcount == 1


async def retry_to_pending(session: AsyncSession, job_id: UUID) -> bool:
    res = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.pending,
            attempts=Job.attempts + 1,
            is_synced_to_redis=False,
            started_at=None,
        )
    )
    await session.commit()
    return res.rowcount == 1


async def retry_to_scheduled(
    session: AsyncSession, job_id: UUID, scheduled_at: datetime
) -> bool:
    res = await session.execute(
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
    await session.commit()
    return res.rowcount == 1


async def reset_failed_to_pending(session: AsyncSession, job_id: UUID) -> bool:
    res = await session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == JobStatus.failed,
            # A stale cancel request must not survive into a retry: the worker
            # treats any non-null cancel_requested_at as "cancel now", so a
            # retried job would self-cancel at its first checkpoint.
            Job.cancel_requested_at.is_(None),
        )
        .values(
            status=JobStatus.pending,
            attempts=0,
            error=None,
            started_at=None,
            completed_at=None,
            is_synced_to_redis=False,
        )
    )
    await session.commit()
    return res.rowcount == 1


async def promote_scheduled_to_pending(
    session: AsyncSession, job_ids: list[UUID]
) -> int:
    if not job_ids:
        return 0
    result = await session.execute(
        update(Job)
        .where(Job.id.in_(job_ids), Job.status == JobStatus.scheduled)
        .values(status=JobStatus.pending)
    )
    await session.commit()
    return result.rowcount


async def list_unsynced(
    session: AsyncSession, *, older_than: datetime, limit: int
) -> list[Job]:
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
    return list((await session.execute(stmt)).scalars())


async def get_promotion_info(
    session: AsyncSession, job_ids: list[UUID]
) -> dict[UUID, tuple[JobPriority, dict | None]]:
    if not job_ids:
        return {}
    rows = (
        await session.execute(
            select(Job.id, Job.priority, Job.trace_context).where(Job.id.in_(job_ids))
        )
    ).all()
    return {row.id: (row.priority, row.trace_context) for row in rows}


async def init_progress(session: AsyncSession, job_id: UUID) -> bool:
    res = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(progress=0)
    )
    await session.commit()
    return res.rowcount == 1


async def cancel_pending_or_scheduled(session: AsyncSession, job_id: UUID) -> bool:
    res = await session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
        )
        .values(status=JobStatus.cancelled, completed_at=_now())
    )
    await session.commit()
    return res.rowcount == 1


async def request_cancel(session: AsyncSession, job_id: UUID) -> bool:
    res = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(cancel_requested_at=_now())
    )
    await session.commit()
    return res.rowcount == 1


async def cancel_job(session: AsyncSession, job_id: UUID, summary: dict) -> bool:
    res = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(status=JobStatus.cancelled, result=summary, completed_at=_now())
    )
    await session.commit()
    return res.rowcount == 1


async def get_by_idempotency_key(
    session: AsyncSession, key: str, user_id: UUID
) -> Job | None:
    return (
        await session.execute(
            select(Job).where(Job.idempotency_key == key, Job.user_id == user_id)
        )
    ).scalar_one_or_none()


async def count_by_status(session: AsyncSession) -> list[tuple[JobStatus, int]]:
    return (
        await session.execute(select(Job.status, func.count()).group_by(Job.status))
    ).all()
