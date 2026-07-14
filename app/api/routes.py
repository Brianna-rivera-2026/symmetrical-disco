import logging
from datetime import datetime, timezone
from uuid import UUID

import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app import repository as repo
from app.api.deps import get_current_user, get_db, get_redis
from app.api.ratelimit import rate_limit
from app.auth.identity import AuthedUser
from app.core import metrics as app_metrics
from app.core.telemetry import current_trace_carrier
from app.idempotency import canonical_hash
from app.observability import check_readiness, gather_stats
from app.queue.delayed import schedule
from app.queue.producer import enqueue
from app.schemas.api import (
    HealthChecks,
    HealthResponse,
    JobAccepted,
    JobList,
    JobOut,
    JobSubmission,
    LivenessResponse,
    StatsResponse,
)
from app.schemas.enums import JobPriority, JobStatus, JobType
from app.schemas.payloads import validate_payload

router = APIRouter()
log = logging.getLogger("app.api")


@router.get("/health", response_model=LivenessResponse)
def health() -> LivenessResponse:
    """Liveness: the process is serving requests."""
    return LivenessResponse(status="ok")


@router.get("/ready", response_model=HealthResponse)
async def ready(
    session: AsyncSession = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
):
    checks = await check_readiness(session, client)
    ok = all(value == "ok" for value in checks.values())
    if not ok:
        return JSONResponse(
            status_code=503,
            content={"status": "unavailable", "checks": checks},
        )
    return HealthResponse(status="ok", checks=HealthChecks(**checks))


@router.get(
    "/stats",
    response_model=StatsResponse,
    dependencies=[Depends(rate_limit("stats"))],
)
async def stats(
    request: Request,
    session: AsyncSession = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> StatsResponse:
    settings = request.app.state.settings
    try:
        return await gather_stats(session, client, settings)
    except (redis.RedisError, SQLAlchemyError) as exc:
        log.warning("stats.unavailable", extra={"error": str(exc)})
        raise HTTPException(status_code=503, detail="stats unavailable") from exc


async def _create_and_handoff(
    session, client, settings, submission, key, req_hash, user_id, user_name
):
    scheduled_at = submission.scheduled_at
    if scheduled_at is not None and scheduled_at > datetime.now(timezone.utc):
        job = await repo.create_job(
            session,
            submission.type,
            submission.payload,
            status=JobStatus.scheduled,
            scheduled_at=scheduled_at,
            priority=submission.priority,
            max_attempts=settings.max_attempts,
            idempotency_key=key,
            idempotency_hash=req_hash,
            trace_context=current_trace_carrier(),
            user_id=user_id,
            user_name=user_name,
        )
        await schedule(
            client, settings.delayed_zset, str(job.id), scheduled_at.timestamp()
        )
    else:
        job = await repo.create_job(
            session,
            submission.type,
            submission.payload,
            priority=submission.priority,
            max_attempts=settings.max_attempts,
            idempotency_key=key,
            idempotency_hash=req_hash,
            trace_context=current_trace_carrier(),
            user_id=user_id,
            user_name=user_name,
        )
        await enqueue(
            client, settings.stream_for_priority(submission.priority), str(job.id)
        )
    await repo.mark_synced(session, job.id)
    app_metrics.jobs_submitted.add(
        1,
        {
            "type": submission.type.value,
            "priority": submission.priority.value,
            "scheduled": job.status is JobStatus.scheduled,
        },
    )
    return job


def _accepted(job) -> JobAccepted:
    return JobAccepted(
        id=job.id,
        type=job.type,
        status=job.status,
        priority=job.priority,
        created_at=job.created_at,
        scheduled_at=job.scheduled_at,
    )


async def _replay_or_conflict(existing, req_hash, response) -> JobAccepted:
    if existing is not None and existing.idempotency_hash == req_hash:
        response.status_code = 200
        return _accepted(existing)
    raise HTTPException(
        status_code=409, detail="idempotency key reused with a different payload"
    )


@router.post(
    "/jobs",
    response_model=JobAccepted,
    status_code=202,
    dependencies=[Depends(get_current_user), Depends(rate_limit("submit"))],
)
async def submit_job(
    submission: JobSubmission,
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
    user: AuthedUser = Depends(get_current_user),
) -> JobAccepted:
    settings = request.app.state.settings
    try:
        validate_payload(submission.type, submission.payload, settings)
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    key = submission.idempotency_key
    if key is None:
        job = await _create_and_handoff(
            session, client, settings, submission, None, None, user.id, user.name
        )
        return _accepted(job)

    req_hash = canonical_hash(submission.type, submission.payload)
    existing = await repo.get_by_idempotency_key(session, key, user.id)
    if existing is not None:
        return await _replay_or_conflict(existing, req_hash, response)
    try:
        job = await _create_and_handoff(
            session, client, settings, submission, key, req_hash, user.id, user.name
        )
    except IntegrityError:
        await session.rollback()
        existing = await repo.get_by_idempotency_key(session, key, user.id)
        return await _replay_or_conflict(existing, req_hash, response)
    return _accepted(job)


@router.get(
    "/jobs/{job_id}",
    response_model=JobOut,
    dependencies=[Depends(get_current_user), Depends(rate_limit("read"))],
)
async def get_job(
    job_id: UUID,
    session: AsyncSession = Depends(get_db),
    user: AuthedUser = Depends(get_current_user),
) -> JobOut:
    job = await repo.get_job(session, job_id, user_id=user.id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobOut.model_validate(job)


@router.post(
    "/jobs/{job_id}/retry",
    response_model=JobOut,
    dependencies=[Depends(get_current_user), Depends(rate_limit("control"))],
)
async def retry_job(
    job_id: UUID,
    request: Request,
    session: AsyncSession = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
    user: AuthedUser = Depends(get_current_user),
) -> JobOut:
    job = await repo.get_job(session, job_id, user_id=user.id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if not await repo.reset_failed_to_pending(session, job_id):
        raise HTTPException(
            status_code=409, detail="job is not in a terminal failed state"
        )
    settings = request.app.state.settings
    await enqueue(client, settings.stream_for_priority(job.priority), str(job_id))
    await repo.mark_synced(session, job_id)
    await session.refresh(job)
    return JobOut.model_validate(job)


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobOut,
    dependencies=[Depends(get_current_user), Depends(rate_limit("control"))],
)
async def cancel_job_route(
    job_id: UUID,
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
    user: AuthedUser = Depends(get_current_user),
) -> JobOut:
    settings = request.app.state.settings
    if await repo.get_job(session, job_id, user_id=user.id) is None:
        raise HTTPException(status_code=404, detail="job not found")
    for _ in range(3):  # bounded re-resolve for the legal processing<->pending flap
        if await repo.cancel_pending_or_scheduled(session, job_id):
            await client.zrem(
                settings.delayed_zset, str(job_id)
            )  # harmless no-op if absent
            return JobOut.model_validate(await repo.get_job(session, job_id))
        if await repo.request_cancel(session, job_id):
            response.status_code = 202
            return JobOut.model_validate(await repo.get_job(session, job_id))
        job = await repo.get_job(session, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status is JobStatus.cancelled:
            return JobOut.model_validate(job)  # idempotent 200
        if job.status in (JobStatus.completed, JobStatus.failed):
            raise HTTPException(
                status_code=409, detail="job cannot be cancelled in its current state"
            )
        # pending/scheduled/processing again → loop and retry the guarded transitions
    raise HTTPException(status_code=409, detail="job state is changing; retry")


@router.get(
    "/jobs",
    response_model=JobList,
    dependencies=[Depends(get_current_user), Depends(rate_limit("read"))],
)
async def list_jobs(
    session: AsyncSession = Depends(get_db),
    status: JobStatus | None = Query(default=None),
    type: JobType | None = Query(default=None),
    priority: JobPriority | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None, max_length=512),
    user: AuthedUser = Depends(get_current_user),
) -> JobList:
    try:
        jobs, next_cursor = await repo.list_jobs(
            session,
            status=status,
            job_type=type,
            priority=priority,
            user_id=user.id,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc
    return JobList(
        items=[JobOut.model_validate(j) for j in jobs], next_cursor=next_cursor
    )
