from datetime import datetime, timezone
from uuid import UUID

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app import repository as repo
from app.api.deps import get_db, get_redis
from app.idempotency import canonical_hash
from app.queue.delayed import schedule
from app.queue.producer import enqueue
from app.schemas.api import JobAccepted, JobList, JobOut, JobSubmission
from app.schemas.enums import JobPriority, JobStatus, JobType
from app.schemas.payloads import validate_payload

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _create_and_handoff(session, client, settings, submission, key, req_hash):
    scheduled_at = submission.scheduled_at
    if scheduled_at is not None and scheduled_at > datetime.now(timezone.utc):
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            status=JobStatus.scheduled,
            scheduled_at=scheduled_at,
            priority=submission.priority,
            max_attempts=settings.max_attempts,
            idempotency_key=key,
            idempotency_hash=req_hash,
        )
        schedule(client, settings.delayed_zset, str(job.id), scheduled_at.timestamp())
    else:
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            priority=submission.priority,
            max_attempts=settings.max_attempts,
            idempotency_key=key,
            idempotency_hash=req_hash,
        )
        enqueue(client, settings.stream_for_priority(submission.priority), str(job.id))
    repo.mark_synced(session, job.id)
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


def _replay_or_conflict(existing, req_hash, response) -> JobAccepted:
    if existing is not None and existing.idempotency_hash == req_hash:
        response.status_code = 200
        return _accepted(existing)
    raise HTTPException(
        status_code=409, detail="idempotency key reused with a different payload"
    )


@router.post("/jobs", response_model=JobAccepted, status_code=202)
def submit_job(
    submission: JobSubmission,
    request: Request,
    response: Response,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobAccepted:
    settings = request.app.state.settings
    try:
        validate_payload(submission.type, submission.payload)
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    key = submission.idempotency_key
    if key is None:
        job = _create_and_handoff(session, client, settings, submission, None, None)
        return _accepted(job)

    req_hash = canonical_hash(submission.type, submission.payload)
    existing = repo.get_by_idempotency_key(session, key)
    if existing is not None:
        return _replay_or_conflict(existing, req_hash, response)
    try:
        job = _create_and_handoff(session, client, settings, submission, key, req_hash)
    except IntegrityError:
        session.rollback()
        existing = repo.get_by_idempotency_key(session, key)
        return _replay_or_conflict(existing, req_hash, response)
    return _accepted(job)


@router.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: UUID, session: Session = Depends(get_db)) -> JobOut:
    job = repo.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobOut.model_validate(job)


@router.post("/jobs/{job_id}/retry", response_model=JobOut)
def retry_job(
    job_id: UUID,
    request: Request,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobOut:
    job = repo.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if not repo.reset_failed_to_pending(session, job_id):
        raise HTTPException(
            status_code=409, detail="job is not in a terminal failed state"
        )
    settings = request.app.state.settings
    enqueue(client, settings.stream_for_priority(job.priority), str(job_id))
    repo.mark_synced(session, job_id)
    session.refresh(job)
    return JobOut.model_validate(job)


@router.post("/jobs/{job_id}/cancel", response_model=JobOut)
def cancel_job_route(
    job_id: UUID,
    request: Request,
    response: Response,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobOut:
    settings = request.app.state.settings
    for _ in range(3):  # bounded re-resolve for the legal processing<->pending flap
        if repo.cancel_pending_or_scheduled(session, job_id):
            client.zrem(settings.delayed_zset, str(job_id))  # harmless no-op if absent
            return JobOut.model_validate(repo.get_job(session, job_id))
        if repo.request_cancel(session, job_id):
            response.status_code = 202
            return JobOut.model_validate(repo.get_job(session, job_id))
        job = repo.get_job(session, job_id)
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


@router.get("/jobs", response_model=JobList)
def list_jobs(
    session: Session = Depends(get_db),
    status: JobStatus | None = Query(default=None),
    type: JobType | None = Query(default=None),
    priority: JobPriority | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None),
) -> JobList:
    try:
        jobs, next_cursor = repo.list_jobs(
            session,
            status=status,
            job_type=type,
            priority=priority,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc
    return JobList(
        items=[JobOut.model_validate(j) for j in jobs], next_cursor=next_cursor
    )
