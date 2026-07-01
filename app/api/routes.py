from datetime import datetime, timezone
from uuid import UUID

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app import repository as repo
from app.api.deps import get_db, get_redis
from app.queue.delayed import schedule
from app.queue.producer import enqueue
from app.schemas.api import JobAccepted, JobList, JobOut, JobSubmission
from app.schemas.enums import JobPriority, JobStatus, JobType
from app.schemas.payloads import validate_payload

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.post("/jobs", response_model=JobAccepted, status_code=202)
def submit_job(
    submission: JobSubmission,
    request: Request,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobAccepted:
    try:
        validate_payload(submission.type, submission.payload)
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    settings = request.app.state.settings
    scheduled_at = submission.scheduled_at
    if scheduled_at is not None and scheduled_at > datetime.now(timezone.utc):
        # Scheduled path: persist SCHEDULED + park in the delayed ZSET.
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            status=JobStatus.scheduled,
            scheduled_at=scheduled_at,
            priority=submission.priority,
        )
        schedule(client, settings.delayed_zset, str(job.id), scheduled_at.timestamp())
    else:
        # Immediate path: persist PENDING + push to the priority stream.
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            priority=submission.priority,
        )
        enqueue(client, settings.stream_for_priority(submission.priority), str(job.id))
    # Handoff confirmed → flip the flag so the reconciler ignores this row.
    repo.mark_synced(session, job.id)
    return JobAccepted(
        id=job.id,
        type=job.type,
        status=job.status,
        priority=job.priority,
        created_at=job.created_at,
        scheduled_at=job.scheduled_at,
    )


@router.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: UUID, session: Session = Depends(get_db)) -> JobOut:
    job = repo.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobOut.model_validate(job)


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
