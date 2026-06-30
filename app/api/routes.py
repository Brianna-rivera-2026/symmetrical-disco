from uuid import UUID

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app import repository as repo
from app.api.deps import get_db, get_redis
from app.queue.producer import enqueue
from app.schemas.api import JobAccepted, JobList, JobOut, JobSubmission
from app.schemas.enums import JobStatus, JobType
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

    job = repo.create_job(session, submission.type, submission.payload)
    # Enqueue invariant: XADD only after create_job() committed the INSERT.
    enqueue(client, request.app.state.settings.jobs_stream, str(job.id))
    return JobAccepted(id=job.id, type=job.type, status=job.status, created_at=job.created_at)


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
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None),
) -> JobList:
    try:
        jobs, next_cursor = repo.list_jobs(
            session, status=status, job_type=type, limit=limit, cursor=cursor
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc
    return JobList(items=[JobOut.model_validate(j) for j in jobs], next_cursor=next_cursor)
