from datetime import datetime, timezone
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from app.schemas.enums import JobPriority, JobStatus, JobType


class JobSubmission(BaseModel):
    type: JobType
    payload: dict
    priority: JobPriority = JobPriority.normal
    scheduled_at: datetime | None = None
    idempotency_key: str | None = None

    @field_validator("scheduled_at")
    @classmethod
    def _normalize_utc(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


class JobAccepted(BaseModel):
    id: UUID
    type: JobType
    status: JobStatus
    priority: JobPriority
    created_at: datetime
    scheduled_at: datetime | None = None


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: JobType
    status: JobStatus
    priority: JobPriority
    payload: dict
    result: dict | None
    error: dict | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    scheduled_at: datetime | None
    attempts: int
    max_attempts: int
    progress: int | None = None
    cancel_requested_at: datetime | None = None


class JobList(BaseModel):
    items: list[JobOut]
    next_cursor: str | None
