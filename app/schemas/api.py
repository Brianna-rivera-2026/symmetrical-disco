from datetime import datetime, timedelta, timezone
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas.enums import JobPriority, JobStatus, JobType


class JobSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: JobType
    payload: dict
    priority: JobPriority = JobPriority.normal
    scheduled_at: datetime | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)

    @field_validator("scheduled_at")
    @classmethod
    def _normalize_utc(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        v = (
            v.replace(tzinfo=timezone.utc)
            if v.tzinfo is None
            else v.astimezone(timezone.utc)
        )
        if v > datetime.now(timezone.utc) + timedelta(days=365):
            raise ValueError("scheduled_at more than 365 days in the future")
        return v


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


class LivenessResponse(BaseModel):
    status: str


class HealthChecks(BaseModel):
    postgres: str
    redis: str


class HealthResponse(BaseModel):
    status: str
    checks: HealthChecks


class StreamStat(BaseModel):
    depth: int | None
    in_flight: int


class QueueStats(BaseModel):
    streams: dict[str, StreamStat]
    scheduled: int
    workers: int


class JobStats(BaseModel):
    by_status: dict[str, int]
    oldest_pending_age_seconds: float | None


class StatsResponse(BaseModel):
    queue: QueueStats
    jobs: JobStats
