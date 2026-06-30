from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.schemas.enums import JobStatus, JobType


class JobSubmission(BaseModel):
    type: JobType
    payload: dict


class JobAccepted(BaseModel):
    id: UUID
    type: JobType
    status: JobStatus
    created_at: datetime


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: JobType
    status: JobStatus
    payload: dict
    result: dict | None
    error: dict | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class JobList(BaseModel):
    items: list[JobOut]
    next_cursor: str | None
