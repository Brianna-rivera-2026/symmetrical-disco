import uuid
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import Boolean, Enum as SAEnum
from sqlalchemy import Index, TIMESTAMP, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base
from app.schemas.enums import JobPriority, JobStatus, JobType


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    type: Mapped[JobType] = mapped_column(SAEnum(JobType, name="job_type"))
    payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus, name="job_status"),
        default=JobStatus.pending,
        index=True,
    )
    priority: Mapped[JobPriority] = mapped_column(
        SAEnum(JobPriority, name="job_priority"),
        default=JobPriority.normal,
        server_default="normal",
        nullable=False,
        index=True,
    )
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    attempts: Mapped[int] = mapped_column(
        sa.Integer, nullable=False, default=0, server_default="0"
    )
    max_attempts: Mapped[int] = mapped_column(
        sa.Integer, nullable=False, default=4, server_default="4"
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    scheduled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    is_synced_to_redis: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=sa.false()
    )
    progress: Mapped[int | None] = mapped_column(sa.Integer, nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_hash: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index(
            "uq_jobs_idempotency_key",
            "idempotency_key",
            unique=True,
            postgresql_where=sa.text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_jobs_status_created_at", "status", "created_at"),
    )

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("id", uuid.uuid4())
        kwargs.setdefault("status", JobStatus.pending)
        kwargs.setdefault("priority", JobPriority.normal)
        kwargs.setdefault("attempts", 0)
        kwargs.setdefault("max_attempts", 4)
        super().__init__(**kwargs)
