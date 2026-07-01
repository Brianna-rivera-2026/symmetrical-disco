"""create jobs table

Revision ID: 0001
Revises:
Create Date: 2026-06-30
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

JOB_TYPE = postgresql.ENUM("email", "webhook", "report", name="job_type")
JOB_STATUS = postgresql.ENUM(
    "scheduled",
    "pending",
    "processing",
    "completed",
    "failed",
    "cancelled",
    name="job_status",
)


def upgrade() -> None:
    bind = op.get_bind()
    JOB_TYPE.create(bind, checkfirst=True)
    JOB_STATUS.create(bind, checkfirst=True)

    JOB_TYPE_REF = postgresql.ENUM(
        "email", "webhook", "report", name="job_type", create_type=False
    )
    JOB_STATUS_REF = postgresql.ENUM(
        "scheduled",
        "pending",
        "processing",
        "completed",
        "failed",
        "cancelled",
        name="job_status",
        create_type=False,
    )

    op.create_table(
        "jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("type", JOB_TYPE_REF, nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("status", JOB_STATUS_REF, nullable=False),
        sa.Column("result", postgresql.JSONB, nullable=True),
        sa.Column("error", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_index("ix_jobs_type", "jobs", ["type"])
    op.create_index("ix_jobs_created_at_id", "jobs", ["created_at", "id"])


def downgrade() -> None:
    op.drop_index("ix_jobs_created_at_id", table_name="jobs")
    op.drop_index("ix_jobs_type", table_name="jobs")
    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_table("jobs")
    JOB_STATUS.drop(op.get_bind(), checkfirst=True)
    JOB_TYPE.drop(op.get_bind(), checkfirst=True)
