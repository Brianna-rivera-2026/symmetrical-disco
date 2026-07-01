"""add priority

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-01
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

JOB_PRIORITY = postgresql.ENUM("high", "normal", "low", name="job_priority")


def upgrade() -> None:
    bind = op.get_bind()
    JOB_PRIORITY.create(bind, checkfirst=True)

    job_priority_ref = postgresql.ENUM(
        "high", "normal", "low", name="job_priority", create_type=False
    )
    op.add_column(
        "jobs",
        sa.Column(
            "priority", job_priority_ref, nullable=False, server_default="normal"
        ),
    )
    op.create_index("ix_jobs_priority", "jobs", ["priority"])


def downgrade() -> None:
    op.drop_index("ix_jobs_priority", table_name="jobs")
    op.drop_column("jobs", "priority")
    JOB_PRIORITY.drop(op.get_bind(), checkfirst=True)
