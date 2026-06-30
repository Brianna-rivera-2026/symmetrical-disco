"""add scheduled_at and is_synced_to_redis

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-01
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs", sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "jobs",
        sa.Column(
            "is_synced_to_redis",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    # Rows created under Phase 1 were already handed off to Redis; mark them
    # synced so the reconciler never re-enqueues historical jobs.
    op.execute("UPDATE jobs SET is_synced_to_redis = TRUE")
    op.create_index(
        "ix_jobs_unsynced",
        "jobs",
        ["created_at", "id"],
        postgresql_where=sa.text(
            "is_synced_to_redis = false AND status IN ('pending', 'scheduled')"
        ),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_unsynced", table_name="jobs")
    op.drop_column("jobs", "is_synced_to_redis")
    op.drop_column("jobs", "scheduled_at")
