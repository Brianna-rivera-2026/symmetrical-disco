"""add cancellation, batch, progress & idempotency

Revision ID: 0005
Revises: 0004
Create Date: 2026-07-01
"""

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("progress", sa.Integer(), nullable=True))
    op.add_column(
        "jobs",
        sa.Column("cancel_requested_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column("jobs", sa.Column("idempotency_key", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("idempotency_hash", sa.Text(), nullable=True))
    # PostgreSQL 12+ allows ADD VALUE in a transaction as long as the value is not
    # USED in the same transaction (it isn't here). IF NOT EXISTS keeps re-runs safe.
    op.execute("ALTER TYPE job_type ADD VALUE IF NOT EXISTS 'batch'")
    op.create_index(
        "uq_jobs_idempotency_key",
        "jobs",
        ["idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_jobs_idempotency_key", table_name="jobs")
    op.drop_column("jobs", "idempotency_hash")
    op.drop_column("jobs", "idempotency_key")
    op.drop_column("jobs", "cancel_requested_at")
    op.drop_column("jobs", "progress")
    # Enum value 'batch' is intentionally left in place (Postgres cannot drop an
    # enum value cleanly; it is inert if unused).
