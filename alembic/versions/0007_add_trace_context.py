"""add trace_context for OpenTelemetry propagation

Revision ID: 0007
Revises: 0006
Create Date: 2026-07-10
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("trace_context", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("jobs", "trace_context")
