"""add users table and job ownership

Revision ID: 0008
Revises: 0007
Create Date: 2026-07-10

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("key_hash", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("uq_users_name", "users", ["name"], unique=True)
    op.create_index("uq_users_key_hash", "users", ["key_hash"], unique=True)

    op.add_column(
        "jobs", sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True)
    )
    op.create_foreign_key(
        "fk_jobs_user_id_users",
        "jobs",
        "users",
        ["user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_jobs_user_id_created_at_id", "jobs", ["user_id", "created_at", "id"]
    )
    op.drop_index("uq_jobs_idempotency_key", table_name="jobs")
    op.create_index(
        "uq_jobs_user_idempotency_key",
        "jobs",
        ["user_id", "idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_jobs_user_idempotency_key", table_name="jobs")
    op.create_index(
        "uq_jobs_idempotency_key",
        "jobs",
        ["idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.drop_index("ix_jobs_user_id_created_at_id", table_name="jobs")
    op.drop_constraint("fk_jobs_user_id_users", "jobs", type_="foreignkey")
    op.drop_column("jobs", "user_id")
    op.drop_index("uq_users_key_hash", table_name="users")
    op.drop_index("uq_users_name", table_name="users")
    op.drop_table("users")
