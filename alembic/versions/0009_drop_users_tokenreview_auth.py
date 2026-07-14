"""drop users table; auth moves to cluster TokenReview

Ownership survives as bare UUIDs stamped from TokenReview's
status.user.uid. Existing jobs.user_id values were app-generated and no
longer resolve to anything — old jobs are effectively unowned (accepted;
see docs/superpowers/specs/2026-07-14-sso-tokenreview-design.md).

Revision ID: 0009
Revises: 0008
Create Date: 2026-07-14

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("user_name", sa.Text(), nullable=True))
    op.drop_constraint("fk_jobs_user_id_users", "jobs", type_="foreignkey")
    op.drop_index("uq_users_key_hash", table_name="users")
    op.drop_index("uq_users_name", table_name="users")
    op.drop_table("users")


def downgrade() -> None:
    # Recreates an EMPTY users table — ownership data is not restorable.
    # user_id values reference cluster UIDs that have no users row, so they
    # must be nulled before the FK can be restored.
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
    op.execute("UPDATE jobs SET user_id = NULL")
    op.create_foreign_key(
        "fk_jobs_user_id_users",
        "jobs",
        "users",
        ["user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.drop_column("jobs", "user_name")
