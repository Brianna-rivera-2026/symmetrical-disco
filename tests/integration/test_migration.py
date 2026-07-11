from sqlalchemy import inspect

import pytest
from sqlalchemy.exc import IntegrityError

from app import repository as repo
from app.models.job import Job
from app.models.user import User
from app.schemas.enums import JobType


async def test_jobs_table_exists_with_indexes(pg_engine):
    async with pg_engine.begin() as conn:
        def check_indexes(c):
            insp = inspect(c)
            return "jobs" in insp.get_table_names() and "ix_jobs_created_at_id" in {ix["name"] for ix in insp.get_indexes("jobs")}
        result = await conn.run_sync(check_indexes)
    assert result


async def test_priority_column_and_index(pg_engine):
    async with pg_engine.begin() as conn:
        def check_priority(c):
            insp = inspect(c)
            cols = {col["name"] for col in insp.get_columns("jobs")}
            index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
            return "priority" in cols and "ix_jobs_priority" in index_names
        result = await conn.run_sync(check_priority)
    assert result


async def test_batch_type_and_new_columns_persist(db_session):
    job = await repo.create_job(db_session, JobType.batch, {"items": []})
    await db_session.refresh(job)
    assert job.progress is None
    assert job.cancel_requested_at is None
    assert job.idempotency_key is None
    assert job.idempotency_hash is None


async def test_idempotency_key_partial_unique(db_session):
    # Idempotency uniqueness is scoped per user (uq_jobs_user_idempotency_key),
    # so the collision must be tested with a shared, non-null user_id.
    user = User(name="idempotency-test-user", key_hash="hash1")
    db_session.add(user)
    await db_session.commit()

    db_session.add(
        Job(
            type=JobType.email,
            payload={"to": "a", "subject": "b"},
            idempotency_key="k1",
            user_id=user.id,
        )
    )
    await db_session.commit()
    db_session.add(
        Job(
            type=JobType.email,
            payload={"to": "a", "subject": "b"},
            idempotency_key="k1",
            user_id=user.id,
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


async def test_idempotency_key_scoped_per_user(db_session):
    # Two different users may share the same idempotency_key without colliding.
    user_a = User(name="user-a", key_hash="hash-a")
    user_b = User(name="user-b", key_hash="hash-b")
    db_session.add_all([user_a, user_b])
    await db_session.commit()

    db_session.add(
        Job(
            type=JobType.email,
            payload={"to": "a", "subject": "b"},
            idempotency_key="shared-key",
            user_id=user_a.id,
        )
    )
    db_session.add(
        Job(
            type=JobType.email,
            payload={"to": "a", "subject": "b"},
            idempotency_key="shared-key",
            user_id=user_b.id,
        )
    )
    await db_session.commit()  # must not raise; different users, same key


async def test_null_idempotency_keys_do_not_collide(db_session):
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}))
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}))
    await db_session.commit()  # two NULL keys must not violate the partial index


async def test_status_created_at_index_exists(pg_engine):
    async with pg_engine.begin() as conn:
        def check_status_index(c):
            insp = inspect(c)
            index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
            return "ix_jobs_status_created_at" in index_names
        result = await conn.run_sync(check_status_index)
    assert result


async def test_users_table_and_job_ownership_columns(pg_engine):
    from sqlalchemy import inspect

    async with pg_engine.begin() as conn:
        def check_ownership_schema(c):
            inspector = inspect(c)

            user_cols = {col["name"] for col in inspector.get_columns("users")}
            assert {"id", "name", "key_hash", "created_at"} <= user_cols

            job_cols = {col["name"] for col in inspector.get_columns("jobs")}
            assert "user_id" in job_cols

            job_indexes = {ix["name"] for ix in inspector.get_indexes("jobs")}
            assert "ix_jobs_user_id_created_at_id" in job_indexes
            assert "uq_jobs_user_idempotency_key" in job_indexes
            assert "uq_jobs_idempotency_key" not in job_indexes

            fks = inspector.get_foreign_keys("jobs")
            user_fk = next(fk for fk in fks if fk["constrained_columns"] == ["user_id"])
            assert user_fk["referred_table"] == "users"
            assert user_fk["options"].get("ondelete") == "SET NULL"

        await conn.run_sync(check_ownership_schema)
