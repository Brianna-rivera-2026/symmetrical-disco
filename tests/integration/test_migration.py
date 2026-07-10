from sqlalchemy import inspect

import pytest
from sqlalchemy.exc import IntegrityError

from app import repository as repo
from app.models.job import Job
from app.models.user import User
from app.schemas.enums import JobType


def test_jobs_table_exists_with_indexes(pg_engine):
    insp = inspect(pg_engine)
    assert "jobs" in insp.get_table_names()
    index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
    assert "ix_jobs_created_at_id" in index_names


def test_priority_column_and_index(pg_engine):
    insp = inspect(pg_engine)
    cols = {c["name"] for c in insp.get_columns("jobs")}
    assert "priority" in cols
    index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
    assert "ix_jobs_priority" in index_names


def test_batch_type_and_new_columns_persist(db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    db_session.refresh(job)
    assert job.progress is None
    assert job.cancel_requested_at is None
    assert job.idempotency_key is None
    assert job.idempotency_hash is None


def test_idempotency_key_partial_unique(db_session):
    # Idempotency uniqueness is scoped per user (uq_jobs_user_idempotency_key),
    # so the collision must be tested with a shared, non-null user_id.
    user = User(name="idempotency-test-user", key_hash="hash1")
    db_session.add(user)
    db_session.commit()

    db_session.add(
        Job(
            type=JobType.email,
            payload={"to": "a", "subject": "b"},
            idempotency_key="k1",
            user_id=user.id,
        )
    )
    db_session.commit()
    db_session.add(
        Job(
            type=JobType.email,
            payload={"to": "a", "subject": "b"},
            idempotency_key="k1",
            user_id=user.id,
        )
    )
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_idempotency_key_scoped_per_user(db_session):
    # Two different users may share the same idempotency_key without colliding.
    user_a = User(name="user-a", key_hash="hash-a")
    user_b = User(name="user-b", key_hash="hash-b")
    db_session.add_all([user_a, user_b])
    db_session.commit()

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
    db_session.commit()  # must not raise; different users, same key


def test_null_idempotency_keys_do_not_collide(db_session):
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}))
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}))
    db_session.commit()  # two NULL keys must not violate the partial index


def test_status_created_at_index_exists(pg_engine):
    insp = inspect(pg_engine)
    index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
    assert "ix_jobs_status_created_at" in index_names


def test_users_table_and_job_ownership_columns(pg_engine):
    from sqlalchemy import inspect

    inspector = inspect(pg_engine)

    user_cols = {c["name"] for c in inspector.get_columns("users")}
    assert {"id", "name", "key_hash", "created_at"} <= user_cols

    job_cols = {c["name"] for c in inspector.get_columns("jobs")}
    assert "user_id" in job_cols

    job_indexes = {ix["name"] for ix in inspector.get_indexes("jobs")}
    assert "ix_jobs_user_id_created_at_id" in job_indexes
    assert "uq_jobs_user_idempotency_key" in job_indexes
    assert "uq_jobs_idempotency_key" not in job_indexes

    fks = inspector.get_foreign_keys("jobs")
    user_fk = next(fk for fk in fks if fk["constrained_columns"] == ["user_id"])
    assert user_fk["referred_table"] == "users"
    assert user_fk["options"].get("ondelete") == "SET NULL"
