from sqlalchemy import inspect

import pytest
from sqlalchemy.exc import IntegrityError

from app import repository as repo
from app.models.job import Job
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
    db_session.add(
        Job(
            type=JobType.email,
            payload={"to": "a", "subject": "b"},
            idempotency_key="k1",
        )
    )
    db_session.commit()
    db_session.add(
        Job(
            type=JobType.email,
            payload={"to": "a", "subject": "b"},
            idempotency_key="k1",
        )
    )
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_null_idempotency_keys_do_not_collide(db_session):
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}))
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}))
    db_session.commit()  # two NULL keys must not violate the partial index


def test_status_created_at_index_exists(pg_engine):
    insp = inspect(pg_engine)
    index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
    assert "ix_jobs_status_created_at" in index_names
