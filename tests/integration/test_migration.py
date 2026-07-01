from sqlalchemy import inspect


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
