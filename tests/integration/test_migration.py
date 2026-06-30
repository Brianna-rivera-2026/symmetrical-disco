from sqlalchemy import inspect


def test_jobs_table_exists_with_indexes(pg_engine):
    insp = inspect(pg_engine)
    assert "jobs" in insp.get_table_names()
    index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
    assert "ix_jobs_created_at_id" in index_names
