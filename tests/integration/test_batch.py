from app import repository as repo
from app.core.db import make_session_factory
from app.schemas.enums import JobType
from app.worker.context import PgJobContext


def test_pg_context_writes_progress_and_reads_cancel(db_session, pg_engine):
    sf = make_session_factory(pg_engine)
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    repo.claim_job(db_session, job.id)  # -> processing

    ctx = PgJobContext(job.id, sf, poll_interval_s=0.0)
    ctx.set_progress(42)
    assert ctx.cancelled() is False
    db_session.refresh(job)
    assert job.progress == 42

    repo.request_cancel(db_session, job.id)
    ctx.set_progress(43)  # change so the next poll writes + re-reads the flag
    assert ctx.cancelled() is True
