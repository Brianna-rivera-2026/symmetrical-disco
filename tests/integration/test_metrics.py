import pytest

from app import repository as repo
from app.jobs import handlers
from app.schemas.enums import JobType
from app.worker.runner import process_job


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def _points(metric_reader, name):
    data = metric_reader.get_metrics_data()
    points = []
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    points.extend(metric.data.data_points)
    return points


def test_jobs_processed_counts_completed(
    db_session, redis_client, test_settings, metric_reader, owner_id
):
    job = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    process_job(db_session, redis_client, test_settings, job.id)
    points = _points(metric_reader, "jobs.processed")
    completed = [
        p
        for p in points
        if p.attributes.get("outcome") == "completed"
        and p.attributes.get("type") == "email"
    ]
    assert completed and completed[0].value >= 1


def test_processing_duration_recorded(
    db_session, redis_client, test_settings, metric_reader, owner_id
):
    job = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, user_id=owner_id
    )
    process_job(db_session, redis_client, test_settings, job.id)
    points = _points(metric_reader, "job.processing.duration")
    assert any(
        p.attributes.get("outcome") == "completed" and p.count >= 1 for p in points
    )


def test_jobs_failed_counts_exhausted_attempts(
    db_session, redis_client, test_settings, metric_reader, monkeypatch, owner_id
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)
    job = repo.create_job(
        db_session,
        JobType.webhook,
        {"url": "https://x.test"},
        max_attempts=1,
        user_id=owner_id,
    )
    process_job(db_session, redis_client, test_settings, job.id)
    points = _points(metric_reader, "jobs.failed")
    failed = [p for p in points if p.attributes.get("type") == "webhook"]
    assert failed and failed[0].value >= 1
