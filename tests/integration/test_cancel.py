import uuid
from datetime import datetime, timedelta, timezone

from app import repository as repo
from app.schemas.enums import JobStatus, JobType

_EMAIL = {"to": "a@b.com", "subject": "Hi"}


def test_cancel_pending_returns_200(client):
    jid = client.post("/jobs", json={"type": "email", "payload": _EMAIL}).json()["id"]
    resp = client.post(f"/jobs/{jid}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


def test_cancel_scheduled_zrems_from_delayed(client):
    when = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    jid = client.post(
        "/jobs", json={"type": "email", "payload": _EMAIL, "scheduled_at": when}
    ).json()["id"]
    settings = client.app.state.settings
    assert client.app.state.redis.zcard(settings.delayed_zset) == 1
    resp = client.post(f"/jobs/{jid}/cancel")
    assert resp.status_code == 200
    assert client.app.state.redis.zcard(settings.delayed_zset) == 0


def test_cancel_processing_returns_202_and_sets_flag(client, db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    repo.claim_job(db_session, job.id)  # -> processing
    resp = client.post(f"/jobs/{job.id}/cancel")
    assert resp.status_code == 202
    db_session.refresh(job)
    assert job.cancel_requested_at is not None
    assert job.status is JobStatus.processing  # endpoint does NOT flip status


def test_cancel_completed_returns_409(client, db_session):
    job = repo.create_job(db_session, JobType.email, _EMAIL)
    repo.claim_job(db_session, job.id)
    repo.complete_job(db_session, job.id, {"message_id": "m"})
    resp = client.post(f"/jobs/{job.id}/cancel")
    assert resp.status_code == 409


def test_cancel_already_cancelled_is_idempotent_200(client):
    jid = client.post("/jobs", json={"type": "email", "payload": _EMAIL}).json()["id"]
    client.post(f"/jobs/{jid}/cancel")
    resp = client.post(f"/jobs/{jid}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


def test_cancel_unknown_returns_404(client):
    assert client.post(f"/jobs/{uuid.uuid4()}/cancel").status_code == 404


def test_job_out_exposes_progress_field(client, db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    got = client.get(f"/jobs/{job.id}").json()
    assert "progress" in got
    assert got["progress"] is None
