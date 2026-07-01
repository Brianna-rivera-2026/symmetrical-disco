def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_submit_creates_job_and_enqueues(client):
    resp = client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "pending"
    assert body["type"] == "email"

    # Job is fetchable.
    got = client.get(f"/jobs/{body['id']}")
    assert got.status_code == 200
    assert got.json()["payload"]["to"] == "a@b.com"

    # Default priority is normal, echoed and routed to the normal stream.
    assert body["priority"] == "normal"
    redis_client = client.app.state.redis
    settings = client.app.state.settings
    assert redis_client.xlen(settings.stream_normal) == 1


def test_submit_rejects_bad_payload(client):
    resp = client.post(
        "/jobs", json={"type": "email", "payload": {"subject": "no recipient"}}
    )
    assert resp.status_code == 422


def test_submit_rejects_unknown_type(client):
    resp = client.post("/jobs", json={"type": "translate", "payload": {}})
    assert resp.status_code == 422


def test_get_missing_returns_404(client):
    import uuid

    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404


def test_list_filters_by_type(client):
    client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    client.post("/jobs", json={"type": "report", "payload": {"report_type": "sales"}})
    resp = client.get("/jobs", params={"type": "email"})
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["type"] == "email"


def test_submit_scheduled_job_parks_in_zset(client):
    from datetime import datetime, timedelta, timezone

    client.app.state.redis.flushdb()
    when = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    resp = client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.com", "subject": "Hi"},
            "scheduled_at": when,
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "scheduled"
    assert body["scheduled_at"] is not None

    redis_client = client.app.state.redis
    settings = client.app.state.settings
    assert redis_client.zcard(settings.delayed_zset) == 1
    assert redis_client.xlen(settings.stream_normal) == 0


def test_submit_past_scheduled_at_runs_immediately(client):
    from datetime import datetime, timedelta, timezone

    client.app.state.redis.flushdb()
    when = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    resp = client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.com", "subject": "Hi"},
            "scheduled_at": when,
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "pending"

    redis_client = client.app.state.redis
    settings = client.app.state.settings
    assert redis_client.xlen(settings.stream_normal) == 1
    assert redis_client.zcard(settings.delayed_zset) == 0


def test_submit_high_priority_routes_to_high_stream(client):
    client.app.state.redis.flushdb()
    resp = client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.com", "subject": "Hi"},
            "priority": "high",
        },
    )
    assert resp.status_code == 202
    assert resp.json()["priority"] == "high"

    redis_client = client.app.state.redis
    settings = client.app.state.settings
    assert redis_client.xlen(settings.stream_high) == 1
    assert redis_client.xlen(settings.stream_normal) == 0


def test_list_filters_by_priority(client):
    client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.com", "subject": "Hi"},
            "priority": "high",
        },
    )
    client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    resp = client.get("/jobs", params={"priority": "high"})
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["priority"] == "high"


def test_job_out_exposes_attempts(client):
    resp = client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    job_id = resp.json()["id"]
    got = client.get(f"/jobs/{job_id}").json()
    assert got["attempts"] == 0
    assert got["max_attempts"] == 4


def test_retry_failed_job_reenqueues(client, db_session):
    from app import repository as repo
    from app.schemas.enums import JobType

    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    repo.claim_job(db_session, job.id)
    repo.fail_job(db_session, job.id, {"type": "E", "message": "boom"})

    resp = client.post(f"/jobs/{job.id}/retry")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "pending"
    assert body["attempts"] == 0


def test_retry_non_failed_returns_409(client):
    resp = client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    job_id = resp.json()["id"]
    retry = client.post(f"/jobs/{job_id}/retry")
    assert retry.status_code == 409


def test_retry_unknown_returns_404(client):
    import uuid

    resp = client.post(f"/jobs/{uuid.uuid4()}/retry")
    assert resp.status_code == 404
