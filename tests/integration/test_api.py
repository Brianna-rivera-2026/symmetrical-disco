def test_health_is_pure_liveness(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_stays_200_when_redis_down(client):
    from app.core.redis import create_redis_client

    # Point the app at a closed port -> PING raises a RedisError.
    client.app.state.redis = create_redis_client("redis://127.0.0.1:6390/0")
    response = client.get("/health")
    assert response.status_code == 200


def test_ready_ok_when_backends_up(client):
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "checks": {"postgres": "ok", "redis": "ok"},
    }


def test_ready_503_when_redis_down(client):
    from app.core.redis import create_redis_client

    # Point the app at a closed port -> PING raises a RedisError.
    client.app.state.redis = create_redis_client("redis://127.0.0.1:6390/0")
    resp = client.get("/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "unavailable"
    assert body["checks"]["redis"] == "error"
    assert body["checks"]["postgres"] == "ok"


async def test_submit_creates_job_and_enqueues(client, redis_client):
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
    settings = client.app.state.settings
    # A dedicated connection (rather than `client.app.state.redis`, which is
    # bound to TestClient's own event-loop thread) so this awaits cleanly
    # from the test's loop while checking the same Redis server.
    assert await redis_client.xlen(settings.stream_normal) == 1


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


async def test_submit_scheduled_job_parks_in_zset(client, redis_client):
    from datetime import datetime, timedelta, timezone

    await redis_client.flushdb()
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

    settings = client.app.state.settings
    assert await redis_client.zcard(settings.delayed_zset) == 1
    assert await redis_client.xlen(settings.stream_normal) == 0


async def test_submit_past_scheduled_at_runs_immediately(client, redis_client):
    from datetime import datetime, timedelta, timezone

    await redis_client.flushdb()
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

    settings = client.app.state.settings
    assert await redis_client.xlen(settings.stream_normal) == 1
    assert await redis_client.zcard(settings.delayed_zset) == 0


async def test_submit_high_priority_routes_to_high_stream(client, redis_client):
    await redis_client.flushdb()
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

    settings = client.app.state.settings
    assert await redis_client.xlen(settings.stream_high) == 1
    assert await redis_client.xlen(settings.stream_normal) == 0


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


def test_batch_submission_gets_single_attempt(client):
    """Batch jobs are never retried: a retry re-runs already-executed items
    from scratch (duplicating their side effects), so max_attempts is 1."""
    resp = client.post(
        "/jobs",
        json={
            "type": "batch",
            "payload": {"items": [{"type": "email", "to": "a@b.com", "subject": "Hi"}]},
        },
    )
    assert resp.status_code == 202
    got = client.get(f"/jobs/{resp.json()['id']}").json()
    assert got["max_attempts"] == 1


async def test_failed_batch_is_not_retried(client, db_session, redis_client, pg_engine):
    """A batch that fails (HandlerTimeout) is terminal on its first attempt:
    no re-enqueue to a stream, no delayed retry, status goes straight to
    failed."""
    from uuid import UUID

    from app import repository as repo
    from app.core.db import make_session_factory
    from app.schemas.enums import JobStatus
    from app.worker.runner import process_job

    await redis_client.flushdb()
    resp = client.post(
        "/jobs",
        json={
            "type": "batch",
            "payload": {"items": [{"type": "email", "to": "a@b.com", "subject": "Hi"}]},
        },
    )
    assert resp.status_code == 202
    job_id = UUID(resp.json()["id"])
    # The email item's handler sleeps ≥1s, so a 0.05s budget guarantees a
    # HandlerTimeout on the batch.
    settings = client.app.state.settings.model_copy(
        update={"job_handler_timeout_s": 0.05}
    )

    outcome = await process_job(
        db_session,
        redis_client,
        settings,
        job_id,
        session_factory=make_session_factory(pg_engine),
    )

    assert outcome.label == "timeout"
    assert outcome.ack is True
    db_session.expire_all()
    job = await repo.get_job(db_session, job_id)
    assert job.status is JobStatus.failed
    assert job.attempts == 1
    # No retry handoff happened: only the original submission message exists,
    # and nothing was parked for a delayed retry.
    assert await redis_client.xlen(settings.stream_normal) == 1
    assert await redis_client.zcard(settings.delayed_zset) == 0


async def test_retry_failed_job_reenqueues(client, db_session, default_user_id):
    from app import repository as repo
    from app.schemas.enums import JobType

    job = await repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, user_id=default_user_id
    )
    await repo.claim_job(db_session, job.id)
    await repo.fail_job(db_session, job.id, {"type": "E", "message": "boom"})

    resp = client.post(f"/jobs/{job.id}/retry")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "pending"
    assert body["attempts"] == 0


async def test_retry_cancel_requested_returns_409(client, db_session, default_user_id):
    from app import repository as repo
    from app.schemas.enums import JobType

    job = await repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, user_id=default_user_id
    )
    await repo.claim_job(db_session, job.id)
    await repo.request_cancel(db_session, job.id)
    await repo.fail_job(db_session, job.id, {"type": "E", "message": "boom"})

    resp = client.post(f"/jobs/{job.id}/retry")
    assert resp.status_code == 409
    got = client.get(f"/jobs/{job.id}").json()
    assert got["status"] == "failed"


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
