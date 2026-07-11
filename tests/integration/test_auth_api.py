import uuid

import pytest

EMAIL_JOB = {"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/jobs"),
        ("get", "/jobs"),
        ("get", f"/jobs/{uuid.uuid4()}"),
        ("post", f"/jobs/{uuid.uuid4()}/retry"),
        ("post", f"/jobs/{uuid.uuid4()}/cancel"),
    ],
)
def test_missing_key_is_401(unauth_client, method, path):
    kwargs = {"json": EMAIL_JOB} if (method, path) == ("post", "/jobs") else {}
    resp = getattr(unauth_client, method)(path, **kwargs)
    assert resp.status_code == 401


def test_unknown_key_is_401(unauth_client):
    resp = unauth_client.get("/jobs", headers={"X-API-Key": "not-a-real-key"})
    assert resp.status_code == 401


def test_probes_and_stats_stay_open(unauth_client):
    assert unauth_client.get("/health").status_code == 200
    assert unauth_client.get("/ready").status_code == 200
    assert unauth_client.get("/stats").status_code == 200


def test_cross_user_get_retry_cancel_are_404(client, second_user):
    job_id = client.post("/jobs", json=EMAIL_JOB).json()["id"]

    assert client.get(f"/jobs/{job_id}").status_code == 200
    assert client.get(f"/jobs/{job_id}", headers=second_user).status_code == 404
    assert client.post(f"/jobs/{job_id}/retry", headers=second_user).status_code == 404
    assert client.post(f"/jobs/{job_id}/cancel", headers=second_user).status_code == 404


def test_list_returns_only_own_jobs(client, second_user):
    mine = client.post("/jobs", json=EMAIL_JOB).json()["id"]
    client.post("/jobs", json=EMAIL_JOB, headers=second_user)

    items = client.get("/jobs").json()["items"]
    assert [j["id"] for j in items] == [mine]


def test_idempotency_key_scoped_per_user(client, second_user):
    body = {**EMAIL_JOB, "idempotency_key": "shared-key"}

    first = client.post("/jobs", json=body)
    assert first.status_code == 202
    replay = client.post("/jobs", json=body)
    assert replay.status_code == 200
    assert replay.json()["id"] == first.json()["id"]

    other = client.post("/jobs", json=body, headers=second_user)
    assert other.status_code == 202  # fresh job, no cross-user collision
    assert other.json()["id"] != first.json()["id"]


async def test_submitted_job_is_owned(client, pg_engine):
    from sqlalchemy import text

    job_id = client.post("/jobs", json=EMAIL_JOB).json()["id"]
    async with pg_engine.begin() as conn:
        owner = (
            await conn.execute(
                text("SELECT user_id FROM jobs WHERE id = :id"), {"id": job_id}
            )
        ).scalar_one()
    assert owner is not None


async def test_deleted_user_still_authenticates_within_ttl(
    client, second_user, pg_engine
):
    from sqlalchemy import text

    # Prime the cache.
    assert client.get("/jobs", headers=second_user).status_code == 200
    async with pg_engine.begin() as conn:
        await conn.execute(text("DELETE FROM users WHERE name = 'second-user'"))
    # Row is gone but the cache entry (default TTL 60s) still validates.
    assert client.get("/jobs", headers=second_user).status_code == 200


async def test_ttl_zero_makes_revocation_immediate(pg_engine, test_settings):
    from fastapi.testclient import TestClient
    from sqlalchemy import text

    from app.core.db import make_session_factory
    from app.main import create_app
    from app.users.keys import hash_key
    from app import repository as repo

    settings = test_settings.model_copy(update={"auth_cache_ttl_s": 0.0})
    factory = make_session_factory(pg_engine)
    async with factory() as session:
        await repo.upsert_user(session, "ephemeral", hash_key("ephemeral-key"))
        await session.commit()

    app = create_app(settings)
    with TestClient(app) as c:
        headers = {"X-API-Key": "ephemeral-key"}
        assert c.get("/jobs", headers=headers).status_code == 200
        async with pg_engine.begin() as conn:
            await conn.execute(text("DELETE FROM users WHERE name = 'ephemeral'"))
        assert c.get("/jobs", headers=headers).status_code == 401


async def test_unknown_key_is_not_negatively_cached(client, pg_engine):
    from app.core.db import make_session_factory
    from app.users.keys import hash_key
    from app import repository as repo

    headers = {"X-API-Key": "late-key"}
    assert client.get("/jobs", headers=headers).status_code == 401

    factory = make_session_factory(pg_engine)
    async with factory() as session:
        await repo.upsert_user(session, "late-user", hash_key("late-key"))
        await session.commit()

    # The earlier 401 must not have poisoned anything.
    assert client.get("/jobs", headers=headers).status_code == 200
