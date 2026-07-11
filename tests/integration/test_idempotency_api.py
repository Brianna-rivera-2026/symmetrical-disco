from app import repository as repo
from app.api import routes
from app.idempotency import canonical_hash
from app.schemas.enums import JobType

_EMAIL = {"to": "a@b.com", "subject": "Hi"}


async def test_replay_returns_200_and_same_job(client, redis_client):
    body = {"type": "email", "payload": _EMAIL, "idempotency_key": "k1"}
    r1 = client.post("/jobs", json=body)
    assert r1.status_code == 202
    r2 = client.post("/jobs", json=body)
    assert r2.status_code == 200
    assert r2.json()["id"] == r1.json()["id"]
    settings = client.app.state.settings
    assert await redis_client.xlen(settings.stream_normal) == 1  # only one enqueue


def test_same_key_different_payload_returns_409(client):
    client.post(
        "/jobs", json={"type": "email", "payload": _EMAIL, "idempotency_key": "k2"}
    )
    resp = client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "z@z.com", "subject": "Diff"},
            "idempotency_key": "k2",
        },
    )
    assert resp.status_code == 409


def test_no_key_always_creates(client):
    r1 = client.post("/jobs", json={"type": "email", "payload": _EMAIL})
    r2 = client.post("/jobs", json={"type": "email", "payload": _EMAIL})
    assert r1.json()["id"] != r2.json()["id"]


async def test_race_path_different_payload_conflicts(
    client, db_session, default_user_id, monkeypatch
):
    # Pre-create the "winner" row with key "race".
    await repo.create_job(
        db_session,
        JobType.email,
        _EMAIL,
        idempotency_key="race",
        idempotency_hash=canonical_hash(JobType.email, _EMAIL),
        user_id=default_user_id,
    )
    # Force the first lookup to miss so the route takes the create -> IntegrityError
    # -> rollback -> re-lookup branch (the concurrent-race path).
    real = repo.get_by_idempotency_key
    calls = {"n": 0}

    async def flaky(session, key, user_id):
        calls["n"] += 1
        return None if calls["n"] == 1 else await real(session, key, user_id)

    monkeypatch.setattr(routes.repo, "get_by_idempotency_key", flaky)
    resp = client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "z@z.com", "subject": "Diff"},
            "idempotency_key": "race",
        },
    )
    assert resp.status_code == 409  # loser does NOT receive the winner's job
