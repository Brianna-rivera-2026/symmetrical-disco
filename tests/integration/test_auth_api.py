import uuid

import pytest

from tests.integration.conftest import NON_UUID_UID_TEST_TOKEN, OUTSIDER_TEST_TOKEN

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
def test_missing_token_is_401(unauth_client, method, path):
    kwargs = {"json": EMAIL_JOB} if (method, path) == ("post", "/jobs") else {}
    resp = getattr(unauth_client, method)(path, **kwargs)
    assert resp.status_code == 401


def test_unknown_token_is_401(unauth_client):
    resp = unauth_client.get(
        "/jobs", headers={"Authorization": "Bearer not-a-real-token"}
    )
    assert resp.status_code == 401


def test_authenticated_but_wrong_group_is_403(unauth_client):
    resp = unauth_client.get(
        "/jobs", headers={"Authorization": f"Bearer {OUTSIDER_TEST_TOKEN}"}
    )
    assert resp.status_code == 403


def test_authenticated_in_group_but_non_uuid_uid_is_401(unauth_client):
    resp = unauth_client.get(
        "/jobs", headers={"Authorization": f"Bearer {NON_UUID_UID_TEST_TOKEN}"}
    )
    assert resp.status_code == 401


def test_probes_and_stats_stay_open(unauth_client):
    assert unauth_client.get("/health").status_code == 200
    assert unauth_client.get("/ready").status_code == 200
    assert unauth_client.get("/stats").status_code == 200


async def test_apiserver_down_is_503(pg_engine, test_settings):
    from fastapi.testclient import TestClient

    from app.main import create_app

    settings = test_settings.model_copy(
        update={
            # RFC 5737 TEST-NET, nothing listens there; short timeout.
            "auth_tokenreview_url": "http://192.0.2.1:6443/apis/authentication.k8s.io/v1/tokenreviews",
            "auth_timeout_s": 0.2,
        }
    )
    app = create_app(settings)
    with TestClient(app) as c:
        resp = c.get("/jobs", headers={"Authorization": "Bearer whatever"})
    assert resp.status_code == 503


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


async def test_submitted_job_is_owned_with_display_name(
    client, pg_engine, default_user_id
):
    from sqlalchemy import text

    job_id = client.post("/jobs", json=EMAIL_JOB).json()["id"]
    async with pg_engine.begin() as conn:
        row = (
            await conn.execute(
                text("SELECT user_id, user_name FROM jobs WHERE id = :id"),
                {"id": job_id},
            )
        ).one()
    assert row.user_id == default_user_id
    assert row.user_name == "default-user"
