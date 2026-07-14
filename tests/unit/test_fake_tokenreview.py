import httpx
import pytest

from tests.support.fake_tokenreview import create_fake_tokenreview

TOKENS = {
    "tok-a": {
        "username": "alice",
        "uid": "11111111-1111-4111-8111-111111111111",
        "groups": ["jobprocessor-users"],
    }
}
URL = "http://fake/apis/authentication.k8s.io/v1/tokenreviews"


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://fake"
    )


def _body(token: str) -> dict:
    return {
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": {"token": token},
    }


async def test_known_token_is_authenticated_with_identity():
    async with _client(create_fake_tokenreview(TOKENS)) as c:
        resp = await c.post(URL, json=_body("tok-a"))
    assert resp.status_code == 201
    status = resp.json()["status"]
    assert status["authenticated"] is True
    assert status["user"]["username"] == "alice"
    assert status["user"]["uid"] == "11111111-1111-4111-8111-111111111111"
    assert status["user"]["groups"] == ["jobprocessor-users"]


async def test_unknown_token_is_unauthenticated():
    async with _client(create_fake_tokenreview(TOKENS)) as c:
        resp = await c.post(URL, json=_body("nope"))
    assert resp.status_code == 201
    assert resp.json()["status"] == {"authenticated": False}


async def test_wrong_sa_token_is_401():
    app = create_fake_tokenreview(TOKENS, required_sa_token="good-sa")
    async with _client(app) as c:
        resp = await c.post(
            URL, json=_body("tok-a"), headers={"Authorization": "Bearer stale-sa"}
        )
    assert resp.status_code == 401


@pytest.mark.parametrize("headers", [{"Authorization": "Bearer good-sa"}])
async def test_correct_sa_token_is_accepted(headers):
    app = create_fake_tokenreview(TOKENS, required_sa_token="good-sa")
    async with _client(app) as c:
        resp = await c.post(URL, json=_body("tok-a"), headers=headers)
    assert resp.status_code == 201


async def test_fail_mode_returns_500():
    async with _client(create_fake_tokenreview(TOKENS, fail=True)) as c:
        resp = await c.post(URL, json=_body("tok-a"))
    assert resp.status_code == 500
