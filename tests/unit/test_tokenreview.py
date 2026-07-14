import httpx
import pytest

from app.auth.tokenreview import ReviewedUser, TokenReviewer, TokenReviewUnavailable
from app.core.config import Settings
from tests.support.fake_tokenreview import create_fake_tokenreview

TOKENS = {
    "tok-a": {
        "username": "alice",
        "uid": "11111111-1111-4111-8111-111111111111",
        "groups": ["jobprocessor-users", "other"],
    }
}


def _settings(tmp_path, sa_token: str | None = None) -> Settings:
    token_file = tmp_path / "sa-token"
    if sa_token is not None:
        token_file.write_text(sa_token)
    return Settings(
        database_url="postgresql://x/x",
        redis_url="redis://x",
        auth_tokenreview_url="http://fake/apis/authentication.k8s.io/v1/tokenreviews",
        auth_sa_token_file=str(token_file),
        auth_ca_file=str(tmp_path / "absent-ca.crt"),
    )


def _reviewer(tmp_path, fake_app, sa_token: str | None = None) -> TokenReviewer:
    return TokenReviewer(
        _settings(tmp_path, sa_token), transport=httpx.ASGITransport(app=fake_app)
    )


async def test_valid_token_returns_reviewed_user(tmp_path):
    reviewer = _reviewer(tmp_path, create_fake_tokenreview(TOKENS))
    user = await reviewer.review("tok-a")
    assert user == ReviewedUser(
        uid="11111111-1111-4111-8111-111111111111",
        username="alice",
        groups=("jobprocessor-users", "other"),
    )
    await reviewer.aclose()


async def test_unknown_token_returns_none(tmp_path):
    reviewer = _reviewer(tmp_path, create_fake_tokenreview(TOKENS))
    assert await reviewer.review("nope") is None
    await reviewer.aclose()


async def test_apiserver_500_raises_unavailable(tmp_path):
    reviewer = _reviewer(tmp_path, create_fake_tokenreview(TOKENS, fail=True))
    with pytest.raises(TokenReviewUnavailable):
        await reviewer.review("tok-a")
    await reviewer.aclose()


async def test_connect_error_raises_unavailable(tmp_path):
    class Boom(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ConnectError("down", request=request)

    reviewer = TokenReviewer(_settings(tmp_path, "sa"), transport=Boom())
    with pytest.raises(TokenReviewUnavailable):
        await reviewer.review("tok-a")
    await reviewer.aclose()


async def test_sa_rotation_rereads_token_and_retries_once(tmp_path):
    # Apiserver only accepts "new-sa"; reviewer starts holding "old-sa".
    fake = create_fake_tokenreview(TOKENS, required_sa_token="new-sa")
    reviewer = _reviewer(tmp_path, fake, sa_token="old-sa")
    # Kubelet rotated the projected file after startup:
    (tmp_path / "sa-token").write_text("new-sa")
    user = await reviewer.review("tok-a")
    assert user is not None and user.username == "alice"
    await reviewer.aclose()


async def test_persistent_401_raises_unavailable(tmp_path):
    fake = create_fake_tokenreview(TOKENS, required_sa_token="right-sa")
    reviewer = _reviewer(tmp_path, fake, sa_token="wrong-sa")
    with pytest.raises(TokenReviewUnavailable):
        await reviewer.review("tok-a")
    await reviewer.aclose()


async def test_missing_sa_token_file_sends_no_auth_header(tmp_path):
    # Outside a cluster there is no projected token; the fake (no
    # required_sa_token) must still be reachable.
    reviewer = _reviewer(tmp_path, create_fake_tokenreview(TOKENS), sa_token=None)
    assert await reviewer.review("tok-a") is not None
    await reviewer.aclose()
