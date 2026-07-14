import uuid

import pytest
from starlette.requests import Request

from app.api.ratelimit import user_or_ip_identifier


def make_request(headers: list = None, client=("10.0.0.9", 1234)) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/jobs",
        "headers": headers or [],
        "client": client,
        "query_string": b"",
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_identifier_keys_on_authed_user_id():
    user_id = uuid.uuid4()
    req = make_request(headers=[(b"authorization", b"Bearer secret-token")])
    req.state.authed_user_id = user_id
    assert await user_or_ip_identifier(req) == "u:" + str(user_id)


@pytest.mark.asyncio
async def test_identifier_falls_back_to_client_ip_without_authed_user_id():
    # Even with an Authorization header present, an unset `authed_user_id`
    # (i.e. get_current_user never validated it) must not be trusted for
    # bucketing — otherwise a garbage token rotated per-request would dodge
    # the limiter.
    req = make_request(headers=[(b"authorization", b"Bearer secret-token")])
    assert await user_or_ip_identifier(req) == "ip:10.0.0.9"


@pytest.mark.asyncio
async def test_identifier_falls_back_to_client_ip():
    assert await user_or_ip_identifier(make_request()) == "ip:10.0.0.9"
