import pytest
from starlette.requests import Request

from app.api.ratelimit import user_or_ip_identifier
from app.users.keys import hash_key


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
async def test_identifier_keys_on_api_key_hash():
    req = make_request(headers=[(b"x-api-key", b"secret-key")])
    assert await user_or_ip_identifier(req) == "u:" + hash_key("secret-key")


@pytest.mark.asyncio
async def test_identifier_falls_back_to_client_ip():
    assert await user_or_ip_identifier(make_request()) == "ip:10.0.0.9"
