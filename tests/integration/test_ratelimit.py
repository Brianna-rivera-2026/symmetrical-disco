import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.core.config import Settings
from app.core.redis import create_redis_client
from app.main import create_app

from .conftest import DEFAULT_TEST_TOKEN, SECOND_TEST_TOKEN


@pytest.fixture
async def limited_client(pg_engine, database_url, redis_container, test_settings):
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    settings = Settings(
        database_url=database_url,
        redis_url=redis_url,
        rate_limit_enabled=True,
        read_rate_limit_per_min=3,
        webhook_allowed_hosts=["x.test"],
        email_allowed_domains=["b.com"],
        auth_tokenreview_url=test_settings.auth_tokenreview_url,
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs"))
    real_redis = create_redis_client(redis_url)
    try:
        await real_redis.flushdb()
    finally:
        await real_redis.aclose()


def test_over_limit_returns_429_with_retry_after(limited_client):
    headers = {"Authorization": f"Bearer {DEFAULT_TEST_TOKEN}"}
    for _ in range(3):
        assert limited_client.get("/jobs", headers=headers).status_code == 200
    r = limited_client.get("/jobs", headers=headers)
    assert r.status_code == 429
    assert "retry-after" in {k.lower() for k in r.headers}


def test_other_user_unaffected(limited_client):
    for _ in range(4):
        limited_client.get(
            "/jobs", headers={"Authorization": f"Bearer {DEFAULT_TEST_TOKEN}"}
        )
    r = limited_client.get(
        "/jobs", headers={"Authorization": f"Bearer {SECOND_TEST_TOKEN}"}
    )
    assert r.status_code == 200


def test_disabled_flag_bypasses_limits(client):
    # `client` fixture has rate_limit_enabled=False
    for _ in range(10):
        assert client.get("/jobs").status_code == 200


@pytest.fixture
async def cross_group_client(pg_engine, database_url, redis_container, test_settings):
    """Distinct low limit on "stats" vs the default-high "read" limit, to
    prove the two groups don't share a rate-limit bucket for the same user
    (the whole reason `_GroupRateLimiter` folds the group name into its
    Redis key instead of reusing fastapi-limiter's broken route lookup)."""
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    settings = Settings(
        database_url=database_url,
        redis_url=redis_url,
        rate_limit_enabled=True,
        stats_rate_limit_per_min=3,
        webhook_allowed_hosts=["x.test"],
        email_allowed_domains=["b.com"],
        auth_tokenreview_url=test_settings.auth_tokenreview_url,
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs"))
    real_redis = create_redis_client(redis_url)
    try:
        await real_redis.flushdb()
    finally:
        await real_redis.aclose()


def test_groups_have_independent_counters(cross_group_client):
    headers = {"Authorization": f"Bearer {DEFAULT_TEST_TOKEN}"}
    for _ in range(3):
        assert cross_group_client.get("/stats", headers=headers).status_code == 200
    r = cross_group_client.get("/stats", headers=headers)
    assert r.status_code == 429

    # Same user, different group ("read", default 120/min) — must be
    # unaffected by the exhausted "stats" bucket.
    r = cross_group_client.get("/jobs", headers=headers)
    assert r.status_code == 200


def test_garbage_tokens_never_consume_a_rate_limit_bucket(limited_client):
    """Regression guard for the auth-before-rate-limit ordering fix: an
    attacker rotating a fresh, never-valid bearer token on every request must
    always 401 from get_current_user (declared before rate_limit(...) in the
    route's dependencies=[...]) and never reach the rate limiter's Redis
    interaction — so no garbage token ever consumes, or is throttled by, a
    submit-group bucket. Every one of these must 401; none may be a 202
    (would mean auth was bypassed) or a 429 (would mean a real bucket was
    consumed by an invalid token)."""
    for _ in range(25):
        headers = {"Authorization": f"Bearer garbage-{uuid.uuid4()}"}
        r = limited_client.post(
            "/jobs", headers=headers, json={"type": "email", "payload": {}}
        )
        assert r.status_code == 401
