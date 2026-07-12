import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_session_factory
from app.core.redis import create_redis_client
from app.main import create_app
from app.users.keys import hash_key

from .conftest import DEFAULT_TEST_KEY, SECOND_TEST_KEY


@pytest.fixture
async def limited_client(pg_engine, database_url, redis_container):
    factory = make_session_factory(pg_engine)
    async with factory() as session:
        await repo.upsert_user(session, "default-user", hash_key(DEFAULT_TEST_KEY))
        await repo.upsert_user(session, "second-user", hash_key(SECOND_TEST_KEY))
        await session.commit()
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    settings = Settings(
        database_url=database_url,
        redis_url=redis_url,
        rate_limit_enabled=True,
        read_rate_limit_per_min=3,
        webhook_allowed_hosts=["x.test"],
        email_allowed_domains=["b.com"],
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs, users"))
    real_redis = create_redis_client(redis_url)
    try:
        await real_redis.flushdb()
    finally:
        await real_redis.aclose()


def test_over_limit_returns_429_with_retry_after(limited_client):
    headers = {"X-API-Key": DEFAULT_TEST_KEY}
    for _ in range(3):
        assert limited_client.get("/jobs", headers=headers).status_code == 200
    r = limited_client.get("/jobs", headers=headers)
    assert r.status_code == 429
    assert "retry-after" in {k.lower() for k in r.headers}


def test_other_user_unaffected(limited_client):
    for _ in range(4):
        limited_client.get("/jobs", headers={"X-API-Key": DEFAULT_TEST_KEY})
    r = limited_client.get("/jobs", headers={"X-API-Key": SECOND_TEST_KEY})
    assert r.status_code == 200


def test_disabled_flag_bypasses_limits(client):
    # `client` fixture has rate_limit_enabled=False
    for _ in range(10):
        assert client.get("/jobs").status_code == 200
