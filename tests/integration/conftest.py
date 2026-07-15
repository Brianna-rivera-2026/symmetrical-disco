import asyncio
import sys
import threading
import time
import uuid

import pytest
import pytest_asyncio
import uvicorn
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.redis import create_redis_client
from app.main import create_app
from tests.support.fake_tokenreview import create_fake_tokenreview

# Fix for Windows asyncio event loop policy with psycopg
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

DEFAULT_TEST_TOKEN = "tok-default"
SECOND_TEST_TOKEN = "tok-second"
OUTSIDER_TEST_TOKEN = "tok-outsider"  # authenticated but not in the group
NON_UUID_UID_TEST_TOKEN = "tok-non-uuid-uid"  # authenticated, in-group, bad uid
DEFAULT_TEST_UID = "00000000-0000-4000-8000-000000000001"
SECOND_TEST_UID = "00000000-0000-4000-8000-000000000002"

TEST_TOKENS = {
    DEFAULT_TEST_TOKEN: {
        "username": "default-user",
        "uid": DEFAULT_TEST_UID,
        "groups": ["jobprocessor-users"],
    },
    SECOND_TEST_TOKEN: {
        "username": "second-user",
        "uid": SECOND_TEST_UID,
        "groups": ["jobprocessor-users"],
    },
    OUTSIDER_TEST_TOKEN: {
        "username": "outsider",
        "uid": "00000000-0000-4000-8000-000000000003",
        "groups": ["some-other-group"],
    },
    NON_UUID_UID_TEST_TOKEN: {
        "username": "weird-idp-user",
        "uid": "not-a-uuid",
        "groups": ["jobprocessor-users"],
    },
}


@pytest.fixture(scope="session")
def fake_tokenreview_url():
    """Real-socket fake TokenReview server (the app reaches it via httpx
    over the network, exactly like the in-cluster apiserver)."""
    config = uvicorn.Config(
        create_fake_tokenreview(TEST_TOKENS),
        host="127.0.0.1",
        port=0,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("fake tokenreview server failed to start")
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}/apis/authentication.k8s.io/v1/tokenreviews"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16", driver="psycopg") as pg:
        yield pg


@pytest.fixture(scope="session")
def database_url(postgres_container) -> str:
    return postgres_container.get_connection_url()


@pytest.fixture(scope="session")
async def pg_engine(database_url):
    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "alembic")
    cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(cfg, "head")
    engine = make_engine(database_url)
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_session(pg_engine):
    factory = make_session_factory(pg_engine)
    session = factory()
    try:
        yield session
    finally:
        await session.close()
        async with pg_engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE jobs"))


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7") as rc:
        yield rc


@pytest_asyncio.fixture(loop_scope="function")
async def redis_client(redis_container):
    url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    client = create_redis_client(url)
    yield client
    await client.flushdb()
    await client.aclose()


@pytest.fixture
def sync_redis_client(redis_container):
    """Plain sync `redis.Redis`, for exercising the ticker's OTel gauge
    callbacks which run on the exporter thread and cannot await."""
    import redis as sync_redis

    url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    client = sync_redis.Redis.from_url(url, decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def sync_session_factory(database_url):
    """Plain sync sessionmaker, for exercising the ticker's OTel gauge
    callbacks which run on the exporter thread and cannot await."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(database_url, pool_pre_ping=True)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


@pytest.fixture(scope="session")
def test_settings(database_url, redis_container, fake_tokenreview_url) -> Settings:
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    return Settings(
        database_url=database_url,
        redis_url=redis_url,
        rate_limit_enabled=False,  # rate-limit tests opt in explicitly
        webhook_allowed_hosts=["x.test"],
        email_allowed_domains=["b.com"],
        auth_tokenreview_url=fake_tokenreview_url,
    )


@pytest.fixture
async def client(pg_engine, test_settings):
    app = create_app(test_settings)
    with TestClient(app) as c:
        c.headers.update({"Authorization": f"Bearer {DEFAULT_TEST_TOKEN}"})
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs"))
    real_redis = create_redis_client(test_settings.redis_url)
    try:
        await real_redis.flushdb()
    finally:
        await real_redis.aclose()


@pytest.fixture
def default_user_id():
    """UID the `client` fixture's token resolves to, for tests that create
    jobs via repo.create_job(...) owned by the same user."""
    return uuid.UUID(DEFAULT_TEST_UID)


@pytest.fixture
async def unauth_client(pg_engine, test_settings):
    """TestClient with no Authorization header."""
    app = create_app(test_settings)
    with TestClient(app) as c:
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs"))


@pytest.fixture
def second_user():
    """Headers for a second, independent user."""
    return {"Authorization": f"Bearer {SECOND_TEST_TOKEN}"}


@pytest.fixture
def owner_id():
    """An owner id for tests that create jobs directly via the repo — any
    UUID works now (ownership is a bare cluster UID, no FK)."""
    return uuid.uuid4()
