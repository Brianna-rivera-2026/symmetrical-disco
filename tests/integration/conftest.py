import asyncio
import sys

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.db import make_session_factory as _make_session_factory
from app.core.redis import create_redis_client
from app.main import create_app
from app.users.keys import hash_key

# Fix for Windows asyncio event loop policy with psycopg
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

DEFAULT_TEST_KEY = "default-user-key"
SECOND_TEST_KEY = "second-user-key"


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
            await conn.execute(text("TRUNCATE TABLE jobs, users"))


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7") as rc:
        yield rc


@pytest.fixture
async def redis_client(redis_container):
    url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    client = create_redis_client(url)
    yield client
    try:
        await client.flushdb()
    except RuntimeError:
        # Event loop may be closed on Windows/pytest-asyncio; ignore cleanup errors
        pass
    try:
        await client.aclose()
    except RuntimeError:
        # Event loop may be closed; graceful cleanup attempted
        pass


@pytest.fixture(scope="session")
def test_settings(database_url, redis_container) -> Settings:
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    return Settings(database_url=database_url, redis_url=redis_url)


@pytest.fixture
async def client(pg_engine, test_settings):
    factory = _make_session_factory(pg_engine)
    async with factory() as session:
        await repo.upsert_user(session, "default-user", hash_key(DEFAULT_TEST_KEY))
        await session.commit()
    app = create_app(test_settings)
    with TestClient(app) as c:
        c.headers.update({"X-API-Key": DEFAULT_TEST_KEY})
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs, users"))
    real_redis = create_redis_client(test_settings.redis_url)
    try:
        await real_redis.flushdb()
    finally:
        await real_redis.aclose()


@pytest.fixture
async def default_user_id(client, pg_engine):
    """UUID of the default test user seeded by the `client` fixture, for tests
    that create jobs directly via `repo.create_job(db_session, ...)` and need
    them owned by the same user the `client` fixture authenticates as."""
    factory = _make_session_factory(pg_engine)
    async with factory() as session:
        return (await repo.get_user_by_key_hash(session, hash_key(DEFAULT_TEST_KEY))).id


@pytest.fixture
async def unauth_client(pg_engine, test_settings):
    """TestClient with no default X-API-Key header."""
    app = create_app(test_settings)
    with TestClient(app) as c:
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs, users"))


@pytest.fixture
async def second_user(pg_engine):
    """Headers for a second, independent user."""
    factory = _make_session_factory(pg_engine)
    async with factory() as session:
        await repo.upsert_user(session, "second-user", hash_key(SECOND_TEST_KEY))
        await session.commit()
    return {"X-API-Key": SECOND_TEST_KEY}


@pytest.fixture
async def owner_id(pg_engine):
    """A persisted user id for tests that create jobs directly via the repo."""
    factory = _make_session_factory(pg_engine)
    async with factory() as session:
        uid = await repo.upsert_user(session, "job-owner", hash_key("job-owner-key"))
        await session.commit()
    return uid
