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
def pg_engine(database_url):
    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "alembic")
    cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(cfg, "head")
    engine = make_engine(database_url)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(pg_engine):
    factory = make_session_factory(pg_engine)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        with pg_engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE jobs, users"))


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7") as rc:
        yield rc


@pytest.fixture
def redis_client(redis_container):
    url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    client = create_redis_client(url)
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def test_settings(database_url, redis_container) -> Settings:
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    return Settings(database_url=database_url, redis_url=redis_url)


@pytest.fixture
def client(pg_engine, test_settings):
    factory = _make_session_factory(pg_engine)
    with factory() as session:
        repo.upsert_user(session, "default-user", hash_key(DEFAULT_TEST_KEY))
        session.commit()
    app = create_app(test_settings)
    with TestClient(app) as c:
        c.headers.update({"X-API-Key": DEFAULT_TEST_KEY})
        yield c
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE jobs, users"))
    # Some tests swap app.state.redis for a broken client to simulate an outage;
    # flush the real backing instance directly so teardown never depends on
    # whatever connection a test left in place.
    real_redis = create_redis_client(test_settings.redis_url)
    try:
        real_redis.flushdb()
    finally:
        real_redis.close()


@pytest.fixture
def default_user_id(client, pg_engine):
    """UUID of the default test user seeded by the `client` fixture, for tests
    that create jobs directly via `repo.create_job(db_session, ...)` and need
    them owned by the same user the `client` fixture authenticates as."""
    factory = _make_session_factory(pg_engine)
    with factory() as session:
        return repo.get_user_by_key_hash(session, hash_key(DEFAULT_TEST_KEY)).id


@pytest.fixture
def unauth_client(pg_engine, test_settings):
    """TestClient with no default X-API-Key header."""
    app = create_app(test_settings)
    with TestClient(app) as c:
        yield c
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE jobs, users"))


@pytest.fixture
def second_user(pg_engine):
    """Headers for a second, independent user."""
    factory = _make_session_factory(pg_engine)
    with factory() as session:
        repo.upsert_user(session, "second-user", hash_key(SECOND_TEST_KEY))
        session.commit()
    return {"X-API-Key": SECOND_TEST_KEY}
