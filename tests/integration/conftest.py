import pytest
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
            conn.execute(text("TRUNCATE TABLE jobs"))


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
    app = create_app(test_settings)
    with TestClient(app) as c:
        yield c
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE jobs"))
