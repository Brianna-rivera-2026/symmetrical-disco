from app.core.config import Settings


def test_settings_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.jobs_stream == "jobs:stream"
    assert s.consumer_group == "workers"
    assert s.block_ms == 5000
    assert s.log_level == "INFO"


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@h/db")
    monkeypatch.setenv("REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("BLOCK_MS", "1000")
    s = Settings()
    assert s.block_ms == 1000
