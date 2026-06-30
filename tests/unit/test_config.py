from app.core.config import Settings


def test_scheduling_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.delayed_zset == "jobs:delayed"
    assert s.ticker_interval_s == 1.0
    assert s.ticker_batch_size == 100
    assert s.reconcile_interval_s == 60.0
    assert s.reconcile_grace_s == 10.0
    assert s.reconcile_batch_size == 500


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
