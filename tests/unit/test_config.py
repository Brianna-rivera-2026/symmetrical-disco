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
    assert s.consumer_group == "workers"
    assert s.block_ms == 5000
    assert s.log_level == "INFO"


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@h/db")
    monkeypatch.setenv("REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("BLOCK_MS", "1000")
    s = Settings()
    assert s.block_ms == 1000


def test_job_priority_values():
    from app.schemas.enums import JobPriority

    assert [p.value for p in JobPriority] == ["high", "normal", "low"]


def test_priority_stream_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.stream_high == "jobs:stream:high"
    assert s.stream_normal == "jobs:stream:normal"
    assert s.stream_low == "jobs:stream:low"
    assert s.ordered_streams == [
        "jobs:stream:high",
        "jobs:stream:normal",
        "jobs:stream:low",
    ]


def test_priority_streams_ordering():
    from app.schemas.enums import JobPriority

    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.priority_streams == [
        (JobPriority.high, "jobs:stream:high"),
        (JobPriority.normal, "jobs:stream:normal"),
        (JobPriority.low, "jobs:stream:low"),
    ]


def test_stream_for_priority_maps_each_level():
    from app.schemas.enums import JobPriority

    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.stream_for_priority(JobPriority.high) == "jobs:stream:high"
    assert s.stream_for_priority(JobPriority.normal) == "jobs:stream:normal"
    assert s.stream_for_priority(JobPriority.low) == "jobs:stream:low"


def test_failure_handling_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.job_handler_timeout_s == 45.0
    assert s.visibility_timeout_s == 60.0
    assert s.reaper_interval_s == 30.0
    assert s.reaper_batch_size == 100
    assert s.max_attempts == 4
    assert s.retry_backoff_schedule == [0, 30, 120]


def test_timeout_invariant_rejects_handler_ge_visibility():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql+psycopg://u:p@h/db",
            redis_url="redis://h:6379/0",
            job_handler_timeout_s=60.0,
            visibility_timeout_s=60.0,
        )


def test_worker_concurrency_default_and_env(monkeypatch):
    s = Settings(database_url="postgresql+psycopg://x/y", redis_url="redis://x")
    assert s.worker_concurrency == 10
    monkeypatch.setenv("WORKER_CONCURRENCY", "3")
    s2 = Settings(database_url="postgresql+psycopg://x/y", redis_url="redis://x")
    assert s2.worker_concurrency == 3


def test_new_deployment_settings_defaults():
    settings = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h"
    )
    assert settings.db_pool_size == 5
    assert settings.db_disable_prepared_statements is False
    assert settings.worker_max_rss_mb is None
