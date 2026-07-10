from app.core.config import Settings
from app.core.healthcheck import (
    ticker_heartbeat_threshold_s,
    worker_heartbeat_threshold_s,
)


def _settings(**overrides) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://u:p@localhost/x",
        redis_url="redis://localhost:6379/0",
        **overrides,
    )


def test_worker_threshold():
    settings = _settings(block_ms=5000, job_handler_timeout_s=45.0)
    assert worker_heartbeat_threshold_s(settings) == 5.0 + 45.0 + 10.0


def test_ticker_threshold_floor():
    assert ticker_heartbeat_threshold_s(_settings(ticker_interval_s=1.0)) == 10.0


def test_ticker_threshold_scales():
    assert ticker_heartbeat_threshold_s(_settings(ticker_interval_s=4.0)) == 20.0
