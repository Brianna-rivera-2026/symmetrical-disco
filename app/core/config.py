from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    jobs_stream: str = "jobs:stream"
    consumer_group: str = "workers"
    block_ms: int = 5000
    delayed_zset: str = "jobs:delayed"
    ticker_interval_s: float = 1.0
    ticker_batch_size: int = 100
    reconcile_interval_s: float = 60.0
    reconcile_grace_s: float = 10.0
    reconcile_batch_size: int = 500
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
