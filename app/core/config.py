from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    jobs_stream: str = "jobs:stream"
    consumer_group: str = "workers"
    block_ms: int = 5000
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
