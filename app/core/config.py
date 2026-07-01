from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

from app.schemas.enums import JobPriority


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    jobs_stream: str = "jobs:stream"
    stream_high: str = "jobs:stream:high"
    stream_normal: str = "jobs:stream:normal"
    stream_low: str = "jobs:stream:low"
    consumer_group: str = "workers"
    block_ms: int = 5000
    delayed_zset: str = "jobs:delayed"
    ticker_interval_s: float = 1.0
    ticker_batch_size: int = 100
    reconcile_interval_s: float = 60.0
    reconcile_grace_s: float = 10.0
    reconcile_batch_size: int = 500
    log_level: str = "INFO"

    @property
    def ordered_streams(self) -> list[str]:
        return [self.stream_high, self.stream_normal, self.stream_low]

    @property
    def priority_streams(self) -> list[tuple[JobPriority, str]]:
        return [
            (JobPriority.high, self.stream_high),
            (JobPriority.normal, self.stream_normal),
            (JobPriority.low, self.stream_low),
        ]

    def stream_for_priority(self, priority: JobPriority) -> str:
        return dict(self.priority_streams)[priority]


@lru_cache
def get_settings() -> Settings:
    return Settings()
