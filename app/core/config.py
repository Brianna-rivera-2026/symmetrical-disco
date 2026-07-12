from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.schemas.enums import JobPriority


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
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
    job_handler_timeout_s: float = 45.0
    visibility_timeout_s: float = 60.0
    reaper_interval_s: float = 30.0
    reaper_batch_size: int = 100
    max_attempts: int = 4
    retry_backoff_schedule: list[int] = [0, 30, 120]
    cancel_poll_interval_s: float = 2.0
    worker_concurrency: int = 10
    db_pool_size: int = 5
    db_disable_prepared_statements: bool = False
    worker_max_rss_mb: int | None = None
    auth_cache_ttl_s: float = 60.0
    otel_enabled: bool = False
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    health_port: int | None = None
    api_user_keys_file: str = "/run/secrets/api_user_keys"
    rate_limit_enabled: bool = True
    submit_rate_limit_per_min: int = 20
    control_rate_limit_per_min: int = 30
    read_rate_limit_per_min: int = 120
    stats_rate_limit_per_min: int = 30
    forwarded_allow_ips: str = "127.0.0.1"
    webhook_allowed_hosts: list[str] = []
    email_allowed_domains: list[str] = []
    max_request_body_bytes: int = 262144

    @model_validator(mode="after")
    def _check_timeout_invariant(self) -> "Settings":
        if self.job_handler_timeout_s >= self.visibility_timeout_s:
            raise ValueError(
                "job_handler_timeout_s must be < visibility_timeout_s "
                f"(got {self.job_handler_timeout_s} >= {self.visibility_timeout_s})"
            )
        return self

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
