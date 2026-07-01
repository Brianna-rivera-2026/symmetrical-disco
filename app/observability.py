from datetime import datetime

import redis
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.schemas.enums import JobStatus


def live_worker_count(
    consumer_rows_per_stream: list[list[dict]], cutoff_ms: int
) -> int:
    """Count distinct consumers whose *minimum* idle across all streams is under
    cutoff_ms. A worker saturated on one stream looks stale on the others it did
    not read this round; the minimum is what reflects real liveness."""
    min_idle: dict[str, int] = {}
    for rows in consumer_rows_per_stream:
        for row in rows:
            name = row["name"]
            idle = int(row["idle"])
            if name not in min_idle or idle < min_idle[name]:
                min_idle[name] = idle
    return sum(1 for idle in min_idle.values() if idle < cutoff_ms)


def zero_fill_status_counts(rows: list[tuple]) -> dict[str, int]:
    """Turn a partial ``GROUP BY status`` result into a dict with every
    JobStatus value present (missing statuses -> 0)."""
    counts = {status.value: 0 for status in JobStatus}
    for status, count in rows:
        key = status.value if isinstance(status, JobStatus) else str(status)
        counts[key] = int(count)
    return counts


def pending_age_seconds(min_created_at: datetime | None, now: datetime) -> float | None:
    """Age in seconds of the oldest pending job, or None when none are pending."""
    if min_created_at is None:
        return None
    return (now - min_created_at).total_seconds()


def check_readiness(session: Session, client: redis.Redis) -> dict[str, str]:
    """Ping both backends independently; one failure never masks the other."""
    checks: dict[str, str] = {}
    try:
        session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except SQLAlchemyError:
        checks["postgres"] = "error"
    try:
        client.ping()
        checks["redis"] = "ok"
    except redis.RedisError:
        checks["redis"] = "error"
    return checks
