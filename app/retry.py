from datetime import datetime, timedelta, timezone

import redis
import structlog
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.models.job import Job
from app.queue import delayed
from app.queue.producer import enqueue

log = structlog.get_logger("retry")


def backoff_delay(attempts: int, schedule: list[int]) -> int:
    """Delay (seconds) before the retry that follows `attempts` completed attempts."""
    idx = min(attempts - 1, len(schedule) - 1)
    return schedule[idx]


def schedule_retry_or_fail(
    session: Session,
    client: redis.Redis,
    settings: Settings,
    job: Job,
    error: dict,
) -> bool:
    """Retry with backoff, or permanently fail at max_attempts. Returns True iff
    this actor won the guarded transition. Does not XACK."""
    n = job.attempts + 1  # the attempt that just ended
    if n >= job.max_attempts:
        won = repo.fail_job(session, job.id, error)
        log.info("retry.failed_permanent", job_id=str(job.id), attempts=n, won=won)
        return won

    delay = backoff_delay(n, settings.retry_backoff_schedule)
    if delay <= 0:
        won = repo.retry_to_pending(session, job.id)
        if won:
            enqueue(client, settings.stream_for_priority(job.priority), str(job.id))
            repo.mark_synced(session, job.id)
        log.info("retry.immediate", job_id=str(job.id), attempts=n, won=won)
        return won

    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
    won = repo.retry_to_scheduled(session, job.id, scheduled_at)
    if won:
        delayed.schedule(
            client, settings.delayed_zset, str(job.id), scheduled_at.timestamp()
        )
        repo.mark_synced(session, job.id)
    log.info("retry.delayed", job_id=str(job.id), attempts=n, delay=delay, won=won)
    return won
