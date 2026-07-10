import logging
from datetime import datetime, timedelta, timezone

import redis
from sqlalchemy.orm import Session

from app import repository as repo
from app.core import metrics as app_metrics
from app.core.config import Settings
from app.models.job import Job
from app.queue import delayed
from app.queue.producer import enqueue

log = logging.getLogger("app.retry")


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
    carrier: dict | None = None,
) -> bool:
    """Retry with backoff, or permanently fail at max_attempts. Returns True iff
    this actor won the guarded transition. Does not XACK."""
    n = job.attempts + 1  # the attempt that just ended
    if n >= job.max_attempts:
        won = repo.fail_job(session, job.id, error)
        if won:
            app_metrics.jobs_failed.add(
                1, {"type": job.type.value, "priority": job.priority.value}
            )
        log.info(
            "retry.failed_permanent",
            extra={"job_id": str(job.id), "attempts": n, "won": won},
        )
        return won

    delay = backoff_delay(n, settings.retry_backoff_schedule)
    if delay <= 0:
        won = repo.retry_to_pending(session, job.id)
        if won:
            enqueue(
                client,
                settings.stream_for_priority(job.priority),
                str(job.id),
                carrier=carrier,
            )
            repo.mark_synced(session, job.id)
        log.info(
            "retry.immediate", extra={"job_id": str(job.id), "attempts": n, "won": won}
        )
        return won

    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
    won = repo.retry_to_scheduled(session, job.id, scheduled_at)
    if won:
        delayed.schedule(
            client, settings.delayed_zset, str(job.id), scheduled_at.timestamp()
        )
        repo.mark_synced(session, job.id)
    log.info(
        "retry.delayed",
        extra={"job_id": str(job.id), "attempts": n, "delay": delay, "won": won},
    )
    return won
