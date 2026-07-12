from datetime import datetime, timezone

import redis.asyncio as redis
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.models.job import Job
from app.queue.consumer import REAPER_NAME
from app.schemas.api import JobStats, QueueStats, StatsResponse, StreamStat
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


async def check_readiness(session: AsyncSession, client: redis.Redis) -> dict[str, str]:
    """Ping both backends independently; one failure never masks the other."""
    checks: dict[str, str] = {}
    try:
        await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except SQLAlchemyError:
        checks["postgres"] = "error"
    try:
        await client.ping()
        checks["redis"] = "ok"
    except redis.RedisError:
        checks["redis"] = "error"
    return checks


def _tolerate_nogroup(result, empty):
    """In a pipeline run with raise_on_error=False, a missing stream/group comes
    back as a NOGROUP ResponseError -> treat as empty. Any other error is real
    and re-raised (caller turns it into a 503)."""
    if isinstance(result, redis.ResponseError) and "NOGROUP" in str(result):
        return empty
    if isinstance(result, Exception):
        raise result
    return result


async def gather_stats(
    session: AsyncSession, client: redis.Redis, settings: Settings
) -> StatsResponse:
    stream_names = [stream for _, stream in settings.priority_streams]
    n = len(stream_names)

    async with client.pipeline(transaction=False) as pipe:
        for stream in stream_names:
            pipe.xinfo_groups(stream)
        for stream in stream_names:
            pipe.xinfo_consumers(stream, settings.consumer_group)
        pipe.zcard(settings.delayed_zset)
        results = await pipe.execute(raise_on_error=False)

    groups = [_tolerate_nogroup(res, []) for res in results[:n]]
    # XAUTOCLAIM registers its consumer name in the group even when it claims
    # zero messages, so the ticker's reaper shows up here on every tick. It is
    # not a job-processing worker, so exclude it before counting live workers.
    consumers = [
        [row for row in _tolerate_nogroup(res, []) if row.get("name") != REAPER_NAME]
        for res in results[n : 2 * n]
    ]
    scheduled = int(_tolerate_nogroup(results[2 * n], 0))

    streams: dict[str, StreamStat] = {}
    for (priority, _), group_list in zip(settings.priority_streams, groups):
        group = next(
            (g for g in group_list if g.get("name") == settings.consumer_group),
            None,
        )
        if group is None:
            streams[priority.value] = StreamStat(depth=0, in_flight=0)
            continue
        lag = group.get("lag")
        # lag is only nil after entries are XDEL'd, which this system never does,
        # so in practice it is always an int; fall back to null defensively.
        streams[priority.value] = StreamStat(
            depth=int(lag) if lag is not None else None,
            in_flight=int(group["pending"]),
        )

    cutoff_ms = int(settings.visibility_timeout_s * 1000)
    queue = QueueStats(
        streams=streams,
        scheduled=scheduled,
        workers=live_worker_count(consumers, cutoff_ms),
    )

    status_rows = (
        await session.execute(select(Job.status, func.count()).group_by(Job.status))
    ).all()
    min_created = (
        await session.execute(
            select(func.min(Job.created_at)).where(Job.status == JobStatus.pending)
        )
    ).scalar_one()
    jobs = JobStats(
        by_status=zero_fill_status_counts(status_rows),
        oldest_pending_age_seconds=pending_age_seconds(
            min_created, datetime.now(timezone.utc)
        ),
    )

    return StatsResponse(queue=queue, jobs=jobs)
