from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.asyncio as redis

from app.core.config import Settings
from app.queue.consumer import REAPER_NAME
from app.observability import (
    check_readiness,
    gather_stats,
    live_worker_count,
    pending_age_seconds,
    zero_fill_status_counts,
)
from app.schemas.api import StatsResponse, StreamStat
from app.schemas.enums import JobStatus


def _settings(**overrides) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://x/y",
        redis_url="redis://x",
        **overrides,
    )


class _FakePipeline:
    """Fakes redis.asyncio's pipeline async context manager, queuing commands
    and returning a preconfigured results list on execute()."""

    def __init__(self, results: list):
        self._results = results

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def xinfo_groups(self, *a, **kw):
        pass

    def xinfo_consumers(self, *a, **kw):
        pass

    def zcard(self, *a, **kw):
        pass

    async def execute(self, raise_on_error=False):
        return self._results


def _fake_client(results: list) -> MagicMock:
    client = MagicMock()
    client.pipeline = MagicMock(return_value=_FakePipeline(results))
    return client


def _fake_session(status_rows: list[tuple], min_created_at) -> AsyncMock:
    status_result = MagicMock()
    status_result.all = MagicMock(return_value=status_rows)

    min_result = MagicMock()
    min_result.scalar_one = MagicMock(return_value=min_created_at)

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=[status_result, min_result])
    return session


def test_live_worker_count_uses_min_idle_across_streams():
    # Worker "w1" is saturated on high (idle 0) but looks stale on low (idle 99999).
    # It MUST count as live because its minimum idle is under the cutoff.
    high = [{"name": "w1", "idle": 0}]
    normal = []
    low = [{"name": "w1", "idle": 99_999}]
    assert live_worker_count([high, normal, low], cutoff_ms=60_000) == 1


def test_live_worker_count_excludes_stale_and_dedups():
    high = [{"name": "w1", "idle": 500}, {"name": "dead", "idle": 120_000}]
    normal = [{"name": "w1", "idle": 800}]  # same worker seen twice -> one
    low = []
    assert live_worker_count([high, normal, low], cutoff_ms=60_000) == 1


def test_zero_fill_status_counts_fills_all_six():
    rows = [(JobStatus.pending, 3), (JobStatus.completed, 10)]
    counts = zero_fill_status_counts(rows)
    assert set(counts) == {s.value for s in JobStatus}
    assert counts["pending"] == 3
    assert counts["completed"] == 10
    assert counts["failed"] == 0


def test_pending_age_seconds_none_when_no_pending():
    assert pending_age_seconds(None, datetime.now(timezone.utc)) is None


def test_pending_age_seconds_computes_delta():
    now = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    created = now - timedelta(seconds=42)
    assert pending_age_seconds(created, now) == 42.0


@pytest.mark.asyncio
async def test_check_readiness_postgres_ok_redis_ok():
    session = AsyncMock()
    session.execute = AsyncMock(return_value=AsyncMock())

    redis_client = AsyncMock()
    redis_client.ping = AsyncMock()

    checks = await check_readiness(session, redis_client)
    assert checks["postgres"] == "ok"
    assert checks["redis"] == "ok"


@pytest.mark.asyncio
async def test_check_readiness_postgres_error():
    from sqlalchemy.exc import SQLAlchemyError

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=SQLAlchemyError("db error"))

    redis_client = AsyncMock()
    redis_client.ping = AsyncMock()

    checks = await check_readiness(session, redis_client)
    assert checks["postgres"] == "error"
    assert checks["redis"] == "ok"


@pytest.mark.asyncio
async def test_check_readiness_redis_error():
    import redis.asyncio as redis

    session = AsyncMock()
    session.execute = AsyncMock()

    redis_client = AsyncMock()
    redis_client.ping = AsyncMock(side_effect=redis.RedisError("redis error"))

    checks = await check_readiness(session, redis_client)
    assert checks["postgres"] == "ok"
    assert checks["redis"] == "error"


@pytest.mark.asyncio
async def test_gather_stats_composes_full_response():
    settings = _settings()

    groups_high = [{"name": "workers", "lag": 5, "pending": 2}]
    groups_normal = [{"name": "workers", "lag": 1, "pending": 0}]
    groups_low = [{"name": "workers", "lag": 0, "pending": 0}]

    consumers_high = [
        {"name": "w1", "idle": 100},
        {"name": REAPER_NAME, "idle": 0},
    ]
    consumers_normal = [{"name": "w1", "idle": 500}]
    consumers_low = []

    results = [
        groups_high,
        groups_normal,
        groups_low,
        consumers_high,
        consumers_normal,
        consumers_low,
        7,  # zcard (scheduled)
    ]
    client = _fake_client(results)

    now = datetime.now(timezone.utc)
    min_created = now - timedelta(seconds=30)
    session = _fake_session(
        status_rows=[(JobStatus.pending, 2), (JobStatus.completed, 5)],
        min_created_at=min_created,
    )

    stats = await gather_stats(session, client, settings)

    assert isinstance(stats, StatsResponse)
    assert stats.queue.streams["high"] == StreamStat(depth=5, in_flight=2)
    assert stats.queue.streams["normal"] == StreamStat(depth=1, in_flight=0)
    assert stats.queue.streams["low"] == StreamStat(depth=0, in_flight=0)
    assert stats.queue.scheduled == 7
    # REAPER_NAME ("reaper") is excluded from worker liveness counting
    assert stats.queue.workers == 1

    assert stats.jobs.by_status["pending"] == 2
    assert stats.jobs.by_status["completed"] == 5
    assert stats.jobs.by_status["failed"] == 0
    assert stats.jobs.oldest_pending_age_seconds == pytest.approx(30.0, abs=1.0)


@pytest.mark.asyncio
async def test_gather_stats_tolerates_missing_stream_groups():
    settings = _settings()

    # "normal" and "low" streams have no consumer group yet (NOGROUP errors);
    # only "high" has a real group. No jobs are pending.
    nogroup_err = redis.ResponseError("NOGROUP No such consumer group 'workers'")

    groups_high = [{"name": "workers", "lag": 3, "pending": 1}]

    results = [
        groups_high,
        nogroup_err,
        nogroup_err,
        [{"name": "w1", "idle": 10}],
        nogroup_err,
        nogroup_err,
        0,  # zcard (scheduled)
    ]
    client = _fake_client(results)

    session = _fake_session(status_rows=[], min_created_at=None)

    stats = await gather_stats(session, client, settings)

    assert stats.queue.streams["high"] == StreamStat(depth=3, in_flight=1)
    assert stats.queue.streams["normal"] == StreamStat(depth=0, in_flight=0)
    assert stats.queue.streams["low"] == StreamStat(depth=0, in_flight=0)
    assert stats.queue.scheduled == 0
    assert stats.queue.workers == 1

    assert stats.jobs.by_status == {s.value: 0 for s in JobStatus}
    assert stats.jobs.oldest_pending_age_seconds is None


