import logging
import signal
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from uuid import UUID

import redis
from opentelemetry import metrics, trace
from opentelemetry.metrics import Observation
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app import repository as repo
from app.core import metrics as app_metrics
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.redis import create_redis_client
from app.observability import zero_fill_status_counts
from app.queue import delayed
from app.queue.consumer import REAPER_NAME, ensure_group
from app.queue.producer import enqueue, message_fields
from app.retry import schedule_retry_or_fail
from app.schemas.enums import JobStatus

log = logging.getLogger("app.ticker")

_tracer = trace.get_tracer("app.ticker")


def promote_due(session: Session, client: redis.Redis, settings: Settings) -> int:
    now_epoch = time.time()
    ids = delayed.due_job_ids(
        client, settings.delayed_zset, now_epoch, settings.ticker_batch_size
    )
    if not ids:
        return 0
    with _tracer.start_as_current_span(
        "ticker.promote", attributes={"jobs.count": len(ids)}
    ):
        info = repo.get_promotion_info(session, [UUID(i) for i in ids])
        routed: list[tuple[str, dict]] = []
        for i in ids:
            meta = info.get(UUID(i))
            if meta is None:
                # No row (cancelled/deleted): drop it — do not enqueue —
                # but it is still ZREM'd below so it can't re-accumulate.
                continue
            priority, carrier = meta
            stream = settings.stream_for_priority(priority)
            routed.append((stream, message_fields(stream, i, carrier)))
        delayed.promote(client, settings.delayed_zset, routed, ids)
        repo.promote_scheduled_to_pending(session, [UUID(i) for i in ids])
        app_metrics.ticker_promoted.add(len(routed))
        log.info("ticker.promoted", extra={"enqueued": len(routed), "pulled": len(ids)})
    return len(ids)


def reconcile_orphans(session: Session, client: redis.Redis, settings: Settings) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.reconcile_grace_s)
    total = 0
    while True:
        rows = repo.list_unsynced(
            session, older_than=cutoff, limit=settings.reconcile_batch_size
        )
        if not rows:
            break
        with _tracer.start_as_current_span(
            "ticker.reconcile", attributes={"jobs.count": len(rows)}
        ):
            for job in rows:
                if job.status is JobStatus.scheduled:
                    if job.scheduled_at is None:
                        log.warning(
                            "ticker.reconcile_skipped_null_scheduled_at",
                            extra={"job_id": str(job.id)},
                        )
                        continue
                    delayed.schedule(
                        client,
                        settings.delayed_zset,
                        str(job.id),
                        job.scheduled_at.timestamp(),
                    )
                else:
                    enqueue(
                        client,
                        settings.stream_for_priority(job.priority),
                        str(job.id),
                        carrier=job.trace_context,
                    )
                repo.mark_synced(session, job.id)
                total += 1
        if len(rows) < settings.reconcile_batch_size:
            break
    if total:
        app_metrics.ticker_reconciled.add(total)
    return total


def _reap_one(session, client, settings, stream, message_id, job_id) -> None:
    job = repo.get_job(session, job_id)
    if job is not None:
        if job.status in (JobStatus.completed, JobStatus.failed, JobStatus.cancelled):
            pass  # ghost: terminal status, no Redis handoff needed
        elif job.status is JobStatus.processing:
            schedule_retry_or_fail(
                session,
                client,
                settings,
                job,
                {"type": "WorkerLost", "message": "reclaimed by reaper"},
                carrier=job.trace_context,
            )
        elif not job.is_synced_to_redis:
            # Worker won the guard then died before the Redis handoff → finish it
            # inline (immediate recovery). Do NOT touch attempts / re-decide.
            if job.status is JobStatus.scheduled and job.scheduled_at is not None:
                delayed.schedule(
                    client,
                    settings.delayed_zset,
                    str(job.id),
                    job.scheduled_at.timestamp(),
                )
            else:
                enqueue(
                    client,
                    settings.stream_for_priority(job.priority),
                    str(job.id),
                    carrier=job.trace_context,
                )
            repo.mark_synced(session, job.id)
        # else: pending/scheduled + synced=True → fresh message already live
    # Always clear the reclaimed entry from the PEL.
    client.xack(stream, settings.consumer_group, message_id)


def reap_stale(session: Session, client: redis.Redis, settings: Settings) -> int:
    min_idle = int(settings.visibility_timeout_s * 1000)
    handled = 0
    for stream in settings.ordered_streams:
        # XAUTOCLAIM raises NOGROUP against a stream/group that doesn't exist
        # yet; guard it the same way every other consumer-group reader in this
        # codebase does (main.py, worker/runner.py, run_forever's own startup).
        ensure_group(client, stream, settings.consumer_group)
        cursor = "0-0"
        while True:
            resp = client.xautoclaim(
                name=stream,
                groupname=settings.consumer_group,
                consumername=REAPER_NAME,
                min_idle_time=min_idle,
                start_id=cursor,
                count=settings.reaper_batch_size,
            )
            cursor, messages = resp[0], resp[1]
            if messages:
                with _tracer.start_as_current_span(
                    "ticker.reap", attributes={"jobs.count": len(messages)}
                ):
                    for message_id, fields in messages:
                        _reap_one(
                            session,
                            client,
                            settings,
                            stream,
                            message_id,
                            UUID(fields["job_id"]),
                        )
                        handled += 1
            if cursor == "0-0":
                break
    if handled:
        app_metrics.ticker_reaped.add(handled)
        log.info("ticker.reaped", extra={"count": handled})
    return handled


def queue_depth_observations(
    client: redis.Redis, settings: Settings
) -> list[Observation]:
    """Consumer-group lag per stream (same source as /stats). Errors yield no
    observations — a metrics callback must never raise."""
    out: list[Observation] = []
    try:
        for priority, stream in settings.priority_streams:
            try:
                groups = client.xinfo_groups(stream)
            except redis.ResponseError:
                continue  # stream/group not created yet
            group = next(
                (g for g in groups if g.get("name") == settings.consumer_group),
                None,
            )
            if group is not None and group.get("lag") is not None:
                out.append(
                    Observation(int(group["lag"]), {"stream": priority.value})
                )
    except redis.RedisError:
        return []
    return out


def queue_scheduled_observations(
    client: redis.Redis, settings: Settings
) -> list[Observation]:
    try:
        return [Observation(int(client.zcard(settings.delayed_zset)))]
    except redis.RedisError:
        return []


def job_status_observations(
    session_factory: Callable[[], Session], settings: Settings
) -> list[Observation]:
    """Postgres queue saturation: job counts by status, zero-filled so every
    status is always reported. Errors yield no observations — a metrics
    callback must never raise."""
    try:
        with session_factory() as session:
            rows = repo.count_by_status(session)
    except SQLAlchemyError:
        return []
    counts = zero_fill_status_counts(rows)
    return [Observation(count, {"status": status}) for status, count in counts.items()]


def register_ticker_gauges(
    client: redis.Redis, session_factory: Callable[[], Session], settings: Settings
) -> None:
    meter = metrics.get_meter("app.ticker")
    meter.create_observable_gauge(
        "queue.depth",
        callbacks=[lambda options: queue_depth_observations(client, settings)],
        description="Consumer-group lag per priority stream",
    )
    meter.create_observable_gauge(
        "queue.scheduled",
        callbacks=[lambda options: queue_scheduled_observations(client, settings)],
        description="Jobs waiting in the delayed zset",
    )
    meter.create_observable_gauge(
        "jobs.saturation",
        callbacks=[lambda options: job_status_observations(session_factory, settings)],
        description="Job counts by status (Postgres queue saturation)",
    )


def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> None:
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    client = create_redis_client(settings.redis_url)
    # Make sure the consumer group exists before we XADD, so workers created
    # later (group id "$") don't miss jobs the ticker has already promoted.
    for stream in settings.ordered_streams:
        ensure_group(client, stream, settings.consumer_group)

    if settings.otel_enabled:
        register_ticker_gauges(client, session_factory, settings)

    shutting_down = {"flag": False}

    def _request_stop(*_):
        shutting_down["flag"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

    log.info(
        "ticker.started",
        extra={"zset": settings.delayed_zset, "streams": settings.ordered_streams},
    )
    last_reconcile = 0.0
    last_reap = 0.0

    while not _should_stop():
        try:
            with session_factory() as session:
                promoted = promote_due(session, client, settings)
            now = time.time()
            if now - last_reconcile >= settings.reconcile_interval_s:
                with session_factory() as session:
                    recovered = reconcile_orphans(session, client, settings)
                last_reconcile = now
                if recovered:
                    log.info("ticker.reconciled", extra={"count": recovered})
            if now - last_reap >= settings.reaper_interval_s:
                with session_factory() as session:
                    reap_stale(session, client, settings)
                last_reap = now
            # Full batch → drain immediately without sleeping
            if promoted >= settings.ticker_batch_size:
                continue
            time.sleep(settings.ticker_interval_s)
        except Exception:  # noqa: BLE001
            log.exception("ticker.tick_failed")
            time.sleep(settings.ticker_interval_s)

    log.info("ticker.stopped")
    client.close()
    engine.dispose()
