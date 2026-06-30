import signal
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from uuid import UUID

import redis
import structlog
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.redis import create_redis_client
from app.queue import delayed
from app.queue.consumer import ensure_group
from app.queue.producer import enqueue
from app.schemas.enums import JobStatus

log = structlog.get_logger("ticker")


def promote_due(session: Session, client: redis.Redis, settings: Settings) -> int:
    now_epoch = time.time()
    ids = delayed.due_job_ids(
        client, settings.delayed_zset, now_epoch, settings.ticker_batch_size
    )
    if not ids:
        return 0
    delayed.promote(client, settings.jobs_stream, settings.delayed_zset, ids)
    repo.promote_scheduled_to_pending(session, [UUID(i) for i in ids])
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
        for job in rows:
            if job.status is JobStatus.scheduled:
                if job.scheduled_at is None:
                    log.warning(
                        "ticker.reconcile_skipped_null_scheduled_at", job_id=str(job.id)
                    )
                    continue
                delayed.schedule(
                    client,
                    settings.delayed_zset,
                    str(job.id),
                    job.scheduled_at.timestamp(),
                )
            else:
                enqueue(client, settings.jobs_stream, str(job.id))
            repo.mark_synced(session, job.id)
            total += 1
        if len(rows) < settings.reconcile_batch_size:
            break
    return total


def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> None:
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    client = create_redis_client(settings.redis_url)
    # Make sure the consumer group exists before we XADD, so workers created
    # later (group id "$") don't miss jobs the ticker has already promoted.
    ensure_group(client, settings.jobs_stream, settings.consumer_group)

    shutting_down = {"flag": False}

    def _request_stop(*_):
        shutting_down["flag"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

    log.info("ticker.started", zset=settings.delayed_zset, stream=settings.jobs_stream)
    last_reconcile = 0.0

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
                    log.info("ticker.reconciled", count=recovered)
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
