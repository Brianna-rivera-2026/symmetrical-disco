import time
from datetime import datetime, timedelta, timezone
from uuid import UUID

import redis
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.queue import delayed
from app.queue.producer import enqueue
from app.schemas.enums import JobStatus


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
