import time
from uuid import UUID

import redis
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.queue import delayed


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
