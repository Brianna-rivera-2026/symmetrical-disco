import logging
import signal
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

import redis
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.logging import bind_log_context, bind_static_log_context
from app.core.redis import create_redis_client
from app.jobs.handlers import JobCancelled
from app.jobs.registry import run_handler
from app.queue.consumer import CONSUMER_NAME, ack, ensure_group, read_priority
from app.retry import schedule_retry_or_fail
from app.schemas.enums import JobType
from app.schemas.payloads import validate_payload
from app.worker.context import PgJobContext
from app.worker.timeout import HandlerTimeout, run_with_timeout

log = logging.getLogger("app.worker")


@dataclass
class Outcome:
    ack: bool
    recycle: bool
    label: str


def process_job(
    session: Session,
    client: redis.Redis,
    settings: Settings,
    job_id: UUID,
    session_factory: Callable[[], Session] | None = None,
) -> Outcome:
    if not repo.claim_job(session, job_id):
        log.info("job.skipped", extra={"reason": "not_claimable"})
        return Outcome(ack=True, recycle=False, label="skipped")

    job = repo.get_job(session, job_id)
    is_batch = job.type == JobType.batch
    if is_batch:
        repo.init_progress(session, job.id)
    ctx = PgJobContext(job.id, session_factory, settings.cancel_poll_interval_s)
    try:
        payload = validate_payload(job.type, job.payload)
        result = run_with_timeout(
            lambda: run_handler(job.type, payload, ctx), settings.job_handler_timeout_s
        )
    except JobCancelled as cancelled:
        won = repo.cancel_job(session, job.id, cancelled.summary)
        log.info("job.cancelled", extra={"won": won})
        return Outcome(ack=won, recycle=False, label="cancelled")
    except HandlerTimeout:
        won = schedule_retry_or_fail(
            session,
            client,
            settings,
            job,
            {
                "type": "HandlerTimeout",
                "message": f">{settings.job_handler_timeout_s}s",
            },
        )
        log.warning("job.timeout", extra={"won": won})
        return Outcome(ack=won, recycle=True, label="timeout")
    except Exception as exc:  # noqa: BLE001 — any handler/validation error is retryable
        won = schedule_retry_or_fail(
            session,
            client,
            settings,
            job,
            {"type": type(exc).__name__, "message": str(exc)},
        )
        log.info(
            "job.retry_scheduled", extra={"error_type": type(exc).__name__, "won": won}
        )
        return Outcome(ack=won, recycle=False, label="retried")

    won = repo.complete_job(session, job.id, result, progress=100 if is_batch else None)
    if not won:
        log.critical("job.complete_lost_to_reaper")
        return Outcome(ack=False, recycle=False, label="lost")
    log.info("job.completed")
    return Outcome(ack=True, recycle=False, label="completed")


def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> int:
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    client = create_redis_client(settings.redis_url)
    for stream in settings.ordered_streams:
        ensure_group(client, stream, settings.consumer_group)

    shutting_down = {"flag": False}

    def _request_stop(*_):
        shutting_down["flag"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    bind_static_log_context(consumer=CONSUMER_NAME)
    log.info(
        "worker.started",
        extra={"streams": settings.ordered_streams, "group": settings.consumer_group},
    )

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

    timeouts = 0
    exit_code = 0
    while not _should_stop():
        batch = read_priority(
            client,
            settings.ordered_streams,
            settings.consumer_group,
            CONSUMER_NAME,
            settings.block_ms,
        )
        for stream, message_id, fields in batch:
            job_id = UUID(fields["job_id"])
            with bind_log_context(
                job_id=str(job_id), message_id=message_id, stream=stream
            ):
                log.info("job.received")
                with session_factory() as session:
                    outcome = process_job(
                        session, client, settings, job_id, session_factory
                    )
                if outcome.ack:
                    ack(client, stream, settings.consumer_group, message_id)
                if outcome.recycle:
                    timeouts += 1
                    if timeouts >= settings.max_handler_timeouts_before_recycle:
                        log.warning("worker.recycling", extra={"timeouts": timeouts})
                        exit_code = 1
                        break
        if exit_code:
            break

    log.info("worker.stopped", extra={"exit_code": exit_code})
    client.close()
    engine.dispose()
    return exit_code
