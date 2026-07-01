import signal
from collections.abc import Callable
from uuid import UUID

import structlog
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.redis import create_redis_client
from app.jobs.registry import run_handler
from app.queue.consumer import CONSUMER_NAME, ack, ensure_group, read_priority
from app.schemas.payloads import validate_payload

log = structlog.get_logger("worker")


def process_job(session: Session, job_id: UUID) -> None:
    if not repo.claim_job(session, job_id):
        log.info("job.skipped", reason="not_pending")
        return

    job = repo.get_job(session, job_id)
    try:
        payload = validate_payload(job.type, job.payload)
        result = run_handler(job.type, payload)
    except Exception as exc:  # noqa: BLE001 — any handler/validation error fails the job
        repo.fail_job(
            session, job_id, {"type": type(exc).__name__, "message": str(exc)}
        )
        log.info("job.failed", error_type=type(exc).__name__)
        return

    repo.complete_job(session, job_id, result)
    log.info("job.completed")


def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> None:
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

    structlog.contextvars.bind_contextvars(consumer=CONSUMER_NAME)
    log.info(
        "worker.started",
        streams=settings.ordered_streams,
        group=settings.consumer_group,
    )

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

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
            with structlog.contextvars.bound_contextvars(
                job_id=str(job_id), message_id=message_id, stream=stream
            ):
                log.info("job.received")
                with session_factory() as session:
                    process_job(session, job_id)
                # Ack on the message's own stream, after the PG commit (at-least-once).
                ack(client, stream, settings.consumer_group, message_id)

    log.info("worker.stopped")
    client.close()
    engine.dispose()
