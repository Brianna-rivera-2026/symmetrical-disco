import asyncio
import logging
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import UUID

import psutil
import redis.asyncio as redis
from opentelemetry import metrics, trace
from opentelemetry.metrics import Observation
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind, Status, StatusCode
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.healthcheck import Heartbeat, HealthServer, worker_heartbeat_threshold_s
from app.core import metrics as app_metrics
from app.core.logging import bind_log_context, bind_static_log_context
from app.core.redis import create_redis_client
from app.core.telemetry import configure_telemetry, shutdown_telemetry
from app.jobs.handlers import JobCancelled
from app.jobs.registry import run_handler
from app.queue.consumer import CONSUMER_NAME, ack, ensure_group, read_priority
from app.retry import schedule_retry_or_fail
from app.schemas.enums import JobType
from app.schemas.payloads import validate_payload
from app.worker.context import PgJobContext
from app.worker.timeout import HandlerTimeout, run_with_timeout

log = logging.getLogger("app.worker")
_tracer = trace.get_tracer("app.worker")

_process = psutil.Process()


@dataclass
class Outcome:
    ack: bool
    label: str


def cpu_utilization_observations() -> list[Observation]:
    """Ratio (0.0-1.0) of one CPU core the worker process has used since the
    last call. The first call after process start establishes a baseline and
    reports 0.0 — expected, since there is no prior interval to measure.
    Errors yield no observations — a metrics callback must never raise."""
    try:
        return [Observation(_process.cpu_percent(interval=None) / 100.0)]
    except psutil.Error:
        return []


def register_worker_resource_gauges() -> None:
    meter = metrics.get_meter("app.worker")
    meter.create_observable_gauge(
        "process.cpu.utilization",
        callbacks=[lambda options: cpu_utilization_observations()],
        unit="1",
        description="Worker process CPU utilization (ratio of one core)",
    )


def _record_outcome(job, label: str, started: float) -> None:
    attrs: dict[str, str] = {"outcome": label}
    if job is not None:
        attrs["type"] = job.type.value
        attrs["priority"] = job.priority.value
    app_metrics.jobs_processed.add(1, attrs)
    app_metrics.job_processing_duration.record(
        time.monotonic() - started,
        {k: v for k, v in attrs.items() if k in ("type", "outcome")},
    )


async def process_job(
    session: AsyncSession,
    client: redis.Redis,
    settings: Settings,
    job_id: UUID,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> Outcome:
    started = time.monotonic()
    if not await repo.claim_job(session, job_id):
        log.info("job.skipped", extra={"reason": "not_claimable"})
        _record_outcome(None, "skipped", started)
        return Outcome(ack=True, label="skipped")

    job = await repo.get_job(session, job_id)
    span = trace.get_current_span()
    span.set_attribute("job.type", job.type.value)
    span.set_attribute("job.priority", job.priority.value)
    span.set_attribute("job.attempt", job.attempts + 1)
    if job.user_id is None:
        won = await repo.fail_job(session, job.id, {"reason": "ownerless job dropped"})
        app_metrics.jobs_dropped_ownerless.add(1)
        log.warning("job.dropped_ownerless", extra={"won": won})
        _record_outcome(job, "dropped_ownerless", started)
        return Outcome(ack=won, label="dropped_ownerless")
    span.set_attribute("enduser.id", str(job.user_id))
    is_batch = job.type == JobType.batch
    if is_batch:
        await repo.init_progress(session, job.id)
    ctx = PgJobContext(job.id, session_factory, settings.cancel_poll_interval_s)
    try:
        payload = validate_payload(job.type, job.payload)
        result = await run_with_timeout(
            run_handler(job.type, payload, ctx), settings.job_handler_timeout_s
        )
    except JobCancelled as cancelled:
        won = await repo.cancel_job(session, job.id, cancelled.summary)
        log.info("job.cancelled", extra={"won": won})
        _record_outcome(job, "cancelled", started)
        return Outcome(ack=won, label="cancelled")
    except HandlerTimeout as timeout_exc:
        # Capture every field still needed below *before* the rollback: a
        # rollback expires all ORM objects loaded in this session (regardless
        # of expire_on_commit), and schedule_retry_or_fail/_record_outcome do
        # plain synchronous attribute access on `job` — a lazy-reload of an
        # expired attribute needs an awaited greenlet context that a bare
        # attribute access doesn't have, raising MissingGreenlet.
        job_snapshot = SimpleNamespace(
            id=job.id,
            type=job.type,
            priority=job.priority,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
        )
        await (
            session.rollback()
        )  # discard any state the cancelled handler left mid-flight
        span.record_exception(timeout_exc)
        span.set_status(Status(StatusCode.ERROR, "HandlerTimeout"))
        # Use the pre-rollback snapshot, not `job`, from here on: rollback
        # expires every ORM attribute on `job`, and a bare (un-awaited)
        # attribute access on an expired attribute would try to lazy-load via
        # the async greenlet bridge outside of any awaited call, raising
        # sqlalchemy.exc.MissingGreenlet.
        won = await schedule_retry_or_fail(
            session,
            client,
            settings,
            job_snapshot,
            {
                "type": "HandlerTimeout",
                "message": f">{settings.job_handler_timeout_s}s",
            },
        )
        log.warning("job.timeout", extra={"won": won})
        _record_outcome(job_snapshot, "timeout", started)
        return Outcome(ack=won, label="timeout")
    except asyncio.CancelledError:
        # External cancellation (shutdown): roll back before propagating so the
        # job row isn't left with a half-written transaction; the reaper will
        # reclaim the message. Nothing touches `job` after this, so no
        # capture is needed here.
        await session.rollback()
        raise
    except Exception as exc:  # noqa: BLE001 — any handler/validation error is retryable
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
        won = await schedule_retry_or_fail(
            session,
            client,
            settings,
            job,
            {"type": type(exc).__name__, "message": str(exc)},
        )
        log.info(
            "job.retry_scheduled",
            extra={"error_type": type(exc).__name__, "won": won},
        )
        _record_outcome(job, "retried", started)
        return Outcome(ack=won, label="retried")

    won = await repo.complete_job(
        session, job.id, result, progress=100 if is_batch else None
    )
    if not won:
        log.critical("job.complete_lost_to_reaper")
        _record_outcome(job, "lost", started)
        return Outcome(ack=False, label="lost")
    log.info("job.completed")
    _record_outcome(job, "completed", started)
    return Outcome(ack=True, label="completed")


async def handle_message(
    session_factory: async_sessionmaker[AsyncSession],
    client: redis.Redis,
    settings: Settings,
    stream: str,
    message_id: str,
    fields: dict,
) -> Outcome:
    """Process one delivered message inside a CONSUMER span that continues
    the trace carried in the message fields (or starts a new one)."""
    job_id = UUID(fields["job_id"])
    sent_ms = int(message_id.split("-")[0])
    # Label by priority (e.g. "normal"), not the raw stream key
    # (e.g. "jobs:stream:normal"), so this joins with queue.depth's
    # {"stream": priority.value} on the same dashboard.
    priority_by_stream = {s: p.value for p, s in settings.priority_streams}
    app_metrics.job_queue_wait.record(
        max(0.0, time.time() - sent_ms / 1000),
        {"stream": priority_by_stream.get(stream, stream)},
    )
    parent = extract(fields)  # tolerates absent/malformed traceparent
    with bind_log_context(job_id=str(job_id), message_id=message_id, stream=stream):
        with _tracer.start_as_current_span(
            "process job",
            context=parent,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "redis",
                "messaging.destination.name": stream,
                "job.id": str(job_id),
            },
        ) as span:
            log.info("job.received")
            async with session_factory() as session:
                outcome = await process_job(
                    session, client, settings, job_id, session_factory
                )
            span.set_attribute("job.outcome", outcome.label)
        if outcome.ack:
            await ack(client, stream, settings.consumer_group, message_id)
        return outcome


async def run_forever(
    settings: Settings, *, stop: Callable[[], bool] | None = None
) -> int:
    configure_telemetry(settings, "jobs-worker", instance_id=CONSUMER_NAME)
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    client = create_redis_client(settings.redis_url)

    heartbeat = Heartbeat()
    health_server: HealthServer | None = None
    if settings.health_port is not None:
        # Bind failure raises out of run_forever: fail fast, compose restarts —
        # a silently absent healthcheck is worse than a restart.
        health_server = HealthServer(
            port=settings.health_port,
            heartbeat=heartbeat,
            max_heartbeat_age_s=worker_heartbeat_threshold_s(settings),
            engine=engine,
            redis_client=client,
        )
        await health_server.start()

    for stream in settings.ordered_streams:
        await ensure_group(client, stream, settings.consumer_group)

    if settings.otel_enabled:
        register_worker_resource_gauges()

    shutting_down = {"flag": False}

    def _request_stop(*_):
        shutting_down["flag"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    bind_static_log_context(consumer=CONSUMER_NAME)
    log.info(
        "worker.started",
        extra={
            "streams": settings.ordered_streams,
            "group": settings.consumer_group,
            "concurrency": settings.worker_concurrency,
        },
    )

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

    sem = asyncio.Semaphore(settings.worker_concurrency)

    async def _run_one(stream: str, message_id: str, fields: dict) -> None:
        try:
            await handle_message(
                session_factory, client, settings, stream, message_id, fields
            )
        finally:
            sem.release()

    async with asyncio.TaskGroup() as tg:
        while not _should_stop():
            heartbeat.beat()
            await sem.acquire()  # wait for a free slot before pulling work
            try:
                batch = await read_priority(
                    client,
                    settings.ordered_streams,
                    settings.consumer_group,
                    CONSUMER_NAME,
                    settings.block_ms,
                    count=1,
                )
            except BaseException:
                sem.release()
                raise
            if not batch:
                sem.release()
                continue
            stream, message_id, fields = batch[0]
            tg.create_task(_run_one(stream, message_id, fields))
        # Exiting the `async with` waits for in-flight jobs to finish draining.

    log.info("worker.stopped")
    if health_server is not None:
        await health_server.stop()
    shutdown_telemetry()
    await client.aclose()
    await engine.dispose()
    return 0
