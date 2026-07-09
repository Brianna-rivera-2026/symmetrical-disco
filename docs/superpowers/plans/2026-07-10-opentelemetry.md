# OpenTelemetry & Health Checks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add OpenTelemetry traces/metrics/logs across API, worker and ticker with whole-system trace propagation (including scheduled jobs and retries), replace structlog with stdlib logging, and add split `/health` + `/ready` endpoints to every service. Metrics are shaped around two standard models: **RED** (Rate/Errors/Duration) for the job-submission and processing path, and **USE** (Utilization/Saturation) for worker and queue infrastructure.

**Architecture:** A shared `app/core/telemetry.py` configures OTLP providers per service (gated by `otel_enabled`, default off). Trace context is persisted on the job row at submission and re-injected into Redis Stream message fields at every enqueue, so the worker's consumer span always joins the original trace. Worker/ticker get a daemon-thread HTTP health server; telemetry exports to an OTel Collector added to docker-compose.

**Tech Stack:** opentelemetry-sdk + OTLP gRPC exporters, opentelemetry-instrumentation-{fastapi,sqlalchemy,redis,logging}, stdlib `logging`/`http.server`, SQLAlchemy 2 + Alembic, Redis Streams, pytest + testcontainers.

**Spec:** `docs/superpowers/specs/2026-07-08-opentelemetry-design.md`

## Global Constraints

- Package management is **uv only**: `uv add` / `uv remove` / `uv run pytest` / `uv run ruff check --fix` / `uv run ruff format`. Never pip or venv.
- `otel_enabled` defaults to **False**; the test suite and bare local runs must produce zero OTel network traffic.
- Log **event names and field names are preserved verbatim** during the structlog → stdlib migration (e.g. `job.completed`, `won`, `job_id`).
- No `print` statements — structured logging with job context only.
- App loggers live under the `app.*` namespace; third-party loggers must not emit below WARNING.
- Telemetry must never break the job pipeline: every telemetry code path degrades silently (no-op spans, dropped exports), never raises into app logic.
- Metrics follow **RED** (application layer: Rate/Errors/Duration of job submission and processing, broken down by job `type` so e.g. failed webhooks are distinguishable from failed emails, and email duration is distinguishable from report duration) and **USE** (infrastructure layer: worker process CPU **U**tilization, and Postgres/Redis queue **S**aturation — jobs pending/processing, consumer-group lag, scheduled backlog). **E**rrors for USE is intentionally not tracked as a separate infra metric — job-level errors are already RED's Errors axis (`jobs.failed`, `jobs.processed{outcome=...}`); duplicating them as an infra metric would be redundant.
- Run `uv run pytest` before declaring any task complete. Windows dev machine; docker compose available for manual verification.
- Commit after every task (each task ends with a commit step).

## File Structure

```
app/core/logging.py            REWRITE  stdlib JSON logging, bind_log_context, ContextFilter
app/core/telemetry.py          CREATE   configure/shutdown telemetry, current_trace_carrier
app/core/metrics.py            CREATE   all counter/histogram instruments
app/core/healthcheck.py        CREATE   Heartbeat, HealthServer, threshold helpers
app/core/config.py             MODIFY   otel_enabled, otel_exporter_otlp_endpoint, health_port
app/core/db.py                 MODIFY   pool_timeout=5
app/models/job.py              MODIFY   trace_context column
app/repository.py              MODIFY   create_job(trace_context=), get_promotion_info, count_by_status
app/queue/producer.py          MODIFY   producer span + traceparent injection
app/queue/delayed.py           MODIFY   promote() takes prepared fields
app/worker/runner.py           MODIFY   handle_message (consumer span), heartbeat, health server, CPU gauge (USE)
app/ticker/runner.py           MODIFY   re-injection, ticker spans, gauges (USE: queue + saturation), heartbeat, health server
app/retry.py                   MODIFY   carrier passthrough, jobs.failed metric
app/api/routes.py              MODIFY   /health-/ready split, trace capture, jobs.submitted
app/schemas/api.py             MODIFY   LivenessResponse
app/main.py                    MODIFY   configure_telemetry, FastAPI instrumentation, shutdown
alembic/versions/0007_*.py     CREATE   trace_context migration
otel-collector-config.yaml     CREATE   OTLP receiver → debug exporter
docker-compose.yml             MODIFY   collector service, env vars, probes
CLAUDE.md                      MODIFY   structlog removal
tests/conftest.py              CREATE   global in-memory OTel providers + fixtures
```

---

### Task 1: Rewrite `app/core/logging.py` on stdlib logging

**Files:**
- Modify: `app/core/logging.py` (full rewrite, same entry point `configure_logging`)
- Test: `tests/unit/test_logging.py` (full rewrite)

**Interfaces:**
- Consumes: nothing new.
- Produces: `configure_logging(log_level: str) -> None`, `bind_log_context(**fields) -> ContextManager[None]`, `bind_static_log_context(**fields) -> None`, `ContextFilter` (logging.Filter), `JsonFormatter` (logging.Formatter). Later tasks import `ContextFilter` (telemetry) and `bind_log_context`/`bind_static_log_context` (worker).

Note: structlog stays installed until Task 2 (old call sites still import it); this task only replaces the logging module and its tests.

- [ ] **Step 1: Write the failing tests**

Replace the entire contents of `tests/unit/test_logging.py`:

```python
import json
import logging
import threading

import pytest

from app.core.logging import bind_log_context, configure_logging


@pytest.fixture(autouse=True)
def _restore_logging():
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    app_logger = logging.getLogger("app")
    saved_app_level = app_logger.level
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    app_logger.setLevel(saved_app_level)


def _last_record(capsys) -> dict:
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_emits_json_with_extra_and_bound_context(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")
    with bind_log_context(job_id="abc"):
        log.info("job.received", extra={"job_type": "email"})
    record = _last_record(capsys)
    assert record["event"] == "job.received"
    assert record["job_id"] == "abc"
    assert record["job_type"] == "email"
    assert record["level"] == "info"
    assert record["logger"] == "app.test"
    assert "timestamp" in record


def test_context_cleared_after_block(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")
    with bind_log_context(job_id="abc"):
        pass
    log.info("after")
    assert "job_id" not in _last_record(capsys)


def test_bind_log_context_nests(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")
    with bind_log_context(consumer="w1"):
        with bind_log_context(job_id="abc"):
            log.info("inner")
        record_inner = _last_record(capsys)
        log.info("outer")
        record_outer = _last_record(capsys)
    assert record_inner["consumer"] == "w1" and record_inner["job_id"] == "abc"
    assert record_outer["consumer"] == "w1" and "job_id" not in record_outer


def test_context_not_inherited_by_threads(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")

    def emit():
        log.info("from-thread")

    with bind_log_context(job_id="abc"):
        thread = threading.Thread(target=emit)
        thread.start()
        thread.join()
    assert "job_id" not in _last_record(capsys)


def test_third_party_suppressed_app_passes(capsys):
    configure_logging("INFO")
    logging.getLogger("sqlalchemy.engine").info("third-party info")
    logging.getLogger("app.worker").info("app info")
    lines = [line for line in capsys.readouterr().out.strip().splitlines() if line]
    events = [json.loads(line)["event"] for line in lines]
    assert "app info" in events
    assert "third-party info" not in events


def test_exception_rendered(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")
    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("ticker.tick_failed")
    record = _last_record(capsys)
    assert record["event"] == "ticker.tick_failed"
    assert record["level"] == "error"
    assert "ValueError: boom" in record["exception"]


def test_no_trace_ids_without_instrumentation(capsys):
    configure_logging("INFO")
    logging.getLogger("app.test").info("plain")
    assert "trace_id" not in _last_record(capsys)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: FAIL — `ImportError: cannot import name 'bind_log_context'`.

- [ ] **Step 3: Rewrite the implementation**

Replace the entire contents of `app/core/logging.py`:

```python
import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone

_log_context: ContextVar[dict] = ContextVar("log_context", default={})

# Attribute names present on every LogRecord; anything else came in via
# `extra` (or the ContextFilter) and belongs in the JSON output.
_RESERVED = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | {"message", "asctime", "taskName"}


@contextmanager
def bind_log_context(**fields: object) -> Iterator[None]:
    """Bind fields onto every log record emitted inside the block."""
    token = _log_context.set({**_log_context.get(), **fields})
    try:
        yield
    finally:
        _log_context.reset(token)


def bind_static_log_context(**fields: object) -> None:
    """Bind fields for the remaining lifetime of the current context
    (process-constant values like the worker's consumer name)."""
    _log_context.set({**_log_context.get(), **fields})


class ContextFilter(logging.Filter):
    """Merge the bound context into each record; explicit `extra` fields win."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _log_context.get().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("otel"):
                continue
            out[key] = value
        # Injected by opentelemetry-instrumentation-logging; "0" means no span.
        trace_id = getattr(record, "otelTraceID", "0")
        if trace_id != "0":
            out["trace_id"] = trace_id
            out["span_id"] = getattr(record, "otelSpanID", "0")
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(ContextFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    # Third-party loggers inherit WARNING from root; only app.* speaks at the
    # configured level (keeps noisy libraries out of stdout and OTLP alike).
    root.setLevel(logging.WARNING)
    logging.getLogger("app").setLevel(level)

    # Hijack uvicorn loggers so their records flow through the same handler,
    # pinned to WARNING because uvicorn otherwise sets its own levels.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
        lg.setLevel(logging.WARNING)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: 7 passed.

- [ ] **Step 5: Run the full suite** (old structlog call sites must still work — they route through stdlib logging, whose root handler now formats JSON differently, which is fine; the suite must stay green)

Run: `uv run pytest`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add app/core/logging.py tests/unit/test_logging.py
git commit -m "refactor: rewrite logging on stdlib with JSON formatter and bound context"
```

---

### Task 2: Migrate call sites off structlog, remove the dependency, update CLAUDE.md

**Files:**
- Modify: `app/worker/runner.py`, `app/ticker/runner.py`, `app/retry.py`, `app/api/routes.py`, `CLAUDE.md`, `pyproject.toml` (via `uv remove`)
- Test: existing suite (no new tests — behavior-preserving refactor)

**Interfaces:**
- Consumes: `bind_log_context`, `bind_static_log_context` from Task 1.
- Produces: loggers named `app.worker`, `app.ticker`, `app.retry`, `app.api` — later tasks add log lines through these.

- [ ] **Step 1: Migrate `app/worker/runner.py`**

Replace `import structlog` with `import logging` and `from app.core.logging import bind_log_context, bind_static_log_context`. Replace `log = structlog.get_logger("worker")` with `log = logging.getLogger("app.worker")`. Then convert each call (kwargs move into `extra`):

```python
# in process_job:
log.info("job.skipped", extra={"reason": "not_claimable"})
log.info("job.cancelled", extra={"won": won})
log.warning("job.timeout", extra={"won": won})
log.info("job.retry_scheduled", extra={"error_type": type(exc).__name__, "won": won})
log.critical("job.complete_lost_to_reaper")
log.info("job.completed")

# in run_forever — replace structlog.contextvars.bind_contextvars(consumer=CONSUMER_NAME):
bind_static_log_context(consumer=CONSUMER_NAME)
log.info("worker.started", extra={"streams": settings.ordered_streams, "group": settings.consumer_group})

# in the message loop — replace the structlog.contextvars.bound_contextvars block:
with bind_log_context(job_id=str(job_id), message_id=message_id, stream=stream):
    ...
log.warning("worker.recycling", extra={"timeouts": timeouts})
log.info("worker.stopped", extra={"exit_code": exit_code})
```

- [ ] **Step 2: Migrate `app/ticker/runner.py`**

`log = logging.getLogger("app.ticker")` (drop the structlog import). Conversions:

```python
log.info("ticker.promoted", extra={"enqueued": len(routed), "pulled": len(ids)})
log.warning("ticker.reconcile_skipped_null_scheduled_at", extra={"job_id": str(job.id)})
log.info("ticker.reaped", extra={"count": handled})
log.info("ticker.started", extra={"zset": settings.delayed_zset, "streams": settings.ordered_streams})
log.info("ticker.reconciled", extra={"count": recovered})
log.exception("ticker.tick_failed")   # unchanged call shape
log.info("ticker.stopped")
```

- [ ] **Step 3: Migrate `app/retry.py`**

`log = logging.getLogger("app.retry")`. Conversions:

```python
log.info("retry.failed_permanent", extra={"job_id": str(job.id), "attempts": n, "won": won})
log.info("retry.immediate", extra={"job_id": str(job.id), "attempts": n, "won": won})
log.info("retry.delayed", extra={"job_id": str(job.id), "attempts": n, "delay": delay, "won": won})
```

- [ ] **Step 4: Migrate `app/api/routes.py`**

`log = logging.getLogger("app.api")`. One conversion:

```python
log.warning("stats.unavailable", extra={"error": str(exc)})
```

- [ ] **Step 5: Remove the dependency and verify nothing references it**

```bash
uv remove structlog
```

Then: `grep -r structlog app/ tests/` — expected: no matches.

- [ ] **Step 6: Update `CLAUDE.md`**

In the Tech stack list, replace `* structlog for structured logging` with `* stdlib logging (JSON to stdout) + OpenTelemetry for logs/metrics/traces`. Replace strict guideline 3 with: `3. Don't use print statements; use stdlib logging via logging.getLogger("app.<component>") with job context bound through app.core.logging.bind_log_context.`

- [ ] **Step 7: Run the full suite and lints**

Run: `uv run pytest` — expected: all pass.
Run: `uv run ruff check --fix && uv run ruff format` — expected: clean.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: migrate all logging call sites to stdlib, remove structlog"
```

---

### Task 3: OTel dependencies, settings, telemetry module, test providers

**Files:**
- Create: `app/core/telemetry.py`, `tests/conftest.py`, `tests/unit/test_telemetry.py`
- Modify: `app/core/config.py`, `pyproject.toml` (via `uv add`)

**Interfaces:**
- Consumes: `ContextFilter` from Task 1; `Settings` from `app/core/config.py`.
- Produces:
  - `configure_telemetry(settings: Settings, service_name: str, instance_id: str | None = None) -> None`
  - `shutdown_telemetry() -> None` (idempotent, safe when disabled)
  - `current_trace_carrier() -> dict[str, str] | None`
  - Settings fields: `otel_enabled: bool = False`, `otel_exporter_otlp_endpoint: str = "http://localhost:4317"`
  - Test fixtures `span_exporter` (InMemorySpanExporter, cleared per test) and `metric_reader` (InMemoryMetricReader, cumulative) available suite-wide.

- [ ] **Step 1: Add dependencies**

```bash
uv add opentelemetry-sdk opentelemetry-exporter-otlp opentelemetry-instrumentation-fastapi opentelemetry-instrumentation-sqlalchemy opentelemetry-instrumentation-redis opentelemetry-instrumentation-logging
```

- [ ] **Step 2: Add global test providers**

Create `tests/conftest.py` (new file — applies to the whole suite; installs recording providers once, before any app code creates spans/metrics):

```python
import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

# Installed at import time: global providers may only be set once per process,
# and conftest imports before any test or app module emits telemetry.
_SPAN_EXPORTER = InMemorySpanExporter()
_METRIC_READER = InMemoryMetricReader()

_tracer_provider = TracerProvider()
_tracer_provider.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
trace.set_tracer_provider(_tracer_provider)
metrics.set_meter_provider(MeterProvider(metric_readers=[_METRIC_READER]))


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    _SPAN_EXPORTER.clear()
    return _SPAN_EXPORTER


@pytest.fixture
def metric_reader() -> InMemoryMetricReader:
    # Cumulative across the session; tests assert on attribute sets / deltas.
    return _METRIC_READER
```

- [ ] **Step 3: Write the failing tests**

Create `tests/unit/test_telemetry.py`:

```python
import logging

from opentelemetry import trace

from app.core.config import Settings
from app.core.telemetry import (
    configure_telemetry,
    current_trace_carrier,
    shutdown_telemetry,
)


def _settings(**overrides) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://u:p@localhost/x",
        redis_url="redis://localhost:6379/0",
        **overrides,
    )


def test_settings_defaults():
    settings = _settings()
    assert settings.otel_enabled is False
    assert settings.otel_exporter_otlp_endpoint == "http://localhost:4317"


def test_disabled_is_noop():
    settings = _settings(otel_enabled=False)
    provider_before = trace.get_tracer_provider()
    handlers_before = logging.getLogger().handlers[:]
    configure_telemetry(settings, "test-service")
    assert trace.get_tracer_provider() is provider_before
    assert logging.getLogger().handlers == handlers_before
    shutdown_telemetry()  # must be safe when nothing was configured


def test_current_trace_carrier_inside_span(span_exporter):
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("request") as span:
        carrier = current_trace_carrier()
    assert carrier is not None
    trace_id = format(span.get_span_context().trace_id, "032x")
    assert trace_id in carrier["traceparent"]


def test_current_trace_carrier_outside_span_is_none():
    assert current_trace_carrier() is None
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_telemetry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.core.telemetry'`.

- [ ] **Step 5: Add settings fields**

In `app/core/config.py`, add to `Settings` (after `cancel_poll_interval_s`):

```python
    otel_enabled: bool = False
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
```

- [ ] **Step 6: Create `app/core/telemetry.py`**

```python
import logging

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.propagate import inject
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app.core.config import Settings
from app.core.logging import ContextFilter

_state: dict[str, list] = {"providers": [], "handlers": []}


def configure_telemetry(
    settings: Settings, service_name: str, instance_id: str | None = None
) -> None:
    """Set up OTLP traces/metrics/logs and auto-instrumentation.

    No-op when settings.otel_enabled is False. Must run BEFORE the service
    creates its SQLAlchemy engine so the instrumentation hooks it.
    """
    if not settings.otel_enabled:
        return

    attributes: dict[str, str] = {"service.name": service_name}
    if instance_id is not None:
        attributes["service.instance.id"] = instance_id
    resource = Resource.create(attributes)
    endpoint = settings.otel_exporter_otlp_endpoint

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
    )
    trace.set_tracer_provider(tracer_provider)

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=endpoint, insecure=True)
            )
        ],
    )
    metrics.set_meter_provider(meter_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, insecure=True))
    )
    set_logger_provider(logger_provider)
    otel_handler = LoggingHandler(logger_provider=logger_provider)
    otel_handler.addFilter(ContextFilter())
    logging.getLogger().addHandler(otel_handler)
    _state["handlers"].append(otel_handler)

    LoggingInstrumentor().instrument(set_logging_format=False)
    SQLAlchemyInstrumentor().instrument()
    RedisInstrumentor().instrument()

    _state["providers"] = [tracer_provider, meter_provider, logger_provider]


def shutdown_telemetry() -> None:
    """Flush and shut down providers. Safe (and a no-op) when disabled."""
    for handler in _state["handlers"]:
        logging.getLogger().removeHandler(handler)
    _state["handlers"].clear()
    for provider in _state["providers"]:
        provider.shutdown()
    _state["providers"].clear()


def current_trace_carrier() -> dict[str, str] | None:
    """W3C carrier ({'traceparent': ...}) for the active context, or None
    when there is no active span (e.g. OTel disabled)."""
    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier or None
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_telemetry.py -v`
Expected: 4 passed.

- [ ] **Step 8: Run the full suite** (the new root conftest now records all spans in memory — must not disturb anything)

Run: `uv run pytest`
Expected: all pass.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: add OTel telemetry module, settings, and in-memory test providers"
```

---

### Task 4: `trace_context` column — migration, model, repository, API capture

**Files:**
- Create: `alembic/versions/0007_add_trace_context.py`
- Modify: `app/models/job.py`, `app/repository.py:16-41` (`create_job`), `app/api/routes.py` (`_create_and_handoff`)
- Test: `tests/integration/test_repository.py` (add one test)

**Interfaces:**
- Consumes: `current_trace_carrier()` from Task 3.
- Produces: `Job.trace_context: dict | None`; `create_job(..., trace_context: dict | None = None)`. Task 7 reads `Job.trace_context`.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_repository.py`:

```python
def test_create_job_persists_trace_context(db_session):
    carrier = {"traceparent": f"00-{'ab' * 16}-{'cd' * 8}-01"}
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        trace_context=carrier,
    )
    db_session.refresh(job)
    assert job.trace_context == carrier


def test_create_job_trace_context_defaults_to_none(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert job.trace_context is None
```

(Match the module's existing imports — it already imports `repo` and `JobType`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_repository.py -v -k trace_context`
Expected: FAIL — `create_job() got an unexpected keyword argument 'trace_context'`.

- [ ] **Step 3: Create the migration**

Create `alembic/versions/0007_add_trace_context.py`:

```python
"""add trace_context for OpenTelemetry propagation

Revision ID: 0007
Revises: 0006
Create Date: 2026-07-10
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs", sa.Column("trace_context", postgresql.JSONB(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("jobs", "trace_context")
```

- [ ] **Step 4: Add the model column**

In `app/models/job.py`, after `idempotency_hash`:

```python
    trace_context: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
```

- [ ] **Step 5: Extend `create_job`**

In `app/repository.py`, add the parameter and pass it through:

```python
def create_job(
    session: Session,
    job_type: JobType,
    payload: dict,
    *,
    status: JobStatus = JobStatus.pending,
    scheduled_at: datetime | None = None,
    priority: JobPriority = JobPriority.normal,
    max_attempts: int = 4,
    idempotency_key: str | None = None,
    idempotency_hash: str | None = None,
    trace_context: dict | None = None,
) -> Job:
    job = Job(
        type=job_type,
        payload=payload,
        status=status,
        scheduled_at=scheduled_at,
        priority=priority,
        max_attempts=max_attempts,
        idempotency_key=idempotency_key,
        idempotency_hash=idempotency_hash,
        trace_context=trace_context,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job
```

- [ ] **Step 6: Capture at submission**

In `app/api/routes.py`, import `from app.core.telemetry import current_trace_carrier` and pass `trace_context=current_trace_carrier()` in **both** `repo.create_job(...)` calls inside `_create_and_handoff` (the scheduled and the immediate branch).

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_repository.py tests/integration/test_migration.py -v`
Expected: all pass (migration runs via the session-scoped `pg_engine` fixture).

- [ ] **Step 8: Run the full suite, then commit**

Run: `uv run pytest` — expected: all pass.

```bash
git add -A
git commit -m "feat: persist W3C trace context on the job row at submission"
```

---

### Task 5: Producer span + traceparent injection into stream messages

**Files:**
- Modify: `app/queue/producer.py`
- Test: `tests/unit/test_producer_tracing.py` (create)

**Interfaces:**
- Consumes: global tracer (test provider from Task 3 conftest in tests).
- Produces:
  - `message_fields(stream: str, job_id: str, carrier: dict | None = None) -> dict[str, str]`
  - `enqueue(client, stream, job_id, carrier: dict | None = None) -> str` (signature extended, existing call sites unaffected — new arg is optional). Tasks 6-7 rely on `traceparent` being present in message fields.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_producer_tracing.py`:

```python
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from app.queue.producer import message_fields


def test_fields_carry_active_context(span_exporter):
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("api-request") as parent:
        fields = message_fields("jobs:stream:high", "j1")
    trace_id = format(parent.get_span_context().trace_id, "032x")
    assert fields["job_id"] == "j1"
    assert trace_id in fields["traceparent"]
    producer_spans = [
        s for s in span_exporter.get_finished_spans()
        if s.name == "send jobs:stream:high"
    ]
    assert len(producer_spans) == 1
    assert producer_spans[0].kind is SpanKind.PRODUCER
    assert format(producer_spans[0].context.trace_id, "032x") == trace_id
    assert producer_spans[0].attributes["messaging.system"] == "redis"


def test_fields_from_stored_carrier_join_original_trace(span_exporter):
    stored = {"traceparent": f"00-{'ab' * 16}-{'cd' * 8}-01"}
    fields = message_fields("jobs:stream:low", "j2", carrier=stored)
    assert "ab" * 16 in fields["traceparent"]


def test_fields_with_malformed_carrier_do_not_raise(span_exporter):
    fields = message_fields("jobs:stream:normal", "j3", carrier={"traceparent": "garbage"})
    assert fields["job_id"] == "j3"
    assert "traceparent" in fields  # new root trace, not an error
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_producer_tracing.py -v`
Expected: FAIL — `ImportError: cannot import name 'message_fields'`.

- [ ] **Step 3: Implement**

Replace the contents of `app/queue/producer.py`:

```python
import redis
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind

_tracer = trace.get_tracer("app.queue.producer")


def message_fields(
    stream: str, job_id: str, carrier: dict | None = None
) -> dict[str, str]:
    """XADD fields carrying W3C trace context. With `carrier` (a job's stored
    {'traceparent': ...}) the send joins that original trace; without it, the
    currently active context is used. A malformed carrier degrades to a new
    root trace — this never raises."""
    context = extract(carrier) if carrier else None
    with _tracer.start_as_current_span(
        f"send {stream}",
        context=context,
        kind=SpanKind.PRODUCER,
        attributes={
            "messaging.system": "redis",
            "messaging.destination.name": stream,
            "job.id": job_id,
        },
    ):
        fields = {"job_id": job_id}
        inject(fields)
    return fields


def enqueue(
    client: redis.Redis, stream: str, job_id: str, carrier: dict | None = None
) -> str:
    return client.xadd(stream, message_fields(stream, job_id, carrier))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_producer_tracing.py -v` — expected: 3 passed.
Run: `uv run pytest` — expected: all pass (messages now carry an extra `traceparent` field; consumers only read `fields["job_id"]`).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: producer span and trace context injection on every enqueue"
```

---

### Task 6: Worker consumer span — `handle_message` + span attributes in `process_job`

**Files:**
- Modify: `app/worker/runner.py`
- Test: `tests/integration/test_worker.py` (add tests)

**Interfaces:**
- Consumes: `message_fields`/`enqueue` from Task 5; `bind_log_context` from Task 1.
- Produces: `handle_message(session_factory, client, settings, stream, message_id, fields) -> Outcome` — encapsulates span + logging context + `process_job` + ack. The `run_forever` loop and Task 8 metrics build on it.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_worker.py` (module already has `db_session`, `redis_client`, `test_settings`, `pg_engine` fixtures available and the `_no_sleep` autouse fixture):

```python
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from app.core.db import make_session_factory
from app.queue.consumer import ensure_group, read_priority
from app.queue.producer import enqueue
from app.worker.runner import handle_message


def _read_one(redis_client, test_settings):
    batch = read_priority(
        redis_client,
        test_settings.ordered_streams,
        test_settings.consumer_group,
        "test-consumer",
        block_ms=100,
    )
    assert batch, "expected one message"
    return batch[0]


def test_consumer_span_joins_producer_trace(
    db_session, redis_client, test_settings, pg_engine, span_exporter
):
    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("submit") as submit_span:
        enqueue(redis_client, test_settings.stream_normal, str(job.id))
    stream, message_id, fields = _read_one(redis_client, test_settings)

    outcome = handle_message(
        make_session_factory(pg_engine),
        redis_client,
        test_settings,
        stream,
        message_id,
        fields,
    )

    assert outcome.label == "completed"
    consumer = next(
        s for s in span_exporter.get_finished_spans() if s.name == "process job"
    )
    expected_trace = format(submit_span.get_span_context().trace_id, "032x")
    assert format(consumer.context.trace_id, "032x") == expected_trace
    assert consumer.kind is SpanKind.CONSUMER
    assert consumer.attributes["job.outcome"] == "completed"
    assert consumer.attributes["job.type"] == "email"
    assert consumer.attributes["job.attempt"] == 1
    # message acked: no pending entries left for the group
    pending = redis_client.xpending(stream, test_settings.consumer_group)
    assert pending["pending"] == 0


def test_consumer_span_without_traceparent_starts_new_trace(
    db_session, redis_client, test_settings, pg_engine, span_exporter
):
    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    redis_client.xadd(test_settings.stream_normal, {"job_id": str(job.id)})  # legacy shape
    stream, message_id, fields = _read_one(redis_client, test_settings)
    outcome = handle_message(
        make_session_factory(pg_engine), redis_client, test_settings,
        stream, message_id, fields,
    )
    assert outcome.label == "completed"


def test_consumer_span_records_handler_error(
    db_session, redis_client, test_settings, pg_engine, span_exporter, monkeypatch
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # force webhook fail
    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    enqueue(redis_client, test_settings.stream_normal, str(job.id))
    stream, message_id, fields = _read_one(redis_client, test_settings)
    handle_message(
        make_session_factory(pg_engine), redis_client, test_settings,
        stream, message_id, fields,
    )
    consumer = next(
        s for s in span_exporter.get_finished_spans() if s.name == "process job"
    )
    assert consumer.attributes["job.outcome"] == "retried"
    assert consumer.status.status_code is StatusCode.ERROR
    assert consumer.events  # exception recorded
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_worker.py -v -k "consumer_span"`
Expected: FAIL — `ImportError: cannot import name 'handle_message'`.

- [ ] **Step 3: Implement `handle_message` and span attributes**

In `app/worker/runner.py`:

Add imports:

```python
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind, Status, StatusCode
```

Add near the module logger:

```python
_tracer = trace.get_tracer("app.worker")
```

Add the new function (above `run_forever`):

```python
def handle_message(
    session_factory: Callable[[], Session],
    client: redis.Redis,
    settings: Settings,
    stream: str,
    message_id: str,
    fields: dict,
) -> Outcome:
    """Process one delivered message inside a CONSUMER span that continues
    the trace carried in the message fields (or starts a new one)."""
    job_id = UUID(fields["job_id"])
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
            with session_factory() as session:
                outcome = process_job(
                    session, client, settings, job_id, session_factory
                )
            span.set_attribute("job.outcome", outcome.label)
        if outcome.ack:
            ack(client, stream, settings.consumer_group, message_id)
        return outcome
```

In `process_job`, set job attributes on the current span right after `job = repo.get_job(session, job_id)`:

```python
    span = trace.get_current_span()
    span.set_attribute("job.type", job.type.value)
    span.set_attribute("job.priority", job.priority.value)
    span.set_attribute("job.attempt", job.attempts + 1)
```

Record errors on the span in the timeout and generic-exception handlers (cancellation is not an error). The two except blocks become, in full:

```python
    except HandlerTimeout as timeout_exc:
        span.record_exception(timeout_exc)
        span.set_status(Status(StatusCode.ERROR, "HandlerTimeout"))
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
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
        won = schedule_retry_or_fail(
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
        return Outcome(ack=won, recycle=False, label="retried")
```

Replace the loop body in `run_forever` (the `for stream, message_id, fields in batch:` block) with:

```python
        for stream, message_id, fields in batch:
            outcome = handle_message(
                session_factory, client, settings, stream, message_id, fields
            )
            if outcome.recycle:
                timeouts += 1
                if timeouts >= settings.max_handler_timeouts_before_recycle:
                    log.warning("worker.recycling", extra={"timeouts": timeouts})
                    exit_code = 1
                    break
```

(`handle_message` now owns the ack and the log-context binding; delete those from the loop.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_worker.py -v` — expected: all pass (old + new).
Run: `uv run pytest` — expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: worker consumer span continues the message's trace"
```

---

### Task 7: Ticker re-injection — promote, reconcile, reap + ticker spans

**Files:**
- Modify: `app/repository.py` (replace `get_priorities` with `get_promotion_info`), `app/queue/delayed.py` (`promote`), `app/ticker/runner.py`, `app/retry.py` (carrier passthrough)
- Test: `tests/integration/test_ticker.py` (add tests; update any test using `get_priorities`)

**Interfaces:**
- Consumes: `message_fields`, `enqueue(carrier=)` from Task 5; `Job.trace_context` from Task 4.
- Produces:
  - `repo.get_promotion_info(session, job_ids) -> dict[UUID, tuple[JobPriority, dict | None]]` (replaces `get_priorities` — update all callers/tests)
  - `delayed.promote(client, zset, routed: list[tuple[str, dict]], all_ids)` — routed entries are `(stream, prepared_fields)`
  - `schedule_retry_or_fail(..., carrier: dict | None = None)`

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_ticker.py` (reuse its existing imports/fixtures; add missing imports at top):

```python
def test_promote_reinjects_stored_trace_context(db_session, redis_client, test_settings):
    stored = {"traceparent": f"00-{'ab' * 16}-{'cd' * 8}-01"}
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=past,
        trace_context=stored,
    )
    delayed.schedule(redis_client, test_settings.delayed_zset, str(job.id), past.timestamp())

    promote_due(db_session, redis_client, test_settings)

    entries = redis_client.xrange(test_settings.stream_normal)
    assert len(entries) == 1
    fields = entries[0][1]
    assert fields["job_id"] == str(job.id)
    assert "ab" * 16 in fields["traceparent"]  # original trace id restored


def test_promote_without_stored_context_still_enqueues(
    db_session, redis_client, test_settings
):
    past = datetime.now(timezone.utc) - timedelta(seconds=5)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=past,
    )
    delayed.schedule(redis_client, test_settings.delayed_zset, str(job.id), past.timestamp())
    promote_due(db_session, redis_client, test_settings)
    entries = redis_client.xrange(test_settings.stream_normal)
    assert entries[0][1]["job_id"] == str(job.id)


def test_reconcile_reinjects_stored_trace_context(
    db_session, redis_client, test_settings
):
    stored = {"traceparent": f"00-{'12' * 16}-{'34' * 8}-01"}
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        trace_context=stored,
    )
    # pending + unsynced + old enough → reconcile re-enqueues it
    db_session.execute(
        sa.text("UPDATE jobs SET created_at = now() - interval '1 hour' WHERE id = :id"),
        {"id": str(job.id)},
    )
    db_session.commit()
    reconcile_orphans(db_session, redis_client, test_settings)
    entries = redis_client.xrange(test_settings.stream_normal)
    assert "12" * 16 in entries[0][1]["traceparent"]
```

(Add `import sqlalchemy as sa` and the `promote_due` / `reconcile_orphans` / `delayed` imports if the module doesn't have them.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_ticker.py -v -k reinjects`
Expected: FAIL — messages lack `traceparent` (promotion still XADDs bare `{"job_id": ...}`).

- [ ] **Step 3: Replace `get_priorities` in `app/repository.py`**

```python
def get_promotion_info(
    session: Session, job_ids: list[UUID]
) -> dict[UUID, tuple[JobPriority, dict | None]]:
    if not job_ids:
        return {}
    rows = session.execute(
        select(Job.id, Job.priority, Job.trace_context).where(Job.id.in_(job_ids))
    ).all()
    return {row.id: (row.priority, row.trace_context) for row in rows}
```

Delete `get_priorities` and update every reference (`grep -r get_priorities app/ tests/`).

- [ ] **Step 4: Change `delayed.promote` to take prepared fields**

In `app/queue/delayed.py`:

```python
def promote(
    client: redis.Redis,
    zset: str,
    routed: list[tuple[str, dict]],
    all_ids: list[str],
) -> None:
    if not all_ids:
        return
    # XADD every routed message to its target stream BEFORE removing any id
    # from the ZSET, so a crash mid-promotion leaves the ids in the ZSET to be
    # retried next tick. Duplicate stream entries are absorbed by the worker's
    # idempotent claim guard.
    pipe = client.pipeline(transaction=False)
    for stream, fields in routed:
        pipe.xadd(stream, fields)
    pipe.execute()
    client.zrem(zset, *all_ids)
```

- [ ] **Step 5: Rework `app/ticker/runner.py`**

Add imports:

```python
from opentelemetry import trace

from app.queue.producer import enqueue, message_fields
```

Add near the module logger: `_tracer = trace.get_tracer("app.ticker")`.

`promote_due` becomes (span only when there is work; fields built with the stored carrier):

```python
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
        log.info("ticker.promoted", extra={"enqueued": len(routed), "pulled": len(ids)})
    return len(ids)
```

In `reconcile_orphans`, wrap each non-empty batch in a span and pass the stored carrier — replace the body after `if not rows: break` with:

```python
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
```

In `_reap_one`: pass `carrier=job.trace_context` to the `enqueue(...)` call in the unsynced-handoff branch, and pass the carrier into the retry helper (signature extended in Step 6):

```python
        elif job.status is JobStatus.processing:
            schedule_retry_or_fail(
                session,
                client,
                settings,
                job,
                {"type": "WorkerLost", "message": "reclaimed by reaper"},
                carrier=job.trace_context,
            )
```

In `reap_stale`, wrap the message-handling in a span only when messages were claimed — replace the inner `for message_id, fields in messages:` block with:

```python
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
```

- [ ] **Step 6: Extend `schedule_retry_or_fail` in `app/retry.py`**

Add `carrier: dict | None = None` as the last parameter and pass it through in the immediate-retry branch:

```python
def schedule_retry_or_fail(
    session: Session,
    client: redis.Redis,
    settings: Settings,
    job: Job,
    error: dict,
    carrier: dict | None = None,
) -> bool:
    ...
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
    ...
```

(Worker call sites pass nothing — the consumer span is active there, so the current context already carries the original trace. The delayed-retry branch needs nothing: context re-enters from the DB at promotion.)

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_ticker.py tests/integration/test_reaper.py tests/integration/test_retry.py tests/integration/test_delayed.py -v`
Expected: all pass.
Run: `uv run pytest` — expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: ticker re-injects stored trace context on promote/reconcile/reap"
```

---

### Task 8: Metrics — instruments module and emission points (RED method)

These instruments are the **RED** half of the plan's metrics (Rate/Errors/Duration for the job-submission and processing path — see Global Constraints). Each is broken down by job `type` so the dashboards the requirement asks for are a direct read, no derived queries needed:
- **Rate:** `jobs.submitted` (ingress rate by type/priority) — plus HTTP request rate for free from FastAPI auto-instrumentation (Task 3/13).
- **Errors:** `jobs.failed` and `jobs.processed{outcome=...}`, both attributed by `type` — a dashboard filtering `outcome=retried|timeout|lost` or `jobs.failed` by `type=webhook` vs `type=email` shows exactly "failed webhooks vs. failed emails" without any extra instrumentation.
- **Duration:** `job.processing.duration`, attributed by `type` — `type=email` vs `type=report` on the same histogram is the "fast email submission vs. slow report generation" comparison, directly.

**Files:**
- Create: `app/core/metrics.py`
- Modify: `app/api/routes.py`, `app/worker/runner.py`, `app/retry.py`, `app/ticker/runner.py`
- Test: `tests/integration/test_metrics.py` (create)

**Interfaces:**
- Consumes: global meter (proxy-safe: instruments created at import bind to whichever provider is set); `Outcome.label`; `metric_reader` fixture from Task 3.
- Produces module-level instruments importable as `from app.core import metrics as app_metrics`:
  `jobs_submitted`, `jobs_processed`, `jobs_failed`, `job_processing_duration`, `job_queue_wait`, `ticker_promoted`, `ticker_reaped`, `ticker_reconciled`.

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/test_metrics.py`:

```python
import time

import pytest

from app import repository as repo
from app.jobs import handlers
from app.schemas.enums import JobType
from app.worker.runner import process_job


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def _points(metric_reader, name):
    data = metric_reader.get_metrics_data()
    points = []
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    points.extend(metric.data.data_points)
    return points


def test_jobs_processed_counts_completed(
    db_session, redis_client, test_settings, metric_reader
):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    process_job(db_session, redis_client, test_settings, job.id)
    points = _points(metric_reader, "jobs.processed")
    completed = [
        p for p in points
        if p.attributes.get("outcome") == "completed"
        and p.attributes.get("type") == "email"
    ]
    assert completed and completed[0].value >= 1


def test_processing_duration_recorded(
    db_session, redis_client, test_settings, metric_reader
):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    process_job(db_session, redis_client, test_settings, job.id)
    points = _points(metric_reader, "job.processing.duration")
    assert any(
        p.attributes.get("outcome") == "completed" and p.count >= 1 for p in points
    )


def test_jobs_failed_counts_exhausted_attempts(
    db_session, redis_client, test_settings, metric_reader, monkeypatch
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)
    job = repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, max_attempts=1
    )
    process_job(db_session, redis_client, test_settings, job.id)
    points = _points(metric_reader, "jobs.failed")
    failed = [p for p in points if p.attributes.get("type") == "webhook"]
    assert failed and failed[0].value >= 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_metrics.py -v`
Expected: FAIL — no `jobs.processed` metric found (empty points list).

- [ ] **Step 3: Create `app/core/metrics.py`**

```python
"""Job-pipeline instruments. Created against the global MeterProvider; with
OTel disabled these are proxy no-ops, so emission points never need guards."""

from opentelemetry import metrics

_meter = metrics.get_meter("app.jobs")

jobs_submitted = _meter.create_counter(
    "jobs.submitted", description="Jobs accepted by the API"
)
jobs_processed = _meter.create_counter(
    "jobs.processed", description="Worker outcomes per delivery"
)
jobs_failed = _meter.create_counter(
    "jobs.failed", description="Jobs that exhausted max_attempts"
)
job_processing_duration = _meter.create_histogram(
    "job.processing.duration", unit="s", description="process_job wall time"
)
job_queue_wait = _meter.create_histogram(
    "job.queue.wait", unit="s", description="XADD-to-delivery latency"
)
ticker_promoted = _meter.create_counter(
    "ticker.promoted", description="Scheduled jobs promoted to streams"
)
ticker_reaped = _meter.create_counter(
    "ticker.reaped", description="Stale in-flight messages reclaimed"
)
ticker_reconciled = _meter.create_counter(
    "ticker.reconciled", description="Unsynced jobs re-handed to Redis"
)
```

- [ ] **Step 4: Emit from the worker**

In `app/worker/runner.py` add `import time` and `from app.core import metrics as app_metrics`.

In `process_job`, add a helper call before every return. Put this private helper above `process_job`:

```python
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
```

At the top of `process_job`: `started = time.monotonic()`. The claim-failed branch calls `_record_outcome(None, "skipped", started)`; every other return path calls `_record_outcome(job, <label>, started)` with the same label it puts in the `Outcome` (`cancelled`, `timeout`, `retried`, `lost`, `completed`) immediately before returning.

In `handle_message`, record queue wait right after computing `job_id` (stream IDs embed the XADD epoch-milliseconds):

```python
    sent_ms = int(message_id.split("-")[0])
    app_metrics.job_queue_wait.record(
        max(0.0, time.time() - sent_ms / 1000), {"stream": stream}
    )
```

- [ ] **Step 5: Emit from API, retry, ticker**

`app/api/routes.py` — import `from app.core import metrics as app_metrics`; at the end of `_create_and_handoff`, before `return job`:

```python
    app_metrics.jobs_submitted.add(
        1,
        {
            "type": submission.type.value,
            "priority": submission.priority.value,
            "scheduled": job.status is JobStatus.scheduled,
        },
    )
```

`app/retry.py` — import `from app.core import metrics as app_metrics`; in the exhausted branch:

```python
    if n >= job.max_attempts:
        won = repo.fail_job(session, job.id, error)
        if won:
            app_metrics.jobs_failed.add(
                1, {"type": job.type.value, "priority": job.priority.value}
            )
```

`app/ticker/runner.py` — import `from app.core import metrics as app_metrics`; in `promote_due` after `delayed.promote(...)`: `app_metrics.ticker_promoted.add(len(routed))`. In `reconcile_orphans` just before `return total`: `if total: app_metrics.ticker_reconciled.add(total)`. In `reap_stale` just before `return handled`: `if handled: app_metrics.ticker_reaped.add(handled)`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_metrics.py -v` — expected: 3 passed.
Run: `uv run pytest` — expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: core job-pipeline metrics across api, worker, retry, ticker"
```

---

### Task 9: Observable gauges — queue/saturation (ticker) + CPU utilization (worker) — USE method

This task is the **USE** half of the plan's metrics (Utilization/Saturation for infrastructure — see Global Constraints):
- **Saturation:** `queue.depth` (Redis consumer-group lag per stream) and `queue.scheduled` (delayed zset size) are the Redis-side backlog; `jobs.saturation` is the new Postgres-side signal — job counts grouped by status, so "how many jobs are pending vs. processing" is a direct read (`jobs.saturation{status="pending"}` vs `jobs.saturation{status="processing"}`), not a derived query.
- **Utilization:** `process.cpu.utilization` on the worker process. This system does not run separate worker pods per job type (one worker pool drains all priority streams — see `app/worker/runner.py`'s `read_priority`), so there is no isolated "report-generating worker" to instrument in isolation; the CPU gauge instruments the worker process generically, and `job.processing.duration{type="report"}` (Task 8) is how the report-vs-email cost difference actually shows up. Correlating the two (a worker's CPU gauge rising while its `job.processing.duration{type="report"}` histogram is active) is the intended dashboard read.

**Files:**
- Modify: `app/ticker/runner.py`, `app/worker/runner.py`, `app/repository.py`, `pyproject.toml`/`uv.lock` (via `uv add`)
- Test: `tests/integration/test_ticker.py` (add tests), `tests/integration/test_worker.py` (add tests)

**Interfaces:**
- Consumes: Redis client, `settings.priority_streams`, `settings.delayed_zset` (ticker); `session_factory` (ticker, for the saturation gauge); nothing new for the worker gauge beyond the `psutil` dependency.
- Produces:
  - `repo.count_by_status(session) -> list[tuple[JobStatus, int]]`
  - `queue_depth_observations(client, settings) -> list[Observation]`, `queue_scheduled_observations(client, settings) -> list[Observation]`, `job_status_observations(session_factory, settings) -> list[Observation]`, `register_ticker_gauges(client, session_factory, settings) -> None` (called from ticker's `run_forever` when `settings.otel_enabled`)
  - `cpu_utilization_observations() -> list[Observation]`, `register_worker_resource_gauges() -> None` (called from worker's `run_forever` when `settings.otel_enabled`)

- [ ] **Step 1: Add the `psutil` dependency**

```bash
uv add psutil
```

- [ ] **Step 2: Write the failing tests**

Add to `tests/integration/test_ticker.py`:

```python
from app.core.db import make_session_factory
from app.queue.consumer import ensure_group
from app.ticker.runner import (
    job_status_observations,
    queue_depth_observations,
    queue_scheduled_observations,
)


def test_queue_depth_observations(redis_client, test_settings):
    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    redis_client.xadd(test_settings.stream_high, {"job_id": "x"})
    observations = queue_depth_observations(redis_client, test_settings)
    by_stream = {o.attributes["stream"]: o.value for o in observations}
    assert by_stream["high"] == 1
    assert by_stream["normal"] == 0


def test_queue_scheduled_observations(redis_client, test_settings):
    redis_client.zadd(test_settings.delayed_zset, {"a": 1.0, "b": 2.0})
    observations = queue_scheduled_observations(redis_client, test_settings)
    assert observations[0].value == 2


def test_observations_swallow_redis_errors(test_settings):
    import redis as redis_lib

    dead = redis_lib.Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.2)
    assert queue_depth_observations(dead, test_settings) == []
    assert queue_scheduled_observations(dead, test_settings) == []


def test_job_status_observations_counts_pending_and_processing(
    db_session, test_settings, pg_engine
):
    repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)  # -> processing

    observations = job_status_observations(make_session_factory(pg_engine), test_settings)
    by_status = {o.attributes["status"]: o.value for o in observations}
    assert by_status["pending"] == 1
    assert by_status["processing"] == 1
    assert by_status["completed"] == 0  # zero-filled, not just omitted


def test_job_status_observations_swallow_db_errors(test_settings):
    from app.core.db import make_engine

    dead_factory = make_session_factory(make_engine("postgresql+psycopg://u:p@127.0.0.1:1/x"))
    assert job_status_observations(dead_factory, test_settings) == []
```

(`repo` and `JobType` are already imported at the top of `tests/integration/test_ticker.py`.)

Add to `tests/integration/test_worker.py`:

```python
from app.worker.runner import cpu_utilization_observations


def test_cpu_utilization_observations_returns_one_point():
    observations = cpu_utilization_observations()
    assert len(observations) == 1
    assert isinstance(observations[0].value, float)
    assert observations[0].value >= 0.0
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_ticker.py -v -k "observations"`
Expected: FAIL — `ImportError: cannot import name 'job_status_observations'`.

Run: `uv run pytest tests/integration/test_worker.py -v -k cpu_utilization`
Expected: FAIL — `ImportError: cannot import name 'cpu_utilization_observations'`.

- [ ] **Step 4: Add `repo.count_by_status`**

In `app/repository.py`, add (mirrors the inline query already in `app/observability.py:gather_stats`, extracted so the ticker can reuse it):

```python
def count_by_status(session: Session) -> list[tuple[JobStatus, int]]:
    return session.execute(
        select(Job.status, func.count()).group_by(Job.status)
    ).all()
```

(`select` and `func` are already imported at the top of `app/repository.py`.)

- [ ] **Step 5: Implement the ticker gauges in `app/ticker/runner.py`**

Add imports:

```python
from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation
from sqlalchemy.exc import SQLAlchemyError

from app.observability import zero_fill_status_counts
```

Add functions:

```python
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
```

In `run_forever`, after the `ensure_group` loop:

```python
    if settings.otel_enabled:
        register_ticker_gauges(client, session_factory, settings)
```

(Gated so repeated `run_forever` calls in tests don't register duplicate instruments. `session_factory` is already a local in `run_forever`.)

- [ ] **Step 6: Implement the worker CPU gauge in `app/worker/runner.py`**

Add imports:

```python
import psutil
from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

_process = psutil.Process()
```

Add functions (near the top, with the other module-level helpers):

```python
def cpu_utilization_observations() -> list[Observation]:
    """Ratio (0.0-1.0) of one CPU core the worker process has used since the
    last call. The first call after process start establishes a baseline and
    reports 0.0 — expected, since there is no prior interval to measure."""
    return [Observation(_process.cpu_percent(interval=None) / 100.0)]


def register_worker_resource_gauges() -> None:
    meter = metrics.get_meter("app.worker")
    meter.create_observable_gauge(
        "process.cpu.utilization",
        callbacks=[lambda options: cpu_utilization_observations()],
        unit="1",
        description="Worker process CPU utilization (ratio of one core)",
    )
```

In `run_forever`, after the `ensure_group` loop:

```python
    if settings.otel_enabled:
        register_worker_resource_gauges()
```

(Gated the same way as the ticker's gauges.)

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_ticker.py -v` — expected: all pass.
Run: `uv run pytest tests/integration/test_worker.py -v` — expected: all pass.
Run: `uv run pytest` — expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: USE-method observable gauges — queue/Postgres saturation and worker CPU utilization"
```

---

### Task 10: Health check module — `Heartbeat`, `HealthServer`, thresholds

**Files:**
- Create: `app/core/healthcheck.py`
- Modify: `app/core/db.py:6-7` (`pool_timeout=5` — bounded pool-checkout wait so a saturated pool 503s instead of hanging the probe)
- Test: `tests/integration/test_healthcheck.py` (create), `tests/unit/test_healthcheck_thresholds.py` (create)

**Interfaces:**
- Consumes: SQLAlchemy `Engine`, redis client.
- Produces:
  - `Heartbeat` with `beat() -> None`, `age_seconds() -> float` (lock-protected)
  - `HealthServer(port, heartbeat, max_heartbeat_age_s, engine, redis_client)` with `.start()`, `.stop()`, `.port` (resolved — pass 0 for ephemeral)
  - `worker_heartbeat_threshold_s(settings) -> float`, `ticker_heartbeat_threshold_s(settings) -> float`
  - Endpoints: `GET /health` (liveness: heartbeat freshness), `GET /ready` (readiness: borrowed engine + redis probes); both return `{"status": ..., "checks": {...}}` with 200/503.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_healthcheck_thresholds.py`:

```python
from app.core.config import Settings
from app.core.healthcheck import (
    ticker_heartbeat_threshold_s,
    worker_heartbeat_threshold_s,
)


def _settings(**overrides) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://u:p@localhost/x",
        redis_url="redis://localhost:6379/0",
        **overrides,
    )


def test_worker_threshold():
    settings = _settings(block_ms=5000, job_handler_timeout_s=45.0)
    assert worker_heartbeat_threshold_s(settings) == 5.0 + 45.0 + 10.0


def test_ticker_threshold_floor():
    assert ticker_heartbeat_threshold_s(_settings(ticker_interval_s=1.0)) == 10.0


def test_ticker_threshold_scales():
    assert ticker_heartbeat_threshold_s(_settings(ticker_interval_s=4.0)) == 20.0
```

Create `tests/integration/test_healthcheck.py`:

```python
import httpx
import pytest
import redis as redis_lib

from app.core.healthcheck import Heartbeat, HealthServer


@pytest.fixture
def health_server(pg_engine, redis_client):
    heartbeat = Heartbeat()
    server = HealthServer(
        port=0,
        heartbeat=heartbeat,
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=redis_client,
    )
    server.start()
    yield server, heartbeat
    server.stop()


def _get(server: HealthServer, path: str) -> httpx.Response:
    return httpx.get(f"http://127.0.0.1:{server.port}{path}", timeout=5.0)


def test_health_ok_when_heartbeat_fresh(health_server):
    server, heartbeat = health_server
    heartbeat.beat()
    response = _get(server, "/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {"loop": "ok"}}


def test_health_503_when_heartbeat_stale(pg_engine, redis_client):
    server = HealthServer(
        port=0,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=0.0,  # everything is instantly stale
        engine=pg_engine,
        redis_client=redis_client,
    )
    server.start()
    try:
        response = _get(server, "/health")
        assert response.status_code == 503
        assert response.json()["status"] == "unavailable"
    finally:
        server.stop()


def test_ready_ok_with_live_dependencies(health_server):
    server, _ = health_server
    response = _get(server, "/ready")
    assert response.status_code == 200
    assert response.json()["checks"] == {"postgres": "ok", "redis": "ok"}


def test_ready_503_when_redis_down(pg_engine):
    dead = redis_lib.Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.2)
    server = HealthServer(
        port=0,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=dead,
    )
    server.start()
    try:
        response = _get(server, "/ready")
        assert response.status_code == 503
        assert response.json()["checks"]["redis"] == "error"
        assert response.json()["checks"]["postgres"] == "ok"
    finally:
        server.stop()


def test_unknown_path_404(health_server):
    server, _ = health_server
    assert _get(server, "/nope").status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_healthcheck_thresholds.py tests/integration/test_healthcheck.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.core.healthcheck'`.

- [ ] **Step 3: Create `app/core/healthcheck.py`**

```python
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import Settings


def worker_heartbeat_threshold_s(settings: Settings) -> float:
    """A worker legitimately blocks on XREADGROUP then runs a job; busy is
    healthy, hung is not."""
    return settings.block_ms / 1000 + settings.job_handler_timeout_s + 10.0


def ticker_heartbeat_threshold_s(settings: Settings) -> float:
    return max(10.0, 5 * settings.ticker_interval_s)


class Heartbeat:
    """Thread-safe last-beat tracker for a main loop. A bare float write is
    GIL-atomic today; the lock makes the invariant explicit and future-proof."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last = time.monotonic()

    def beat(self) -> None:
        with self._lock:
            self._last = time.monotonic()

    def age_seconds(self) -> float:
        with self._lock:
            return time.monotonic() - self._last


class HealthServer:
    """Daemon-thread HTTP server: /health = liveness (loop heartbeat),
    /ready = readiness probing the app's own engine pool and Redis client."""

    def __init__(
        self,
        port: int,
        heartbeat: Heartbeat,
        max_heartbeat_age_s: float,
        engine: Engine,
        redis_client: redis.Redis,
    ) -> None:
        self._heartbeat = heartbeat
        self._max_age = max_heartbeat_age_s
        self._engine = engine
        self._redis = redis_client
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 — http.server API
                if self.path == "/health":
                    status, body = outer._liveness()
                elif self.path == "/ready":
                    status, body = outer._readiness()
                else:
                    status, body = 404, {"status": "not found"}
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: object) -> None:
                """Probe hits are noise; suppress default stderr logging."""

        self._server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="health-server", daemon=True
        )

    def _liveness(self) -> tuple[int, dict]:
        age = self._heartbeat.age_seconds()
        if age <= self._max_age:
            return 200, {"status": "ok", "checks": {"loop": "ok"}}
        return 503, {
            "status": "unavailable",
            "checks": {"loop": f"stale ({age:.0f}s > {self._max_age:.0f}s)"},
        }

    def _readiness(self) -> tuple[int, dict]:
        checks: dict[str, str] = {}
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            checks["postgres"] = "ok"
        except Exception:  # noqa: BLE001 — any failure means not ready
            checks["postgres"] = "error"
        try:
            self._redis.ping()
            checks["redis"] = "ok"
        except redis.RedisError:
            checks["redis"] = "error"
        ok = all(value == "ok" for value in checks.values())
        return (200 if ok else 503), {
            "status": "ok" if ok else "unavailable",
            "checks": checks,
        }

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
```

- [ ] **Step 4: Bound the pool-checkout wait**

In `app/core/db.py`:

```python
def make_engine(database_url: str) -> Engine:
    # pool_timeout=5: a saturated pool turns into a fast 503 on /ready
    # instead of a 30s hang (also bounds app-side checkout waits).
    return create_engine(
        database_url, pool_pre_ping=True, pool_timeout=5, future=True
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_healthcheck_thresholds.py tests/integration/test_healthcheck.py -v`
Expected: 9 passed.
Run: `uv run pytest` — expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: health check server with split liveness/readiness endpoints"
```

---

### Task 11: Wire health servers + heartbeats into worker and ticker

**Files:**
- Modify: `app/core/config.py` (add `health_port`), `app/worker/runner.py` (`run_forever`), `app/ticker/runner.py` (`run_forever`)
- Test: existing suite (wiring; endpoint behavior already covered by Task 10 — compose probing verified manually in Task 14)

**Interfaces:**
- Consumes: `Heartbeat`, `HealthServer`, threshold helpers from Task 10.
- Produces: `Settings.health_port: int | None = None`; worker/ticker start a health server iff `health_port` is set, beat every loop iteration, and stop the server on shutdown.

- [ ] **Step 1: Add the setting**

In `app/core/config.py`, after `otel_exporter_otlp_endpoint`:

```python
    health_port: int | None = None
```

- [ ] **Step 2: Wire the worker**

In `app/worker/runner.py`, import:

```python
from app.core.healthcheck import Heartbeat, HealthServer, worker_heartbeat_threshold_s
```

In `run_forever`, after `client = create_redis_client(...)`:

```python
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
        health_server.start()
```

First line inside the `while not _should_stop():` body: `heartbeat.beat()`.

Before `client.close()` at the end: 

```python
    if health_server is not None:
        health_server.stop()
```

- [ ] **Step 3: Wire the ticker**

Same pattern in `app/ticker/runner.py` with `ticker_heartbeat_threshold_s(settings)`; `heartbeat.beat()` goes at the top of the `while` body (before the `try:`), and `health_server.stop()` before `client.close()`.

- [ ] **Step 4: Run the full suite** (default `health_port=None` — nothing binds in tests)

Run: `uv run pytest` — expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: wire heartbeat + health server into worker and ticker loops"
```

---

### Task 12: Split the API's `/health` and `/ready`

**Files:**
- Modify: `app/schemas/api.py`, `app/api/routes.py:34-46`, `docker-compose.yml:51-53` (api healthcheck URL)
- Test: `tests/integration/test_health_stats.py` (update)

**Interfaces:**
- Consumes: existing `check_readiness`.
- Produces: `GET /health` → unconditional `200 {"status": "ok"}` (liveness); `GET /ready` → old dependency-checking behavior; `LivenessResponse` schema.

- [ ] **Step 1: Update the tests**

In `tests/integration/test_health_stats.py`: point every existing dependency-check health test at `/ready` instead of `/health` (same assertions — 200 with `{"postgres": "ok", "redis": "ok"}`, 503 when a dependency is broken). Add:

```python
def test_health_is_pure_liveness(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_stays_200_when_redis_down(client):
    # Same broken-redis setup the existing 503 test uses (swap
    # app.state.redis for a client pointing at a closed port), then:
    response = client.get("/health")
    assert response.status_code == 200
```

(Reuse the module's existing broken-redis pattern verbatim for the second test.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_health_stats.py -v`
Expected: FAIL — `/ready` returns 404; `/health` body includes `checks`.

- [ ] **Step 3: Implement**

`app/schemas/api.py` — add above `HealthChecks`:

```python
class LivenessResponse(BaseModel):
    status: str
```

`app/api/routes.py` — replace the `health` route with both routes (import `LivenessResponse`):

```python
@router.get("/health", response_model=LivenessResponse)
def health() -> LivenessResponse:
    """Liveness: the process is serving requests."""
    return LivenessResponse(status="ok")


@router.get("/ready", response_model=HealthResponse)
def ready(
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
):
    checks = check_readiness(session, client)
    ok = all(value == "ok" for value in checks.values())
    if not ok:
        return JSONResponse(
            status_code=503,
            content={"status": "unavailable", "checks": checks},
        )
    return HealthResponse(status="ok", checks=HealthChecks(**checks))
```

`docker-compose.yml` — in the `api` service healthcheck, change the probed URL from `http://localhost:8000/health` to `http://localhost:8000/ready`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_health_stats.py -v` — expected: all pass.
Run: `uv run pytest` — expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: split API /health (liveness) and /ready (readiness)"
```

---

### Task 13: Entrypoint telemetry wiring — configure, instrument FastAPI, flush on shutdown

**Files:**
- Modify: `app/main.py`, `app/worker/runner.py`, `app/ticker/runner.py`
- Test: existing suite (disabled path); enabled path verified manually in Task 14

**Interfaces:**
- Consumes: `configure_telemetry` / `shutdown_telemetry` from Task 3.
- Produces: each service configures telemetry at startup (before engine creation, so SQLAlchemy instrumentation hooks it) and flushes on the signal → flag → loop-exit path.

- [ ] **Step 1: Wire the API (`app/main.py`)**

```python
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from app.core.telemetry import configure_telemetry, shutdown_telemetry

def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    configure_telemetry(settings, "jobs-api")  # BEFORE make_engine

    engine = make_engine(settings.database_url)
    ...

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        for stream in settings.ordered_streams:
            ensure_group(redis_client, stream, settings.consumer_group)
        yield
        redis_client.close()
        engine.dispose()
        shutdown_telemetry()

    app = FastAPI(title="Job Processor", lifespan=lifespan)
    ...
    app.include_router(router)
    if settings.otel_enabled:
        FastAPIInstrumentor.instrument_app(app)
    return app
```

- [ ] **Step 2: Wire the worker (`app/worker/runner.py`)**

Import `from app.core.telemetry import configure_telemetry, shutdown_telemetry`. First line of `run_forever`:

```python
    configure_telemetry(settings, "jobs-worker", instance_id=CONSUMER_NAME)
```

(before `make_engine`). At the end, the shutdown order becomes:

```python
    log.info("worker.stopped", extra={"exit_code": exit_code})
    if health_server is not None:
        health_server.stop()
    shutdown_telemetry()  # flush before closing app connections (spec §1)
    client.close()
    engine.dispose()
    return exit_code
```

Verify (read, don't change) that the existing SIGTERM/SIGINT handlers still flip `shutting_down["flag"]` so the loop exits and reaches this flush — that chain is the pod-termination guarantee.

- [ ] **Step 3: Wire the ticker (`app/ticker/runner.py`)**

Same: `configure_telemetry(settings, "jobs-ticker")` as the first line of `run_forever`; shutdown order `health_server.stop()` → `shutdown_telemetry()` → `client.close()` → `engine.dispose()`. Verify the signal handlers are intact.

- [ ] **Step 4: Run the full suite** (otel disabled everywhere in tests → all no-ops; repeated `run_forever` calls must stay safe)

Run: `uv run pytest` — expected: all pass.
Run: `uv run ruff check --fix && uv run ruff format` — expected: clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: configure and flush telemetry in all three entrypoints"
```

---

### Task 14: Collector config, docker-compose, manual end-to-end verification

**Files:**
- Create: `otel-collector-config.yaml`
- Modify: `docker-compose.yml`

No pytest here — infra config is verified manually (user preference).

- [ ] **Step 1: Create `otel-collector-config.yaml`**

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:

exporters:
  # Prints received telemetry to the collector's stdout. Swap in a real
  # backend (Tempo/Jaeger/Prometheus/Loki) here without touching app code.
  debug:
    verbosity: normal

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
```

- [ ] **Step 2: Update `docker-compose.yml`**

Add the collector service (no app service depends on it — if it is down the SDK retries then drops):

```yaml
  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.116.1
    command: ["--config=/etc/otelcol/config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otelcol/config.yaml:ro
```

Add to the `environment` of `api`, `worker`, and `ticker`:

```yaml
      OTEL_ENABLED: "true"
      OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4317
```

Add to `worker` and `ticker` environment: `HEALTH_PORT: "8001"`, plus healthcheck blocks (liveness — a wedged loop is restart-actionable; Redis being down is not fixed by restarting a worker):

```yaml
    healthcheck:
      test: ["CMD", "python", "-c",
             "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8001/health').status==200 else 1)"]
      interval: 15s
      timeout: 12s
      retries: 5
      start_period: 10s
```

(The `api` healthcheck already probes `/ready` after Task 12.)

- [ ] **Step 3: Manual verification**

```bash
docker compose up --build -d
docker compose ps       # api, worker, ticker all reach "healthy"
```

Submit an immediate job and a scheduled one:

```bash
curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d '{"type": "email", "payload": {"to": "a@b.com", "subject": "hi"}}'
curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" \
  -d "{\"type\": \"email\", \"payload\": {\"to\": \"a@b.com\", \"subject\": \"later\"}, \"scheduled_at\": \"$(python -c "from datetime import datetime,timedelta,timezone;print((datetime.now(timezone.utc)+timedelta(seconds=10)).isoformat())")\"}"
```

Then check, in `docker compose logs otel-collector`:
- Spans named `POST /jobs`, `send jobs:stream:normal`, `process job` sharing one trace ID for the immediate job.
- For the scheduled job (after ~10s): `ticker.promote` appears, and the `process job` span's trace ID equals the original `POST /jobs` trace ID — the whole-system propagation requirement.
- Metrics `jobs.submitted`, `jobs.processed`, `job.queue.wait`, `queue.depth`, `queue.scheduled` (RED + Redis-side USE).
- `jobs.saturation{status=...}` reflecting real pending/processing counts, and `process.cpu.utilization` from the worker (Postgres/CPU-side USE, Task 9).
- Log records with trace IDs attached; third-party logs absent below WARNING.

Also confirm `docker compose logs api` still shows JSON stdout logs, and `docker compose stop worker` produces a clean `worker.stopped` (signal → flush chain).

- [ ] **Step 4: Commit**

```bash
git add otel-collector-config.yaml docker-compose.yml
git commit -m "feat: otel collector service and health/telemetry compose wiring"
```

---

## Final acceptance

- [ ] `uv run pytest` — entire suite green.
- [ ] `uv run ruff check --fix && uv run ruff format` — clean.
- [ ] `grep -r structlog app/ tests/ pyproject.toml` — no matches.
- [ ] Manual compose verification (Task 14 Step 3) done, including the scheduled-job single-trace check.
