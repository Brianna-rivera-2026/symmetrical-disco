# OpenTelemetry & Health Checks — Design

**Date:** 2026-07-08
**Requirement:** `docs/requirements/07-opentelemetry.md`

Add OpenTelemetry logs, metrics, and traces across the API, worker, and ticker;
propagate traces through the whole job lifecycle (including scheduled jobs and
retries); add health check endpoints for the worker and ticker.

## Decisions made during brainstorming

| Question | Decision |
|---|---|
| Telemetry backend | OTel Collector only (debug exporter; real backend wired later via collector config) |
| Worker/ticker health | Tiny HTTP server per process with split `/health` (liveness) and `/ready` (readiness) |
| Metrics scope | Core job-pipeline metrics + auto-instrumentation |
| Logs | Remove structlog entirely; stdlib `logging` + OTel export, structured JSON stdout kept |
| Trace propagation | Persist trace context on the job row (Approach A) |
| Health probe resources | Borrow from the app's SQLAlchemy engine pool and global Redis client (no fresh connections) |

## 1. Architecture & infrastructure

### `app/core/telemetry.py`

Single entry point `configure_telemetry(settings, service_name)` called once at
startup by each entrypoint:

- API: in `create_app` — `service.name=jobs-api`
- Worker: in `worker/runner.py:run_forever` — `service.name=jobs-worker`,
  `service.instance.id` = the consumer name
- Ticker: in `ticker/runner.py:run_forever` — `service.name=jobs-ticker`

It sets up, against a shared `Resource`:

- `TracerProvider` + `BatchSpanProcessor` + OTLP gRPC span exporter
- `MeterProvider` + `PeriodicExportingMetricReader` + OTLP gRPC metric exporter
- `LoggerProvider` + `BatchLogRecordProcessor` + OTLP gRPC log exporter,
  attached to the root stdlib logger via `LoggingHandler` (alongside the
  stdout handler, never replacing it)

and applies programmatic auto-instrumentation:

- `SQLAlchemyInstrumentor` and `RedisInstrumentor` in all three services
- `FastAPIInstrumentor` in the API only
- `LoggingInstrumentor` in all three services (injects `otelTraceID` /
  `otelSpanID` into stdlib log records so the stdout formatter can print them)

**Gating:** `settings.otel_enabled: bool = False` (default). When False,
`configure_telemetry` is a no-op: no providers, no exporters, no network
chatter. docker-compose sets `OTEL_ENABLED=true` explicitly. The OTLP endpoint
comes from `settings.otel_exporter_otlp_endpoint` (default
`http://localhost:4317`).

**Shutdown:** providers are flushed and shut down on exit — API lifespan
teardown, and at the end of each `run_forever` — so short-lived processes do
not drop buffered telemetry.

### docker-compose

- New service `otel-collector` (`otel/opentelemetry-collector-contrib`) with a
  checked-in `otel-collector-config.yaml`: OTLP receiver (gRPC :4317), batch
  processor, `debug` exporter for traces, metrics, and logs. Swapping in a real
  backend later means editing only this file.
- `api`, `worker`, `ticker` get `OTEL_ENABLED=true` and
  `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317`.
- App services do **not** `depends_on` the collector: if it is down, the SDK
  retries then drops. Observability must never take down the pipeline.

### Dependencies

Added (via `uv add`): `opentelemetry-sdk`, `opentelemetry-exporter-otlp`,
`opentelemetry-instrumentation-fastapi`,
`opentelemetry-instrumentation-sqlalchemy`,
`opentelemetry-instrumentation-redis`,
`opentelemetry-instrumentation-logging`.

Removed (via `uv remove`): `structlog` (see §4). CLAUDE.md is updated in the
same change: drop structlog from the tech stack list and reword guideline 3 to
"structured logging with job context via stdlib logging".

## 2. Tracing & propagation

### Context capture at submission

New nullable JSONB column `trace_context` on `jobs` (one Alembic migration).
Written once at job creation with the W3C fields captured from the active
context: `{"traceparent": "...", "tracestate": "..."}` (tracestate omitted when
empty). Never updated afterwards.

### Producer side

`queue/producer.py:enqueue` starts a producer span `send {stream}`
(kind=PRODUCER, `messaging.system=redis`) and injects the **current** OTel
context into the XADD fields (`traceparent`, optional `tracestate`, alongside
`job_id`).

- API direct enqueue and the retry endpoint run inside the request span, so the
  request context propagates naturally.
- Ticker promotion (`promote_due`), reconcile (`reconcile_orphans`), and reaper
  re-enqueue (`_reap_one`) have no live request context: they attach the
  job's stored `trace_context` as the context before calling `enqueue`, so the
  message carries the **original** submission trace. The promote-path query
  that already fetches priorities also fetches `trace_context`.
- `delayed.schedule` needs no change: the zset only ever stores the job id;
  context re-enters via the DB column at promotion time.

### Consumer side

In the worker loop, per message: extract the context from message fields and
start a `process job` span (kind=CONSUMER) with it as parent, wrapping
`process_job`. Attributes: `job.id`, `job.type`, `job.priority`,
`job.attempt`, `job.outcome` (the existing `Outcome.label`),
`messaging.system=redis`, stream name. On handler exceptions the span records
the exception and sets error status (note: a retryable failure is still an
error on *this* span; the job-level outcome lives in `job.outcome`). A missing
or malformed `traceparent` yields a new root trace — extraction never raises.

**Result:** submit → enqueue → process, and submit → schedule → promote →
process → retry → process, each land in a single trace; attempts are
distinguished by `job.attempt`.

### Ticker's own spans

Root spans `ticker.promote`, `ticker.reconcile`, `ticker.reap`, created **only
when the batch is non-empty** (the loop ticks every second; empty ticks are
noise), with count attributes. Auto-instrumentation nests DB/Redis child spans
under all spans automatically.

## 3. Metrics

Instruments are defined in one module and created lazily from the global
MeterProvider (no-ops when OTel is disabled). Cardinality: job types ×
3 priorities × ~6 outcomes — tiny.

**API** (emitted in routes):

- `jobs.submitted` counter — attrs `{type, priority, scheduled: bool}`
- HTTP request metrics come from FastAPI auto-instrumentation.

**Worker** (emitted in `process_job` / the loop):

- `jobs.processed` counter — attrs `{type, priority, outcome}` where outcome is
  `Outcome.label` (`completed|retried|timeout|cancelled|skipped|lost`)
- `jobs.failed` counter — attrs `{type, priority}`; incremented inside
  `schedule_retry_or_fail` when attempts are exhausted and the job goes
  terminal-failed (so the ticker's reaper path — which also calls it for
  WorkerLost jobs — is counted too, under that process's service name)
- `job.processing.duration` histogram (seconds) — attrs `{type, outcome}`
- `job.queue.wait` histogram (seconds) — attrs `{stream}`; computed from the
  Redis stream message ID, which embeds the XADD timestamp (`<ms>-<seq>`) —
  exact per delivery, zero extra state

**Ticker:**

- `ticker.promoted`, `ticker.reaped`, `ticker.reconciled` counters
- Observable gauges (callback reads Redis once per export cycle):
  `queue.depth` per stream (consumer-group lag, same source as `/stats`) and
  `queue.scheduled` (ZCARD of the delayed zset). These live in the ticker
  because it is a singleton; workers would double-count.

## 4. Logs

**structlog is removed entirely.** OTel becomes the log pipeline; stdlib
`logging` becomes the app-facing API. `app/core/logging.py` is rewritten
(same public entry point `configure_logging`) with stdlib-only pieces:

1. **Structured stdout stays.** A small custom `logging.Formatter` (~20 lines,
   no new dependency) renders each record as one JSON line: timestamp, level,
   logger, message, all `extra` fields, bound context (below), and
   `otelTraceID`/`otelSpanID` when `LoggingInstrumentor` has injected them.
   `docker logs` output stays structured and trace-correlated.
2. **Bound job context survives.** structlog's `contextvars` binding
   (`job_id`, `message_id`, `stream`, `consumer`) is replaced by a
   `ContextVar[dict]` plus a `bind_log_context(**fields)` context manager and
   a `logging.Filter` on the stdout handler and the OTel `LoggingHandler`
   that merges the bound dict into every record. This keeps CLAUDE.md's
   "structured logging with job context" rule intact without structlog.
3. **OTLP export.** The OTel `LoggingHandler` on the root logger ships every
   record (app + uvicorn) to the collector; the SDK attaches trace context,
   and `extra`/bound fields become OTel log attributes. Not added when
   `otel_enabled=False`.

**Call-site refactor:** every module currently using
`structlog.get_logger(...)` moves to `logging.getLogger(...)`; key-value
calls (`log.info("job.completed", won=won)`) become
`log.info("job.completed", extra={"won": won})`;
`structlog.contextvars.bind_contextvars` / `bound_contextvars` call sites
(worker loop, ticker) switch to `bind_log_context`. Event names and fields
are preserved verbatim so existing log-based debugging habits keep working.
The uvicorn logger hijack in `configure_logging` stays as is.

## 5. Health checks

Liveness and readiness are separate endpoints on every service.

### Worker & ticker

New shared module `app/core/healthcheck.py`: a daemon-thread HTTP server
(stdlib `http.server`, no new dependency) started at the top of each
`run_forever`, stopped on shutdown. Setting `health_port: int | None = None`
— server disabled by default; docker-compose sets `HEALTH_PORT=8001` for
worker and ticker. Tests bind port 0 (ephemeral).

- **`GET /health`** (liveness) — loop heartbeat only. The main loop stamps a
  monotonic timestamp each iteration; reads and writes go through a
  `threading.Lock` (a bare float write is GIL-atomic today, but the lock
  makes the invariant explicit and future-proof). Stale beyond a threshold →
  `503`. Thresholds: worker `block_ms/1000 + job_handler_timeout_s + 10s`
  (a worker legitimately blocks on XREADGROUP then runs a job — busy is
  healthy, hung is not); ticker: `max(10s, 5 × ticker_interval_s)`.
- **`GET /ready`** (readiness) — dependency checks using the **application's
  own resources**, not fresh connections: `engine.connect()` borrows from the
  process's existing SQLAlchemy pool for `SELECT 1`, and the process's global
  Redis client issues `PING`. This proves the app can still talk to the
  stores through its configured resources. The engine and client created in
  `run_forever` are handed to the health server. Probes use short timeouts
  (bounded pool-checkout wait) so an exhausted pool degrades to a `503`
  rather than a hanging probe. Both endpoints return the API's response
  shape: `200/503 {"status": ..., "checks": {...}}`.

### API

For consistency, the API's endpoints split the same way: `GET /health`
becomes pure liveness (always `200 {"status": "ok"}` if the process serves
requests), and the existing dependency checks (`check_readiness`) move to a
new `GET /ready`. The API already uses its request-scoped session and shared
Redis client via dependencies, which satisfies the borrowed-resources rule.

### docker-compose probes

- `api`: probe `/ready` (it gates `depends_on` consumers and real traffic).
- `worker` / `ticker`: probe `/health` on port 8001 — a wedged loop is the
  restart-actionable signal; Redis being down is not fixed by restarting a
  worker. `/ready` remains available for humans and future orchestrators.
- All probes keep the existing `python -c urllib` one-liner style.

No dedicated health metrics: the heartbeat plus the queue gauges cover it.

## 6. Error handling

- Telemetry never takes down the pipeline: exporter/collector failures are
  absorbed by the SDK batch processors (retry, then drop with a warning).
- `otel_enabled=False` default keeps local runs and the test suite free of
  OTel side effects.
- Missing/NULL `trace_context` (pre-migration rows) and absent `traceparent`
  fields degrade to fresh traces, never errors.
- Health server bind failure logs and fails the process fast (compose restarts
  it) — a silently absent healthcheck is worse than a restart.

## 7. Testing

Pytest with in-memory OTel exporters (`InMemorySpanExporter`,
`InMemoryMetricReader`) — no collector required:

- Propagation round-trip: inject into a fields dict → extract → same trace_id.
- Worker parenting: enqueue with a traceparent, run one `process_job` cycle,
  assert the consumer span shares the producer trace_id and carries
  `job.outcome`.
- Ticker re-injection: `promote_due` on a job with stored `trace_context` →
  the XADD'd fields contain the original traceparent.
- Metrics: `jobs.processed` increments with the correct outcome attributes.
- Logs: the JSON formatter emits `extra` fields and bound context; trace ids
  appear when a span is active and are absent otherwise; `bind_log_context`
  nesting/reset behaves across threads.
- Health: `/health` → 200 with a fresh heartbeat, 503 once stale; `/ready` →
  200 with live deps, 503 when Redis is unreachable; API `/ready` carries the
  old `/health` checks and API `/health` is unconditional 200.
- Compose/collector wiring is verified manually (`docker compose up`, watch
  the collector's debug output) — no pytest for infra config.
