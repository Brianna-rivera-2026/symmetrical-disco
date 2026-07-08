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
| Worker/ticker health | Tiny HTTP `/health` server per process |
| Metrics scope | Core job-pipeline metrics + auto-instrumentation |
| Logs | Dual: keep structlog stdout JSON, additionally export via OTLP; trace correlation in both |
| Trace propagation | Persist trace context on the job row (Approach A) |

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
  existing stdout handler, never replacing it)

and applies programmatic auto-instrumentation:

- `SQLAlchemyInstrumentor` and `RedisInstrumentor` in all three services
- `FastAPIInstrumentor` in the API only

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

### Dependencies (via `uv add`)

`opentelemetry-sdk`, `opentelemetry-exporter-otlp`,
`opentelemetry-instrumentation-fastapi`,
`opentelemetry-instrumentation-sqlalchemy`,
`opentelemetry-instrumentation-redis`.

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

The structlog → stdout JSON pipeline is unchanged. Two additions:

1. **Trace correlation:** a structlog processor appends `trace_id` and
   `span_id` (from the active span, when recording) to every event dict — on
   stdout and OTLP alike.
2. **OTLP export:** the OTel `LoggingHandler` is added to the root logger
   alongside the stdout handler. structlog already routes through stdlib
   `logging`, so all app and uvicorn logs flow to the collector with trace
   context attached by the SDK. Not added when `otel_enabled=False`.

## 5. Health checks (worker & ticker)

New shared module `app/core/healthcheck.py`: a daemon-thread HTTP server
(stdlib `http.server`, no new dependency) started at the top of each
`run_forever`, stopped on shutdown.

- Setting `health_port: int | None = None` — server disabled by default;
  docker-compose sets `HEALTH_PORT=8001` for worker and ticker. Tests bind
  port 0 (ephemeral).
- **`GET /health`** returns the API's shape: `200 {"status": "ok", "checks":
  {...}}` or `503`. Checks:
  - `loop` — the main loop stamps a shared heartbeat timestamp each iteration;
    stale beyond a threshold → error. Thresholds: worker
    `block_ms/1000 + job_handler_timeout_s + 10s` (a worker legitimately
    blocks on XREADGROUP then runs a job — busy is healthy, hung is not);
    ticker: `max(10s, 5 × ticker_interval_s)`.
  - `redis` — `PING`; `postgres` — `SELECT 1` (same pattern as the API's
    `check_readiness`; a connection per probe is cheap at healthcheck
    frequency).
- docker-compose healthcheck blocks for worker and ticker hit
  `http://localhost:8001/health` (same `python -c urllib` style as the API's).

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
- Logs: the structlog processor adds `trace_id` when a span is active and
  nothing otherwise.
- Health: 200 with a fresh heartbeat; 503 with a stale heartbeat or Redis
  down.
- Compose/collector wiring is verified manually (`docker compose up`, watch
  the collector's debug output) — no pytest for infra config.
