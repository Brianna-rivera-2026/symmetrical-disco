# Distributed Job Processing — Phase 1 (Basic Flow) Design

**Date:** 2026-06-30
**Status:** Approved design, ready for implementation planning
**Scope:** Phase 1 only — the basic flow `Submit → Queue → Process → Complete/Fail` for the three mock job types. Advanced features (priority, scheduling, retry/backoff, batch+progress, idempotency, cancel/retry endpoints, health-with-stats) are explicitly **out of scope** and deferred to later specs.

---

## 1. Goal & Constraints

Build the basic flow of a distributed background job processing system: accept job submissions over an API, queue them, process them with multiple concurrent worker processes, and persist their state and results.

**Fixed by `CLAUDE.md` (not re-decided here):**

- FastAPI for the API
- PostgreSQL as the source of truth
- Redis for the queue — **Redis Streams + one Consumer Group** for dispatch
- SQLAlchemy 2.0 + Alembic
- pytest for testing
- Docker Compose for orchestration
- structlog for structured logging
- No `print` — structured logging with job context only
- All Python tooling via `uv` (`uv run …`, `uv add …`)

**Decisions made during brainstorming:**

| # | Decision | Choice |
|---|----------|--------|
| 1 | Scope | Phase 1 (basic flow) only; leftovers deferred |
| 2 | Crash recovery | **Describe-only** — rely on Redis Streams keeping unacked messages in the PEL; document the recovery flow, do not build a reaper |
| 3 | Worker execution model | **One job per process**, scale concurrency via replicas on a shared consumer group |
| 4 | List pagination | **Cursor (keyset)**, not offset |
| 5 | Payload validation | **Discriminated (tagged) union** in a **shared package**, validated by both API and worker |
| 6 | Delivery semantics | **At-least-once** — claim-guard + ack-after-commit |

---

## 2. Architecture

```
┌──────────┐   POST /jobs    ┌──────────────┐   INSERT (pending)   ┌────────────┐
│  Client  │ ──────────────► │  API service │ ───────────────────► │ PostgreSQL │
└──────────┘                 │  (FastAPI)   │   COMMIT, then…      │ (truth)    │
      ▲                      └──────┬───────┘                      └─────▲──────┘
      │  GET /jobs/{id}             │ XADD job_id (after commit)         │ read job,
      │  GET /jobs?filters          ▼                                    │ update state
      │                      ┌──────────────┐                            │
      └──────────────────────│ Redis Stream │                           │
                             │ + consumer   │ XREADGROUP / XACK   ┌──────┴───────┐
                             │   group      │ ◄─────────────────► │ Worker × N   │
                             └──────────────┘                     │ (1 job each) │
                                                                  └──────────────┘
```

**Components**

- **API service** (FastAPI / uvicorn) — accepts submissions, serves status and list queries. On submit it writes the job to Postgres, commits, then `XADD`s the **job id only** to the stream. Redis carries a pointer; Postgres holds the truth.
- **Redis Stream** `jobs:stream` with a single consumer group `workers`. Every worker replica is a competing consumer in that group.
- **Worker process × N** — identical processes. Each pulls **one** job at a time, runs it, updates Postgres, then acks. Scale with `docker compose up --scale worker=N`.
- **PostgreSQL** — authoritative job state and results.

API and worker share one Docker image with different entrypoints (`app.main:app` vs `python -m app.worker`).

---

## 3. Data Model — `jobs` table

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | server default `gen_random_uuid()` |
| `type` | enum/text | `email` \| `webhook` \| `report` |
| `payload` | JSONB | validated per-type on submit |
| `status` | enum | `scheduled, pending, processing, completed, failed, cancelled` |
| `result` | JSONB, null | set on completion |
| `error` | JSONB, null | `{type, message}` on failure |
| `created_at` | timestamptz | default `now()` |
| `started_at` | timestamptz, null | set at `pending → processing` |
| `completed_at` | timestamptz, null | set at terminal state |

**Indexes:** `status`, `type`, and a composite `(created_at, id)` (required for cursor pagination's row-value compare and ordering).

**Status enum note:** The full enum from the basic-flow requirements is defined up front, including `scheduled` and `cancelled`, even though Phase 1 never transitions into those two. This avoids an enum migration when the deferred features land. **No leftover columns** (priority, attempts, idempotency_key, progress, etc.) are added in Phase 1.

### State machine (Phase 1)

```
pending ──► processing ──► completed
                       └─► failed
(scheduled, cancelled defined in enum but unused in Phase 1)
```

No retries in Phase 1 — a handler failure goes to `failed` and stays there.

---

## 4. API Contract

All JSON. FastAPI + Pydantic validation.

### `POST /jobs` — submit a job

Request envelope:

```json
{ "type": "email", "payload": { "to": "a@b.com", "subject": "Hi" } }
```

- Payload validated via the shared discriminated union (see §5). Unknown `type` or bad payload → **`422`** before anything is written.
- Creates the job (`pending`), commits, then enqueues (per the §6 invariant).
- Response **`202 Accepted`**:

```json
{ "id": "uuid", "type": "email", "status": "pending", "created_at": "..." }
```

### `GET /jobs/{id}` — status / result / error

- `200` with the full job (status, result, error, all timestamps).
- `404` if not found.

### `GET /jobs` — list with filters (cursor pagination)

- Query params: `status`, `type` (both optional, combinable), `limit` (default 50, capped), `cursor` (opaque).
- Stable total ordering on `(created_at DESC, id DESC)` — `id` breaks `created_at` ties.
- `cursor` is an opaque base64 of the last row's `(created_at, id)`; the next page does a row-value compare `WHERE (created_at, id) < (:c_created, :c_id)`.
- Response: `{ "items": [...], "next_cursor": "<opaque>" | null }` (`null` when exhausted).
- **Why keyset over offset:** pages do not shift when new jobs arrive mid-pagination, and there is no deep-offset scan cost.

### `GET /health` — liveness

- `200 {"status":"ok"}`. Minimal, for the Docker healthcheck. The richer *health-with-queue-stats* endpoint is a leftover — deferred.

### Per-type payload shapes (kept light, just enough to validate)

- **email:** `{ to, subject, body? }`
- **webhook:** `{ url, method? }`
- **report:** `{ report_type, params? }`

---

## 5. Shared Schemas — Discriminated (Tagged) Union

A shared, framework-agnostic `app/schemas/` package, imported by **both** the API and the worker — single source of truth.

- `JobType` enum (`email` / `webhook` / `report`).
- One Pydantic model per variant, each carrying a `type: Literal[...]` discriminator: `EmailPayload`, `WebhookPayload`, `ReportPayload`.
- `JobPayload = Annotated[Union[EmailPayload, WebhookPayload, ReportPayload], Field(discriminator="type")]` — Pydantic routes to the correct model by `type`.
- A `validate_payload(job_type, raw) -> <variant model>` helper that raises on mismatch.
- Per-type **result** models in the same package, so results are typed end-to-end.

**Adding a new job type = add one payload model + one result model + register the handler.** Nothing else changes.

**Two validation points:**

1. **API** calls `validate_payload` on submit → bad/unknown type → `422` before writing.
2. **Worker** calls the *same* `validate_payload` immediately before running the handler. If a payload is ever malformed (schema drift, manual DB edit), the worker fails the job with a precise validation error instead of crashing mid-handler.

The external envelope stays `{ "type", "payload" }` (clean mapping to the two DB columns); the discriminator is applied during validation.

---

## 6. Enqueue Invariant

The `XADD` occurs **strictly after** the PostgreSQL transaction has successfully committed:

1. `BEGIN` → `INSERT` job (`pending`) → **`COMMIT`**
2. **Only after the commit returns successfully** → `XADD jobs:stream * job_id <id>`
3. Return `202` to the client.

**Why:** if the `XADD` happened first (or inside the transaction), a worker could pick up the id and `SELECT` the row before it was committed/visible, getting a spurious "not found."

**Documented trade-off (known limitation):** if the API process dies *between* the commit and the `XADD`, the job exists in Postgres as `pending` but was never enqueued — an orphaned job no worker sees. Phase 1 accepts this gap (consistent with describe-only recovery). Future mitigation — a transactional outbox, or a periodic sweeper that re-enqueues `pending` jobs older than N seconds — is recorded in `DECISIONS.md` as future work. A duplicate `XADD` is harmless thanks to the claim guard (§7), so retrying the `XADD` is safe.

---

## 7. Worker Dispatch & Lifecycle (Approach A — one job at a time)

**Startup:** `XGROUP CREATE jobs:stream workers $ MKSTREAM` (ignore `BUSYGROUP`). Each worker uses a **unique per-process consumer name**:

```python
CONSUMER_NAME = f"worker_{os.getenv('HOSTNAME', 'local')}_{uuid.uuid4().hex[:6]}"
```

The static prefix + UUID suffix guarantees uniqueness across replicas even if `HOSTNAME` is reused — a shared/static name would collapse all replicas into one logical consumer and destroy per-consumer PEL isolation.

**Loop:**

1. `XREADGROUP GROUP workers <CONSUMER_NAME> COUNT 1 BLOCK <BLOCK_MS> STREAMS jobs:stream >`
2. On a message → extract `job_id`, load the job from Postgres.
3. **Claim guard:** `UPDATE jobs SET status='processing', started_at=now() WHERE id=:id AND status='pending'`.
   - 0 rows updated → already processing/terminal (duplicate delivery) → `XACK` and skip. This is what makes at-least-once delivery safe.
4. `validate_payload(type, payload)` (shared schema) → run the handler for that type.
5. Success → `UPDATE … status='completed', result=…, completed_at=now()`.
   Handler/validation error → `UPDATE … status='failed', error={type,message}, completed_at=now()`.
6. **`XACK`** the message — *after* the Postgres update commits. Ack-after-commit = at-least-once.

**Delivery semantics:** at-least-once (claim-guard + ack-after-commit). A redelivered already-completed/processing job is a harmless no-op via the guard.

**Graceful shutdown:** on `SIGTERM`, stop reading new messages, let the in-flight job finish, then exit.

### Crash recovery (describe-only — for `DECISIONS.md`)

- **Crash before claim:** the message stays unacked in the group's PEL → redelivered later. Recovery via `XAUTOCLAIM` is *described*, not built.
- **Crash mid-handler (after claim):** the job is stuck in `processing` and the message is unacked in the PEL. A future reaper would `XAUTOCLAIM` the message and reset the job.
- **Restart / consumer churn:** because the consumer name's UUID suffix is regenerated on each start, a restarted worker returns as a *new* consumer and its previous consumer's PEL lingers. A complete reaper therefore needs `XAUTOCLAIM` **plus** `XGROUP DELCONSUMER` cleanup of dead consumers. All of this is documented, not implemented, in Phase 1.

---

## 8. Job Handlers

A registry keyed by `JobType`. Each handler takes a validated payload, returns a typed result, and raises on failure.

| Type | Behavior | Result / Failure |
|---|---|---|
| **email** | `sleep(random 1–3s)` | `{ "message_id": "<mock>" }` |
| **webhook** | `sleep(random 1–2s)` | 80% → `{ "status": 200 }`; 20% → `raise WebhookFailedError` (→ job `failed`; exercises the failure path, no retry in Phase 1) |
| **report** | `sleep(random 3–5s)` | `{ "file_url": "<mock>" }` |

---

## 9. Error Handling & Logging

**Error handling**

- API: validation → `422`, not-found → `404` (FastAPI exception handlers).
- Worker: any handler/validation exception is caught → job `failed` with `{type, message}`; the loop continues to the next message. Transient Redis/DB errors → log and continue; the unacked message will be redelivered.

**Logging (structlog, JSON)**

- Lifecycle events: `job.enqueued`, `job.received`, `job.processing`, `job.completed`, `job.failed`. No `print`.
- Process-level constant (`consumer` name) bound once at startup.
- **Per-job fields** (`job_id`, `job_type`, `message_id`) are scoped with `structlog.contextvars` via a `bound_contextvars(...)` **context manager per loop iteration**, so bindings are torn down on iteration exit *even if an exception fires during cleanup* — preventing one job's metadata from bleeding into another job's logs.
- **Unified JSON across the container matrix:** the root stdlib `logging` and uvicorn's loggers (`uvicorn`, `uvicorn.error`, `uvicorn.access`) are reconfigured to route through structlog's `ProcessorFormatter`. uvicorn banners/access logs and our own logs all emit the same JSON shape → 100% uniform stdout for any log aggregator.

---

## 10. Project Structure

```
app/
  main.py            # FastAPI app factory  → uvicorn app.main:app
  api/
    routes.py        # POST /jobs, GET /jobs/{id}, GET /jobs, /health
    deps.py          # db session dependency
  core/
    config.py        # pydantic-settings
    logging.py       # structlog + stdlib/uvicorn hijack
    db.py            # SQLAlchemy engine/session
    redis.py         # redis client + XGROUP bootstrap
  models/
    job.py           # SQLAlchemy Job model + status/type enums
  schemas/           # SHARED pkg (api + worker)
    payloads.py      # tagged-union payload models + validate_payload
    results.py       # per-type result models
    api.py           # request/response DTOs (JobOut, JobList, …)
  queue/
    producer.py      # enqueue(job_id): XADD after commit
    consumer.py      # XREADGROUP/XACK helpers, CONSUMER_NAME
  jobs/
    registry.py      # JobType → handler
    handlers.py      # email/webhook/report handlers
  worker/
    runner.py        # the worker loop
    __main__.py      # worker process entrypoint  → python -m app.worker
alembic/
  env.py · versions/
tests/
  unit/ · integration/ · conftest.py
docker-compose.yml · Dockerfile · alembic.ini · pyproject.toml
README.md · DECISIONS.md · AI_USAGE.md
```

The root `main.py` stub is replaced. The two real entrypoints are `app.main:app` (API) and `python -m app.worker` (worker).

---

## 11. Configuration

`pydantic-settings`, all env-injected (Docker Compose supplies them):

- `DATABASE_URL`
- `REDIS_URL`
- `JOBS_STREAM` (default `jobs:stream`)
- `CONSUMER_GROUP` (default `workers`)
- `BLOCK_MS` (XREADGROUP block timeout)
- `LOG_LEVEL`

---

## 12. Testing Strategy (`uv run pytest`)

**Two profiles:**

- **Unit tests** → `fakeredis` + no/in-memory DB; fast checks of schemas, handlers, cursor codec, claim-guard logic.
  - Determinism: patch `time.sleep` and `random` so handler tests are instant and the webhook 80/20 is forced either way.
  - Coverage: payload validation (valid / invalid / unknown type via the tagged union); each handler success + failure; cursor encode/decode; claim-guard transition logic.
- **Integration tests** → **real PostgreSQL container + real Redis container**, default via `testcontainers` (a dedicated compose test profile is the fallback if testcontainers proves awkward on the dev OS). This verifies `XGROUP` bootstrap, `XREADGROUP`, `XACK`, and PEL behavior against the actual Redis engine — exactly where `fakeredis` can diverge on Streams/consumer-group state, and where Postgres JSONB + the `(created_at, id)` row-value compare must be exercised for real.
  - Coverage: API (submit → `202` + row + stream message; get; `404`; list filters + cursor paging); end-to-end (enqueue → one worker iteration → assert `pending → processing → completed/failed`, result/error persisted; duplicate delivery is a no-op via the guard).

---

## 13. Orchestration & Ops

**Docker Compose services:**

- `postgres` — with healthcheck.
- `redis` — with healthcheck.
- `migrate` — one-shot, runs `alembic upgrade head`; API and worker depend on it. A one-shot migrate avoids migration races when workers scale.
- `api` — uvicorn on `:8000`, depends on healthy `postgres` + `redis` and successful `migrate`.
- `worker` — same image, `command: python -m app.worker`, scalable via `--scale worker=N`.

**Dockerfile:** single uv-based image (`uv sync`) shared by API and worker.

**Run flow (README):**

- `docker compose up --build` → starts postgres, redis, migrate, api (:8000), worker.
- Scale workers: `docker compose up --scale worker=3`.
- Example submit `curl`.
- Tests: `uv run pytest`.

---

## 14. Deliverables to Fill In

- **`DECISIONS.md`** — job pickup strategy (Redis Streams consumer group, one job/process), worker crash recovery (describe-only PEL/`XAUTOCLAIM`/`DELCONSUMER` flow + the commit-then-XADD orphan gap), and "one thing I'd do differently" (e.g., transactional outbox; reaper; async-concurrent workers). Priority-queue and retry-backoff sections note "deferred to Phase 2."
- **`README.md`** — how to run, how to test, example submit request, architecture overview.
- **`AI_USAGE.md`** — tools used, what helped, what needed fixing (esp. concurrency), what AI struggled with.

---

## 15. Explicitly Out of Scope (Phase 2+)

Priority levels, future scheduling, automatic retry + backoff, batch jobs + progress %, idempotency keys, cancel endpoint, retry endpoint, health-with-queue-stats, and an active crash-recovery reaper. Each gets its own spec.
