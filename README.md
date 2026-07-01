# Distributed Job Processing System — Phase 1

A distributed background job processing system built with **FastAPI**, **PostgreSQL**, **Redis Streams**, and multiple worker processes.

## Quick Start

### Run the project

```bash
docker compose up --build
```

This starts:
- **API service** (http://localhost:8000)
- **PostgreSQL** (jobs table, migrations via Alembic)
- **Redis** (Streams consumer group)
- **1 worker process** by default

**Scale workers** to increase concurrency:

```bash
docker compose up --build --scale worker=3
```

Each worker replica joins the same consumer group and pulls one job at a time. Concurrency = number of worker replicas.

### Run tests

Run the full unit and integration suite:

```bash
uv run pytest
```

For **unit tests only** (fast, no Docker required):

```bash
uv run pytest tests/unit
```

(Integration tests require Docker running with testcontainers.)

### API examples

All endpoints are served at `http://localhost:8000`.

#### Health check

```bash
curl http://localhost:8000/health
```

```json
{ "status": "ok", "checks": { "postgres": "ok", "redis": "ok" } }
```

Returns `503` with `"status": "unavailable"` if either dependency check fails.

#### Queue and job stats

```bash
curl http://localhost:8000/stats
```

```json
{
  "queue": {
    "streams": {
      "high": { "depth": 0, "in_flight": 0 },
      "normal": { "depth": 3, "in_flight": 1 },
      "low": { "depth": 0, "in_flight": 0 }
    },
    "scheduled": 2,
    "workers": 3
  },
  "jobs": {
    "by_status": { "scheduled": 2, "pending": 3, "processing": 1, "completed": 118, "failed": 4, "cancelled": 1 },
    "oldest_pending_age_seconds": 1.42
  }
}
```

#### Submit a job

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.com","subject":"Hi"}}'
```

Response (`202 Accepted`):

```json
{ "id": "550e8400-e29b-41d4-a716-446655440000", "type": "email", "status": "pending", "priority": "normal", "created_at": "2026-06-30T12:00:00Z", "scheduled_at": null }
```

Optional request fields: `priority` (`high`/`normal`/`low`, default `normal`), `scheduled_at` (ISO-8601 — defers the job to the delayed queue instead of running it now), and `idempotency_key` (replaying the same key with the same payload returns the original job with `200`; reusing it with a different payload returns `409`):

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"report","payload":{"report_type":"weekly_summary"},"priority":"high","scheduled_at":"2026-07-02T09:00:00Z","idempotency_key":"weekly-report-2026-07-02"}'
```

#### Get a job

```bash
curl http://localhost:8000/jobs/550e8400-e29b-41d4-a716-446655440000
```

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "email",
  "status": "completed",
  "priority": "normal",
  "payload": { "to": "a@b.com", "subject": "Hi" },
  "result": { "sent": true },
  "error": null,
  "created_at": "2026-06-30T12:00:00Z",
  "started_at": "2026-06-30T12:00:01Z",
  "completed_at": "2026-06-30T12:00:02Z",
  "scheduled_at": null,
  "attempts": 1,
  "max_attempts": 4,
  "progress": 100,
  "cancel_requested_at": null
}
```

`404` if the job doesn't exist.

#### Retry a failed job

```bash
curl -X POST http://localhost:8000/jobs/550e8400-e29b-41d4-a716-446655440000/retry
```

Resets the job to `pending` and re-enqueues it on its original priority stream. `404` if the job doesn't exist, `409` if it isn't currently `failed`.

#### Cancel a job

```bash
curl -X POST http://localhost:8000/jobs/550e8400-e29b-41d4-a716-446655440000/cancel
```

- `200` — job was `pending`/`scheduled` and was cancelled immediately (also returned if it was already `cancelled`)
- `202` — job was `processing`; cancellation is requested and takes effect at the worker's next checkpoint
- `409` — job already `completed` or `failed`
- `404` — job doesn't exist

#### List jobs

```bash
curl "http://localhost:8000/jobs?type=email&status=completed&limit=20"
```

```json
{
  "items": [
    { "id": "550e8400-e29b-41d4-a716-446655440000", "type": "email", "status": "completed" }
  ],
  "next_cursor": "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNi0zMFQxMjowMDowMFoifQ"
}
```

Filters `type`, `status`, and `priority` are optional and combinable; `limit` defaults to 50 (max 200). Pagination is cursor-based keyset — pass the previous response's `next_cursor` back in for the next page:

```bash
curl "http://localhost:8000/jobs?type=email&status=completed&limit=20&cursor=eyJjcmVhdGVkX2F0IjoiMjAyNi0wNi0zMFQxMjowMDowMFoifQ"
```

## Architecture Overview

```
┌────────┐        ┌───────────────┐
│ Client │───────▶│  API Service  │
└────────┘        └───────┬───────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
      ┌───────────────┐          ┌────────┐
      │  PostgreSQL   │          │ Redis  │
      └───────────────┘          └────────┘
              ▲                       ▲
              └───────────┬───────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
         ┌────────┐              ┌────────┐
         │ Worker │              │ Ticker │
         └────────┘              └────────┘
```

- **Client** — any HTTP caller submitting or querying jobs.
- **API service** (FastAPI) — the only entry point. Touches **PostgreSQL** (inserts/reads job rows) and **Redis** (enqueues onto priority streams, schedules delayed jobs).
- **PostgreSQL** — source of truth for job state, payload, results, and attempt counts.
- **Redis** — the queue: one stream per priority (`high`/`normal`/`low`) with a shared consumer group, plus a sorted set for delayed/scheduled jobs.
- **Worker** (N replicas) — executes job handlers. Touches **Redis** (claims and acknowledges stream messages) and **PostgreSQL** (updates job status and results).
- **Ticker** — background maintenance process. Touches **Redis** (promotes due delayed jobs into streams, reclaims stalled in-flight messages) and **PostgreSQL** (reconciles orphaned rows, marks jobs synced).

## Persistence & Deployment

Postgres (`pgdata`) and Redis (`redisdata`) both use named Docker volumes, and
Redis runs with AOF persistence (`appendfsync everysec`). Job history and
queue state (streams, the consumer-group PEL, the delayed-jobs ZSET) survive
`docker compose down` / `up --build` and container restarts. Only an explicit
`docker compose down -v` destroys them.

**Redeploys drain gracefully.** `worker`, `api`, and `ticker` all trap SIGTERM
and finish in-flight work before exiting; each service's `stop_grace_period`
in `docker-compose.yml` is set above its worst-case drain time (the worker's
50s covers the 45s handler timeout) so Docker's SIGKILL never arrives mid-job
during a normal redeploy.

**Rollback:** each buildable service shares an explicit `image:
jobprocessor-app:${APP_TAG:-dev}` tag. Tag a release with
`APP_TAG=v1.2.0 docker compose build`, deploy it with
`APP_TAG=v1.2.0 docker compose up -d`, and roll back by re-running `up -d`
with the previous `APP_TAG`. This is safe because migrations in this project
are additive-only within a release (see `DECISIONS.md` §6) — an older image
never breaks against a newer schema.

**If Redis ever loses all its data** (e.g., the volume itself is deleted),
recovery is a manual procedure — see
[`docs/runbooks/redis-total-loss-recovery.md`](docs/runbooks/redis-total-loss-recovery.md).

For the full system design, see the [Phase 1 design](docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md); for the persistence, redeploy-drain, and migration-discipline hardening described above, see [the production-hardening design](docs/superpowers/specs/2026-07-01-production-hardening-design.md).