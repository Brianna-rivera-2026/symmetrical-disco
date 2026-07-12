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

## Deployment

**`docker-compose.yml` is dev-only** — a quick local loop (build, run, poke at
the API), not a deployment target. Postgres (`pgdata`) and Redis (`redisdata`)
use named Docker volumes, so data survives `docker compose down` / `up
--build` (only `down -v` destroys it), and `worker`/`api`/`ticker` drain
in-flight work on SIGTERM before exiting. That's the extent of it — there's no
TLS, no NetworkPolicy isolation, no autoscaling, and it isn't meant to grow
those.

**Production is the Helm chart** at
[`deploy/chart/jobprocessor/`](deploy/chart/jobprocessor/) — TLS everywhere,
default-deny NetworkPolicy isolation, an edge-TLS Route, PgBouncer + KEDA
autoscaling with enforced connection math, and worker memory-threshold
self-recycling, targeting OpenShift. See
[`deploy/chart/jobprocessor/README.md`](deploy/chart/jobprocessor/README.md)
for cluster prerequisites and install steps, and the section below for how
authentication is configured there.

**If Redis ever loses all its data** (e.g., the volume itself is deleted),
recovery is a manual procedure — see
[`docs/runbooks/redis-total-loss-recovery.md`](docs/runbooks/redis-total-loss-recovery.md).

For the full system design, see the [Phase 1 design](docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md); for the persistence, redeploy-drain, and migration-discipline hardening described above, see [the production-hardening design](docs/superpowers/specs/2026-07-01-production-hardening-design.md); for the OpenShift deployment design, see [the OpenShift deployment design](docs/superpowers/specs/2026-07-11-openshift-deployment-design.md).

## Authentication

All `/jobs*` routes require a per-user API key in the `X-API-Key` header
(`/health`, `/ready`, and `/stats` stay open for probes). Jobs are scoped to
the user that created them — other users' jobs return 404. Users are
provisioned declaratively from a `{"name": "raw key", ...}` JSON file: the
one-shot `users-sync` process hashes each key (SHA-256) and upserts it into
Postgres before the API starts. Raw keys are never stored — only the hash
reaches the database, and only usernames are logged.

### Local development

1. `cp secrets/api_user_keys.example.json secrets/api_user_keys.json` and fill
   in real users/keys (generate one with
   `python -c "import secrets; print(secrets.token_urlsafe(32))"`).
2. `docker compose up` — `users-sync` runs automatically after migrations.

**Add or rotate a key:** edit `secrets/api_user_keys.json`, then
`docker compose up users-sync` to re-sync (upsert-only: existing users not in
the file are left untouched, never auto-deleted).

### OpenShift (production)

Keys live in a Kubernetes Secret (default name `jobprocessor-api-user-keys`,
key `api_user_keys.json`), mounted by the chart's `users-sync` hook Job at
install/upgrade — see `secrets.apiUserKeysSecret` in
[`deploy/chart/jobprocessor/values.yaml`](deploy/chart/jobprocessor/values.yaml).

**Initial setup**, before the first `helm install`:

    deploy/openshift/init-secrets.sh <namespace> jobprocessor-api-user-keys alice bob

Generates a key per user and creates the Secret; refuses to touch it if it
already exists (prints the raw keys to stdout once — they aren't recoverable
after that, only hashes reach the database).

**Add or rotate a key on an already-running deployment** — the script above
won't touch an existing Secret, so update it directly and re-run the sync:

    # 1. Decode the existing keys, add/change entries, then re-create the Secret
    #    (oc create --dry-run=client -o yaml | oc apply -f - updates in place):
    oc get secret jobprocessor-api-user-keys -n <namespace> \
      -o jsonpath='{.data.api_user_keys\.json}' | base64 -d > /tmp/keys.json
    # edit /tmp/keys.json to add/change a user's key
    oc create secret generic jobprocessor-api-user-keys -n <namespace> \
      --from-file=api_user_keys.json=/tmp/keys.json \
      --dry-run=client -o yaml | oc apply -f -
    rm /tmp/keys.json

    # 2. Re-run the users-sync hook Job so Postgres picks up the change.
    #    The hook Job is deleted on success (hook-delete-policy), so re-trigger
    #    it via helm upgrade rather than recreating the Job directly:
    helm upgrade jp deploy/chart/jobprocessor -n <namespace> --reuse-values

Revocation (either environment): remove the user's row from the `users`
table (takes effect within `AUTH_CACHE_TTL_S`, default 60s), or rotate their
key. Users removed from the secret file are NOT auto-deleted (upsert-only
sync).
