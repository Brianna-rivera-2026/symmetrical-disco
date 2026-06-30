# Distributed Job Processing System — Phase 1

A distributed background job processing system built with **FastAPI**, **PostgreSQL**, **Redis Streams**, and multiple worker processes. This is Phase 1 of the system, implementing the basic flow: submit jobs via API, queue them, process them concurrently, and persist results.

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

### Submit a test job

Submit an email job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.com","subject":"Hi"}}'
```

Response:

```json
{ "id": "550e8400-e29b-41d4-a716-446655440000", "type": "email", "status": "pending", "created_at": "2026-06-30T..." }
```

Check the status:

```bash
curl http://localhost:8000/jobs/550e8400-e29b-41d4-a716-446655440000
```

List jobs with filters:

```bash
curl "http://localhost:8000/jobs?type=email&status=completed&limit=20"
```

Pagination uses cursor-based keyset; add `&cursor=<opaque>` for the next page.

## Architecture Overview

The system implements **at-least-once delivery** with three layers:

```
Client
   ↓
API Service (FastAPI)
   ├─ INSERT job (pending) → PostgreSQL
   ├─ COMMIT
   └─ XADD job_id → Redis Stream
   
Redis Stream + Consumer Group
   ↓
Worker Process (1 job at a time)
   ├─ XREADGROUP COUNT 1 → claim a job
   ├─ run_handler(job_type, payload)
   ├─ UPDATE job (completed|failed) → PostgreSQL
   ├─ COMMIT
   └─ XACK → mark message as processed
```

**Key invariants:**

1. **Commit-then-enqueue (§6):** API commits the job to Postgres first, then adds it to the Redis Stream. If the process crashes between these steps, a future recovery sweep (Phase 2 feature) will find the pending job.

2. **Claim-guard (§7):** Before processing, the worker checks if the job is still `pending` in Postgres. A redelivered message (e.g., after a crash) will be skipped because the job is already `completed` or `failed`.

3. **Ack-after-commit (§7):** Worker updates Postgres *and* commits the transaction *before* acknowledging the message in Redis. If the worker crashes after commit but before ack, the message stays in the consumer group's pending list and will be retried.

4. **Unique consumer name per process:** Each worker process has a unique Redis consumer name (prefix + UUID), making consumer tracking unambiguous and enabling future recovery operations to target specific dead consumers.

**Job types (payload schemas):**

- **email:** `{to, subject, body?}` — sends an email (mock handler)
- **webhook:** `{url, method?}` — calls an external URL (mock handler)
- **report:** `{report_type, params?}` — generates a report (mock handler)

For the full design, see [`docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md`](docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md).
