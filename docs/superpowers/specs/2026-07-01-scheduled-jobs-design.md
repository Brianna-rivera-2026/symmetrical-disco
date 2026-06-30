# Scheduled (delayed) job execution — Design

**Date:** 2026-07-01
**Status:** Approved (pre-implementation)
**Branch context:** builds on `feature/job-processing-phase1`

## 1. Context & current state

Phase 1 implements the immediate flow: `POST /jobs` validates the payload,
`create_job` commits the row as `PENDING`, then `enqueue` does `XADD` to the
Redis stream `jobs:stream`. Workers `XREADGROUP` → `claim_job`
(`WHERE status = pending` → `processing`) → run handler → complete/fail → `XACK`.

The `Job` model already carries a `scheduled` status enum value but no
scheduling time column. The worker runs as its own process
(`python -m app.worker`); the API runs FastAPI with a lifespan that calls
`ensure_group`.

This phase adds optional future execution: a job submitted with a future time is
persisted as `SCHEDULED` and parked in a Redis ZSET; a new **ticker** process
promotes mature jobs into the existing stream, where workers process them
unchanged.

## 2. Goals & non-goals

**Goals**
- Accept an optional absolute execution time on `POST /jobs`.
- Park future jobs in a Redis ZSET and promote them to the stream when mature.
- No silent orphans on the submit path: if Postgres commits but the Redis
  handoff (`XADD` or `ZADD`) fails, the job is recovered automatically.
- Recovery cost scales with the number of *failures*, not the size of the
  backlog.

**Non-goals (out of scope)**
- Cancelling or retrying scheduled jobs (tracked in `docs/requirements/leftovers.md`).
- Recurring/cron schedules. A job has at most one execution time.
- Surviving total Redis data loss automatically — see §9 (durability assumption).

## 3. Decisions (locked)

1. **Ticker = separate process** (`python -m app.ticker`), mirroring the worker.
2. **API contract = absolute `scheduled_at`** (ISO 8601, timezone-aware; a naive
   value is interpreted as UTC). Absent or `<= now` → immediate path.
3. **Promotion handshake = `XADD` before `ZREM`** (crash-safe; rare duplicate
   stream entries are absorbed by the idempotent worker claim).
4. **Orphan recovery = explicit sync flag** `is_synced_to_redis`, unified across
   both submit paths, with a flag-based reconciler. Redis is treated as durable
   (§9).

## 4. Data model & migration

Add to the `jobs` table:

- `scheduled_at TIMESTAMPTZ NULL` — the requested execution time (source of
  truth for the ZSET score). `NULL` for immediate jobs.
- `is_synced_to_redis BOOLEAN NOT NULL DEFAULT FALSE` — `TRUE` once the row has
  been successfully handed off to Redis (`XADD` for immediate, `ZADD` for
  scheduled).

Indexes:

- Partial index supporting the reconciler, self-pruning as rows leave the active
  states:
  ```sql
  CREATE INDEX ix_jobs_unsynced ON jobs (created_at, id)
  WHERE is_synced_to_redis = FALSE AND status IN ('pending', 'scheduled');
  ```
  In steady state this index holds only in-flight submits and true orphans — a
  handful of rows — so the reconciler query walks a near-empty index.

Migration `0002_add_scheduled_at_and_sync_flag`:
- `upgrade`: add both columns; add the partial index. **Backfill existing rows**
  to `is_synced_to_redis = TRUE` so the reconciler never re-enqueues historical
  jobs created under the Phase 1 code.
- `downgrade`: drop the index and both columns.

Update the `Job` model (`app/models/job.py`) with the two new mapped columns.

## 5. API contract (`POST /jobs`)

- `JobSubmission` (`app/schemas/api.py`) gains `scheduled_at: datetime | None = None`.
  Timezone-aware ISO 8601; a naive datetime is normalized to UTC.
- `JobAccepted` and `JobOut` gain `scheduled_at: datetime | None` so clients see
  the parked time and the returned status (`scheduled` vs `pending`).
- Validation: malformed `scheduled_at` → 422 (existing handler). Payload
  validation is unchanged and still runs first.

Branch decision in the route, after payload validation:

- `scheduled_at` present **and** `> now(UTC)` → **scheduled path**.
- absent **or** `<= now(UTC)` → **immediate path** (preserves existing behavior;
  a past time runs now rather than erroring).

## 6. Submit flow (both paths share one handshake)

`create_job` (`app/repository.py`) is extended:
`create_job(session, type, payload, *, status=PENDING, scheduled_at=None)`.
Postgres commits **before** the Redis write (preserves the enqueue invariant),
with `is_synced_to_redis = FALSE`. After the Redis handoff succeeds, a second
statement flips the flag.

| Path | Handshake |
|------|-----------|
| Immediate | `INSERT (PENDING, synced=FALSE)` → `XADD jobs:stream {job_id}` → `UPDATE synced=TRUE` |
| Scheduled | `INSERT (SCHEDULED, synced=FALSE, scheduled_at)` → `ZADD jobs:delayed <epoch> <job_id>` → `UPDATE synced=TRUE` |

The ZSET score is `scheduled_at` as Unix epoch seconds (UTC).

This refactors the current best-effort `enqueue(...)` call in
`app/api/routes.py` into the INSERT → handoff → flip sequence.

## 7. Ticker process (`app/ticker/`)

New entrypoint `python -m app.ticker` and `runner.run_forever(settings, *, stop=None)`
mirroring the worker: SIGTERM/SIGINT graceful stop, structlog context, testable
`stop` hook.

### 7.1 Promotion tick — drain loop

The loop must **not** simply sleep `ticker_interval_s` between single batches:
that caps promotion at `ticker_batch_size` jobs per interval (100/s by default),
so 10,000 jobs scheduled for the same instant would take ~100s to promote —
artificial execution lag (a self-inflicted thundering herd). Instead each
iteration:

1. `ids = ZRANGEBYSCORE jobs:delayed 0 <now_epoch> LIMIT 0 <ticker_batch_size>`
2. If `ids` is empty → sleep `ticker_interval_s`, then continue.
3. Otherwise promote the batch (below). If `len(ids) == ticker_batch_size`
   (full batch → backlog likely remains) **loop again immediately, skipping the
   sleep**; if partial, sleep `ticker_interval_s`.

This drains a mature backlog at one batch per round-trip set instead of one batch
per second. We deliberately keep `ticker_batch_size` *moderate* rather than huge:
a giant `ZRANGEBYSCORE`/`XADD` would block Redis's single thread and delay
workers' `XREADGROUP`/`XACK` — the same single-thread fairness concern as the
reconciler. The drain loop gives high aggregate throughput without large blocking
commands.

**Promoting a batch (Approach B — `XADD` before `ZREM`, batched):**
1. Pipeline `XADD jobs:stream {job_id}` for every id; execute.
2. `ZREM jobs:delayed id1 id2 …` (single multi-member call).
3. best-effort bulk `UPDATE jobs SET status='pending'
   WHERE id = ANY(:ids) AND status='scheduled'` (rows already claimed by a worker
   are skipped; see §8).

All `XADD`s are issued before any `ZREM`, so a crash between steps 1 and 2 leaves
the ids in the ZSET → re-promoted next iteration → duplicate stream message →
second worker claim is a no-op. The pipeline is ordered but need not be atomic.

**Guard rails:** the stop flag is checked every iteration so SIGTERM/SIGINT is
honored promptly even under a sustained drain; the reconciler (§7.2) runs on a
wall-clock cadence (`now - last_reconcile >= reconcile_interval_s`) so a busy
drain loop never starves it.

### 7.2 Reconciler (every `reconcile_interval_s`, default 60s)

Targets only true orphans (Postgres committed, Redis handoff never confirmed):

```sql
SELECT id, type, status, scheduled_at FROM jobs
WHERE status IN ('pending', 'scheduled')
  AND is_synced_to_redis = FALSE
  AND created_at < NOW() - (:reconcile_grace_s || ' seconds')::interval
ORDER BY created_at, id
LIMIT :reconcile_batch_size;   -- keyset-paginated via app/cursor.py
```

For each returned row:
- `status = 'pending'` → re-`XADD jobs:stream {job_id}`
- `status = 'scheduled'` → re-`ZADD jobs:delayed <scheduled_at epoch> <id>`
- then `UPDATE is_synced_to_redis = TRUE WHERE id = :id`.

Re-adds use idempotent Redis ops, so a job that was actually synced but whose
flag flip was lost (API crash between handoff and flip) is harmlessly re-added.

The grace window excludes in-flight submits (committed but flag not yet flipped);
it only needs to exceed the submit transaction's handoff+flip latency
(single-digit ms), so `reconcile_grace_s` defaults to 10s — short enough to
recover a near-due scheduled orphan before it is overdue.

In steady state the reconciler query returns 0 rows: near-zero Postgres CPU, no
network payload, no Redis load. It scales up only when handoffs are actually
failing.

## 8. Worker change (`app/repository.py`)

`claim_job` guard widens to accept either pre-state:

```sql
UPDATE jobs SET status='processing', started_at=:now
WHERE id=:id AND status IN ('pending', 'scheduled');
```

This makes the ticker's promotion `UPDATE` (§7.1 step 2.3) genuinely
best-effort: a worker can claim a job whose Postgres status is still
`SCHEDULED` because the promotion's PG update lost the race. The claim remains
idempotent — a second claim returns rowcount 0 and is skipped.

## 9. Failure modes, edge cases & durability

| Scenario | Outcome |
|----------|---------|
| `XADD`/`ZADD` fails after PG commit (orphan) | Row stays `synced=FALSE`; reconciler re-enqueues after grace. |
| API crash between handoff and flag flip, worker hasn't run it | `synced=FALSE`; reconciler re-adds (duplicate); worker claim no-op. |
| Same crash, worker already processed it | Status no longer `pending`/`scheduled`; reconciler excludes it; stale flag ignored. |
| Ticker crash between `XADD` and `ZREM` | Id remains in ZSET; re-promoted next tick; duplicate absorbed by claim. |
| Promotion PG update loses race to worker claim | Expected; rowcount 0, ignored (claim accepts `scheduled`). |
| `scheduled_at` in the past / naive tz | Past → immediate; naive → interpreted as UTC. |
| Redis unavailable for a tick | Tick logs and returns; ZSET is durable; next tick retries. |
| Clock skew (ticker host vs scheduled time) | Promotion uses host clock vs ZSET score; acceptable, documented. |
| Large batch scheduled for the same instant | Drain loop (§7.1) promotes back-to-back batches with no 1s-per-batch cap; no artificial lag, while individual Redis commands stay small. |

**Durability assumption (decision a):** Redis is configured durable (AOF
`appendfsync everysec` + RDB). The sync flag tracks "handed off to Redis once,"
not "still present," so **total Redis data loss is not auto-recovered.** Recovery
runbook: `UPDATE jobs SET is_synced_to_redis = FALSE WHERE status IN ('pending','scheduled');`
— the existing reconciler then rebuilds the stream/ZSET on its next pass.

## 10. Config additions (`app/core/config.py`)

| Setting | Default |
|---------|---------|
| `delayed_zset` | `"jobs:delayed"` |
| `ticker_interval_s` | `1.0` |
| `ticker_batch_size` | `100` |
| `reconcile_interval_s` | `60.0` |
| `reconcile_grace_s` | `10.0` |
| `reconcile_batch_size` | `500` |

## 11. Docker Compose

Add a `ticker` service using the same image/env as `worker`, command
`python -m app.ticker`.

## 12. Testing plan

**Unit**
- Route branch: future `scheduled_at` → SCHEDULED + `ZADD`; absent/past → PENDING + `XADD`.
- `scheduled_at` normalization: naive → UTC; epoch-score conversion.
- Reconciler row → action mapping (`pending`→XADD, `scheduled`→ZADD) and flag flip.

**Integration (real Redis + Postgres, matching the existing suite)**
- Mature scheduled job is promoted: appears in stream, removed from ZSET, status `scheduled`→`pending`.
- Future job is not promoted before its time.
- Duplicate promotion → second worker claim is a no-op.
- Worker claims a job whose status is still `scheduled`.
- Submit-path orphan recovery: simulate `XADD`/`ZADD` failure → row `synced=FALSE` → reconciler re-enqueues after grace → flag flips.
- Reconciler returns 0 rows / does no work when all jobs are synced.
- End-to-end: submit scheduled → ticker promotes → worker → `completed`.
