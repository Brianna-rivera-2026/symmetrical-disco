# Production Hardening — Persistence & Zero-Loss Redeploys — Design

**Date:** 2026-07-01
**Status:** Approved (pre-implementation)
**Branch context:** builds on the full Phase-1→cancellation stack (priority
streams, delayed ZSET + ticker, retries/backoff, worker timeout + recycle, the
reaper/reconciler, guarded `processing → terminal` transitions,
`is_synced_to_redis` handoff tracking).

## 1. Context & current state

Today the stack runs under `docker-compose.yml` with five services: `postgres`,
`redis`, a one-shot `migrate` (`alembic upgrade head`), `api` (uvicorn), and
scalable `worker` replicas, plus a singleton `ticker` (promote-due + reconcile +
reaper). Job flow: `POST /jobs` commits the row to Postgres, hands off to Redis
(priority stream `XADD` or delayed-ZSET `ZADD`), then flips
`is_synced_to_redis=TRUE`. Workers `read_priority` (strict high→normal→low,
`count=1`), `claim_job`, run the handler in a timeout-bounded thread, finalize
via a guarded `UPDATE … WHERE status='processing'`, then `XACK`. The ticker
`reconcile_orphans` re-enqueues rows still `is_synced_to_redis=FALSE`, and the
reaper reclaims stale PEL entries via `XAUTOCLAIM`.

Three production gaps motivate this spec (`docs/requirements/leftovers.md`):

1. **No Docker volumes.** Neither `postgres` nor `redis` mounts a volume. Any
   container recreation — including every `docker compose up --build` that
   changes an image — risks wiping the source-of-truth DB and all queue state.
2. **New-version deploys interrupt work.** Workers trap SIGTERM and drain the
   current job, but compose sets no `stop_grace_period`, so Docker SIGKILLs
   after the default 10s — cutting off a `report` job (handler timeout 45s)
   mid-flight on every redeploy.
3. **A total Redis loss orphans synced jobs.** `reconcile_orphans` only re-syncs
   rows where `is_synced_to_redis=FALSE`. Once a job is handed off it is flagged
   `TRUE`; if Redis is then wiped, every already-synced `pending`/`scheduled`
   job sits in Postgres forever with no Redis presence and is never reprocessed.

## 2. Goals & non-goals

**Goals**
- Persist Postgres and Redis across container recreation and `up --build` via
  named volumes; enable Redis AOF (`appendfsync everysec`).
- Make Redis fully **rebuildable from Postgres** so the system self-heals after
  a *total* Redis data loss (lost volume / corrupt AOF), not just a clean
  restart.
- Make redeploys **zero-loss**: in-flight jobs finish (graceful drain) rather
  than being killed and redone.
- Document and enforce a safe **migration discipline** and a **rollback** path
  for new-version deploys.
- Preserve every existing guarantee: at-least-once delivery,
  commit-then-handoff, idempotent claims, guarded terminal transitions, strict
  priority, the reaper/retry machinery.

**Non-goals**
- **Zero-*downtime*** API. A single `api` container has a brief unavailability
  window on recreate; "zero-loss" (no dropped jobs) holds, but multi-replica +
  reverse-proxy HA is out of scope.
- Redis clustering / replication / Sentinel. Single-node Redis with AOF +
  Postgres rebuild is the durability model.
- Postgres HA / PITR / streaming replication. A named volume is the scope; a
  managed/replicated DB is a deployment-time swap, not designed here.
- Health/readiness endpoints, queue-stats surfacing, stream-length trimming, and
  container resource limits — explicitly deferred (separate leftovers).
- Surviving loss of *both* Postgres and Redis simultaneously. Postgres is the
  single source of truth; if it is lost, the system cannot recover.

## 3. Part A — Persistence & volumes

### 3.1 Postgres volume
Add a named volume `pgdata` mounted at `/var/lib/postgresql/data`. Job history
and all job state now survive container recreation and `up --build`. Only an
explicit `docker compose down -v` destroys it.

### 3.2 Redis volume + AOF
Add a named volume `redisdata` mounted at `/data`, and run Redis with
`--appendonly yes --appendfsync everysec`. Streams, the consumer-group PEL, and
the `jobs:delayed` ZSET survive redeploys/restarts with a ≤1s write-loss window.
The `everysec` fsync policy is the standard durability/throughput trade-off; the
≤1s loss window is covered by the reconcile + reaper machinery and by the
bootstrap rebuild below.

### 3.3 Postgres→Redis bootstrap rebuild (backstop)
The code change that closes gap #3. A **race-safe, idempotent bootstrap** runs
in the **ticker** (the existing singleton that already owns reconcile/reap), once
per fresh Redis, before its main loop:

1. **Detect** via an AOF-persisted sentinel key (`bootstrap_key`, default
   `jobs:bootstrap_generation`). Present → normal restart → **skip**. Absent →
   fresh/wiped Redis → run the rebuild.
2. **Rebuild work (idempotent), committed first:**
   - For every job with `status ∈ {pending, scheduled}`: set
     `is_synced_to_redis=FALSE`.
   - For every `processing` job whose `started_at` is older than
     `visibility_timeout_s`: treat as `WorkerLost` via the existing
     `schedule_retry_or_fail(...)` — its PEL entry died with the wipe, so the
     reaper (which reads the PEL) is blind to it. Younger `processing` jobs are
     left alone: their worker survived the wipe and will finish and commit
     normally (its claim already won; the missing `XACK` target is harmless).
3. **Mark done last:** `SET <bootstrap_key> <uuid> NX`.
4. The already-running `reconcile_orphans` loop then re-derives each
   `is_synced_to_redis=FALSE` job on its next tick: `pending` → its priority
   stream (`enqueue`), `scheduled` → `jobs:delayed` at `scheduled_at.timestamp()`.
   Any duplicate delivery is absorbed by the worker's idempotent claim-guard.

**Ordering rationale (work-before-sentinel):** if the ticker crashes mid-rebuild
the sentinel is absent, so the next start re-runs the (idempotent) resets — no
job is stranded. Two tickers racing both perform idempotent resets and one wins
the `NX`; the loser's redundant resets are harmless. A "sentinel-first" ordering
would risk a ticker claiming the sentinel then dying before doing the work,
stranding every job — so it is rejected.

**Interaction with new submissions:** if `api` accepts new jobs after a wipe but
before the ticker bootstrap runs, those jobs `XADD` fresh and set
`synced=TRUE` normally — independent of the old rows the bootstrap rebuilds. No
conflict.

## 4. Part B — Zero-loss redeploys

### 4.1 Worker graceful drain (config-only)
Add `stop_grace_period: 50s` to the `worker` service (≥ `job_handler_timeout_s`
= 45s, +5s margin). Docker sends SIGTERM, waits up to 50s, then SIGKILL. The
worker already: traps SIGTERM/SIGINT → sets the stop flag; checks
`_should_stop()` at the loop top; and, because `read_priority` uses `count=1`,
holds at most one in-flight job — so worst-case drain is a single handler
(≤45s). No worker code change. The reaper remains the crash backstop for the
rare case a job exceeds the grace window and is SIGKILLed.

### 4.2 API graceful shutdown (config-only)
Add `stop_grace_period: 30s` to `api`. uvicorn already drains in-flight HTTP on
SIGTERM. `commit-then-XADD` is already crash-safe: a kill between the commit and
the `XADD` leaves `is_synced_to_redis=FALSE`, and `reconcile_orphans`
re-enqueues it. The lifespan already closes the Redis client and disposes the
engine. Brief API downtime on recreate is accepted (see non-goals).

### 4.3 Ticker graceful shutdown (config-only)
Add `stop_grace_period: 15s` to `ticker`. It already traps signals and checks
`_should_stop()` between quick promote/reconcile/reap passes; 15s comfortably
covers one pass.

### 4.4 Migration ordering, discipline & rollback
- **Ordering (already correct):** `migrate` runs `alembic upgrade head` and
  `api`/`worker`/`ticker` depend on `service_completed_successfully`, so schema
  is current before any new-version process starts.
- **Discipline (to document + enforce):** **expand/contract**, additive-only
  within a release. During `up --build`, old and new containers briefly overlap,
  so a migration must never drop/rename a column (or tighten a constraint) that
  the currently-running old code still reads/writes in the *same* release.
  Removals happen in a *later* release, after all old code is gone.
- **Rollback:** give services an explicit `image:` name/tag so a build is
  addressable and the previous tag can be redeployed. Because migrations are
  expand/contract, the previous image runs against the newer (backward-
  compatible) schema without a down-migration.

## 5. Data model & config changes

- **No schema/migration change.** The bootstrap reuses existing columns
  (`status`, `is_synced_to_redis`, `started_at`, `scheduled_at`).
- **`Settings`:** add `bootstrap_key: str = "jobs:bootstrap_generation"`.
- **Compose:** two named volumes (`pgdata`, `redisdata`); Redis `command` with
  AOF flags + `redisdata:/data`; `pgdata:/var/lib/postgresql/data`;
  `stop_grace_period` on `api` (30s), `worker` (50s), `ticker` (15s); explicit
  `image:` names for buildable services.

## 6. Testing strategy

**Unit (no Docker):**
- Bootstrap, sentinel **absent**: given a set of jobs across statuses + a mock
  Redis, asserts `is_synced_to_redis` is reset to `FALSE` for `pending`/
  `scheduled`, `processing` older than `visibility_timeout_s` is routed through
  `schedule_retry_or_fail`, younger `processing` is untouched, and the sentinel
  is `SET … NX` afterward.
- Bootstrap, sentinel **present**: no-op (no status/flag writes, no `NX`).
- Ordering/idempotency: a simulated crash after resets but before the `NX` leaves
  resets applied and re-runs cleanly on the next call.

**Integration (testcontainers):**
1. **Restart-with-volume:** submit jobs, restart the Redis container keeping the
   volume, assert jobs still complete (no rebuild needed — sentinel survived).
2. **Total Redis loss:** submit `pending` + `scheduled` jobs, `FLUSHALL` Redis,
   run the ticker → assert streams + `jobs:delayed` are rebuilt and all jobs
   eventually complete.
3. **Graceful drain:** start a long `report` job, send SIGTERM to the worker →
   assert the job completes, worker exits 0, and the message is not redelivered.

## 7. Rollout & docs

- Update `DECISIONS.md`: persistence stance (PG volume + Redis AOF + Postgres
  rebuild backstop), drain policy, migration discipline/rollback.
- Update `README.md`: volumes, what survives `up --build` vs `down -v`, redeploy
  drain behavior.
- Tick off the corresponding `docs/requirements/leftovers.md` items.

## 8. Risks & mitigations

- **AOF ≤1s write-loss window.** Mitigated by reconcile (unsynced rows) + the
  bootstrap rebuild (total loss) — no durable job depends solely on the AOF tail.
- **Longer deploys.** Graceful drain adds up to ~50s per worker to a redeploy.
  Accepted: it is bounded (one handler) and avoids redone work + redelivery churn.
- **Double-execution on rebuild.** Re-deriving a job that still has a live
  message would double-deliver, but the claim-guard makes redelivery a no-op, so
  the rebuild is safe even if it overlaps a partially-surviving Redis.
- **Simultaneous worker + Redis loss of an in-flight job** beyond the
  `visibility_timeout_s` window is recovered by the bootstrap's stale-`processing`
  sweep; within the window it is left to the surviving worker / normal reaper.
