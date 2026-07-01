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
- Provide a documented **manual recovery playbook** to rebuild Redis from
  Postgres after a *total* Redis data loss (lost volume / corrupt AOF), reusing
  the existing `is_synced_to_redis` + reconcile machinery — no new automation.
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
- Redis clustering / replication / Sentinel. Single-node Redis with AOF, plus
  the manual Postgres-rebuild playbook (§3.3) for total loss, is the durability
  model.
- Postgres HA / PITR / streaming replication. A named volume is the scope; a
  managed/replicated DB is a deployment-time swap, not designed here.
- **Automated self-heal / bootstrap-on-startup** for a wiped Redis. Total-loss
  recovery is a documented operator playbook (§3.3), not code — this keeps the
  runtime free of a startup rebuild path and any `is_synced` reset semantics.
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
≤1s loss window is covered by the reconcile + reaper machinery, and a *total*
loss by the recovery playbook below (§3.3).

### 3.3 Total Redis loss — recovery playbook (documented, not automated)
Gap #3 — already-synced `pending`/`scheduled` jobs stranded after a full Redis
wipe — is handled **operationally**, not by startup code. The volume + AOF make
a total loss rare; when it happens, an operator runs the playbook below, which
reuses the existing `is_synced_to_redis` + `reconcile_orphans` machinery with no
new code. This section is surfaced as an operator runbook per §7.

**Symptoms.** Redis is empty (streams / `jobs:delayed` / consumer group missing)
while Postgres holds live jobs; `pending`/`scheduled` rows are flagged
`is_synced_to_redis=TRUE` with no Redis presence, and `processing` rows have lost
their PEL entry (the reaper, which reads the PEL, is now blind to them).

**Steps.**
1. **Confirm scope.** Verify Redis has no streams/ZSET/group while Postgres still
   holds `pending`/`scheduled`/`processing` rows.
2. **Recreate groups.** Bring the services up; `ensure_group` runs on
   `api`/`worker`/`ticker` startup and recreates the consumer group on each
   stream (or create them manually).
3. **Re-arm reconcile.** Run once:
   `UPDATE jobs SET is_synced_to_redis = FALSE WHERE status IN ('pending','scheduled');`
   The ticker's `reconcile_orphans` (every `reconcile_interval_s`) then
   re-derives each row — `pending` → its priority stream, `scheduled` →
   `jobs:delayed` at `scheduled_at`. Duplicate deliveries are absorbed by the
   worker's idempotent claim-guard.
4. **Recover stuck `processing`.** For `processing` rows older than
   `visibility_timeout_s` whose worker is confirmed dead, re-enqueue via the
   retry endpoint or
   `UPDATE jobs SET status='pending', is_synced_to_redis=FALSE WHERE id = …;`.
   **Do not** touch `processing` rows whose worker may still be alive — that
   worker will finish and commit normally, and resetting it risks double
   execution.
5. **Verify.** Watch for `ticker.reconciled count=…` logs and confirm job
   statuses transition to `completed`/`failed`.

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

- **No schema/migration change.**
- **No application code change.** This work is compose configuration + docs
  only; total-loss recovery reuses the existing reconcile path (§3.3) driven by
  a one-off SQL `UPDATE`, not new runtime code.
- **Compose:** two named volumes (`pgdata`, `redisdata`); Redis `command` with
  AOF flags + `redisdata:/data`; `pgdata:/var/lib/postgresql/data`;
  `stop_grace_period` on `api` (30s), `worker` (50s), `ticker` (15s); explicit
  `image:` names for buildable services.

## 6. Testing strategy

**Unit (no Docker):** none — this change adds no application code.

**Update (post-implementation):** the two integration tests originally specified
below were implemented, then reclassified to manual verification per a project-wide
policy: config-only changes (Docker Compose, env vars, infra flags) are verified
manually, not via automated pytest tests — reinforced when the graceful-drain test
hit a genuine Windows-only trap (`os.kill(pid, SIGTERM)` hard-kills via
`TerminateProcess` on Windows instead of invoking the registered handler). Both are
now manual steps in the implementation plan
(`docs/superpowers/plans/2026-07-01-production-hardening.md`, Tasks 1 and 2) instead
of automated tests. The original intent (below) is preserved for context; treat the
plan and `DECISIONS.md` §6 as authoritative for what actually shipped.

**Integration (testcontainers) — superseded, see note above:**
1. **Restart-with-volume:** submit jobs, restart the Redis container keeping the
   volume, assert jobs still complete (state survived via the volume + AOF).
2. **Graceful drain:** start a long `report` job, send SIGTERM to the worker →
   assert the job completes, worker exits 0, and the message is not redelivered.

**Manual (playbook validation):**
- **Total Redis loss:** submit `pending` + `scheduled` jobs, `FLUSHALL` Redis,
  then run the §3.3 playbook steps → confirm streams + `jobs:delayed` are rebuilt
  and all jobs eventually complete. Run once by hand to validate the runbook; it
  is not part of the automated suite (there is no code path to assert against).

## 7. Rollout & docs

- Update `DECISIONS.md`: persistence stance (PG volume + Redis AOF; total-loss
  recovery is a manual playbook, not automated self-heal), drain policy,
  migration discipline/rollback.
- Update `README.md`: volumes, what survives `up --build` vs `down -v`, redeploy
  drain behavior, and a pointer to the §3.3 recovery playbook (mirror it into a
  `docs/runbooks/` entry so operators can find it without the design doc).
- Tick off the corresponding `docs/requirements/leftovers.md` items.

## 8. Risks & mitigations

- **AOF ≤1s write-loss window.** Mitigated by reconcile (unsynced rows); for a
  *total* loss the manual §3.3 playbook rebuilds from Postgres. No durable job
  depends solely on the AOF tail.
- **Total Redis loss requires manual intervention.** Recovery is not automatic:
  until an operator runs the §3.3 playbook, affected `pending`/`scheduled` jobs
  stay stuck. Accepted trade-off for keeping the runtime simple — the volume +
  AOF make total loss rare.
- **Longer deploys.** Graceful drain adds up to ~50s per worker to a redeploy.
  Accepted: it is bounded (one handler) and avoids redone work + redelivery churn.
- **Double-execution during playbook recovery.** Re-arming a job that still has a
  live message would double-deliver, but the claim-guard makes redelivery a
  no-op, so step 3 is safe even against a partially-surviving Redis. Step 4's
  `processing` reset is the one manual judgement call — gated on "worker
  confirmed dead" to avoid racing a live worker.
