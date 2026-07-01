# Failure handling & retries — Design

**Date:** 2026-07-01
**Status:** Approved (pre-implementation)
**Branch context:** builds on the priority-scheduling work (three priority
streams + delayed ZSET + ticker with promotion & orphan reconciliation)

## 1. Context & current state

Today a job flows: `POST /jobs` validates the payload, `create_job` commits the
row (`PENDING` immediate or `SCHEDULED` parked in `jobs:delayed`), hands off to
Redis (priority stream `XADD` or ZSET `ZADD`), then flips
`is_synced_to_redis=TRUE`. A worker runs `read_priority` (strict high→normal→low
drain), `claim_job` (`pending|scheduled → processing`), runs the handler,
`complete_job`/`fail_job`, then `XACK`. The ticker `promote_due`s mature
scheduled jobs and `reconcile_orphans` re-enqueues rows still
`is_synced_to_redis=FALSE` after a grace period.

What is **missing** and added here:

- No attempt tracking, no retries, no backoff — a failed handler is terminal.
- No worker-side job timeout — a slow/stuck handler runs unbounded.
- No reaper — a job whose worker dies mid-run is stuck in the stream's Pending
  Entries List (PEL) forever.
- No manual retry endpoint.

## 2. Goals & non-goals

**Goals**
- Track `attempts` / `max_attempts` per job.
- Automatic retries with backoff: **immediate** first retry, then delayed
  (default 30s, 2m); after `max_attempts`, mark `FAILED` permanently.
- A worker-level **Max Job Handler Timeout** enforced *below* the infrastructure
  **Visibility Timeout** (Layer 1 proactive defense).
- A **reaper** in the ticker that reclaims stale PEL entries (dead/hung workers)
  via `XAUTOCLAIM` and re-drives them, guarded by Postgres (Layer 2 reactive
  defense / optimistic concurrency).
- `POST /jobs/{id}/retry` to re-run a permanently-failed job.
- Preserve every existing guarantee: at-least-once delivery, commit-then-handoff,
  `is_synced_to_redis` orphan reconciliation, idempotent claims, strict priority.

**Non-goals**
- Dead-letter queue / alerting on permanent failure (log-only for now).
- Per-job custom backoff curves (schedule is a global config list).
- Cancellation interplay (cancellation is a later phase; the retry endpoint
  covers `failed` only — see §8).
- Killing a hung handler in-place (we recycle the worker process instead — §6).

## 3. Decisions (locked)

1. **Worker timeout = thread-based**, `future.result(timeout=...)` around the
   sync handler. Portable (Windows dev + Linux prod) and fully testable.
2. **Zombie handling = recycle the worker process** on timeout, rather than
   killing the thread (Python can't). See §6.
3. **Retry ownership = one shared `schedule_retry_or_fail` helper**, called by
   **both** the worker (handler failure / self-timeout) and the reaper
   (dead-worker reclaim).
4. **Immediate first retry = guarded re-enqueue to the priority stream**
   (fresh visibility lease per attempt); delayed retries reuse the existing
   **delayed ZSET + `promote_due`** path verbatim.
5. **The core safety invariant:** *every* transition out of `status='processing'`
   is a single guarded `UPDATE … WHERE id=:id AND status='processing'` that also
   increments `attempts`, and its Redis side-effect runs **only if
   `rowcount == 1`**. The Postgres row is the sole arbiter; worker and reaper can
   never both act on the same lease.
6. **Reaper lives in the ticker process**, using `XAUTOCLAIM` with
   `min-idle = visibility_timeout` and a dedicated `reaper` consumer name. Its
   recovery decision keys off **`status × is_synced_to_redis`** (§7).

## 4. Data model & migration (`0004_add_attempts`)

Add to `jobs`:

| Column | Type | Notes |
|--------|------|-------|
| `attempts` | `INTEGER NOT NULL DEFAULT 0` | executions that have reached an outcome |
| `max_attempts` | `INTEGER NOT NULL DEFAULT 4` | total executions allowed; set from config at creation, stored per-row so config drift can't change a live job's budget |

- `upgrade`: add both columns with server defaults (backfills existing rows to
  `attempts=0`, `max_attempts=4`).
- `downgrade`: drop both columns.

`Job` model (`app/models/job.py`) gains the two mapped columns.
`create_job` (`app/repository.py`) sets `max_attempts` from
`settings.max_attempts`.

## 5. Config additions (`app/core/config.py`)

| Setting | Default | Meaning |
|---------|---------|---------|
| `job_handler_timeout_s` | `45.0` | Max Job Handler Timeout (Layer 1) |
| `visibility_timeout_s` | `60.0` | Infrastructure Visibility Timeout; reaper `min-idle` |
| `reaper_interval_s` | `30.0` | how often the ticker runs the reaper scan |
| `max_attempts` | `4` | default per-job execution budget |
| `retry_backoff_schedule` | `[0, 30, 120]` | seconds of delay before retry #1/#2/#3 |
| `max_handler_timeouts_before_recycle` | `1` | timeouts a worker tolerates before self-recycling (§6) |

**Startup invariant (fail-fast):** a pydantic `model_validator` enforces
`job_handler_timeout_s < visibility_timeout_s`; the service refuses to start
otherwise. This is the Layer-1 sizing guarantee (a job cuts itself off before
the reaper can consider it stale).

### Backoff / attempts semantics

`attempts` counts executions that reached an outcome. After an attempt fails,
let `n = attempts + 1` (the attempt just finished):

- `n >= max_attempts` → **FAILED permanently.**
- else → retry with `delay = retry_backoff_schedule[min(n-1, len-1)]`
  (clamp past the end):
  - `delay <= 0` → **immediate** re-enqueue to the priority stream.
  - `delay > 0` → park in the delayed ZSET at `now + delay`.

With defaults (`max_attempts=4`, `[0, 30, 120]`): attempt 1 runs → retry 1
immediate → retry 2 after 30s → retry 3 after 2m → FAILED. This honors the
requirement's "first retry immediate (in the worker)" plus exponential tiers.
Both `max_attempts` and the schedule are tunable via config.

## 6. Worker — Layer 1 (proactive timeout) + recycle

`process_job` runs the handler in a **single-use** `ThreadPoolExecutor`,
`future.result(timeout=settings.job_handler_timeout_s)`.

In all three outcomes the worker `XACK`s its own message **iff** the guarded
transition won (§7.4):

- **Success:** guarded `complete_job` (`processing → completed`, `attempts=n`).
  Won → `XACK`. `rowcount == 0` → the reaper already reclaimed this lease →
  **skip XACK, log critical**; the computed result is discarded (accepted
  at-least-once cost).
- **Handler exception:** `schedule_retry_or_fail(...)` (§7.1); `XACK` iff it
  returned `won`.
- **`TimeoutError`:** `schedule_retry_or_fail(...)`, `XACK` iff `won`, **then
  recycle** (below).

The worker processes **one in-flight job per process** (scaling is by process
count, `--scale worker=N`); the executor exists only to bound the sync handler,
not for concurrency.

**Recycle-on-timeout (zombie black-hole defense).** A timed-out handler thread
keeps running — Python can't kill it — and in a one-job worker a leaked thread
means the worker has *zero* remaining capacity. If it kept pulling from the
stream, claimed jobs would rot in memory while their visibility clocks tick,
turning the worker into a black hole. So after the guarded transition + `XACK`
(state is fully consistent at that point), once the worker has hit
`max_handler_timeouts_before_recycle` timeouts it **stops its loop, closes Redis/
DB, and exits non-zero**; Docker (`restart: unless-stopped`) starts a fresh
process. The OS reclaims the zombie, and killing the process also kills the
still-running handler mid-flight (reducing double-execution). Handlers normally
finish in ≤5s against a 45s timeout, so a real timeout is exceptional and
recycle churn in steady state is ~zero.

Handlers are pure (sleep + return a dict; **no DB writes**), so a lingering
zombie can't corrupt state even before the process exits — its result is simply
never read.

The worker loop moves `XACK` responsibility **into** `process_job` (today it acks
externally in the loop), because the retry path must `XADD`-new-then-`XACK`-old
together and the timeout path must ack-then-recycle.

## 7. Retry orchestration & the reaper — Layer 2 (optimistic concurrency)

### 7.1 `schedule_retry_or_fail` (shared helper)

Called by the worker (handler failure / timeout) and the reaper (a genuinely
abandoned `processing` job). Returns `won: bool` (did it win the guard). It does
**not** `XACK` — that is the caller's job (§7.4). Pseudocode:

```
n = job.attempts + 1                         # attempt that just ended
if n >= job.max_attempts:
    return guarded UPDATE processing→failed  (attempts=n, error=error)
delay = backoff[min(n-1, len-1)]
if delay <= 0:                                # IMMEDIATE
    won = guarded UPDATE processing→pending
          (attempts=n, is_synced_to_redis=FALSE, started_at=NULL)
    if won: XADD priority_stream(job); mark_synced
else:                                         # DELAYED
    won = guarded UPDATE processing→scheduled
          (attempts=n, scheduled_at=now+delay,
           is_synced_to_redis=FALSE, started_at=NULL)
    if won: ZADD delayed_zset score=now+delay; mark_synced
return won
```

`guarded` = `UPDATE jobs SET … WHERE id=:id AND status='processing'`, returning
`rowcount == 1`. The `delay > 0` branch feeds the **existing** `promote_due` path
(the ticker flips `scheduled → pending` and routes by priority) with zero new
promotion code.

### 7.2 `complete_job` / `fail_job` become guarded

Both currently `UPDATE … WHERE id`. They change to
`… WHERE id=:id AND status='processing'`, increment `attempts=n`, and return
`rowcount` so the worker can decide whether to `XACK`.

### 7.3 Reaper (`reap_stale`, in `app/ticker/runner.py`)

Runs every `reaper_interval_s` (alongside `reconcile_orphans`). Per stream:

```
claimed = XAUTOCLAIM stream group "reaper" min-idle=visibility_timeout_ms 0 COUNT n
for (message_id, fields) in claimed:
    row = read Postgres job row
    <decide via the matrix below>            # may call schedule_retry_or_fail
    XACK stream group message_id             # ALWAYS — reaper clears the entry it reclaimed
```

**Recovery matrix (`status × is_synced_to_redis`)** — the flag disambiguates
"handoff finished" from "handoff never happened":

| Row when reaper reads it | What already happened | Reaper action (besides `XACK`) |
|---|---|---|
| `completed` / `failed` | Worker finished; `XACK` was dropped | none (ghost) |
| `pending`/`scheduled` **+ `synced=TRUE`** | Handoff complete; fresh message live | none (stale duplicate) |
| `pending`/`scheduled` **+ `synced=FALSE`** | Worker won the guard then **died before `XADD`/`ZADD`** → `attempts` already counted, decision already made, only Redis handoff missing | **finish the handoff inline**: `XADD` (pending) or `ZADD` at `scheduled_at` (scheduled), then `mark_synced`. **Do not** touch `attempts` or call `schedule_retry_or_fail`. |
| `processing` | Worker died **before** its guarded transition → `attempts` not yet counted | `schedule_retry_or_fail(...)` (guarded, increments `attempts`, decides backoff/fail) |

The `synced=FALSE` row is the critical fix: XACK-and-forget there would delete the
only remaining Redis artifact and strand the job until the 60s reconciler,
degrading an "immediate retry" into a minute of latency. Finishing the handoff
inline restores immediate recovery, and keying off the flag (not status alone)
also tells the reaper *which* recovery to run so `attempts` stays exactly-once.

The reaper is a **strict superset** of the reconciler for stale-PEL rows (same
flag, same recovery, faster). The 60s `reconcile_orphans` remains the backstop
for orphans that never entered any PEL (e.g., the API crashing between commit and
`XADD`).

### 7.4 XACK ownership (who clears the PEL entry)

`XACK` is group-level (it clears a message from the PEL regardless of which
consumer holds it), so exactly one actor must own it per message:

- **Worker** (its own message, in its own PEL): `XACK` **iff** its guarded
  transition won (`complete_job` won, or `schedule_retry_or_fail` returned
  `won`). If it **lost** (`rowcount=0`), the reaper has reclaimed the message —
  the worker must **not** `XACK` (that would yank it out of the reaper's PEL
  mid-recovery) and instead logs critical.
- **Reaper** (a message it reclaimed via `XAUTOCLAIM`): **always** `XACK` after
  handling, in every matrix row — even when `schedule_retry_or_fail` *lost* the
  guard (a slow worker completed the row in the read→update gap). The reclaimed
  entry is a duplicate that must be cleared regardless; the worker that won will
  have re-enqueued a fresh message.

This is why `schedule_retry_or_fail` never `XACK`s internally: its two callers
have opposite ack rules.

## 8. API

- `JobOut` (`app/schemas/api.py`) gains `attempts: int`, `max_attempts: int`.
- **`POST /jobs/{job_id}/retry`** → `JobOut`:
  - Guarded `UPDATE … WHERE id=:id AND status='failed'` setting
    `status=pending, attempts=0, error=NULL, started_at=NULL, completed_at=NULL,
    is_synced_to_redis=FALSE`.
  - `rowcount == 1` → `XADD` to `stream_for_priority(job.priority)`,
    `mark_synced`, return the refreshed `JobOut`.
  - `rowcount == 0` → **409** (`job is not in a terminal failed state`); **404**
    if the job doesn't exist.
  - Resets the full retry budget (`attempts=0`) and re-enqueues immediately,
    reusing the commit-then-handoff invariant (a crash before `XADD` is caught by
    the reconciler; a duplicate is absorbed by the idempotent claim).

## 9. Failure modes & edge cases

| Scenario | Outcome |
|----------|---------|
| Handler raises (e.g. 20% webhook) | Worker `schedule_retry_or_fail` → immediate re-enqueue (retry 1), `attempts=1`. |
| Handler exceeds `job_handler_timeout_s` | `TimeoutError` → `schedule_retry_or_fail`, then worker recycles. |
| Attempts reach `max_attempts` | Guarded `processing → failed`; job terminal until manual retry. |
| Worker `SIGKILL`/OOM mid-run (`processing`) | PEL entry goes stale → reaper `schedule_retry_or_fail` (counts the dead attempt → poison jobs still terminate at `max_attempts`). |
| **Worker wins guard → `pending`,`synced=FALSE` → crashes before `XADD`** | Reaper (stale PEL) finishes the handoff inline (immediate); reconciler is the 60s backstop. |
| Crash after `XADD`, before `mark_synced` | `synced=FALSE`; reconciler/reaper re-enqueues again → duplicate absorbed by idempotent `claim_job`. |
| Crash after `mark_synced`, before `XACK` | `synced=TRUE`, new message live, old lingers in PEL → reaper `XACK`s the ghost. |
| Slow worker finishes right as reaper reclaims | Worker's guarded `complete_job` returns `rowcount=0` → skips `XACK`, logs critical; reaper's re-drive wins. Successful result discarded (accepted). |
| Reaper reads a `completed`/`failed` ghost | `XACK` only. |
| Manual retry on a non-`failed` job | `409`. |
| Reconciler grace vs retry rows | A retry row's `created_at` is old, so it's immediately reconciler-eligible with no grace during the ~ms `commit→mark_synced` window; still correct (any double-enqueue is idempotent-absorbed). Documented, not guarded — the grace is a duplicate-churn optimization, not a correctness guard. |

## 10. Docker Compose

- `worker` service gains `restart: unless-stopped` (or `on-failure`) so
  recycle-on-timeout brings up a fresh process. No new service; the reaper runs
  inside the existing `ticker`.

## 11. Testing plan (`uv run pytest`)

**Unit**
- Backoff mapping: `n → delay` including clamp past the schedule end; permanent
  fail at `n >= max_attempts`.
- Config `model_validator` rejects `job_handler_timeout_s >= visibility_timeout_s`.
- Thread timeout wrapper raises `TimeoutError` for a fn slower than the budget and
  returns the value otherwise.

**Integration (real Redis + Postgres)**
- Handler failure → job re-enqueued to its **priority** stream, `attempts=1`.
- `max_attempts` exhausted → status `failed`, no stream entry left.
- Timeout path → `schedule_retry_or_fail` invoked and worker exits (recycle);
  assert the guarded transition + re-enqueue happened before exit.
- **Reaper `processing`:** hand-plant a stale PEL entry (claim without ack, or set
  `min-idle=0`) with a `processing` row → reaper re-drives, `attempts++`.
- **Reaper `synced=FALSE` recovery:** row `pending`,`synced=FALSE` with a stale
  PEL entry and **no** live message → reaper `XADD`s it (immediate), marks synced,
  and does **not** bump `attempts`.
- **Reaper ghost:** row `completed` + lingering PEL entry → reaper only `XACK`s.
- **Guarded complete loses to reaper:** force row to `pending` before
  `complete_job` → `rowcount=0`, worker skips `XACK`.
- Retry endpoint: `failed` job → `attempts=0`, re-enqueued, `JobOut` reflects it;
  `409` on a non-`failed` job; `404` on unknown id.

**Existing-test migration**
- Update `complete_job`/`fail_job` callers/assertions for the new guard + `attempts`.
- Any fixture creating jobs picks up `attempts=0` / `max_attempts` defaults.
