# Cancellation, Batch Jobs, Progress & Idempotency — Design

**Date:** 2026-07-01
**Status:** Approved (pre-implementation)
**Branch context:** builds on the failure-handling work (attempts/retries with
backoff, worker timeout + recycle, the reaper/reconciler, guarded
`processing → terminal` transitions, `POST /jobs/{id}/retry`).

## 1. Context & current state

Today a job flows: `POST /jobs` validates the payload, `create_job` commits the
row (`PENDING` immediate or `SCHEDULED` parked in `jobs:delayed`), hands off to
Redis (priority stream `XADD` or ZSET `ZADD`), then flips
`is_synced_to_redis=TRUE`. A worker `read_priority`s (strict high→normal→low),
`claim_job`s (`pending|scheduled → processing`), runs the handler in a
timeout-bounded thread, then finalizes via a **guarded**
`UPDATE … WHERE id=:id AND status='processing'` (`complete_job` / `fail_job` /
`retry_to_pending` / `retry_to_scheduled`) and `XACK`s iff its guard won. The
ticker `promote_due`s mature scheduled jobs, `reconcile_orphans` re-enqueues
rows still `is_synced_to_redis=FALSE`, and the reaper reclaims stale PEL entries.

Handlers are **pure**: sleep + return a dict, no DB writes. That purity is what
makes recycle-on-timeout safe (a leaked handler thread can't corrupt state).

Requirement `docs/requirements/05-cancellation.md` bundles four related features
that this spec covers together:

1. **Cancellation** — `POST /jobs/{id}/cancel`. `JobStatus.cancelled` already
   exists in the enum but is unused today.
2. **Batch job** — a new `JobType.batch` that processes multiple items with a
   small per-item delay and returns a summary.
3. **Progress percentage** — a `progress` field updated as a batch runs.
4. **Idempotency key** — duplicate-submission prevention on `POST /jobs`.

## 2. Goals & non-goals

**Goals**
- Cancel a **pending/scheduled** job (pre-execution) directly.
- Cancel a **running (`processing`) batch cooperatively** — the endpoint records
  a *request*; the handler stops early and the **worker** performs the guarded
  `processing → cancelled` (preserving "only the worker leaves `processing`").
- New `batch` job type: iterate items, collect per-item outcomes, **always
  complete with a summary** (even if every item failed).
- Track and expose a batch's `progress` percentage.
- Optional **idempotency key** with replay semantics (repeat key → existing job).
- Preserve every existing guarantee: at-least-once delivery, commit-then-handoff,
  `is_synced_to_redis` reconciliation, idempotent claims, guarded terminal
  transitions, strict priority, the reaper/retry machinery.

**Non-goals**
- Killing a running non-cooperative handler (email/webhook/report run to
  completion; cancellation of a `processing` non-batch job is best-effort — see
  §6). We do not interrupt Python threads.
- Per-item retry / resuming a batch mid-way after an infra failure (a reclaimed
  batch re-runs from the start; §8).
- Truly long batches that exceed the handler timeout — rejected at submission
  (§5), not supported via a new execution model.
- Idempotency-key expiry/TTL (keys are unique for the life of the row; §9).
- A live progress *stream* / websockets (progress is read via `GET /jobs/{id}`).

## 3. Decisions (locked)

1. **Cooperative in-flight cancellation.** The cancel endpoint never flips a
   `processing` row's status; it sets `cancel_requested_at`. Only handlers that
   check the injected context honor it (batch does; the short handlers don't).
   The worker owns the `processing → cancelled` transition.
2. **Postgres is the single source of truth for the cancel signal and
   progress** — no Redis signal key. The worker injects a `JobContext` that the
   handler uses to read the cancel flag and report progress.
3. **Throttled polling.** The context polls Postgres **at most once per
   `cancel_poll_interval_s`** and caches the flag in worker memory between polls,
   coalescing the cancel-read and the progress-write into **one** guarded
   round-trip. This bounds DB load to `~duration / interval` reads per batch
   regardless of item count (no per-item N+1).
4. **Batch item failures are collected, never raised.** A failing item is
   recorded in the summary and the loop continues; the job ends `completed`. A
   batch therefore only ever retries on genuine *infra* failure (timeout / dead
   worker), routed through the existing `schedule_retry_or_fail`.
5. **Idempotency = replay.** Optional client key, **partial unique index**. A
   repeat with the same key returns the existing job (`200`); same key + a
   different payload → `409`; the DB unique constraint is the race arbiter.
6. **Doomed batches are rejected at the gateway (`422`)**, not left to time out
   in a worker. A `BatchPayload` validator rejects a submission whose estimated
   duration would exceed the handler-timeout budget.

## 4. Data model & migration (`0005_add_cancellation_batch_progress`)

Add to `jobs`:

| Column | Type | Notes |
|--------|------|-------|
| `progress` | `INTEGER NULL` | 0–100; `NULL` for non-batch jobs, set to `0` when a batch begins |
| `cancel_requested_at` | `TIMESTAMPTZ NULL` | cooperative-cancel flag **and** audit timestamp; `NULL` = not requested |
| `idempotency_key` | `TEXT NULL` | client-supplied dedupe key |

Plus a new enum value: `ALTER TYPE job_type ADD VALUE IF NOT EXISTS 'batch'`.

Index:
- `CREATE UNIQUE INDEX uq_jobs_idempotency_key ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;`
  (partial — many rows may have `NULL`; only real keys are constrained unique).

- `upgrade`: add three columns (all nullable, no backfill needed), add the enum
  value, create the partial unique index.
- `downgrade`: drop the index and the three columns. **Leave the enum value** —
  Postgres cannot drop an enum value cleanly; it is inert if unused.

> ⚠️ `ALTER TYPE … ADD VALUE` cannot run inside a transaction block on older
> PostgreSQL. The migration adds the enum value in its own step / with
> `autocommit` (Alembic `op.execute` outside the transactional DDL, or
> `IF NOT EXISTS`) before the index/column DDL. Verify against the project's PG
> version during implementation.

`Job` model (`app/models/job.py`) gains the three mapped columns
(`progress: int | None`, `cancel_requested_at: datetime | None`,
`idempotency_key: str | None`). `JobType` (`app/schemas/enums.py`) gains
`batch = "batch"`.

## 5. Config additions (`app/core/config.py`)

| Setting | Default | Meaning |
|---------|---------|---------|
| `cancel_poll_interval_s` | `2.0` | min wall-clock between a batch's DB polls (cancel-read + progress-flush) |
| `batch_timeout_safety_factor` | `0.8` | fraction of `job_handler_timeout_s` a batch's estimated duration must stay under |

A separate **static module constant** `MAX_BATCH_ITEMS = 500` lives in
`app/schemas/payloads.py` and bounds `len(items)` via a
`Field(max_length=MAX_BATCH_ITEMS)` constraint on the list. It is a compile-time
constant (not a `Settings` value) precisely so it is **always** enforced —
including when `validate_payload` is called without a context (unit tests) —
independent of the runtime duration budget below.

No new invariant validators beyond the existing
`job_handler_timeout_s < visibility_timeout_s`.

### Gateway rejection of doomed batches

`BatchPayload` (see §8) carries a `model_validator(mode="after")` that rejects a
submission whose estimated runtime would exceed the worker budget, returning
`422` **before** anything touches Postgres or Redis:

```python
@model_validator(mode="after")
def _fits_timeout_budget(self, info: ValidationInfo) -> "BatchPayload":
    budget = (info.context or {}).get("handler_timeout_s")
    if budget is not None:
        est_s = (len(self.items) * self.item_delay_ms) / 1000
        if est_s >= budget * (info.context.get("safety_factor", 0.8)):
            raise ValueError(
                "estimated batch duration exceeds worker timeout budget"
            )
    return self
```

A Pydantic validator has no ambient access to `Settings`, so the budget is
plumbed through the **validation context**: `validate_payload` (which already
runs in the route with `settings` in hand) passes it in —
`_ADAPTER.validate_python({**raw, "type": …}, context={"handler_timeout_s":
settings.job_handler_timeout_s, "safety_factor": settings.batch_timeout_safety_factor})`.
When no context is supplied (e.g. a unit test calling `validate_payload` without
settings), the duration budget check is skipped — the static
`MAX_BATCH_ITEMS` field constraint still applies as a hard cap. The safety factor
leaves headroom for the polling/progress round-trips and per-item work beyond the
pure delay.

## 6. Cancellation semantics (`POST /jobs/{job_id}/cancel` → `JobOut`)

The endpoint resolves the job's state through **guarded UPDATEs whose rowcount
decides the outcome** — never a read-then-assume. A bounded re-resolve loop
(≈3 iterations) handles the legal `processing → pending` flap:

```
resolve(job_id):                              # bounded loop, ~3 iterations
  A) guarded: UPDATE … SET status='cancelled'
              WHERE id=:id AND status IN ('pending','scheduled')
     rowcount == 1  ->  if the row was scheduled: ZREM jobs:delayed (best-effort)
                        return 200  JobOut(cancelled)
  B) guarded: UPDATE … SET cancel_requested_at=now()
              WHERE id=:id AND status='processing'
     rowcount == 1  ->  return 202  (cancellation requested, not guaranteed)
  neither won -> re-read the row and map:
      not found             -> 404
      cancelled             -> 200  (idempotent — already cancelled)
      completed | failed    -> 409  (too late to cancel)
      pending | scheduled   -> loop  (a retry re-queued it; try A again)
      processing            -> loop  (claimed between our reads; try B again)
  loop exhausted (pathological flapping) -> 409
```

Why the rowcount checks and the loop matter:

- **Step-B race (must not blindly `202`).** If a fast handler completes/fails the
  job between the re-read and the request-cancel UPDATE, that UPDATE's
  `rowcount == 0`. Returning `202` there would be a lie; instead we fall through
  and re-map to `409`.
- **The `processing → pending` flap.** `schedule_retry_or_fail` (immediate retry)
  and the retry endpoint both legally move a row *back* to `pending`. So "no
  longer `processing`" does **not** imply "terminal" — the loop re-attempts step
  A rather than mis-reporting `409`.

What this leans on (already true, no new code in the hot path):

- A cancelled **pending** job still has a live stream message. The worker's
  existing `claim_job` guard (`WHERE status IN ('pending','scheduled')`) rejects
  it → `job.skipped` → `XACK`. **No change to `claim_job`.**
- A cancelled **scheduled** job is `ZREM`'d from `jobs:delayed`. Even if the
  ticker's `promote` races and `XADD`s a stream message first, that message hits
  the same claim guard and is absorbed. The `ZREM` is best-effort cleanliness;
  correctness rests on the claim guard.

**Cooperative in-flight (`202` path).** Setting `cancel_requested_at` only
*requests* cancellation. Only handlers that poll the context honor it — **batch
does; email/webhook/report (1–5 s) do not** and run to completion. Cancelling
already-finished work is pointless, so a non-cooperative `processing` job that
returns `202` may still end `completed`. This is why the endpoint returns `202`
(requested), not `200` (done), for the processing case. The response body is the
current `JobOut` so the client can observe the state.

## 7. `JobContext` — the injected handler capability

The worker injects a context so a handler can read cancel-state and report
progress without knowing about sessions or Redis. The handler runs inside the
timeout thread (`run_with_timeout`), and SQLAlchemy sessions are not
thread-safe, so the context opens its **own short-lived session** from the
session factory for each poll.

```python
class JobContext(Protocol):
    def set_progress(self, pct: int) -> None: ...   # stash pending %, in-memory
    def cancelled(self) -> bool: ...                # throttled DB poll; cached flag
```

Real implementation `PgJobContext(job_id, session_factory, poll_interval_s)`:

- `set_progress(pct)` stores `self._pending_pct = pct` in memory only — **no DB
  I/O**.
- `cancelled()` is the throttle point. If `monotonic() - self._last_poll >=
  poll_interval_s` (or it's the first call), it runs **one** guarded, coalesced
  round-trip and refreshes the cache:

  ```sql
  UPDATE jobs SET progress = :pending_pct
   WHERE id = :id AND status = 'processing'
   RETURNING cancel_requested_at;
  ```

  This simultaneously (a) flushes the stashed progress, (b) reads the cancel
  flag, and (c) confirms the row is still `processing` (guarded — a late write
  can't resurrect a finalized row; `rowcount == 0` ⇒ treat as cancelled/gone and
  stop). Between ticks, `cancelled()` returns the cached boolean with no I/O.

The handler shape (unchanged from what was approved) — `cancelled()` is called
before the per-item work, so it flushes the *previous* iteration's stashed
percent:

```python
def handle_batch(payload: BatchPayload, ctx: JobContext) -> dict:
    n = len(payload.items)
    summary = {"total": n, "succeeded": 0, "failed": 0, "errors": []}
    for i, item in enumerate(payload.items):
        if ctx.cancelled():
            raise JobCancelled(summary)          # partial summary
        try:
            _process_item(item, payload.item_delay_ms)
            summary["succeeded"] += 1
        except Exception as exc:                 # noqa: BLE001 — per-item, collected
            summary["failed"] += 1
            summary["errors"].append({"index": i, "error": str(exc)})
        ctx.set_progress(int((i + 1) / n * 100))
    return summary                               # job -> completed (even if all failed)
```

- A final forced progress flush to `100` happens on normal completion — the
  worker's `complete_job` sets `progress = 100` for any row whose
  `progress IS NOT NULL` (i.e. a batch that began tracking), leaving non-batch
  jobs `NULL`. So a completed batch always reads `100` even if the last poll
  didn't tick.
- **Testability:** unit tests pass a fake `JobContext` (e.g. `cancelled()`
  returns `True` on the k-th call, `set_progress` records calls). Handlers never
  import a session.
- **Load:** a ~40 s batch with `poll_interval_s=2.0` performs ~20 DB round-trips
  regardless of item count; 10 concurrent such workers do ~200 hits/interval
  rather than thousands at once.

Handler dispatch: all handlers adopt a uniform `(payload, ctx)` signature; the
three existing pure handlers accept and ignore `ctx`. `run_handler`
(`app/jobs/registry.py`) passes the context through.

## 8. Batch handler & worker wiring

**Payload** (`app/schemas/payloads.py`) joins the discriminated union:

```python
class BatchPayload(BaseModel):
    type: Literal[JobType.batch] = JobType.batch
    items: list[dict] = Field(max_length=MAX_BATCH_ITEMS)   # opaque per-item descriptors
    item_delay_ms: int = 50
    # runtime duration budget enforced by the context-aware validator in §5
```

`_process_item` simulates work: `time.sleep(item_delay_ms / 1000)` and, to make
per-item failure demonstrable and deterministic in tests, treats an item
carrying `{"fail": true}` as a failure (raises), otherwise succeeds. Summary
shape: `{total, succeeded, failed, errors: [{index, error}]}`.

**`JobCancelled`** (new exception, `app/jobs/…`) carries the partial summary.

**Worker `process_job`** (`app/worker/runner.py`) gains one `except` clause
**before** the generic handler-exception clause, and builds/injects the context:

```python
job = repo.get_job(session, job_id)
ctx = PgJobContext(job.id, session_factory, settings.cancel_poll_interval_s)
try:
    payload = validate_payload(job.type, job.payload)
    result = run_with_timeout(
        lambda: run_handler(job.type, payload, ctx),
        settings.job_handler_timeout_s,
    )
except JobCancelled as cancelled:
    won = repo.cancel_job(session, job.id, cancelled.summary)   # guarded proc->cancelled
    log.info("job.cancelled", won=won)
    return Outcome(ack=won, recycle=False, label="cancelled")
except HandlerTimeout:
    ...   # unchanged
except Exception as exc:                                       # unchanged
    ...
won = repo.complete_job(session, job.id, result)               # sets progress=100 for batch
...
```

- `cancel_job` is a **guarded terminal** transition mirroring `complete_job`:
  `UPDATE … SET status='cancelled', result=:summary, completed_at=now()
   WHERE id=:id AND status='processing'`, returning `rowcount == 1`.
  `XACK` iff won (same XACK-iff-won rule; a lost guard means the reaper reclaimed
  the lease → skip `XACK`, log critical). Cancellation is terminal and **not**
  counted as a failed attempt (does not touch `attempts`).
- The context needs `session_factory`; `process_job` gains it as a parameter
  (threaded through from `run_forever`, which already builds it).

**Reaper / retry interaction.** Because batch item errors are collected (never
raised), a batch reaches `complete_job` on its own. It only re-enters the retry
machinery on genuine infra failure — handler **timeout** (doomed batches are
rejected at submission per §5, so this is exceptional) or a **dead worker**
(reaper `schedule_retry_or_fail` → `processing → pending`). A reclaimed batch
**re-runs from the start**; `progress` resets to `0`. Accepted: batches are not
per-item idempotent across attempts, and this only happens on infra failure.

## 9. Idempotency on `POST /jobs`

```
submit(submission):
  key = submission.idempotency_key
  if key is not None:
    existing = repo.get_by_idempotency_key(key)
    if existing is not None:
        if (existing.type, existing.payload) == (submission.type, submission.payload):
            return 200  JobAccepted(existing)          # replay
        return 409  ("idempotency key reused with a different payload")
    # create with key; the partial unique index is the race arbiter:
    try:
        job = repo.create_job(..., idempotency_key=key)
    except IntegrityError:                              # concurrent same-key insert lost
        session.rollback()
        existing = repo.get_by_idempotency_key(key)     # the winner's row
        return 200  JobAccepted(existing)
  else:
    job = repo.create_job(...)                          # today's behavior, always create
  # …existing handoff (enqueue/schedule + mark_synced)… -> 202 JobAccepted(job)
```

- **New job → `202`** (unchanged status for creation); **replay → `200`**.
- The enqueue/schedule handoff and `mark_synced` run **only** on the create
  path, never on a replay (the original submission already did the handoff).
- `create_job` (`app/repository.py`) gains an `idempotency_key: str | None = None`
  parameter written onto the row.
- Keys never expire; a cancelled/failed job's key stays claimed. Resubmitting the
  same key returns that terminal job (documented; use `/retry` to re-run, or a
  fresh key to submit anew). This is the standard "idempotent create" contract,
  not a scheduling-dedupe window.

## 10. API & schema surface

- **`JobSubmission`** (`app/schemas/api.py`) gains `idempotency_key: str | None =
  None`.
- **`JobOut`** gains `progress: int | None` and `cancel_requested_at: datetime |
  None`.
- **`POST /jobs/{job_id}/cancel` → `JobOut`** — new route (§6); `404`/`409`/`200`/
  `202` per the resolution table.
- **`POST /jobs`** — idempotency handling (§9); success stays `202`, replay `200`.
- `validate_payload` passes the timeout budget via validation context (§5) and
  registers `BatchPayload` in the discriminated union.

## 11. Failure modes & edge cases

| Scenario | Outcome |
|----------|---------|
| Cancel a `pending`/`scheduled` job | Guarded → `cancelled`; `ZREM` if scheduled; `200`. Stale stream msg absorbed by claim guard. |
| Cancel a `processing` **batch** | `cancel_requested_at` set → `202`; handler polls within `cancel_poll_interval_s`, raises `JobCancelled` → worker guarded `→ cancelled` with partial summary. |
| Cancel a `processing` **non-batch** (email/webhook/report) | `202` (requested) but handler never polls → job runs to `completed`. Best-effort; documented. |
| Cancel a `completed`/`failed` job | `409` (too late). |
| Cancel an already-`cancelled` job | `200` (idempotent). |
| Cancel unknown id | `404`. |
| Job finalizes between re-read and request-cancel UPDATE | Step-B `rowcount==0` → fall through → re-map → `409`. (Not a misleading `202`.) |
| Job flaps `processing → pending` (immediate retry) mid-cancel | Bounded loop re-attempts step A; not mis-reported as terminal. |
| Batch with some failing items | Loop collects errors, `succeeded/failed` counts; job `completed`, `progress=100`. |
| Batch where **every** item fails | Still `completed` with a summary (`succeeded=0`); not `failed`. |
| Batch estimated duration ≥ budget | Rejected at submission with `422` before DB/Redis touched. |
| Batch worker dies mid-run | Reaper `schedule_retry_or_fail` → `processing → pending` → **re-runs whole batch**, `progress` resets to 0. |
| Batch self-times-out | Exceptional (gateway rejects doomed batches); `HandlerTimeout` → retry + recycle, same as any handler. |
| Progress write after finalize (late poll) | Guarded `WHERE status='processing'` → `rowcount==0`, no-op; context treats as gone and stops. |
| Duplicate idempotency key, same payload | `200`, existing job; no new row, no re-enqueue. |
| Duplicate idempotency key, different payload | `409`. |
| Concurrent same-key submits | Unique index → one INSERT wins; loser catches `IntegrityError`, re-looks-up, returns `200` existing. |
| Submit without a key | Unchanged — always creates, `202`. |

## 12. Testing plan (`uv run pytest`)

**Unit**
- Batch summary: mixed pass/fail items → correct counts + `errors`; **all-fail**
  batch still returns a summary (worker will mark it `completed`).
- Cancel mid-batch: fake `JobContext.cancelled()` returns `True` on the k-th
  call → `handle_batch` raises `JobCancelled` carrying the partial summary at k.
- Throttle: fake clock — `cancelled()` hits the DB only once per
  `poll_interval_s`; `set_progress` between ticks does no I/O; the flushed value
  is the latest stashed percent.
- `BatchPayload` budget validator: rejects `items × item_delay_ms ≥ budget ×
  factor` (`422`), accepts under budget; skipped when no context supplied.
- Idempotency helper: same-payload match vs different-payload mismatch decision.

**Integration (real Redis + Postgres)**
- Cancel `pending` → `cancelled`, stream msg later absorbed by claim guard (job
  never runs).
- Cancel `scheduled` → `cancelled` **and** `ZREM`'d from `jobs:delayed`.
- Cancel `processing` batch end-to-end: submit a batch, let the worker start,
  `POST /cancel` → `202`, assert worker transitions to `cancelled` with a
  partial summary and `progress < 100`.
- Cancel `completed`/`failed` → `409`; cancel already-`cancelled` → `200`;
  unknown → `404`.
- Guarded `cancel_job` loses to reaper: force row off `processing` before it runs
  → `rowcount==0`, worker skips `XACK`.
- Progress advances: submit a batch, poll `GET /jobs/{id}` and observe `progress`
  climbing, `100` at completion.
- Idempotency: same key twice → second returns `200` with the first job's id, no
  second row / no second enqueue; same key + different payload → `409`;
  concurrent same-key submits → exactly one row, both callers observe it.

**Existing-test migration**
- Handler-calling tests updated for the `(payload, ctx)` signature (pure handlers
  ignore `ctx`).
- Fixtures creating jobs pick up the new nullable columns (`progress=NULL`,
  `cancel_requested_at=NULL`, `idempotency_key=NULL`).
- `JobOut` assertions updated for the two new fields.

## 13. Docker Compose

No new services and no topology change. The reaper/ticker and worker roles are
unchanged; all new behavior lives in the existing API, worker, and handler code
plus one migration.
