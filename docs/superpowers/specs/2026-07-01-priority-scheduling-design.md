# Priority scheduling — Design

**Date:** 2026-07-01
**Status:** Approved (pre-implementation)
**Branch context:** builds on the scheduled-jobs work (delayed ZSET + ticker)

## 1. Context & current state

Today the system uses a **single** Redis stream `jobs:stream`. `POST /jobs`
validates the payload, `create_job` commits the row (`PENDING` immediate, or
`SCHEDULED` parked in the `jobs:delayed` ZSET), then hands off to Redis and flips
`is_synced_to_redis`. Each worker runs one `XREADGROUP … COUNT 1` against that
one stream, claims the job, runs the handler, and `XACK`s. The ticker promotes
mature scheduled jobs from the ZSET into the stream; a reconciler re-enqueues
orphans (`is_synced_to_redis = FALSE`).

The `Job` model has no notion of priority — all jobs share one FIFO stream.

This phase adds **priority scheduling**: every job carries a priority, jobs flow
through **three** priority streams, and workers drain them **high-first**.

## 2. Goals & non-goals

**Goals**
- Every job carries a priority: `high` / `normal` / `low` (default `normal`).
- Three Redis streams, one per priority; jobs route to the stream matching their
  priority on both the immediate and the scheduled (promoted) paths.
- Workers enforce **strict** priority: a `high` backlog is fully drained before
  any `normal` is processed, `normal` before `low`.
- API: accept `priority` on submit, echo it in job responses, and filter
  `GET /jobs` by priority.
- Preserve every existing guarantee: at-least-once delivery, the
  commit-then-handoff invariant, orphan reconciliation, and idempotent claims.

**Non-goals (out of scope)**
- Numeric/arbitrary priority levels — priority is a fixed 3-value enum.
- Re-prioritizing a job after submission (no PATCH/re-queue endpoint).
- Anti-starvation weighting / fair scheduling — strict drain is intentional; a
  weighting knob is a documented future extension.
- Per-stream queue-depth endpoint / health stats — tracked separately in
  `docs/requirements/leftovers.md`.

## 3. Decisions (locked)

1. **Priority model = 3-value enum** `JobPriority(high|normal|low)`, default
   `normal`, mapping 1:1 to three streams. Higher = more urgent.
2. **Worker policy = strict sequential drain.** Probe streams in priority order
   non-blocking; only block when all are empty. Accepts `low` starvation under
   sustained `high` load. This deviates from the requirement's literal "single
   simultaneous `XREADGROUP`" wording in exchange for a true high-first guarantee.
3. **Scheduled-job routing = DB lookup at promotion (Approach A).** Postgres is
   the source of truth for priority. The ticker resolves the priority of a due
   batch with **one batched** `SELECT id, priority WHERE id IN (:due_ids)`; the
   reconciler already loads full `Job` rows and routes for free.
4. **Stream naming = three explicit names**, ordered by a single accessor so the
   priority order can never drift between producer, worker, ticker, and API.

## 4. Data model & migration

Add to the `jobs` table:

- `priority job_priority NOT NULL DEFAULT 'normal'` — a new PG enum type
  `job_priority` with values `high`, `normal`, `low`.

Index:

- `ix_jobs_priority` on `(priority)` — supports the `GET /jobs?priority=` filter.
  (A plain btree is sufficient for the assignment; a composite
  `(priority, created_at, id)` would further help the filtered+ordered list but
  is deferred as an optimization.)

Migration `0003_add_priority`:
- `upgrade`: create the `job_priority` enum; add the `priority` column NOT NULL
  with `server_default='normal'` (this backfills every existing row to `normal`);
  create `ix_jobs_priority`.
- `downgrade`: drop the index, drop the column, drop the enum type.

Update the `Job` model (`app/models/job.py`) with the new mapped column:
`priority: Mapped[JobPriority] = mapped_column(SAEnum(JobPriority,
name="job_priority"), default=JobPriority.normal, server_default="normal",
index=True)`.

## 5. Enum & stream mapping

- New `JobPriority(str, Enum)` in `app/schemas/enums.py`: `high`, `normal`, `low`.
- Config (`app/core/config.py`) replaces `jobs_stream` with three names:

  | Setting | Default |
  |---------|---------|
  | `stream_high` | `"jobs:stream:high"` |
  | `stream_normal` | `"jobs:stream:normal"` |
  | `stream_low` | `"jobs:stream:low"` |

- One ordered accessor is the **single source of priority order**, e.g.
  `Settings.priority_streams -> list[tuple[JobPriority, str]]` returning
  `[(high, stream_high), (normal, stream_normal), (low, stream_low)]`, plus a
  helper `stream_for_priority(settings, priority) -> str`. Everything (API,
  producer callers, worker, ticker, reconciler, `ensure_group`) consumes these —
  never a hardcoded name or order.
- `consumer_group` stays `"workers"`, but the group is now created on **all
  three** streams: `ensure_group` is looped over `priority_streams` in the worker,
  the ticker, and the API lifespan (`app/main.py`).

## 6. Producer & routing

`producer.enqueue(client, stream, job_id)` stays generic (takes an explicit
stream). Callers select the target stream with `stream_for_priority(settings,
priority)`:

- **Immediate submit** (`POST /jobs`, no future `scheduled_at`) →
  `XADD stream_for_priority(job.priority) {job_id}`.
- **Ticker promotion / reconciler** → route by the job's stored priority (§8).

## 7. Worker — strict sequential drain

New consumer primitive in `app/queue/consumer.py`:

```
read_priority(client, priority_streams, group, consumer, block_ms)
    -> list[tuple[stream, message_id, fields]]
```

1. **Probe** each stream in priority order, **non-blocking**
   (`XREADGROUP GROUP g c COUNT 1 STREAMS <one> >`). The first non-empty stream
   returns immediately → a list of one, tagged with its stream.
2. If **all** probes are empty, issue **one blocking** read across all three
   (`XREADGROUP … BLOCK block_ms COUNT 1 STREAMS high normal low > > >`), which
   may return up to one entry per stream. Return them ordered by priority.

Worker loop (`app/worker/runner.py`), otherwise unchanged from today:

```
for (stream, message_id, fields) in read_priority(...):
    job_id = UUID(fields["job_id"])
    process_job(session, job_id)          # claim → run → complete/fail, commits
    ack(client, stream, group, message_id)  # ack on the message's OWN stream
```

Because every processed batch restarts the loop at the `high` probe, `high` is
fully drained before `normal` is touched, and `normal` before `low`. The only
softness is the idle blocking wait in step 2, which is reached only when all
streams were empty — it cannot reorder an existing backlog.

**Correctness notes:**
- The blocking multi-stream read moves entries into this consumer's PEL, so the
  worker must process **and ack every** returned message — that is why the
  primitive returns a list, not a single entry.
- `ack` targets the stream the message came from; a message id is only valid on
  its own stream/group.
- `COUNT 1` preserves today's one-job-at-a-time claim/commit/ack semantics. A
  `read_count` batch knob is a future throughput optimization, not in scope.
- The non-blocking probes do not busy-spin: when everything is empty, control
  reaches the single blocking read in step 2.

## 8. Ticker & reconciler routing (batched)

### 8.1 Promotion (`promote_due`)

1. `ids = ZRANGEBYSCORE jobs:delayed 0 <now_epoch> LIMIT 0 <ticker_batch_size>`
   (unchanged).
2. **One batched lookup**: `repo.get_priorities(session, ids) ->
   dict[UUID, JobPriority]` (`SELECT id, priority FROM jobs WHERE id IN (:ids)`).
3. Group ids by target stream via `stream_for_priority`; pipeline `XADD` each
   group to its stream. Ids with **no** row (e.g. cancelled/deleted) are dropped —
   not enqueued.
4. `ZREM jobs:delayed <all due ids>` (after the XADDs — preserves the existing
   `XADD`-before-`ZREM` crash-safety; a crash between leaves ids in the ZSET for
   next tick, and the duplicate stream entry is absorbed by the idempotent claim).
5. `repo.promote_scheduled_to_pending(session, ids)` — unchanged (flips
   `scheduled → pending` for the matching rows).

### 8.2 Reconciler (`reconcile_orphans`)

Already loads full `Job` rows via `list_unsynced`, so each row carries
`priority`. The only change: the `pending` re-enqueue path routes to
`stream_for_priority(settings, job.priority)` instead of the single stream. The
`scheduled` path (re-`ZADD` to the delayed ZSET) is unchanged.

## 9. API

- `JobSubmission` (`app/schemas/api.py`) gains `priority: JobPriority =
  JobPriority.normal`. Absent → `normal`.
- `submit_job` (`app/api/routes.py`) passes `priority` to `create_job` and, on
  the immediate path, enqueues to `stream_for_priority(settings, priority)`. The
  scheduled path is unchanged (the ZSET holds only the id; priority is read back
  at promotion per §8.1).
- `create_job` (`app/repository.py`) gains a `priority: JobPriority =
  JobPriority.normal` keyword and sets it on the row.
- `JobAccepted` and `JobOut` gain `priority: JobPriority`.
- `list_jobs` (repo + route) gains a `priority: JobPriority | None` filter
  (`GET /jobs?priority=high`), mirroring the existing `status` / `type` filters
  (`WHERE priority = :priority` when provided).

## 10. Failure modes & edge cases

| Scenario | Outcome |
|----------|---------|
| Submit with no `priority` | Defaults to `normal`; routes to `stream_normal`. |
| Immediate `XADD` fails after PG commit | Row stays `synced=FALSE`; reconciler re-enqueues to the priority stream after grace (§8.2). |
| Ticker crash between `XADD` and `ZREM` | Ids remain in ZSET; re-promoted next tick; duplicate absorbed by idempotent claim. |
| Due id in ZSET but no DB row (cancelled/deleted) | Dropped in step 3 — not enqueued; still `ZREM`'d so it can't re-accumulate. |
| Blocking read returns entries from multiple streams | All are in the PEL; worker processes/acks each in priority order. |
| Sustained `high` load | `low` (and possibly `normal`) starve — accepted per Decision 2; anti-starvation weighting is a future knob. |
| In-flight messages on the old `jobs:stream` during cutover | Abandoned. Redis has no volume (`leftovers.md`), so streams are ephemeral; dev/assignment-acceptable. |

## 11. Docker Compose

No new service. The existing `worker` and `ticker` services are unchanged in
shape; they pick up the three-stream behavior from the new config/code. (Scaling
workers with `--scale worker=N` continues to work; all workers share the
`workers` group across all three streams.)

## 12. Testing plan (`uv run pytest`)

**Unit**
- `JobPriority` default and enum values; `JobSubmission` default `priority ==
  normal`.
- `stream_for_priority` mapping and `priority_streams` ordering
  (high → normal → low).
- Config exposes the three stream names with correct defaults.

**Integration (real Redis + Postgres, matching the existing suite)**
- **Routing:** submit `high` / `normal` / `low` → each lands in the correct
  stream (assert per-stream `XLEN`).
- **Strict drain:** enqueue a `low` then a `high`; run the worker; assert `high`
  is claimed/completed before `low` (via `started_at` ordering or a recording
  handler).
- **Ack correctness:** after processing, no entries linger in any stream's PEL
  (`XPENDING` empty) — proves acks target the right stream.
- **Ticker:** a due `high` scheduled job is promoted to `stream_high` via the
  batched `get_priorities` lookup; ZSET cleared; status `scheduled → pending`.
- **Reconciler:** an unsynced `high` orphan is re-enqueued to `stream_high`.
- **API:** submit with `priority` echoes it in the response and persists it;
  `GET /jobs?priority=high` returns only high-priority jobs; default is `normal`.

**Existing-test migration**
- Update helpers/assertions referencing `settings.jobs_stream` to the new stream
  config: `tests/integration/test_api.py`, `tests/integration/test_ticker.py`,
  `tests/integration/test_worker.py`, `tests/integration/test_queue.py`,
  `tests/unit/test_config.py`.
