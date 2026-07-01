# Health check & queue statistics — Design

**Date:** 2026-07-01
**Status:** Approved (pre-implementation)
**Branch context:** builds on the priority-scheduling + failure-handling +
cancellation/batch work (three priority streams `jobs:stream:{high,normal,low}`,
one consumer group `workers`, delayed ZSET `jobs:delayed`, ticker with
promotion / orphan reconciliation / reaper). Alembic head is `0005`.

## 1. Context & current state

Today the API exposes a single trivial probe — `GET /health` returns
`{"status": "ok"}` ([app/api/routes.py](../../../app/api/routes.py)) — with no
dependency checks and no operational visibility into the queue. There is no way
to answer "is the API actually able to reach Redis and Postgres?" or "how deep
is the backlog, how many workers are alive, how many jobs are stuck pending?"
without shelling into Redis / Postgres directly.

This item is the first leftover in
[docs/requirements/leftovers.md](../../requirements/leftovers.md): *"Health
check with queue statistics."*

Relevant facts about the current system that shape the metric semantics:

- **Streams are never trimmed.** There is no `XTRIM` / `XDEL` / `MAXLEN`
  anywhere in `app/`. Workers `XACK` but do not delete entries, so `XLEN` counts
  *every entry ever added* and grows monotonically — it is **not** a valid
  "waiting" depth.
- **Dead consumers are never removed.** The reaper reclaims stale PEL entries via
  `XAUTOCLAIM` but never calls `XGROUP DELCONSUMER` (documented, not implemented —
  see `DECISIONS.md`). Because consumer names are unique-per-process
  (`<host>_<uuid>`), every restart/crash leaves a dead consumer in the group
  forever. A raw consumer count therefore overcounts live workers.
- **The `api` service has no Docker healthcheck** in `docker-compose.yml`, so
  enriching `/health`'s response breaks no existing consumer.
- Routes use **sync** SQLAlchemy `Session` + **sync** `redis.Redis` via the
  existing `get_db` / `get_redis` dependencies. The Redis client has a 10s
  `socket_timeout`.

## 2. Goals & non-goals

**Goals**
- `GET /health` — combined **liveness + readiness**: pings Postgres and Redis,
  returns `200` when both reachable, `503` when either is not, and always reports
  per-backend status in the body.
- `GET /stats` — operational **queue statistics** for humans / monitoring tools:
  per-priority ready depth + in-flight, live worker count, delayed-set size,
  job counts by status, and oldest-pending age. **All-or-nothing**: `503` if any
  data source fails (no partial payloads).
- Correct metric semantics given the never-trimmed streams and never-reaped
  consumers (see §5).
- Wire `/health` as the `api` service's Docker healthcheck.

**Non-goals**
- Prometheus / OpenMetrics exposition format (plain JSON only).
- A background snapshot collector / caching layer — `/stats` computes live on
  request (revisit only if high-frequency scraping proves it necessary; noted in
  §11).
- Per-worker or per-job-type breakdowns beyond what is listed in §4.
- Making the full `GROUP BY status` count cheap at very large scale — kept as-is
  and documented as a known limitation with remedies in §11.
- Alerting / thresholds / autoscaling logic on top of the metrics.

## 3. Decisions (locked)

1. **Two endpoints.** `GET /health` (liveness + readiness combined) and
   `GET /stats` (statistics). Rationale: the frequently-polled probe stays cheap
   and never runs aggregation queries, while stats live on a separate path.
2. **`/stats` is all-or-nothing** — `503` if Redis *or* Postgres is unavailable.
   The contract is "complete data or an error," never a partial payload.
3. **Depth = consumer-group `lag`, in-flight = consumer-group `pending`** (from
   `XINFO GROUPS`), **not `XLEN`** — because acked messages linger in the
   never-trimmed stream.
4. **Live worker count uses minimum idle across streams.** A worker registers as
   a separate consumer in each stream's group; a worker saturated on `high` looks
   stale on `low`. Liveness is computed from a `dict[consumer_name, min_idle_ms]`
   folded across all three streams, `live = min_idle_ms < visibility_timeout_s *
   1000`. Reuses the existing `visibility_timeout_s` (the reaper's staleness
   boundary) — no new knob.
5. **`GROUP BY status` is kept as-is** (all six statuses, exact) and documented as
   a scaling limitation (§11). **One new index** is added — a composite
   `(status, created_at)` — solely to make `oldest_pending_age` an instant
   index-min. The existing `status` index is left untouched (`list_jobs` relies
   on it).
6. **Computed live, sync, single Redis round-trip.** Redis calls are batched into
   one non-transactional pipeline; Postgres runs two small queries. Logic lives in
   a new `app/observability.py`; routes stay thin.

## 4. Endpoints & response contracts

### `GET /health` — liveness + readiness

Replaces the current `{"status": "ok"}`. Runs a Postgres `SELECT 1` and a Redis
`PING`. No aggregation — stays cheap for frequent polling. Both checks run even
if the first fails, so the body always reports each backend.

```jsonc
// 200 OK — both reachable
{ "status": "ok", "checks": { "postgres": "ok", "redis": "ok" } }

// 503 Service Unavailable — either backend unreachable
{ "status": "unavailable", "checks": { "postgres": "ok", "redis": "error" } }
```

`status` is `"ok"` iff both checks are `"ok"`, else `"unavailable"` with HTTP
`503`.

### `GET /stats` — queue statistics

```jsonc
// 200 OK
{
  "queue": {
    "streams": {
      "high":   { "depth": 3,  "in_flight": 1 },
      "normal": { "depth": 12, "in_flight": 4 },
      "low":    { "depth": 0,  "in_flight": 0 }
    },
    "scheduled": 5,
    "workers": 3
  },
  "jobs": {
    "by_status": {
      "scheduled": 5, "pending": 15, "processing": 5,
      "completed": 1200, "failed": 8, "cancelled": 2
    },
    "oldest_pending_age_seconds": 42.7
  }
}

// 503 Service Unavailable — any source failed
{ "detail": "stats unavailable" }
```

**Field definitions**

| Field | Source | Meaning |
|---|---|---|
| `queue.streams.<prio>.depth` | `XINFO GROUPS` → `lag` | Ready backlog not yet delivered to the group. `null` if Redis cannot compute lag (see §5.1). |
| `queue.streams.<prio>.in_flight` | `XINFO GROUPS` → `pending` | Messages delivered but not yet acked (PEL size). |
| `queue.scheduled` | `ZCARD jobs:delayed` | Delayed jobs parked in the ZSET awaiting promotion. |
| `queue.workers` | `XINFO CONSUMERS` × 3 streams | Distinct live consumers (min-idle < cutoff — §5.2). |
| `jobs.by_status.<status>` | Postgres `GROUP BY status` | Exact count per status; all six keys always present (zero-filled). |
| `jobs.oldest_pending_age_seconds` | `now − min(created_at) WHERE status='pending'` | Age of the oldest still-pending job; `null` when none. |

Note: `queue.scheduled` (Redis ZSET size) and `jobs.by_status.scheduled`
(Postgres) are two independent views of the same concept and may momentarily
differ during promotion/reconciliation; both are reported intentionally.

## 5. Metric computation

All logic lives in `app/observability.py`, exposing two sync functions:

- `check_readiness(session, client) -> dict[str, str]`
- `gather_stats(session, client, settings) -> StatsResponse`

### 5.1 Redis — one pipelined round-trip

On `client.pipeline(transaction=False)`, queued once and `.execute()`d:

- For each stream in `settings.ordered_streams`: `XINFO GROUPS <stream>` — the
  entry for group `workers` yields `lag` (→ `depth`) and `pending` (→
  `in_flight`).
- For each stream: `XINFO CONSUMERS <stream> workers` — each consumer's `name`
  and `idle` (ms), used for the worker count.
- `ZCARD jobs:delayed` — `queue.scheduled`.

**Lag fallback.** `lag` can be `nil` if Redis cannot compute it (only after
entries are deleted — which this system never does, so it is effectively always
present). Defensive order: use `lag` when numeric; else derive
`entries-added − entries-read` from `XINFO STREAM` / group fields; else report
`depth: null` rather than raising.

**Group-not-found.** If a priority stream/group does not exist yet (fresh
deploy before any job of that priority), treat that stream's `depth`/`in_flight`
as `0` rather than erroring (mirror the `NOGROUP`/`BUSYGROUP` guarding used
elsewhere, e.g. `ensure_group`).

### 5.2 Worker liveness — minimum idle across streams

The strict-priority reader (`read_priority`,
[app/queue/consumer.py](../../../app/queue/consumer.py)) probes high→normal→low
and returns on the first non-empty stream, so a worker draining `high` never
touches its `low`-group consumer record; that record's `idle` grows past the
visibility timeout even though the worker is alive.

```python
def live_worker_count(consumer_rows_per_stream, cutoff_ms) -> int:
    min_idle: dict[str, int] = {}
    for rows in consumer_rows_per_stream:          # one list per stream
        for row in rows:
            name, idle = row["name"], int(row["idle"])
            min_idle[name] = min(min_idle.get(name, idle), idle)
    return sum(1 for idle in min_idle.values() if idle < cutoff_ms)
```

`cutoff_ms = int(settings.visibility_timeout_s * 1000)`. A live worker has
near-zero idle on at least one stream (the one it is draining, or all three
during an idle blocking read), so its minimum falls under the cutoff. Dead /
restarted consumers (never reaped) have large idle on every stream and are
excluded. This is a pure function → unit-tested without Redis.

### 5.3 Postgres — two small queries

- **Status counts:** `SELECT status, count(*) FROM jobs GROUP BY status`, then
  zero-fill every `JobStatus` value in Python so the payload shape is stable.
  Kept as a full scan by decision §5/§11.
- **Oldest pending age:** `SELECT min(created_at) FROM jobs WHERE
  status='pending'`; `age = (now(utc) − min).total_seconds()`, or `null` when no
  rows. Served by the new `(status, created_at)` composite index as an index-only
  min (§6).

Pure transforms (`zero_fill_status_counts`, `pending_age_seconds`) are extracted
for unit testing.

## 6. Data model & migration (`0006_add_status_created_at_index`)

No column changes. One new index, chained after `0005`:

```python
# revision = "0006"; down_revision = "0005"
def upgrade() -> None:
    op.create_index(
        "ix_jobs_status_created_at", "jobs", ["status", "created_at"]
    )

def downgrade() -> None:
    op.drop_index("ix_jobs_status_created_at", table_name="jobs")
```

Turns `min(created_at) WHERE status='pending'` into an instant index-only min
regardless of table size. The pre-existing single-column `status` index is left
in place (used by `list_jobs`' status filter). A matching
`Index(...)` / `index=True` declaration is added to
[app/models/job.py](../../../app/models/job.py) so the ORM metadata and the
migration agree.

## 7. Error handling & resilience

- **`/health`:** run both checks independently; a failure in one must not mask
  the other in the body. Return `503` (via `fastapi.responses.JSONResponse` /
  setting `response.status_code`) when either check fails, still reporting
  `postgres`/`redis` as `"ok"`/`"error"`. Worst-case latency is bounded by the
  Redis client's 10s `socket_timeout` — acceptable, noted as a future tuning
  point (a dedicated short-timeout ping client).
- **`/stats`:** wrap the whole gather in `try/except (redis.RedisError,
  sqlalchemy.exc.SQLAlchemyError)`. On any failure → `HTTPException(status_code=
  503, detail="stats unavailable")` plus a structured `log.warning` naming the
  failed source (per CLAUDE.md: structured logging, no `print`). Never emit a
  partial payload.
- Both endpoints reuse the existing `get_db` / `get_redis` dependencies and the
  sync `Session` + sync `redis.Redis` client, matching every other route.

## 8. Module & file structure

```
app/
  observability.py      # NEW — check_readiness, gather_stats, pure helpers
  api/routes.py         # /health enriched, /stats added (thin; delegate)
  schemas/api.py        # NEW response models (see below)
  models/job.py         # + composite index declaration
alembic/versions/
  0006_add_status_created_at_index.py   # NEW
docker-compose.yml      # api service healthcheck (see §9)
tests/
  unit/test_observability.py            # NEW — pure helpers
  integration/test_health_stats.py      # NEW — real Redis + Postgres
```

New Pydantic response models in `app/schemas/api.py`: `HealthResponse`
(`status`, `checks`), `StreamStat` (`depth`, `in_flight`), `QueueStats`
(`streams`, `scheduled`, `workers`), `JobStats` (`by_status`,
`oldest_pending_age_seconds`), `StatsResponse` (`queue`, `jobs`).

## 9. Docker healthcheck wiring

The `api` service currently has no healthcheck. Add one that hits `/health` using
the Python already present in the image (no extra `curl` dependency):

```yaml
  api:
    # ...
    healthcheck:
      test: ["CMD", "python", "-c",
             "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').status==200 else 1)"]
      interval: 15s
      timeout: 12s
      retries: 5
      start_period: 10s
```

`timeout: 12s` is chosen to clear the shared Redis client's 10s `socket_timeout`
(§7): a probe waiting on a hung Redis can take up to ~10s, so a shorter
healthcheck timeout would spuriously fail it. `interval` is set to `15s` to stay
above the timeout. `urlopen` raises on non-2xx, so a `503` (dependency down)
fails the probe as intended. This mirrors the `pg_isready` / `redis-cli ping`
healthchecks already on the `postgres` / `redis` services.

## 10. Testing

**Unit (fast, no Docker) — `tests/unit/test_observability.py`:**
- `live_worker_count`: the strict-priority trap (idle 0 on `high`, stale on
  `low` → counted **live**); a genuinely stale consumer excluded; dedup of the
  same name across streams.
- `zero_fill_status_counts`: partial `GROUP BY` result → all six keys present.
- `pending_age_seconds`: `null` when no pending rows; correct delta otherwise.

**Integration (testcontainers, real Redis + Postgres) —
`tests/integration/test_health_stats.py`:** Redis Streams consumer-group state
(`lag`, consumer `idle`) is exactly where `fakeredis` diverges (per the Phase 1
spec), so these run against real Redis.
- `/health` → `200` `{postgres:ok, redis:ok}` when both up; `503` with the
  failing backend marked when a dependency is unreachable (dependency-override a
  client/session pointed at a dead port).
- `/stats` happy path: `XADD` across the three priority streams and read a subset
  via `XREADGROUP` without `XACK` → assert `depth` (lag) drops and `in_flight`
  (pending) rises per priority; park jobs in `jobs:delayed` → `scheduled`; insert
  jobs across all statuses → exact `by_status` + non-null
  `oldest_pending_age_seconds`.
- `/stats` → `503` when a backend is down (all-or-nothing).
- Migration: `alembic upgrade head` creates `ix_jobs_status_created_at`;
  downgrade drops it.

All via `uv run pytest`, following the existing `tests/unit` + `tests/integration`
split and the `test_settings` fixture.

## 11. Known limitations & future work

- **Full `GROUP BY status` is O(table size).** A plain B-tree on the
  low-cardinality `status` column does not make it cheap; terminal history grows
  unbounded. Accepted at current scale and for the lower-frequency `/stats`
  endpoint. Remedies when it matters: a partial index on active states
  (`WHERE status IN ('pending','processing','scheduled')`) for the counts
  autoscalers watch, and/or a transactionally-maintained counters table for
  exact O(1) terminal totals (invasive — touches every guarded transition).
- **`/health` latency is bounded by the shared Redis client's 10s socket
  timeout.** A dedicated short-timeout ping client would tighten probe response
  under a hung Redis.
- **`queue.workers` counts consumers, not processes with capacity.** Until
  `XGROUP DELCONSUMER` cleanup exists, correctness relies on the min-idle cutoff;
  a truly wedged-but-connected worker within the cutoff window would still count.
- **No caching / snapshotting.** If `/stats` is scraped at high frequency, a
  background collector writing a periodic snapshot (in the ticker) would decouple
  endpoint latency from query cost — deferred (conflicts with the live
  all-or-nothing contract).
