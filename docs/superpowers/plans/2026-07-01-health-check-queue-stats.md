# Health Check & Queue Statistics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a combined liveness+readiness `GET /health` and an all-or-nothing `GET /stats` queue-statistics endpoint to the job-processing API.

**Architecture:** A new `app/observability.py` holds the read-only logic (a readiness probe plus a stats gatherer that batches Redis calls into one pipeline and runs two small Postgres queries); thin routes in `app/api/routes.py` delegate to it and shape the HTTP response. Metric semantics account for two existing realities: streams are never trimmed (so depth = consumer-group `lag`, in-flight = `pending`) and dead consumers are never reaped (so live workers are counted by minimum idle across streams). One new `(status, created_at)` index makes the oldest-pending query an index-min.

**Tech Stack:** FastAPI, SQLAlchemy 2.0 + Alembic, redis-py (sync, Redis Streams), structlog, Pytest + testcontainers, uv.

## Global Constraints

- All Python/tools run via `uv` — `uv run pytest`, `uv run ruff check --fix`, `uv run ruff format`. Never `pip`/`venv`/`poetry`.
- Structured logging only (`structlog`); never `print`.
- Endpoints use the **sync** SQLAlchemy `Session` (via `get_db`) and **sync** `redis.Redis` (via `get_redis`) — match every existing route.
- `GET /stats` is **all-or-nothing**: `503` `{"detail": "stats unavailable"}` if Redis *or* Postgres fails; never a partial payload.
- `depth` = consumer-group `lag`; `in_flight` = consumer-group `pending` — **not** `XLEN`.
- Live worker count = distinct consumers with `min_idle_ms < visibility_timeout_s * 1000`, minimum idle taken across all three streams.
- `GROUP BY status` is kept as-is (all six `JobStatus` values, zero-filled). The **only** new index is composite `(status, created_at)`; migration `0006` chains after `0005`. Leave the existing single-column `status` index in place.
- Redis reads for `/stats` use one `pipeline(transaction=False)` round-trip.
- Docker healthcheck for the `api` service: `timeout: 12s`, `interval: 15s` (clears the client's 10s `socket_timeout`).
- Spec: [docs/superpowers/specs/2026-07-01-health-check-queue-stats-design.md](../specs/2026-07-01-health-check-queue-stats-design.md).

---

### Task 1: Composite `(status, created_at)` index + migration

**Files:**
- Modify: `app/models/job.py` (`__table_args__`)
- Create: `alembic/versions/0006_add_status_created_at_index.py`
- Test: `tests/integration/test_migration.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: index `ix_jobs_status_created_at` on `jobs (status, created_at)`; Alembic head becomes `0006`.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_migration.py`:

```python
def test_status_created_at_index_exists(pg_engine):
    insp = inspect(pg_engine)
    index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
    assert "ix_jobs_status_created_at" in index_names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_migration.py::test_status_created_at_index_exists -v`
Expected: FAIL — `ix_jobs_status_created_at` not in the index set (the session-scoped `pg_engine` upgrades to current head `0005`, which lacks it).

- [ ] **Step 3: Create the migration**

Create `alembic/versions/0006_add_status_created_at_index.py`:

```python
"""add (status, created_at) composite index for oldest-pending stats

Revision ID: 0006
Revises: 0005
Create Date: 2026-07-01
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_jobs_status_created_at", "jobs", ["status", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_status_created_at", table_name="jobs")
```

- [ ] **Step 4: Declare the index on the model**

In `app/models/job.py`, extend the existing `__table_args__` tuple (keep the idempotency index) so the ORM metadata matches the migration:

```python
    __table_args__ = (
        Index(
            "uq_jobs_idempotency_key",
            "idempotency_key",
            unique=True,
            postgresql_where=sa.text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_jobs_status_created_at", "status", "created_at"),
    )
```

(`Index` is already imported in this file — no new imports.)

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_migration.py -v`
Expected: PASS — the new index test plus the pre-existing migration tests all pass.

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check --fix app/models/job.py alembic/versions/0006_add_status_created_at_index.py
uv run ruff format app/models/job.py alembic/versions/0006_add_status_created_at_index.py tests/integration/test_migration.py
git add app/models/job.py alembic/versions/0006_add_status_created_at_index.py tests/integration/test_migration.py
git commit -m "feat: add (status, created_at) index for oldest-pending stats (migration 0006)"
```

---

### Task 2: Pure stats helpers in `app/observability.py`

**Files:**
- Create: `app/observability.py`
- Test: `tests/unit/test_observability.py`

**Interfaces:**
- Consumes: `app.schemas.enums.JobStatus`.
- Produces:
  - `live_worker_count(consumer_rows_per_stream: list[list[dict]], cutoff_ms: int) -> int`
  - `zero_fill_status_counts(rows: list[tuple]) -> dict[str, int]`
  - `pending_age_seconds(min_created_at: datetime | None, now: datetime) -> float | None`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_observability.py`:

```python
from datetime import datetime, timedelta, timezone

from app.observability import (
    live_worker_count,
    pending_age_seconds,
    zero_fill_status_counts,
)
from app.schemas.enums import JobStatus


def test_live_worker_count_uses_min_idle_across_streams():
    # Worker "w1" is saturated on high (idle 0) but looks stale on low (idle 99999).
    # It MUST count as live because its minimum idle is under the cutoff.
    high = [{"name": "w1", "idle": 0}]
    normal = []
    low = [{"name": "w1", "idle": 99_999}]
    assert live_worker_count([high, normal, low], cutoff_ms=60_000) == 1


def test_live_worker_count_excludes_stale_and_dedups():
    high = [{"name": "w1", "idle": 500}, {"name": "dead", "idle": 120_000}]
    normal = [{"name": "w1", "idle": 800}]  # same worker seen twice -> one
    low = []
    assert live_worker_count([high, normal, low], cutoff_ms=60_000) == 1


def test_zero_fill_status_counts_fills_all_six():
    rows = [(JobStatus.pending, 3), (JobStatus.completed, 10)]
    counts = zero_fill_status_counts(rows)
    assert set(counts) == {s.value for s in JobStatus}
    assert counts["pending"] == 3
    assert counts["completed"] == 10
    assert counts["failed"] == 0


def test_pending_age_seconds_none_when_no_pending():
    assert pending_age_seconds(None, datetime.now(timezone.utc)) is None


def test_pending_age_seconds_computes_delta():
    now = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    created = now - timedelta(seconds=42)
    assert pending_age_seconds(created, now) == 42.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_observability.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.observability'`.

- [ ] **Step 3: Write the pure helpers**

Create `app/observability.py`:

```python
from datetime import datetime

from app.schemas.enums import JobStatus


def live_worker_count(
    consumer_rows_per_stream: list[list[dict]], cutoff_ms: int
) -> int:
    """Count distinct consumers whose *minimum* idle across all streams is under
    cutoff_ms. A worker saturated on one stream looks stale on the others it did
    not read this round; the minimum is what reflects real liveness."""
    min_idle: dict[str, int] = {}
    for rows in consumer_rows_per_stream:
        for row in rows:
            name = row["name"]
            idle = int(row["idle"])
            if name not in min_idle or idle < min_idle[name]:
                min_idle[name] = idle
    return sum(1 for idle in min_idle.values() if idle < cutoff_ms)


def zero_fill_status_counts(rows: list[tuple]) -> dict[str, int]:
    """Turn a partial ``GROUP BY status`` result into a dict with every
    JobStatus value present (missing statuses -> 0)."""
    counts = {status.value: 0 for status in JobStatus}
    for status, count in rows:
        key = status.value if isinstance(status, JobStatus) else str(status)
        counts[key] = int(count)
    return counts


def pending_age_seconds(
    min_created_at: datetime | None, now: datetime
) -> float | None:
    """Age in seconds of the oldest pending job, or None when none are pending."""
    if min_created_at is None:
        return None
    return (now - min_created_at).total_seconds()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_observability.py -v`
Expected: PASS — all five tests green.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix app/observability.py tests/unit/test_observability.py
uv run ruff format app/observability.py tests/unit/test_observability.py
git add app/observability.py tests/unit/test_observability.py
git commit -m "feat: pure stats helpers (min-idle worker count, status zero-fill, pending age)"
```

---

### Task 3: Enrich `GET /health` with liveness + readiness

**Files:**
- Modify: `app/observability.py` (add `check_readiness`)
- Modify: `app/schemas/api.py` (add `HealthChecks`, `HealthResponse`)
- Modify: `app/api/routes.py` (replace `health`)
- Test: `tests/integration/test_api.py` (update `test_health`, add a 503 test)

**Interfaces:**
- Consumes: `get_db`, `get_redis` (existing deps).
- Produces:
  - `check_readiness(session: Session, client: redis.Redis) -> dict[str, str]` — keys `"postgres"`, `"redis"`, each `"ok"` or `"error"`.
  - `HealthResponse` model: `status: str`, `checks: HealthChecks(postgres: str, redis: str)`.

- [ ] **Step 1: Write the failing tests**

Replace the existing `test_health` in `tests/integration/test_api.py` and add a 503 case:

```python
def test_health_ok_when_backends_up(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "checks": {"postgres": "ok", "redis": "ok"},
    }


def test_health_503_when_redis_down(client):
    from app.core.redis import create_redis_client

    # Point the app at a closed port -> PING raises a RedisError.
    client.app.state.redis = create_redis_client("redis://127.0.0.1:6390/0")
    resp = client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "unavailable"
    assert body["checks"]["redis"] == "error"
    assert body["checks"]["postgres"] == "ok"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_api.py::test_health_ok_when_backends_up tests/integration/test_api.py::test_health_503_when_redis_down -v`
Expected: FAIL — current `/health` returns `{"status": "ok"}` with no `checks`, and never 503.

- [ ] **Step 3: Add `check_readiness` to `app/observability.py`**

At the top of `app/observability.py` add imports and the function:

```python
import redis
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
```

```python
def check_readiness(session: Session, client: redis.Redis) -> dict[str, str]:
    """Ping both backends independently; one failure never masks the other."""
    checks: dict[str, str] = {}
    try:
        session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except SQLAlchemyError:
        checks["postgres"] = "error"
    try:
        client.ping()
        checks["redis"] = "ok"
    except redis.RedisError:
        checks["redis"] = "error"
    return checks
```

- [ ] **Step 4: Add response models to `app/schemas/api.py`**

Append to `app/schemas/api.py`:

```python
class HealthChecks(BaseModel):
    postgres: str
    redis: str


class HealthResponse(BaseModel):
    status: str
    checks: HealthChecks
```

- [ ] **Step 5: Replace the `health` route in `app/api/routes.py`**

Add imports near the top:

```python
from fastapi.responses import JSONResponse

from app.observability import check_readiness
from app.schemas.api import HealthChecks, HealthResponse
```

Replace the existing `health` handler:

```python
@router.get("/health", response_model=HealthResponse)
def health(
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
):
    checks = check_readiness(session, client)
    ok = all(value == "ok" for value in checks.values())
    if not ok:
        return JSONResponse(
            status_code=503,
            content={"status": "unavailable", "checks": checks},
        )
    return HealthResponse(status="ok", checks=HealthChecks(**checks))
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_api.py -v`
Expected: PASS — both new health tests plus every other API test green.

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check --fix app/observability.py app/schemas/api.py app/api/routes.py tests/integration/test_api.py
uv run ruff format app/observability.py app/schemas/api.py app/api/routes.py tests/integration/test_api.py
git add app/observability.py app/schemas/api.py app/api/routes.py tests/integration/test_api.py
git commit -m "feat: GET /health does liveness + readiness (503 when a backend is down)"
```

---

### Task 4: `GET /stats` queue statistics

**Files:**
- Modify: `app/observability.py` (add `gather_stats`)
- Modify: `app/schemas/api.py` (add stats models)
- Modify: `app/api/routes.py` (add `/stats` route + logger)
- Test: `tests/integration/test_health_stats.py` (new)

**Interfaces:**
- Consumes: `check_readiness` (not directly), the pure helpers from Task 2, `Settings` (`ordered_streams`, `priority_streams`, `consumer_group`, `delayed_zset`, `visibility_timeout_s`), the `ix_jobs_status_created_at` index from Task 1.
- Produces:
  - `gather_stats(session: Session, client: redis.Redis, settings: Settings) -> StatsResponse`
  - Models: `StreamStat(depth: int | None, in_flight: int)`, `QueueStats(streams: dict[str, StreamStat], scheduled: int, workers: int)`, `JobStats(by_status: dict[str, int], oldest_pending_age_seconds: float | None)`, `StatsResponse(queue: QueueStats, jobs: JobStats)`.

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/test_health_stats.py`:

```python
from datetime import datetime, timezone
from uuid import uuid4

from app import repository as repo
from app.queue.consumer import ensure_group
from app.schemas.enums import JobType


def _reset_streams(r, s):
    r.flushdb()
    for stream in s.ordered_streams:
        ensure_group(r, stream, s.consumer_group)


def test_stats_reports_queue_and_job_metrics(client, db_session):
    r = client.app.state.redis
    s = client.app.state.settings
    _reset_streams(r, s)

    # high: 3 waiting, none delivered -> depth 3, in_flight 0
    for _ in range(3):
        r.xadd(s.stream_high, {"job_id": str(uuid4())})
    # normal: 2 added, 1 delivered to consumer "w1" -> depth 1, in_flight 1
    for _ in range(2):
        r.xadd(s.stream_normal, {"job_id": str(uuid4())})
    r.xreadgroup(
        groupname=s.consumer_group,
        consumername="w1",
        streams={s.stream_normal: ">"},
        count=1,
    )
    # one delayed (scheduled) member
    r.zadd(s.delayed_zset, {str(uuid4()): 9999999999})

    # DB rows across statuses
    repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})  # pending
    done = repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    repo.claim_job(db_session, done.id)
    repo.complete_job(db_session, done.id, {"ok": True})  # completed

    body = client.get("/stats").json()

    assert body["queue"]["streams"]["high"] == {"depth": 3, "in_flight": 0}
    assert body["queue"]["streams"]["normal"] == {"depth": 1, "in_flight": 1}
    assert body["queue"]["streams"]["low"] == {"depth": 0, "in_flight": 0}
    assert body["queue"]["scheduled"] == 1
    assert body["queue"]["workers"] == 1
    assert body["jobs"]["by_status"]["pending"] == 1
    assert body["jobs"]["by_status"]["completed"] == 1
    assert body["jobs"]["by_status"]["failed"] == 0
    assert body["jobs"]["oldest_pending_age_seconds"] is not None
    assert body["jobs"]["oldest_pending_age_seconds"] >= 0


def test_stats_503_when_redis_down(client):
    from app.core.redis import create_redis_client

    client.app.state.redis = create_redis_client("redis://127.0.0.1:6390/0")
    resp = client.get("/stats")
    assert resp.status_code == 503
    assert resp.json() == {"detail": "stats unavailable"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_health_stats.py -v`
Expected: FAIL — `/stats` route does not exist yet (404).

- [ ] **Step 3: Add stats response models to `app/schemas/api.py`**

Append to `app/schemas/api.py`:

```python
class StreamStat(BaseModel):
    depth: int | None
    in_flight: int


class QueueStats(BaseModel):
    streams: dict[str, StreamStat]
    scheduled: int
    workers: int


class JobStats(BaseModel):
    by_status: dict[str, int]
    oldest_pending_age_seconds: float | None


class StatsResponse(BaseModel):
    queue: QueueStats
    jobs: JobStats
```

- [ ] **Step 4: Add `gather_stats` to `app/observability.py`**

Add imports at the top of `app/observability.py`:

```python
from datetime import datetime, timezone

from sqlalchemy import func, select

from app.core.config import Settings
from app.models.job import Job
from app.schemas.api import JobStats, QueueStats, StatsResponse, StreamStat
```

Add the gatherer:

```python
def _tolerate_nogroup(result, empty):
    """In a pipeline run with raise_on_error=False, a missing stream/group comes
    back as a NOGROUP ResponseError -> treat as empty. Any other error is real
    and re-raised (caller turns it into a 503)."""
    if isinstance(result, redis.ResponseError) and "NOGROUP" in str(result):
        return empty
    if isinstance(result, Exception):
        raise result
    return result


def gather_stats(
    session: Session, client: redis.Redis, settings: Settings
) -> StatsResponse:
    stream_names = [stream for _, stream in settings.priority_streams]
    n = len(stream_names)

    pipe = client.pipeline(transaction=False)
    for stream in stream_names:
        pipe.xinfo_groups(stream)
    for stream in stream_names:
        pipe.xinfo_consumers(stream, settings.consumer_group)
    pipe.zcard(settings.delayed_zset)
    results = pipe.execute(raise_on_error=False)

    groups = [_tolerate_nogroup(res, []) for res in results[:n]]
    consumers = [_tolerate_nogroup(res, []) for res in results[n : 2 * n]]
    scheduled = int(_tolerate_nogroup(results[2 * n], 0))

    streams: dict[str, StreamStat] = {}
    for (priority, _), group_list in zip(settings.priority_streams, groups):
        group = next(
            (g for g in group_list if g.get("name") == settings.consumer_group),
            None,
        )
        if group is None:
            streams[priority.value] = StreamStat(depth=0, in_flight=0)
            continue
        lag = group.get("lag")
        # lag is only nil after entries are XDEL'd, which this system never does,
        # so in practice it is always an int; fall back to null defensively.
        streams[priority.value] = StreamStat(
            depth=int(lag) if lag is not None else None,
            in_flight=int(group["pending"]),
        )

    cutoff_ms = int(settings.visibility_timeout_s * 1000)
    queue = QueueStats(
        streams=streams,
        scheduled=scheduled,
        workers=live_worker_count(consumers, cutoff_ms),
    )

    status_rows = session.execute(
        select(Job.status, func.count()).group_by(Job.status)
    ).all()
    min_created = session.execute(
        select(func.min(Job.created_at)).where(Job.status == JobStatus.pending)
    ).scalar_one()
    jobs = JobStats(
        by_status=zero_fill_status_counts(status_rows),
        oldest_pending_age_seconds=pending_age_seconds(
            min_created, datetime.now(timezone.utc)
        ),
    )

    return StatsResponse(queue=queue, jobs=jobs)
```

- [ ] **Step 5: Add the `/stats` route to `app/api/routes.py`**

Add imports / module logger near the top:

```python
import structlog
from sqlalchemy.exc import SQLAlchemyError

from app.observability import check_readiness, gather_stats
from app.schemas.api import HealthChecks, HealthResponse, StatsResponse

log = structlog.get_logger("api")
```

(Merge these with the Task 3 imports rather than duplicating `check_readiness` / the schema imports.)

Add the route:

```python
@router.get("/stats", response_model=StatsResponse)
def stats(
    request: Request,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> StatsResponse:
    settings = request.app.state.settings
    try:
        return gather_stats(session, client, settings)
    except (redis.RedisError, SQLAlchemyError) as exc:
        log.warning("stats.unavailable", error=str(exc))
        raise HTTPException(status_code=503, detail="stats unavailable") from exc
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_health_stats.py -v`
Expected: PASS — both stats tests green.

- [ ] **Step 7: Run the full suite**

Run: `uv run pytest`
Expected: PASS — no regressions across unit + integration.

- [ ] **Step 8: Lint and commit**

```bash
uv run ruff check --fix app/observability.py app/schemas/api.py app/api/routes.py tests/integration/test_health_stats.py
uv run ruff format app/observability.py app/schemas/api.py app/api/routes.py tests/integration/test_health_stats.py
git add app/observability.py app/schemas/api.py app/api/routes.py tests/integration/test_health_stats.py
git commit -m "feat: GET /stats queue statistics (lag/pending depths, live workers, status counts)"
```

---

### Task 5: Wire `/health` as the `api` Docker healthcheck

**Files:**
- Modify: `docker-compose.yml` (`api` service)

**Interfaces:**
- Consumes: the `GET /health` endpoint from Task 3.
- Produces: a Docker healthcheck on the `api` service (no code interface).

- [ ] **Step 1: Add the healthcheck to the `api` service**

In `docker-compose.yml`, under the `api:` service (after `ports:`), add:

```yaml
    healthcheck:
      test: ["CMD", "python", "-c",
             "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').status==200 else 1)"]
      interval: 15s
      timeout: 12s
      retries: 5
      start_period: 10s
```

- [ ] **Step 2: Validate the compose file parses**

Run: `docker compose config >/dev/null && echo OK`
Expected: `OK` (YAML is valid and the healthcheck is merged into the `api` service). If Docker is unavailable in the environment, skip with a note — this is a config-only change with no test cycle.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "chore: wire /health as the api service Docker healthcheck (timeout 12s)"
```

---

## Self-Review

**1. Spec coverage**

| Spec section | Task |
|---|---|
| §4 `GET /health` (liveness+readiness, 503) | Task 3 |
| §4 `GET /stats` payload + fields | Task 4 |
| §5.1 Redis one-pipeline; lag/pending; NOGROUP→0; lag-null fallback | Task 4 (`gather_stats`, `_tolerate_nogroup`) |
| §5.2 min-idle live worker count | Task 2 (`live_worker_count`) + Task 4 wiring |
| §5.3 GROUP BY status zero-fill; oldest-pending age | Task 2 (`zero_fill_status_counts`, `pending_age_seconds`) + Task 4 |
| §6 migration `0006` + model index | Task 1 |
| §7 error handling (`/health` per-backend; `/stats` all-or-nothing 503 + structured log) | Task 3, Task 4 |
| §8 module/file structure + response models | Tasks 2–4 |
| §9 Docker healthcheck (timeout 12s, interval 15s) | Task 5 |
| §10 unit (pure helpers) + integration (real Redis+PG, 503 cases, migration) | Tasks 1–4 |

No gaps.

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to"/bare-prose steps — every code step shows complete code and every run step shows the exact command + expected result.

**3. Type consistency:** `check_readiness`, `gather_stats`, `live_worker_count`, `zero_fill_status_counts`, `pending_age_seconds` and the models `HealthChecks`/`HealthResponse`/`StreamStat`/`QueueStats`/`JobStats`/`StatsResponse` are named identically in their producing task and every consuming task. `depth`/`in_flight`/`by_status`/`oldest_pending_age_seconds`/`scheduled`/`workers` field names match between the Task 4 models, the `gather_stats` construction, and the Task 4 test assertions. Index name `ix_jobs_status_created_at` is identical in the migration, the model, and the migration test.
