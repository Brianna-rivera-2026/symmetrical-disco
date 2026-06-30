# Scheduled (Delayed) Job Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let clients submit jobs with an optional future `scheduled_at`; park those jobs in a Redis ZSET and have a new ticker process promote them into the existing stream when due, with no silent orphans on the submit path.

**Architecture:** A future-dated submission is persisted as `SCHEDULED` and `ZADD`ed to a delayed ZSET; an independent ticker process drains mature jobs (`ZRANGEBYSCORE` → `XADD` → `ZREM` → flip Postgres status) and reconciles orphaned rows using an explicit `is_synced_to_redis` flag. Workers process promoted jobs unchanged.

**Tech Stack:** FastAPI, SQLAlchemy 2.0 + Alembic, PostgreSQL, Redis (Streams + ZSET), structlog, pytest + testcontainers, uv.

Design reference: [docs/superpowers/specs/2026-07-01-scheduled-jobs-design.md](../specs/2026-07-01-scheduled-jobs-design.md)

## Global Constraints

- Package/runtime manager is **uv**. Run everything via `uv run …` (e.g. `uv run pytest`, `uv run ruff check --fix`, `uv run ruff format`). Never use `pip`, `venv`, or `poetry`.
- No `print` — use structlog with job context.
- Postgres is the source of truth; Redis is the queue. The ZSET key is `jobs:delayed`; the stream is `jobs:stream`.
- Redis is assumed **durable** (AOF + RDB). Total Redis data loss is recovered manually (see spec §9), not automatically.
- Always run `uv run pytest` before declaring a task complete.
- Integration tests use real Postgres + Redis via the existing `testcontainers` fixtures in `tests/integration/conftest.py` (`db_session`, `redis_client`, `test_settings`, `client`, `pg_engine`).

---

### Task 1: Migration + model columns

Add `scheduled_at` and `is_synced_to_redis` to the `jobs` table and ORM model, plus the self-pruning partial index used by the reconciler.

**Files:**
- Create: `alembic/versions/0002_add_scheduled_at_and_sync_flag.py`
- Modify: `app/models/job.py`
- Test: `tests/integration/test_repository.py`

**Interfaces:**
- Consumes: nothing (foundation task).
- Produces: `Job.scheduled_at: datetime | None`, `Job.is_synced_to_redis: bool` (default `False`); migration revision `0002` (down_revision `0001`); partial index `ix_jobs_unsynced`.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_repository.py`:

```python
def test_job_has_scheduling_columns(db_session):
    from datetime import datetime, timezone

    from app.models.job import Job

    when = datetime(2030, 1, 1, tzinfo=timezone.utc)
    job = Job(
        type=JobType.email,
        payload={"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    db_session.add(job)
    db_session.commit()
    db_session.refresh(job)
    assert job.scheduled_at == when
    assert job.is_synced_to_redis is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_repository.py::test_job_has_scheduling_columns -v`
Expected: FAIL (`TypeError`/`AttributeError` — `Job` has no `scheduled_at`; or the migrated table lacks the column).

- [ ] **Step 3: Create the migration**

Create `alembic/versions/0002_add_scheduled_at_and_sync_flag.py`:

```python
"""add scheduled_at and is_synced_to_redis

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-01
"""
import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs", sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "jobs",
        sa.Column(
            "is_synced_to_redis",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    # Rows created under Phase 1 were already handed off to Redis; mark them
    # synced so the reconciler never re-enqueues historical jobs.
    op.execute("UPDATE jobs SET is_synced_to_redis = TRUE")
    op.create_index(
        "ix_jobs_unsynced",
        "jobs",
        ["created_at", "id"],
        postgresql_where=sa.text(
            "is_synced_to_redis = false AND status IN ('pending', 'scheduled')"
        ),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_unsynced", table_name="jobs")
    op.drop_column("jobs", "is_synced_to_redis")
    op.drop_column("jobs", "scheduled_at")
```

- [ ] **Step 4: Add the model columns**

In `app/models/job.py`, add `Boolean` to the SQLAlchemy import line and add the two mapped columns after `completed_at`:

```python
from sqlalchemy import Boolean, TIMESTAMP, func
from sqlalchemy import Enum as SAEnum
```

```python
    scheduled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    is_synced_to_redis: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=sa.false()
    )
```

Add `import sqlalchemy as sa` at the top of the file (for `sa.false()`).

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_repository.py::test_job_has_scheduling_columns -v`
Expected: PASS (the `pg_engine` fixture runs `alembic upgrade head`, applying `0002`).

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add alembic/versions/0002_add_scheduled_at_and_sync_flag.py app/models/job.py tests/integration/test_repository.py
git commit -m "feat: add scheduled_at and is_synced_to_redis columns"
```

---

### Task 2: Scheduling config settings

Add ticker/reconciler tunables to `Settings`.

**Files:**
- Modify: `app/core/config.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `Settings.delayed_zset: str`, `ticker_interval_s: float`, `ticker_batch_size: int`, `reconcile_interval_s: float`, `reconcile_grace_s: float`, `reconcile_batch_size: int`.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_config.py`:

```python
def test_scheduling_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.delayed_zset == "jobs:delayed"
    assert s.ticker_interval_s == 1.0
    assert s.ticker_batch_size == 100
    assert s.reconcile_interval_s == 60.0
    assert s.reconcile_grace_s == 10.0
    assert s.reconcile_batch_size == 500
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_config.py::test_scheduling_defaults -v`
Expected: FAIL (`AttributeError: 'Settings' object has no attribute 'delayed_zset'`).

- [ ] **Step 3: Add the settings**

In `app/core/config.py`, add these fields to `Settings` (after `block_ms`):

```python
    delayed_zset: str = "jobs:delayed"
    ticker_interval_s: float = 1.0
    ticker_batch_size: int = 100
    reconcile_interval_s: float = 60.0
    reconcile_grace_s: float = 10.0
    reconcile_batch_size: int = 500
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/core/config.py tests/unit/test_config.py
git commit -m "feat: add ticker and reconciler config settings"
```

---

### Task 3: API schemas — `scheduled_at` field + UTC normalization

Add `scheduled_at` to the request/response schemas and normalize incoming values to UTC.

**Files:**
- Modify: `app/schemas/api.py`
- Test: `tests/unit/test_schemas_scheduling.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `JobSubmission.scheduled_at: datetime | None` (naive → UTC, aware → converted to UTC); `JobAccepted.scheduled_at: datetime | None`; `JobOut.scheduled_at: datetime | None`.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_schemas_scheduling.py`:

```python
from datetime import datetime, timedelta, timezone

from app.schemas.api import JobSubmission

_PAYLOAD = {"to": "a@b.com", "subject": "Hi"}


def test_naive_scheduled_at_becomes_utc():
    s = JobSubmission(
        type="email", payload=_PAYLOAD, scheduled_at=datetime(2030, 1, 1, 12, 0, 0)
    )
    assert s.scheduled_at == datetime(2030, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_aware_scheduled_at_converted_to_utc():
    tz = timezone(timedelta(hours=2))
    s = JobSubmission(
        type="email",
        payload=_PAYLOAD,
        scheduled_at=datetime(2030, 1, 1, 12, 0, 0, tzinfo=tz),
    )
    assert s.scheduled_at == datetime(2030, 1, 1, 10, 0, 0, tzinfo=timezone.utc)


def test_scheduled_at_is_optional():
    s = JobSubmission(type="email", payload=_PAYLOAD)
    assert s.scheduled_at is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_schemas_scheduling.py -v`
Expected: FAIL (`TypeError`/validation error — `JobSubmission` has no `scheduled_at`).

- [ ] **Step 3: Update the schemas**

Edit `app/schemas/api.py`:

```python
from datetime import datetime, timezone
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from app.schemas.enums import JobStatus, JobType


class JobSubmission(BaseModel):
    type: JobType
    payload: dict
    scheduled_at: datetime | None = None

    @field_validator("scheduled_at")
    @classmethod
    def _normalize_utc(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


class JobAccepted(BaseModel):
    id: UUID
    type: JobType
    status: JobStatus
    created_at: datetime
    scheduled_at: datetime | None = None


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: JobType
    status: JobStatus
    payload: dict
    result: dict | None
    error: dict | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    scheduled_at: datetime | None


class JobList(BaseModel):
    items: list[JobOut]
    next_cursor: str | None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_schemas_scheduling.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/schemas/api.py tests/unit/test_schemas_scheduling.py
git commit -m "feat: add scheduled_at to job schemas with UTC normalization"
```

---

### Task 4: Repository — submit & claim changes

Extend `create_job` to accept a status/scheduled time, add `mark_synced`, and widen the claim guard to accept `scheduled` as a pre-state.

**Files:**
- Modify: `app/repository.py`
- Test: `tests/integration/test_repository.py`

**Interfaces:**
- Consumes: `Job.scheduled_at`, `Job.is_synced_to_redis` (Task 1); `JobStatus` enum.
- Produces:
  - `create_job(session, job_type, payload, *, status: JobStatus = JobStatus.pending, scheduled_at: datetime | None = None) -> Job`
  - `mark_synced(session, job_id: UUID) -> None`
  - `claim_job(session, job_id: UUID) -> bool` now succeeds from `pending` **or** `scheduled`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_repository.py`:

```python
def test_create_scheduled_job_sets_fields(db_session):
    from datetime import datetime, timezone

    when = datetime(2030, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    assert job.status is JobStatus.scheduled
    assert job.scheduled_at == when
    assert job.is_synced_to_redis is False


def test_mark_synced_sets_flag(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.mark_synced(db_session, job.id)
    db_session.refresh(job)
    assert job.is_synced_to_redis is True


def test_claim_accepts_scheduled_state(db_session):
    from datetime import datetime, timezone

    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    assert repo.claim_job(db_session, job.id) is True
    db_session.refresh(job)
    assert job.status is JobStatus.processing
    assert job.started_at is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_repository.py -k "scheduled_job_sets_fields or mark_synced or claim_accepts_scheduled" -v`
Expected: FAIL (`create_job` has no `status`/`scheduled_at` kwargs; `mark_synced` undefined; claim rejects `scheduled`).

- [ ] **Step 3: Implement the repository changes**

In `app/repository.py`, replace `create_job` and `claim_job` and add `mark_synced`:

```python
def create_job(
    session: Session,
    job_type: JobType,
    payload: dict,
    *,
    status: JobStatus = JobStatus.pending,
    scheduled_at: datetime | None = None,
) -> Job:
    job = Job(
        type=job_type, payload=payload, status=status, scheduled_at=scheduled_at
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def mark_synced(session: Session, job_id: UUID) -> None:
    session.execute(
        update(Job).where(Job.id == job_id).values(is_synced_to_redis=True)
    )
    session.commit()


def claim_job(session: Session, job_id: UUID) -> bool:
    stmt = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
        )
        .values(status=JobStatus.processing, started_at=_now())
    )
    result = session.execute(stmt)
    session.commit()
    return result.rowcount == 1
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: PASS (including the pre-existing `test_claim_guard_only_succeeds_once`, which still works since `pending` is in the allowed set).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/repository.py tests/integration/test_repository.py
git commit -m "feat: support scheduled jobs in create_job, add mark_synced, widen claim guard"
```

---

### Task 5: Redis delayed-queue helpers

Create the ZSET handshake primitives: schedule, find-due, and crash-safe promote (XADD-before-ZREM, batched).

**Files:**
- Create: `app/queue/delayed.py`
- Test: `tests/integration/test_delayed.py` (create)

**Interfaces:**
- Consumes: a `redis.Redis` client (built with `decode_responses=True`, so reads return `str`).
- Produces:
  - `schedule(client, zset: str, job_id: str, score: float) -> None`
  - `due_job_ids(client, zset: str, now_epoch: float, limit: int) -> list[str]`
  - `promote(client, stream: str, zset: str, job_ids: list[str]) -> None`

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/test_delayed.py`:

```python
import time

from app.queue import delayed

ZSET = "jobs:delayed"
STREAM = "jobs:stream"


def test_schedule_and_due_filtering(redis_client):
    delayed.schedule(redis_client, ZSET, "past", time.time() - 10)
    delayed.schedule(redis_client, ZSET, "future", time.time() + 1000)
    due = delayed.due_job_ids(redis_client, ZSET, time.time(), limit=100)
    assert due == ["past"]


def test_due_respects_limit(redis_client):
    for i in range(5):
        delayed.schedule(redis_client, ZSET, f"j{i}", time.time() - i - 1)
    due = delayed.due_job_ids(redis_client, ZSET, time.time(), limit=2)
    assert len(due) == 2


def test_promote_moves_ids_to_stream_and_removes(redis_client):
    delayed.schedule(redis_client, ZSET, "a", time.time() - 1)
    delayed.schedule(redis_client, ZSET, "b", time.time() - 1)
    delayed.promote(redis_client, STREAM, ZSET, ["a", "b"])
    assert redis_client.xlen(STREAM) == 2
    assert redis_client.zcard(ZSET) == 0


def test_promote_empty_is_noop(redis_client):
    delayed.promote(redis_client, STREAM, ZSET, [])
    assert redis_client.xlen(STREAM) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_delayed.py -v`
Expected: FAIL (`ModuleNotFoundError: app.queue.delayed`).

- [ ] **Step 3: Implement the helpers**

Create `app/queue/delayed.py`:

```python
import redis


def schedule(client: redis.Redis, zset: str, job_id: str, score: float) -> None:
    client.zadd(zset, {job_id: score})


def due_job_ids(
    client: redis.Redis, zset: str, now_epoch: float, limit: int
) -> list[str]:
    return client.zrangebyscore(zset, min=0, max=now_epoch, start=0, num=limit)


def promote(
    client: redis.Redis, stream: str, zset: str, job_ids: list[str]
) -> None:
    if not job_ids:
        return
    # XADD every id to the stream BEFORE removing any from the ZSET, so a crash
    # mid-promotion leaves the ids in the ZSET to be retried next tick. Duplicate
    # stream entries are absorbed by the worker's idempotent claim guard.
    pipe = client.pipeline(transaction=False)
    for job_id in job_ids:
        pipe.xadd(stream, {"job_id": job_id})
    pipe.execute()
    client.zrem(zset, *job_ids)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_delayed.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/queue/delayed.py tests/integration/test_delayed.py
git commit -m "feat: add Redis delayed-queue helpers (schedule, due, promote)"
```

---

### Task 6: Repository — ticker queries

Add the bulk status flip used by promotion and the orphan lookup used by the reconciler.

**Files:**
- Modify: `app/repository.py`
- Test: `tests/integration/test_repository.py`

**Interfaces:**
- Consumes: `Job`, `JobStatus`, `datetime`.
- Produces:
  - `promote_scheduled_to_pending(session, job_ids: list[UUID]) -> int` — bulk `UPDATE … WHERE id = ANY AND status='scheduled'`; returns rows changed.
  - `list_unsynced(session, *, older_than: datetime, limit: int) -> list[Job]` — orphans (`is_synced_to_redis = False`, status in `pending`/`scheduled`, `created_at < older_than`), ordered `(created_at, id)`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_repository.py`:

```python
def test_promote_scheduled_to_pending_only_scheduled(db_session):
    from datetime import datetime, timezone

    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    scheduled = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    already_pending = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    changed = repo.promote_scheduled_to_pending(
        db_session, [scheduled.id, already_pending.id]
    )
    assert changed == 1
    db_session.refresh(scheduled)
    assert scheduled.status is JobStatus.pending


def test_list_unsynced_filters_synced_and_grace(db_session):
    from datetime import datetime, timedelta, timezone

    synced = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    repo.mark_synced(db_session, synced.id)
    orphan = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}
    )
    now = datetime.now(timezone.utc)

    rows = repo.list_unsynced(db_session, older_than=now + timedelta(seconds=1), limit=100)
    ids = {r.id for r in rows}
    assert orphan.id in ids
    assert synced.id not in ids

    # Grace window: nothing is old enough when the cutoff is in the past.
    none_rows = repo.list_unsynced(
        db_session, older_than=now - timedelta(seconds=1000), limit=100
    )
    assert none_rows == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_repository.py -k "promote_scheduled_to_pending or list_unsynced" -v`
Expected: FAIL (`AttributeError` — functions undefined).

- [ ] **Step 3: Implement the queries**

Add to `app/repository.py`:

```python
def promote_scheduled_to_pending(session: Session, job_ids: list[UUID]) -> int:
    if not job_ids:
        return 0
    result = session.execute(
        update(Job)
        .where(Job.id.in_(job_ids), Job.status == JobStatus.scheduled)
        .values(status=JobStatus.pending)
    )
    session.commit()
    return result.rowcount


def list_unsynced(
    session: Session, *, older_than: datetime, limit: int
) -> list[Job]:
    stmt = (
        select(Job)
        .where(
            Job.is_synced_to_redis.is_(False),
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
            Job.created_at < older_than,
        )
        .order_by(Job.created_at, Job.id)
        .limit(limit)
    )
    return list(session.execute(stmt).scalars())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/repository.py tests/integration/test_repository.py
git commit -m "feat: add promote_scheduled_to_pending and list_unsynced repository queries"
```

---

### Task 7: Ticker — promotion of due jobs

Promote one mature batch from the ZSET into the stream and flip the Postgres status.

**Files:**
- Create: `app/ticker/__init__.py` (empty), `app/ticker/runner.py`
- Test: `tests/integration/test_ticker.py` (create)

**Interfaces:**
- Consumes: `delayed.due_job_ids`, `delayed.promote` (Task 5); `repo.promote_scheduled_to_pending` (Task 6); `Settings`.
- Produces: `promote_due(session, client, settings) -> int` — promotes up to `ticker_batch_size` due jobs; returns the count found in the ZSET.

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/test_ticker.py`:

```python
from datetime import datetime, timezone

from app import repository as repo
from app.queue import delayed
from app.schemas.enums import JobStatus, JobType
from app.ticker.runner import promote_due


def test_promote_due_moves_mature_job(db_session, redis_client, test_settings):
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), when.timestamp()
    )

    promoted = promote_due(db_session, redis_client, test_settings)

    assert promoted == 1
    assert redis_client.xlen(test_settings.jobs_stream) == 1
    assert redis_client.zcard(test_settings.delayed_zset) == 0
    db_session.refresh(job)
    assert job.status is JobStatus.pending


def test_promote_due_skips_future(db_session, redis_client, test_settings):
    future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=future,
    )
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(job.id), future.timestamp()
    )

    promoted = promote_due(db_session, redis_client, test_settings)

    assert promoted == 0
    assert redis_client.xlen(test_settings.jobs_stream) == 0
    assert redis_client.zcard(test_settings.delayed_zset) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_ticker.py -v`
Expected: FAIL (`ModuleNotFoundError: app.ticker.runner`).

- [ ] **Step 3: Implement `promote_due`**

Create empty `app/ticker/__init__.py`, then create `app/ticker/runner.py`:

```python
import time
from uuid import UUID

import redis
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.queue import delayed


def promote_due(session: Session, client: redis.Redis, settings: Settings) -> int:
    now_epoch = time.time()
    ids = delayed.due_job_ids(
        client, settings.delayed_zset, now_epoch, settings.ticker_batch_size
    )
    if not ids:
        return 0
    delayed.promote(client, settings.jobs_stream, settings.delayed_zset, ids)
    repo.promote_scheduled_to_pending(session, [UUID(i) for i in ids])
    return len(ids)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_ticker.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/ticker/__init__.py app/ticker/runner.py tests/integration/test_ticker.py
git commit -m "feat: ticker promote_due moves mature jobs to the stream"
```

---

### Task 8: Ticker — orphan reconciler

Re-enqueue rows that committed in Postgres but never confirmed their Redis handoff (`is_synced_to_redis = False`), draining batch-by-batch and flipping the flag.

**Files:**
- Modify: `app/ticker/runner.py`
- Test: `tests/integration/test_ticker.py`

**Interfaces:**
- Consumes: `repo.list_unsynced`, `repo.mark_synced`; `delayed.schedule`; `enqueue` (`app/queue/producer`).
- Produces: `reconcile_orphans(session, client, settings) -> int` — re-adds every orphan older than `reconcile_grace_s` (scheduled → `ZADD`, pending → `XADD`), flips its flag, returns total recovered.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_ticker.py`:

```python
from sqlalchemy import text

from app.ticker.runner import reconcile_orphans


def _backdate(db_session, job_id):
    db_session.execute(
        text("UPDATE jobs SET created_at = now() - interval '1 hour' WHERE id = :id"),
        {"id": str(job_id)},
    )
    db_session.commit()


def test_reconcile_reenqueues_pending_orphan(db_session, redis_client, test_settings):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    _backdate(db_session, job.id)

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 1
    assert redis_client.xlen(test_settings.jobs_stream) == 1
    db_session.refresh(job)
    assert job.is_synced_to_redis is True


def test_reconcile_readds_scheduled_orphan(db_session, redis_client, test_settings):
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
    )
    _backdate(db_session, job.id)

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 1
    assert redis_client.zcard(test_settings.delayed_zset) == 1
    db_session.refresh(job)
    assert job.is_synced_to_redis is True


def test_reconcile_noop_when_synced(db_session, redis_client, test_settings):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.mark_synced(db_session, job.id)
    _backdate(db_session, job.id)

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 0
    assert redis_client.xlen(test_settings.jobs_stream) == 0


def test_reconcile_respects_grace_for_recent_jobs(db_session, redis_client, test_settings):
    repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    recovered = reconcile_orphans(db_session, redis_client, test_settings)

    assert recovered == 0
    assert redis_client.xlen(test_settings.jobs_stream) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_ticker.py -k reconcile -v`
Expected: FAIL (`ImportError`/`AttributeError` — `reconcile_orphans` undefined).

- [ ] **Step 3: Implement `reconcile_orphans`**

Add to `app/ticker/runner.py` (and extend the imports at the top):

```python
from datetime import datetime, timedelta, timezone

from app.queue.producer import enqueue
from app.schemas.enums import JobStatus
```

```python
def reconcile_orphans(session: Session, client: redis.Redis, settings: Settings) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.reconcile_grace_s)
    total = 0
    while True:
        rows = repo.list_unsynced(
            session, older_than=cutoff, limit=settings.reconcile_batch_size
        )
        if not rows:
            break
        for job in rows:
            if job.status is JobStatus.scheduled:
                delayed.schedule(
                    client,
                    settings.delayed_zset,
                    str(job.id),
                    job.scheduled_at.timestamp(),
                )
            else:
                enqueue(client, settings.jobs_stream, str(job.id))
            repo.mark_synced(session, job.id)
            total += 1
        if len(rows) < settings.reconcile_batch_size:
            break
    return total
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_ticker.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/ticker/runner.py tests/integration/test_ticker.py
git commit -m "feat: ticker reconciler re-enqueues unsynced orphans"
```

---

### Task 9: Ticker — drain loop runner + entrypoint

Wrap promotion and reconciliation in a long-running loop with the drain optimization, graceful shutdown, and a `python -m app.ticker` entrypoint.

**Files:**
- Modify: `app/ticker/runner.py`
- Create: `app/ticker/__main__.py`
- Test: `tests/integration/test_ticker.py`

**Interfaces:**
- Consumes: `promote_due`, `reconcile_orphans`; `make_engine`/`make_session_factory`; `create_redis_client`; `ensure_group`; `Settings`.
- Produces: `run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> None`.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_ticker.py`:

```python
from app.core.db import make_session_factory
from app.ticker.runner import run_forever


def test_run_forever_promotes_then_stops(redis_client, test_settings, pg_engine):
    settings = test_settings.model_copy(
        update={"ticker_interval_s": 0.01, "reconcile_interval_s": 0.01}
    )
    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        job = repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            status=JobStatus.scheduled,
            scheduled_at=when,
        )
    delayed.schedule(redis_client, settings.delayed_zset, str(job.id), when.timestamp())

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    run_forever(settings, stop=stop)

    with factory() as s:
        refreshed = repo.get_job(s, job.id)
    assert refreshed.status is JobStatus.pending
    assert redis_client.xlen(settings.jobs_stream) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_ticker.py::test_run_forever_promotes_then_stops -v`
Expected: FAIL (`ImportError`/`AttributeError` — `run_forever` undefined).

- [ ] **Step 3: Implement `run_forever`**

Add to `app/ticker/runner.py` (extend imports at the top):

```python
import signal
from collections.abc import Callable

import structlog

from app.core.db import make_engine, make_session_factory
from app.core.redis import create_redis_client
from app.queue.consumer import ensure_group

log = structlog.get_logger("ticker")
```

```python
def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> None:
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    client = create_redis_client(settings.redis_url)
    # Make sure the consumer group exists before we XADD, so workers created
    # later (group id "$") don't miss jobs the ticker has already promoted.
    ensure_group(client, settings.jobs_stream, settings.consumer_group)

    shutting_down = {"flag": False}

    def _request_stop(*_):
        shutting_down["flag"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

    log.info(
        "ticker.started", zset=settings.delayed_zset, stream=settings.jobs_stream
    )
    last_reconcile = 0.0

    while not _should_stop():
        try:
            with session_factory() as session:
                promoted = promote_due(session, client, settings)
            now = time.time()
            if now - last_reconcile >= settings.reconcile_interval_s:
                with session_factory() as session:
                    recovered = reconcile_orphans(session, client, settings)
                last_reconcile = now
                if recovered:
                    log.info("ticker.reconciled", count=recovered)
            # Drain: a full batch means more is waiting, so loop without sleeping.
            if promoted >= settings.ticker_batch_size:
                continue
            time.sleep(settings.ticker_interval_s)
        except Exception:  # noqa: BLE001 — one bad tick must not kill the ticker
            log.exception("ticker.tick_failed")
            time.sleep(settings.ticker_interval_s)

    log.info("ticker.stopped")
    client.close()
    engine.dispose()
```

- [ ] **Step 4: Create the entrypoint**

Create `app/ticker/__main__.py`:

```python
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.ticker.runner import run_forever


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    run_forever(settings)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_ticker.py -v`
Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/ticker/runner.py app/ticker/__main__.py tests/integration/test_ticker.py
git commit -m "feat: ticker drain-loop runner and python -m app.ticker entrypoint"
```

---

### Task 10: API route — branch immediate vs scheduled

Wire `POST /jobs` to choose the scheduled path (future `scheduled_at`) or the immediate path, perform the handoff, then flip the sync flag.

**Files:**
- Modify: `app/api/routes.py`
- Test: `tests/integration/test_api.py`

**Interfaces:**
- Consumes: `repo.create_job` (status/scheduled_at), `repo.mark_synced` (Task 4); `delayed.schedule` (Task 5); `enqueue` (existing); `Settings.delayed_zset`.
- Produces: updated `submit_job` returning `JobAccepted` with `scheduled_at`; `SCHEDULED`+`ZADD` for future times, `PENDING`+`XADD` otherwise.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_api.py`:

```python
def test_submit_scheduled_job_parks_in_zset(client):
    from datetime import datetime, timedelta, timezone

    client.app.state.redis.flushdb()
    when = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    resp = client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.com", "subject": "Hi"},
            "scheduled_at": when,
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "scheduled"
    assert body["scheduled_at"] is not None

    redis_client = client.app.state.redis
    settings = client.app.state.settings
    assert redis_client.zcard(settings.delayed_zset) == 1
    assert redis_client.xlen(settings.jobs_stream) == 0


def test_submit_past_scheduled_at_runs_immediately(client):
    from datetime import datetime, timedelta, timezone

    client.app.state.redis.flushdb()
    when = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    resp = client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.com", "subject": "Hi"},
            "scheduled_at": when,
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "pending"

    redis_client = client.app.state.redis
    settings = client.app.state.settings
    assert redis_client.xlen(settings.jobs_stream) == 1
    assert redis_client.zcard(settings.delayed_zset) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_api.py -k "scheduled_job_parks or past_scheduled_at" -v`
Expected: FAIL (response has no `scheduled_at`; nothing is `ZADD`ed; status never `scheduled`).

- [ ] **Step 3: Update the route**

Replace the imports and `submit_job` in `app/api/routes.py`:

```python
from datetime import datetime, timezone
from uuid import UUID

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app import repository as repo
from app.api.deps import get_db, get_redis
from app.queue.delayed import schedule
from app.queue.producer import enqueue
from app.schemas.api import JobAccepted, JobList, JobOut, JobSubmission
from app.schemas.enums import JobStatus, JobType
from app.schemas.payloads import validate_payload

router = APIRouter()


@router.post("/jobs", response_model=JobAccepted, status_code=202)
def submit_job(
    submission: JobSubmission,
    request: Request,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobAccepted:
    try:
        validate_payload(submission.type, submission.payload)
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    settings = request.app.state.settings
    scheduled_at = submission.scheduled_at
    if scheduled_at is not None and scheduled_at > datetime.now(timezone.utc):
        # Scheduled path: persist SCHEDULED + park in the delayed ZSET.
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            status=JobStatus.scheduled,
            scheduled_at=scheduled_at,
        )
        schedule(client, settings.delayed_zset, str(job.id), scheduled_at.timestamp())
    else:
        # Immediate path: persist PENDING + push to the stream.
        job = repo.create_job(session, submission.type, submission.payload)
        enqueue(client, settings.jobs_stream, str(job.id))
    # Handoff confirmed → flip the flag so the reconciler ignores this row.
    repo.mark_synced(session, job.id)
    return JobAccepted(
        id=job.id,
        type=job.type,
        status=job.status,
        created_at=job.created_at,
        scheduled_at=job.scheduled_at,
    )
```

Leave `health`, `get_job`, and `list_jobs` as they are.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_api.py -v`
Expected: PASS (including the pre-existing `test_submit_creates_job_and_enqueues`).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/api/routes.py tests/integration/test_api.py
git commit -m "feat: route future-dated submissions to the scheduled path"
```

---

### Task 11: Docker Compose — ticker service + full suite

Add the ticker service so the system runs end-to-end under Compose, and run the whole test suite.

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: the `python -m app.ticker` entrypoint (Task 9).
- Produces: a `ticker` Compose service.

- [ ] **Step 1: Add the service**

Append to `docker-compose.yml` (sibling of `worker`):

```yaml
  ticker:
    build: .
    command: uv run python -m app.ticker
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
```

- [ ] **Step 2: Validate the Compose file**

Run: `docker compose config -q`
Expected: no output, exit 0 (valid Compose). If Docker is unavailable in this environment, instead run `uv run python -c "import yaml; yaml.safe_load(open('docker-compose.yml')); print('ok')"` and expect `ok`.

- [ ] **Step 3: Run the full test suite**

Run: `uv run pytest`
Expected: PASS — all unit and integration tests green.

- [ ] **Step 4: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add docker-compose.yml
git commit -m "feat: add ticker service to docker-compose"
```

---

## Self-Review

**1. Spec coverage**

| Spec section | Task(s) |
|---|---|
| §4 `scheduled_at` + `is_synced_to_redis` columns, partial index, backfill | Task 1 |
| §5 API `scheduled_at` field, UTC normalization, response fields | Task 3, Task 10 |
| §5/§6 submit branch (future → SCHEDULED+ZADD; else PENDING+XADD), flag flip | Task 10 (uses Task 4 `create_job`/`mark_synced`, Task 5 `schedule`) |
| §6 `create_job` status/scheduled_at | Task 4 |
| §7.1 promotion drain loop, batched XADD-before-ZREM, bulk status flip, moderate batch | Task 5 (`promote`), Task 6 (`promote_scheduled_to_pending`), Task 7 (`promote_due`), Task 9 (drain loop) |
| §7.2 flag-based reconciler, grace window, batched drain | Task 6 (`list_unsynced`), Task 8 (`reconcile_orphans`) |
| §7 ticker process + entrypoint + ensure_group + graceful stop + wall-clock reconcile | Task 9 |
| §8 widened claim guard (`pending`/`scheduled`) | Task 4 |
| §9 durability assumption + manual runbook | Documented in spec; no code (Global Constraints) |
| §10 config settings | Task 2 |
| §11 Compose `ticker` service | Task 11 |
| §12 testing plan | Tests across Tasks 1–11 |

No gaps.

**2. Placeholder scan:** No `TBD`/`TODO`/"handle edge cases"/"similar to Task N" — every code step contains complete code.

**3. Type consistency:**
- `create_job(..., *, status=JobStatus.pending, scheduled_at=None)` — same signature in Tasks 4, 7, 8, 10.
- `mark_synced(session, job_id)` — defined Task 4, used Tasks 8, 10.
- `promote_scheduled_to_pending(session, list[UUID]) -> int` — defined Task 6, used Task 7.
- `list_unsynced(session, *, older_than, limit) -> list[Job]` — defined Task 6, used Task 8.
- `delayed.schedule / due_job_ids / promote` — defined Task 5, used Tasks 7, 8, 10.
- `promote_due(session, client, settings) -> int` / `reconcile_orphans(...) -> int` / `run_forever(settings, *, stop=None)` — consistent across Tasks 7, 8, 9.
- ZSET score is always `scheduled_at.timestamp()` (epoch seconds); `due_job_ids` compares against `time.time()`.

Consistent.
