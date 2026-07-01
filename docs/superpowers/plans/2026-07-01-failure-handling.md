# Failure Handling & Retries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add attempt tracking, automatic exponential-backoff retries, a per-job worker timeout, a ticker reaper for dead/hung workers, and a manual retry endpoint — all guarded by Postgres optimistic locking.

**Architecture:** Every exit from `status='processing'` is a single guarded `UPDATE … WHERE id=:id AND status='processing'` that increments `attempts`; its Redis side-effect runs only if it won the guard. A shared `schedule_retry_or_fail` helper (called by both the worker on failure/timeout and the reaper on reclaim) either re-enqueues with backoff (immediate → priority stream; delayed → existing delayed ZSET) or marks the job permanently failed at `max_attempts`. The worker bounds each handler with a thread-based timeout and recycles its process on timeout to avoid zombie-thread saturation.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.0 + Alembic, redis-py (Streams + consumer groups + `XAUTOCLAIM`), Postgres, structlog, pytest + testcontainers. Package manager: **uv**.

## Global Constraints

- Run everything through **uv**: `uv run pytest`, `uv run ruff check --fix`, `uv run ruff format`. Never `pip`/`venv`/`poetry`.
- No `print` — use structlog with job context (`structlog.get_logger(...)`, bound contextvars).
- Source of truth is Postgres; Redis is the queue. Preserve existing invariants: at-least-once delivery, commit-then-handoff, `is_synced_to_redis` reconciliation, idempotent claim.
- Redis client uses `decode_responses=True` (stream fields and message ids are `str`).
- Spec: `docs/superpowers/specs/2026-07-01-failure-handling-design.md`. Defaults: `job_handler_timeout_s=45`, `visibility_timeout_s=60`, `max_attempts=4`, `retry_backoff_schedule=[0, 30, 120]`.
- Every commit message ends with the trailer: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Current Alembic head is revision `0003`; the new migration is `0004` with `down_revision = "0003"`.

---

### Task 1: Config — failure-handling settings + startup invariant

**Files:**
- Modify: `app/core/config.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces: `Settings.job_handler_timeout_s: float`, `Settings.visibility_timeout_s: float`, `Settings.reaper_interval_s: float`, `Settings.reaper_batch_size: int`, `Settings.max_attempts: int`, `Settings.retry_backoff_schedule: list[int]`, `Settings.max_handler_timeouts_before_recycle: int`. A `model_validator(mode="after")` raises if `job_handler_timeout_s >= visibility_timeout_s`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_config.py`:

```python
def test_failure_handling_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.job_handler_timeout_s == 45.0
    assert s.visibility_timeout_s == 60.0
    assert s.reaper_interval_s == 30.0
    assert s.reaper_batch_size == 100
    assert s.max_attempts == 4
    assert s.retry_backoff_schedule == [0, 30, 120]
    assert s.max_handler_timeouts_before_recycle == 1


def test_timeout_invariant_rejects_handler_ge_visibility():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql+psycopg://u:p@h/db",
            redis_url="redis://h:6379/0",
            job_handler_timeout_s=60.0,
            visibility_timeout_s=60.0,
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_config.py::test_failure_handling_defaults tests/unit/test_config.py::test_timeout_invariant_rejects_handler_ge_visibility -v`
Expected: FAIL (`AttributeError`/no such field, and no validator raised).

- [ ] **Step 3: Implement the settings**

In `app/core/config.py`, add the import and fields, then the validator:

```python
from pydantic import model_validator
```

Add these fields to `Settings` (after `log_level`):

```python
    job_handler_timeout_s: float = 45.0
    visibility_timeout_s: float = 60.0
    reaper_interval_s: float = 30.0
    reaper_batch_size: int = 100
    max_attempts: int = 4
    retry_backoff_schedule: list[int] = [0, 30, 120]
    max_handler_timeouts_before_recycle: int = 1
```

Add the validator method to `Settings`:

```python
    @model_validator(mode="after")
    def _check_timeout_invariant(self) -> "Settings":
        if self.job_handler_timeout_s >= self.visibility_timeout_s:
            raise ValueError(
                "job_handler_timeout_s must be < visibility_timeout_s "
                f"(got {self.job_handler_timeout_s} >= {self.visibility_timeout_s})"
            )
        return self
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS (all config tests).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix app/core/config.py tests/unit/test_config.py
uv run ruff format app/core/config.py tests/unit/test_config.py
git add app/core/config.py tests/unit/test_config.py
git commit -m "feat: config for timeouts, retries, reaper + startup invariant"
```

---

### Task 2: Data model & migration — attempts / max_attempts

**Files:**
- Create: `alembic/versions/0004_add_attempts.py`
- Modify: `app/models/job.py`, `app/repository.py` (only `create_job` gains a `max_attempts` kwarg)
- Test: `tests/integration/test_repository.py`

**Interfaces:**
- Produces: `Job.attempts: int` (default 0), `Job.max_attempts: int` (default 4). `create_job(..., max_attempts: int = 4)` sets `max_attempts` on the row.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_repository.py`:

```python
def test_create_job_defaults_attempts(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    db_session.refresh(job)
    assert job.attempts == 0
    assert job.max_attempts == 4


def test_create_job_sets_max_attempts(db_session):
    job = repo.create_job(
        db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"}, max_attempts=2
    )
    db_session.refresh(job)
    assert job.max_attempts == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_repository.py::test_create_job_defaults_attempts -v`
Expected: FAIL (`AttributeError: attempts` / unexpected `max_attempts` kwarg).

- [ ] **Step 3: Write the migration**

Create `alembic/versions/0004_add_attempts.py`:

```python
"""add attempts tracking

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-01
"""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "jobs",
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="4"),
    )


def downgrade() -> None:
    op.drop_column("jobs", "max_attempts")
    op.drop_column("jobs", "attempts")
```

- [ ] **Step 4: Add the model columns**

In `app/models/job.py`, add after the `error` column:

```python
    attempts: Mapped[int] = mapped_column(
        sa.Integer, nullable=False, default=0, server_default="0"
    )
    max_attempts: Mapped[int] = mapped_column(
        sa.Integer, nullable=False, default=4, server_default="4"
    )
```

And in `Job.__init__`, add the setdefaults alongside the existing ones:

```python
        kwargs.setdefault("attempts", 0)
        kwargs.setdefault("max_attempts", 4)
```

- [ ] **Step 5: Add `max_attempts` to `create_job`**

In `app/repository.py`, update `create_job`'s signature and body:

```python
def create_job(
    session: Session,
    job_type: JobType,
    payload: dict,
    *,
    status: JobStatus = JobStatus.pending,
    scheduled_at: datetime | None = None,
    priority: JobPriority = JobPriority.normal,
    max_attempts: int = 4,
) -> Job:
    job = Job(
        type=job_type,
        payload=payload,
        status=status,
        scheduled_at=scheduled_at,
        priority=priority,
        max_attempts=max_attempts,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: PASS (new tests + all existing repository tests).

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check --fix app/models/job.py app/repository.py alembic/versions/0004_add_attempts.py tests/integration/test_repository.py
uv run ruff format app/models/job.py app/repository.py alembic/versions/0004_add_attempts.py tests/integration/test_repository.py
git add app/models/job.py app/repository.py alembic/versions/0004_add_attempts.py tests/integration/test_repository.py
git commit -m "feat: add attempts/max_attempts columns (migration 0004)"
```

---

### Task 3: Guarded repository transitions

**Files:**
- Modify: `app/repository.py`
- Test: `tests/integration/test_repository.py`

**Interfaces:**
- Consumes: `Job` columns from Task 2.
- Produces (all guarded on `status='processing'` unless noted; all increment `attempts` by 1 in SQL unless noted; all return `bool` = won):
  - `complete_job(session, job_id, result: dict) -> bool`
  - `fail_job(session, job_id, error: dict) -> bool` (→ `failed`, sets `completed_at`)
  - `retry_to_pending(session, job_id) -> bool` (→ `pending`, `is_synced_to_redis=False`, `started_at=None`)
  - `retry_to_scheduled(session, job_id, scheduled_at: datetime) -> bool` (→ `scheduled`, sets `scheduled_at`, `is_synced_to_redis=False`, `started_at=None`)
  - `reset_failed_to_pending(session, job_id) -> bool` (guarded on `status='failed'`; sets `attempts=0`, clears `error`/`started_at`/`completed_at`, `is_synced_to_redis=False`)

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_repository.py`:

```python
def test_complete_job_guarded_increments_attempts(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)
    assert repo.complete_job(db_session, job.id, {"message_id": "m1"}) is True
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1


def test_complete_job_loses_when_not_processing(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)
    repo.retry_to_pending(db_session, job.id)  # someone re-queued it → now pending
    assert repo.complete_job(db_session, job.id, {"message_id": "m1"}) is False
    db_session.refresh(job)
    assert job.status is JobStatus.pending


def test_fail_job_guarded_increments_attempts(db_session):
    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    repo.claim_job(db_session, job.id)
    assert repo.fail_job(db_session, job.id, {"type": "E", "message": "boom"}) is True
    db_session.refresh(job)
    assert job.status is JobStatus.failed
    assert job.attempts == 1


def test_retry_to_pending_resets_sync_and_counts(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.mark_synced(db_session, job.id)
    repo.claim_job(db_session, job.id)
    assert repo.retry_to_pending(db_session, job.id) is True
    db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 1
    assert job.is_synced_to_redis is False
    assert job.started_at is None


def test_retry_to_scheduled_sets_when(db_session):
    from datetime import datetime, timezone

    when = datetime(2030, 1, 1, tzinfo=timezone.utc)
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)
    assert repo.retry_to_scheduled(db_session, job.id, when) is True
    db_session.refresh(job)
    assert job.status is JobStatus.scheduled
    assert job.scheduled_at == when
    assert job.attempts == 1
    assert job.is_synced_to_redis is False


def test_reset_failed_to_pending_only_from_failed(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)
    repo.fail_job(db_session, job.id, {"type": "E", "message": "x"})
    assert repo.reset_failed_to_pending(db_session, job.id) is True
    db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 0
    assert job.error is None
    assert job.completed_at is None
    assert job.is_synced_to_redis is False
    # A second reset finds it already pending → guard fails.
    assert repo.reset_failed_to_pending(db_session, job.id) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_repository.py -k "guarded or retry_to or reset_failed" -v`
Expected: FAIL (`retry_to_pending`/`retry_to_scheduled`/`reset_failed_to_pending` undefined; `complete_job`/`fail_job` return `None`).

- [ ] **Step 3: Implement the guarded transitions**

In `app/repository.py`, replace `complete_job` and `fail_job` and add the three new functions:

```python
def complete_job(session: Session, job_id: UUID, result: dict) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.completed,
            result=result,
            completed_at=_now(),
            attempts=Job.attempts + 1,
        )
    )
    session.commit()
    return res.rowcount == 1


def fail_job(session: Session, job_id: UUID, error: dict) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.failed,
            error=error,
            completed_at=_now(),
            attempts=Job.attempts + 1,
        )
    )
    session.commit()
    return res.rowcount == 1


def retry_to_pending(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.pending,
            attempts=Job.attempts + 1,
            is_synced_to_redis=False,
            started_at=None,
        )
    )
    session.commit()
    return res.rowcount == 1


def retry_to_scheduled(
    session: Session, job_id: UUID, scheduled_at: datetime
) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.scheduled,
            scheduled_at=scheduled_at,
            attempts=Job.attempts + 1,
            is_synced_to_redis=False,
            started_at=None,
        )
    )
    session.commit()
    return res.rowcount == 1


def reset_failed_to_pending(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.failed)
        .values(
            status=JobStatus.pending,
            attempts=0,
            error=None,
            started_at=None,
            completed_at=None,
            is_synced_to_redis=False,
        )
    )
    session.commit()
    return res.rowcount == 1
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: PASS (new + existing; `test_complete_and_fail` still passes since it ignores the return value).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix app/repository.py tests/integration/test_repository.py
uv run ruff format app/repository.py tests/integration/test_repository.py
git add app/repository.py tests/integration/test_repository.py
git commit -m "feat: guarded processing-exit transitions with attempt counting"
```

---

### Task 4: Backoff helper + `schedule_retry_or_fail`

**Files:**
- Create: `app/retry.py`
- Test: `tests/unit/test_backoff.py`, `tests/integration/test_retry.py`

**Interfaces:**
- Consumes: repo transitions from Task 3; `producer.enqueue`, `delayed.schedule`, `settings.stream_for_priority`, `settings.retry_backoff_schedule`, `settings.delayed_zset`.
- Produces:
  - `backoff_delay(attempts: int, schedule: list[int]) -> int` — delay (s) before the retry that follows `attempts` completed attempts; clamps past the end of `schedule`.
  - `schedule_retry_or_fail(session, client, settings, job: Job, error: dict) -> bool` — returns `won`. Does the guarded transition + (on win) the Redis re-enqueue + `mark_synced`. **Never `XACK`s** (caller's job).

- [ ] **Step 1: Write the failing backoff unit test**

Create `tests/unit/test_backoff.py`:

```python
from app.retry import backoff_delay


def test_backoff_immediate_first_retry():
    assert backoff_delay(1, [0, 30, 120]) == 0


def test_backoff_second_and_third():
    assert backoff_delay(2, [0, 30, 120]) == 30
    assert backoff_delay(3, [0, 30, 120]) == 120


def test_backoff_clamps_past_end():
    assert backoff_delay(9, [0, 30, 120]) == 120
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_backoff.py -v`
Expected: FAIL (`ModuleNotFoundError: app.retry`).

- [ ] **Step 3: Implement `app/retry.py`**

Create `app/retry.py`:

```python
from datetime import datetime, timedelta, timezone

import redis
import structlog
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.models.job import Job
from app.queue import delayed
from app.queue.producer import enqueue

log = structlog.get_logger("retry")


def backoff_delay(attempts: int, schedule: list[int]) -> int:
    """Delay (seconds) before the retry that follows `attempts` completed attempts."""
    idx = min(attempts - 1, len(schedule) - 1)
    return schedule[idx]


def schedule_retry_or_fail(
    session: Session,
    client: redis.Redis,
    settings: Settings,
    job: Job,
    error: dict,
) -> bool:
    """Retry with backoff, or permanently fail at max_attempts. Returns True iff
    this actor won the guarded transition. Does not XACK."""
    n = job.attempts + 1  # the attempt that just ended
    if n >= job.max_attempts:
        won = repo.fail_job(session, job.id, error)
        log.info("retry.failed_permanent", job_id=str(job.id), attempts=n, won=won)
        return won

    delay = backoff_delay(n, settings.retry_backoff_schedule)
    if delay <= 0:
        won = repo.retry_to_pending(session, job.id)
        if won:
            enqueue(client, settings.stream_for_priority(job.priority), str(job.id))
            repo.mark_synced(session, job.id)
        log.info("retry.immediate", job_id=str(job.id), attempts=n, won=won)
        return won

    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
    won = repo.retry_to_scheduled(session, job.id, scheduled_at)
    if won:
        delayed.schedule(
            client, settings.delayed_zset, str(job.id), scheduled_at.timestamp()
        )
        repo.mark_synced(session, job.id)
    log.info("retry.delayed", job_id=str(job.id), attempts=n, delay=delay, won=won)
    return won
```

- [ ] **Step 4: Run backoff test to verify it passes**

Run: `uv run pytest tests/unit/test_backoff.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing integration tests**

Create `tests/integration/test_retry.py`:

```python
from app import repository as repo
from app.retry import schedule_retry_or_fail
from app.schemas.enums import JobStatus, JobType


def test_retry_immediate_reenqueues_to_priority_stream(
    db_session, redis_client, test_settings
):
    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    repo.claim_job(db_session, job.id)  # → processing
    db_session.refresh(job)

    won = schedule_retry_or_fail(
        db_session, redis_client, test_settings, job, {"type": "E", "message": "boom"}
    )

    assert won is True
    db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 1
    assert job.is_synced_to_redis is True
    assert redis_client.xlen(test_settings.stream_normal) == 1


def test_retry_delayed_parks_in_zset(db_session, redis_client, test_settings):
    settings = test_settings.model_copy(update={"retry_backoff_schedule": [30, 30, 30]})
    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    repo.claim_job(db_session, job.id)
    db_session.refresh(job)

    won = schedule_retry_or_fail(
        db_session, redis_client, settings, job, {"type": "E", "message": "boom"}
    )

    assert won is True
    db_session.refresh(job)
    assert job.status is JobStatus.scheduled
    assert redis_client.zcard(settings.delayed_zset) == 1
    assert redis_client.xlen(settings.stream_normal) == 0


def test_retry_permanent_fail_at_max_attempts(db_session, redis_client, test_settings):
    job = repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, max_attempts=1
    )
    repo.claim_job(db_session, job.id)
    db_session.refresh(job)

    won = schedule_retry_or_fail(
        db_session, redis_client, test_settings, job, {"type": "E", "message": "boom"}
    )

    assert won is True
    db_session.refresh(job)
    assert job.status is JobStatus.failed
    assert job.attempts == 1
    assert redis_client.xlen(test_settings.stream_normal) == 0
```

- [ ] **Step 6: Run integration tests to verify they pass**

Run: `uv run pytest tests/integration/test_retry.py -v`
Expected: PASS.

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check --fix app/retry.py tests/unit/test_backoff.py tests/integration/test_retry.py
uv run ruff format app/retry.py tests/unit/test_backoff.py tests/integration/test_retry.py
git add app/retry.py tests/unit/test_backoff.py tests/integration/test_retry.py
git commit -m "feat: shared schedule_retry_or_fail helper with exponential backoff"
```

---

### Task 5: Worker timeout wrapper

**Files:**
- Create: `app/worker/timeout.py`
- Test: `tests/unit/test_timeout.py`

**Interfaces:**
- Produces: `HandlerTimeout(Exception)`; `run_with_timeout(fn: Callable[[], T], timeout_s: float) -> T` — runs `fn` in a single-use thread; raises `HandlerTimeout` if it exceeds `timeout_s`; does not wait for the leaked thread.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_timeout.py`:

```python
import time

import pytest

from app.worker.timeout import HandlerTimeout, run_with_timeout


def test_returns_value_when_fast():
    assert run_with_timeout(lambda: 21 * 2, timeout_s=1.0) == 42


def test_raises_handler_timeout_when_slow():
    with pytest.raises(HandlerTimeout):
        run_with_timeout(lambda: time.sleep(1.0), timeout_s=0.05)


def test_propagates_handler_exception():
    def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        run_with_timeout(boom, timeout_s=1.0)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_timeout.py -v`
Expected: FAIL (`ModuleNotFoundError: app.worker.timeout`).

- [ ] **Step 3: Implement `app/worker/timeout.py`**

```python
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from typing import TypeVar

T = TypeVar("T")


class HandlerTimeout(Exception):
    """Raised when a job handler exceeds its allotted execution time."""


def run_with_timeout(fn: Callable[[], T], timeout_s: float) -> T:
    """Run `fn` in a single-use worker thread and wait up to `timeout_s`.

    On timeout raise HandlerTimeout; the underlying thread is abandoned (Python
    cannot kill it) — the worker recycles its process to reclaim it.
    """
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn)
    try:
        return future.result(timeout=timeout_s)
    except FuturesTimeout as exc:
        raise HandlerTimeout(f"handler exceeded {timeout_s}s") from exc
    finally:
        executor.shutdown(wait=False)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_timeout.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix app/worker/timeout.py tests/unit/test_timeout.py
uv run ruff format app/worker/timeout.py tests/unit/test_timeout.py
git add app/worker/timeout.py tests/unit/test_timeout.py
git commit -m "feat: thread-based run_with_timeout for handler execution"
```

---

### Task 6: Worker — timeout, retry routing, and recycle-on-timeout

**Files:**
- Modify: `app/worker/runner.py`, `app/worker/__main__.py`, `docker-compose.yml`
- Test: `tests/integration/test_worker.py`

**Interfaces:**
- Consumes: `run_with_timeout`/`HandlerTimeout` (Task 5); `schedule_retry_or_fail` (Task 4); guarded `complete_job` (Task 3); `settings.job_handler_timeout_s`, `settings.max_handler_timeouts_before_recycle`.
- Produces: `Outcome(ack: bool, recycle: bool, label: str)` dataclass; `process_job(session, client, settings, job_id) -> Outcome`; `run_forever(settings, *, stop=None) -> int` (0 = normal stop, 1 = recycled).

- [ ] **Step 1: Write the failing tests (migrate existing + add new)**

Rewrite `tests/integration/test_worker.py` so `process_job` takes the new signature and add timeout/retry/recycle coverage. Replace the whole file with:

```python
import time

import pytest

from app import repository as repo
from app.jobs import handlers
from app.schemas.enums import JobPriority, JobStatus, JobType
from app.worker.runner import process_job


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def test_process_job_completes_email(db_session, redis_client, test_settings):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "completed"
    assert outcome.ack is True
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1


def test_process_job_retries_on_handler_failure(
    db_session, redis_client, test_settings, monkeypatch
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # force webhook fail
    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "retried"
    db_session.refresh(job)
    assert job.status is JobStatus.pending  # immediate retry re-enqueued
    assert job.attempts == 1
    assert redis_client.xlen(test_settings.stream_normal) == 1


def test_process_job_permanent_fail_when_attempts_exhausted(
    db_session, redis_client, test_settings, monkeypatch
):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)
    job = repo.create_job(
        db_session, JobType.webhook, {"url": "https://x.test"}, max_attempts=1
    )
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.ack is True
    db_session.refresh(job)
    assert job.status is JobStatus.failed


def test_process_job_skips_unclaimable(db_session, redis_client, test_settings):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)
    repo.complete_job(db_session, job.id, {"message_id": "m1"})  # already terminal
    outcome = process_job(db_session, redis_client, test_settings, job.id)
    assert outcome.label == "skipped"
    assert outcome.ack is True


def test_process_job_timeout_recycles(
    db_session, redis_client, test_settings, monkeypatch
):
    settings = test_settings.model_copy(update={"job_handler_timeout_s": 0.05})
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: time.sleep(0.5))
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    outcome = process_job(db_session, redis_client, settings, job.id)
    assert outcome.recycle is True
    db_session.refresh(job)
    assert job.status is JobStatus.pending  # timeout → immediate retry
    assert job.attempts == 1


def test_run_forever_processes_one_then_stops(test_settings, redis_client, pg_engine):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        job = repo.create_job(s, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    enqueue(redis_client, test_settings.stream_normal, str(job.id))

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    assert run_forever(test_settings, stop=stop) == 0
    with factory() as s:
        assert repo.get_job(s, job.id).status is JobStatus.completed


def test_run_forever_drains_high_before_low(test_settings, redis_client, pg_engine):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        low = repo.create_job(
            s, JobType.email, {"to": "a@b.com", "subject": "Hi"}, priority=JobPriority.low
        )
        high = repo.create_job(
            s, JobType.email, {"to": "a@b.com", "subject": "Hi"}, priority=JobPriority.high
        )
    enqueue(redis_client, test_settings.stream_low, str(low.id))
    enqueue(redis_client, test_settings.stream_high, str(high.id))

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    run_forever(test_settings, stop=stop)
    with factory() as s:
        assert repo.get_job(s, high.id).status is JobStatus.completed
        assert repo.get_job(s, low.id).status is JobStatus.pending
    assert (
        redis_client.xpending(test_settings.stream_high, test_settings.consumer_group)[
            "pending"
        ]
        == 0
    )
    assert redis_client.xlen(test_settings.stream_low) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_worker.py -v`
Expected: FAIL (`process_job` signature mismatch / `Outcome` undefined / `run_forever` returns `None`).

- [ ] **Step 3: Rewrite `app/worker/runner.py`**

Replace the file contents with:

```python
import signal
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

import redis
import structlog
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.redis import create_redis_client
from app.jobs.registry import run_handler
from app.queue.consumer import CONSUMER_NAME, ack, ensure_group, read_priority
from app.retry import schedule_retry_or_fail
from app.schemas.payloads import validate_payload
from app.worker.timeout import HandlerTimeout, run_with_timeout

log = structlog.get_logger("worker")


@dataclass
class Outcome:
    ack: bool
    recycle: bool
    label: str


def process_job(
    session: Session, client: redis.Redis, settings: Settings, job_id: UUID
) -> Outcome:
    if not repo.claim_job(session, job_id):
        log.info("job.skipped", reason="not_claimable")
        return Outcome(ack=True, recycle=False, label="skipped")

    job = repo.get_job(session, job_id)
    try:
        payload = validate_payload(job.type, job.payload)
        result = run_with_timeout(
            lambda: run_handler(job.type, payload), settings.job_handler_timeout_s
        )
    except HandlerTimeout:
        won = schedule_retry_or_fail(
            session,
            client,
            settings,
            job,
            {"type": "HandlerTimeout", "message": f">{settings.job_handler_timeout_s}s"},
        )
        log.warning("job.timeout", won=won)
        return Outcome(ack=won, recycle=True, label="timeout")
    except Exception as exc:  # noqa: BLE001 — any handler/validation error is retryable
        won = schedule_retry_or_fail(
            session,
            client,
            settings,
            job,
            {"type": type(exc).__name__, "message": str(exc)},
        )
        log.info("job.retry_scheduled", error_type=type(exc).__name__, won=won)
        return Outcome(ack=won, recycle=False, label="retried")

    won = repo.complete_job(session, job.id, result)
    if not won:
        log.critical("job.complete_lost_to_reaper")
        return Outcome(ack=False, recycle=False, label="lost")
    log.info("job.completed")
    return Outcome(ack=True, recycle=False, label="completed")


def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> int:
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    client = create_redis_client(settings.redis_url)
    for stream in settings.ordered_streams:
        ensure_group(client, stream, settings.consumer_group)

    shutting_down = {"flag": False}

    def _request_stop(*_):
        shutting_down["flag"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    structlog.contextvars.bind_contextvars(consumer=CONSUMER_NAME)
    log.info(
        "worker.started",
        streams=settings.ordered_streams,
        group=settings.consumer_group,
    )

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

    timeouts = 0
    exit_code = 0
    while not _should_stop():
        batch = read_priority(
            client,
            settings.ordered_streams,
            settings.consumer_group,
            CONSUMER_NAME,
            settings.block_ms,
        )
        for stream, message_id, fields in batch:
            job_id = UUID(fields["job_id"])
            with structlog.contextvars.bound_contextvars(
                job_id=str(job_id), message_id=message_id, stream=stream
            ):
                log.info("job.received")
                with session_factory() as session:
                    outcome = process_job(session, client, settings, job_id)
                if outcome.ack:
                    ack(client, stream, settings.consumer_group, message_id)
                if outcome.recycle:
                    timeouts += 1
                    if timeouts >= settings.max_handler_timeouts_before_recycle:
                        log.warning("worker.recycling", timeouts=timeouts)
                        exit_code = 1
                        break
        if exit_code:
            break

    log.info("worker.stopped", exit_code=exit_code)
    client.close()
    engine.dispose()
    return exit_code
```

- [ ] **Step 4: Propagate the exit code from the entrypoint**

Replace `app/worker/__main__.py` with:

```python
import sys

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.worker.runner import run_forever


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    sys.exit(run_forever(settings))


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Add the worker restart policy**

In `docker-compose.yml`, add `restart: on-failure` to the `worker` service (so a recycle exit=1 restarts it, while a graceful SIGTERM exit=0 does not):

```yaml
  worker:
    build: .
    command: python -m app.worker
    restart: on-failure
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
```

- [ ] **Step 6: Migrate the ticker test's `process_job` calls**

In `tests/integration/test_ticker.py`, the two end-to-end tests call `process_job(s, job.id)`. Update both call sites to the new signature:

```python
        process_job(s, redis_client, test_settings, job.id)
```

(Both `test_end_to_end_scheduled_job_completes` and `test_duplicate_promotion_second_claim_is_noop` already receive `redis_client` and `test_settings` fixtures.)

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_worker.py tests/integration/test_ticker.py -v`
Expected: PASS.

- [ ] **Step 8: Lint and commit**

```bash
uv run ruff check --fix app/worker/runner.py app/worker/__main__.py tests/integration/test_worker.py tests/integration/test_ticker.py
uv run ruff format app/worker/runner.py app/worker/__main__.py tests/integration/test_worker.py tests/integration/test_ticker.py
git add app/worker/runner.py app/worker/__main__.py docker-compose.yml tests/integration/test_worker.py tests/integration/test_ticker.py
git commit -m "feat: worker timeout + retry routing + recycle-on-timeout"
```

---

### Task 7: Reaper — reclaim stale PEL entries in the ticker

**Files:**
- Modify: `app/queue/consumer.py` (add `REAPER_NAME`), `app/ticker/runner.py`
- Test: `tests/integration/test_reaper.py`

**Interfaces:**
- Consumes: `schedule_retry_or_fail` (Task 4); `repo.get_job`, `repo.mark_synced`; `producer.enqueue`, `delayed.schedule`; `settings.visibility_timeout_s`, `settings.reaper_interval_s`, `settings.reaper_batch_size`.
- Produces: `consumer.REAPER_NAME: str`; `ticker.reap_stale(session, client, settings) -> int` (count of PEL entries handled). Wired into `ticker.run_forever` on a `reaper_interval_s` cadence.

- [ ] **Step 1: Add the reaper consumer name**

In `app/queue/consumer.py`, add below `CONSUMER_NAME`:

```python
REAPER_NAME = "reaper"
```

- [ ] **Step 2: Write the failing tests**

Create `tests/integration/test_reaper.py`:

```python
from app import repository as repo
from app.queue.consumer import ensure_group
from app.schemas.enums import JobStatus, JobType
from app.ticker.runner import reap_stale


def _plant_pel(client, group, stream, job_id):
    """Add a message and read it into a dead consumer's PEL without acking."""
    ensure_group(client, stream, group)
    client.xadd(stream, {"job_id": str(job_id)})
    client.xreadgroup(
        groupname=group, consumername="deadworker", streams={stream: ">"}, count=10
    )


def test_reaper_requeues_abandoned_processing_job(
    db_session, redis_client, test_settings
):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)
    repo.claim_job(db_session, job.id)  # → processing (worker "died")

    handled = reap_stale(db_session, redis_client, settings)

    assert handled == 1
    db_session.refresh(job)
    assert job.status is JobStatus.pending  # immediate retry
    assert job.attempts == 1
    # Original PEL entry cleared; a fresh message was enqueued.
    assert (
        redis_client.xpending(settings.stream_normal, settings.consumer_group)["pending"]
        == 0
    )
    assert redis_client.xlen(settings.stream_normal) == 2  # planted + re-enqueued


def test_reaper_finishes_handoff_for_unsynced_pending(
    db_session, redis_client, test_settings
):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    # pending + is_synced_to_redis=False (create_job leaves it False), worker
    # "died" after winning the guard but before XADD.
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)

    handled = reap_stale(db_session, redis_client, settings)

    assert handled == 1
    db_session.refresh(job)
    assert job.status is JobStatus.pending
    assert job.attempts == 0  # handoff finished, NOT a new retry
    assert job.is_synced_to_redis is True
    assert redis_client.xlen(settings.stream_normal) == 2  # planted + reaper's re-add


def test_reaper_only_acks_completed_ghost(db_session, redis_client, test_settings):
    settings = test_settings.model_copy(update={"visibility_timeout_s": 0})
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, job.id)
    repo.complete_job(db_session, job.id, {"message_id": "m1"})  # terminal
    repo.mark_synced(db_session, job.id)
    _plant_pel(redis_client, settings.consumer_group, settings.stream_normal, job.id)

    handled = reap_stale(db_session, redis_client, settings)

    assert handled == 1
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.attempts == 1  # unchanged
    assert (
        redis_client.xpending(settings.stream_normal, settings.consumer_group)["pending"]
        == 0
    )
    assert redis_client.xlen(settings.stream_normal) == 1  # no re-enqueue
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_reaper.py -v`
Expected: FAIL (`ImportError: reap_stale`).

- [ ] **Step 4: Implement `reap_stale` in `app/ticker/runner.py`**

The ticker already imports `repo`, `delayed`, `enqueue`, `JobStatus`, `UUID`,
`redis`, `Session`, `Settings`, and `ensure_group`. Only two names are new:

1. Change the existing line `from app.queue.consumer import ensure_group` to:

```python
from app.queue.consumer import REAPER_NAME, ensure_group
```

2. Add one new import:

```python
from app.retry import schedule_retry_or_fail
```

Add the functions:

```python
def _reap_one(session, client, settings, stream, message_id, job_id) -> None:
    job = repo.get_job(session, job_id)
    if job is not None:
        if job.status in (JobStatus.completed, JobStatus.failed):
            pass  # ghost: worker finished, XACK was dropped
        elif job.status is JobStatus.processing:
            schedule_retry_or_fail(
                session,
                client,
                settings,
                job,
                {"type": "WorkerLost", "message": "reclaimed by reaper"},
            )
        elif not job.is_synced_to_redis:
            # Worker won the guard then died before the Redis handoff → finish it
            # inline (immediate recovery). Do NOT touch attempts / re-decide.
            if job.status is JobStatus.scheduled and job.scheduled_at is not None:
                delayed.schedule(
                    client, settings.delayed_zset, str(job.id), job.scheduled_at.timestamp()
                )
            else:
                enqueue(client, settings.stream_for_priority(job.priority), str(job.id))
            repo.mark_synced(session, job.id)
        # else: pending/scheduled + synced=True → fresh message already live
    # Always clear the reclaimed entry from the PEL.
    client.xack(stream, settings.consumer_group, message_id)


def reap_stale(session: Session, client: redis.Redis, settings: Settings) -> int:
    min_idle = int(settings.visibility_timeout_s * 1000)
    handled = 0
    for stream in settings.ordered_streams:
        cursor = "0-0"
        while True:
            resp = client.xautoclaim(
                name=stream,
                groupname=settings.consumer_group,
                consumername=REAPER_NAME,
                min_idle_time=min_idle,
                start_id=cursor,
                count=settings.reaper_batch_size,
            )
            cursor, messages = resp[0], resp[1]
            for message_id, fields in messages:
                _reap_one(
                    session, client, settings, stream, message_id, UUID(fields["job_id"])
                )
                handled += 1
            if cursor == "0-0":
                break
    if handled:
        log.info("ticker.reaped", count=handled)
    return handled
```

- [ ] **Step 5: Wire the reaper into `run_forever`**

In `app/ticker/runner.py`'s `run_forever`, add a `last_reap` tracker next to `last_reconcile` and a reaper call in the loop. After the `last_reconcile = 0.0` line add:

```python
    last_reap = 0.0
```

Inside the `while` loop's `try`, after the reconcile block and before the batch-drain `continue`, add:

```python
            if now - last_reap >= settings.reaper_interval_s:
                with session_factory() as session:
                    reap_stale(session, client, settings)
                last_reap = now
```

(`now` is already computed as `time.time()` earlier in the loop body.)

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_reaper.py -v`
Expected: PASS.

- [ ] **Step 7: Lint and commit**

```bash
uv run ruff check --fix app/queue/consumer.py app/ticker/runner.py tests/integration/test_reaper.py
uv run ruff format app/queue/consumer.py app/ticker/runner.py tests/integration/test_reaper.py
git add app/queue/consumer.py app/ticker/runner.py tests/integration/test_reaper.py
git commit -m "feat: ticker reaper reclaims stale PEL entries (XAUTOCLAIM + guards)"
```

---

### Task 8: API — expose attempts + manual retry endpoint

**Files:**
- Modify: `app/schemas/api.py`, `app/api/routes.py`
- Test: `tests/integration/test_api.py`

**Interfaces:**
- Consumes: `repo.reset_failed_to_pending` (Task 3); `Job.attempts/max_attempts` (Task 2); `settings.max_attempts`; `producer.enqueue`, `settings.stream_for_priority`.
- Produces: `JobOut.attempts: int`, `JobOut.max_attempts: int`; `POST /jobs/{job_id}/retry` → `JobOut` (404 unknown, 409 not-`failed`). `submit_job` passes `max_attempts=settings.max_attempts` to `create_job`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_api.py`:

```python
def test_job_out_exposes_attempts(client):
    resp = client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    job_id = resp.json()["id"]
    got = client.get(f"/jobs/{job_id}").json()
    assert got["attempts"] == 0
    assert got["max_attempts"] == 4


def test_retry_failed_job_reenqueues(client, db_session):
    from app import repository as repo
    from app.schemas.enums import JobStatus, JobType

    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    repo.claim_job(db_session, job.id)
    repo.fail_job(db_session, job.id, {"type": "E", "message": "boom"})

    resp = client.post(f"/jobs/{job.id}/retry")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "pending"
    assert body["attempts"] == 0


def test_retry_non_failed_returns_409(client):
    resp = client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    job_id = resp.json()["id"]
    retry = client.post(f"/jobs/{job_id}/retry")
    assert retry.status_code == 409


def test_retry_unknown_returns_404(client):
    import uuid

    resp = client.post(f"/jobs/{uuid.uuid4()}/retry")
    assert resp.status_code == 404
```

Note: `test_retry_failed_job_reenqueues` uses both `client` and `db_session`; the `client` fixture builds its own app but shares the same Postgres (`pg_engine`), so a row created via `db_session` is visible to the API.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_api.py -k "attempts or retry" -v`
Expected: FAIL (missing `attempts` in response / no retry route → 405).

- [ ] **Step 3: Add attempts to `JobOut`**

In `app/schemas/api.py`, add to `JobOut` (after `error`):

```python
    attempts: int
    max_attempts: int
```

- [ ] **Step 4: Pass `max_attempts` on submit and add the retry route**

In `app/api/routes.py`, update both `create_job` calls in `submit_job` to include the budget from settings, e.g. the immediate path:

```python
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            priority=submission.priority,
            max_attempts=settings.max_attempts,
        )
```

and the scheduled path likewise (add `max_attempts=settings.max_attempts` to that `create_job(...)` call).

Then add the retry endpoint (after `get_job`):

```python
@router.post("/jobs/{job_id}/retry", response_model=JobOut)
def retry_job(
    job_id: UUID,
    request: Request,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobOut:
    job = repo.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if not repo.reset_failed_to_pending(session, job_id):
        raise HTTPException(
            status_code=409, detail="job is not in a terminal failed state"
        )
    settings = request.app.state.settings
    enqueue(client, settings.stream_for_priority(job.priority), str(job_id))
    repo.mark_synced(session, job_id)
    session.refresh(job)
    return JobOut.model_validate(job)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_api.py -v`
Expected: PASS (new + existing API tests).

- [ ] **Step 6: Full suite + lint, then commit**

Run: `uv run pytest`
Expected: PASS (entire suite).

```bash
uv run ruff check --fix app/schemas/api.py app/api/routes.py tests/integration/test_api.py
uv run ruff format app/schemas/api.py app/api/routes.py tests/integration/test_api.py
git add app/schemas/api.py app/api/routes.py tests/integration/test_api.py
git commit -m "feat: expose attempts and add POST /jobs/{id}/retry endpoint"
```

---

## Self-Review

**Spec coverage:**
- Attempt tracking → Task 2 (columns), Task 3 (increment in every guarded exit).
- Exponential backoff, immediate first retry → Task 4 (`backoff_delay`, `schedule_retry_or_fail`).
- Permanent fail at max attempts → Task 4 / Task 6 (`max_attempts=1` test).
- Worker Max Job Handler Timeout (Layer 1) → Task 5 (`run_with_timeout`), Task 6 (wired in `process_job`).
- Startup invariant `handler < visibility` → Task 1 validator.
- Reaper via `XAUTOCLAIM` with `status × is_synced_to_redis` matrix → Task 7 (`_reap_one`, `reap_stale`).
- Worker Update Guard / Ticker Reaper Guard (optimistic locking) → Task 3 (`WHERE status='processing'`), consumed by Tasks 4/6/7.
- XACK ownership (worker acks on win; reaper always acks) → Task 6 (`Outcome.ack`), Task 7 (`_reap_one` final `xack`).
- Recycle-on-timeout / zombie defense → Task 5 (`shutdown(wait=False)`), Task 6 (`Outcome.recycle`, `run_forever` exit code), compose `restart: on-failure`.
- Manual retry endpoint (reset attempts) → Task 8.
- `JobOut` exposes attempts → Task 8.

**Placeholder scan:** No TBD/TODO; every code and test step contains full content. ✅

**Type consistency:** `process_job(session, client, settings, job_id) -> Outcome` used identically in Task 6 tests and Task 7 is untouched by it; `schedule_retry_or_fail(session, client, settings, job, error) -> bool` defined in Task 4 and called with the same argument order in Tasks 6 and 7; guarded repo functions return `bool` consistently across Tasks 3, 4, 6, 8. ✅

**Sequencing:** 1 (config) → 2 (model) → 3 (repo) → 4 (retry, needs 1+3) → 5 (timeout) → 6 (worker, needs 4+5) → 7 (reaper, needs 4) → 8 (API, needs 3). Each task ends green and commits.
