# Priority Scheduling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every job a `high`/`normal`/`low` priority that routes it through one of three Redis streams, and have workers drain them strictly high-first, with priority accepted and filterable through the API.

**Architecture:** A new `JobPriority` enum and a `priority` column drive a 1:1 map to three streams (`jobs:stream:{high,normal,low}`). The producer/API/ticker/reconciler pick the target stream via `Settings.stream_for_priority`. Workers probe the three streams highest-first non-blocking and only block when all are empty, so a higher-priority backlog is always drained first. The scheduled path stores only the id in the delayed ZSET, so the ticker resolves priority with one batched `SELECT id, priority WHERE id IN (:ids)` at promotion time; the reconciler already loads full rows.

**Tech Stack:** FastAPI, PostgreSQL, Redis Streams + consumer group, SQLAlchemy 2.0 + Alembic, Pytest, structlog. Managed with **uv**.

## Global Constraints

- Run everything through **uv**: tests `uv run pytest`, lint `uv run ruff check --fix`, format `uv run ruff format`. Never `pip`/`venv`/`poetry`.
- Structured logging only (`structlog`) — no `print`.
- Priority is a fixed 3-value enum: `high`, `normal`, `low`. Default `normal`. Higher = more urgent.
- `consumer_group` stays `"workers"`; the group must exist on **all three** streams before reading.
- Worker reads `COUNT 1`, strict sequential drain (probe high→normal→low non-blocking; block on all three only when empty).
- Promotion preserves the existing **XADD-before-ZREM** crash-safety ordering.
- Scheduled-job priority is resolved with **one batched** DB lookup per tick, never per-id.
- The single `jobs_stream` setting is kept temporarily (Tasks 1–7) and removed in Task 8, so the suite stays green at every task boundary.
- Every task ends green: `uv run pytest` passes and `uv run ruff check` is clean before committing.

---

### Task 1: `JobPriority` enum + stream mapping in Settings

**Files:**
- Modify: `app/schemas/enums.py`
- Modify: `app/core/config.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces:
  - `JobPriority(str, Enum)` with members `high`, `normal`, `low` (values `"high"`, `"normal"`, `"low"`).
  - `Settings.stream_high: str = "jobs:stream:high"`, `Settings.stream_normal: str = "jobs:stream:normal"`, `Settings.stream_low: str = "jobs:stream:low"`.
  - `Settings.ordered_streams -> list[str]` — `[stream_high, stream_normal, stream_low]`.
  - `Settings.priority_streams -> list[tuple[JobPriority, str]]` — same order, paired with priority.
  - `Settings.stream_for_priority(priority: JobPriority) -> str`.
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_config.py`:

```python
def test_job_priority_values():
    from app.schemas.enums import JobPriority

    assert [p.value for p in JobPriority] == ["high", "normal", "low"]


def test_priority_stream_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.stream_high == "jobs:stream:high"
    assert s.stream_normal == "jobs:stream:normal"
    assert s.stream_low == "jobs:stream:low"
    assert s.ordered_streams == [
        "jobs:stream:high",
        "jobs:stream:normal",
        "jobs:stream:low",
    ]


def test_priority_streams_ordering():
    from app.schemas.enums import JobPriority

    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.priority_streams == [
        (JobPriority.high, "jobs:stream:high"),
        (JobPriority.normal, "jobs:stream:normal"),
        (JobPriority.low, "jobs:stream:low"),
    ]


def test_stream_for_priority_maps_each_level():
    from app.schemas.enums import JobPriority

    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.stream_for_priority(JobPriority.high) == "jobs:stream:high"
    assert s.stream_for_priority(JobPriority.normal) == "jobs:stream:normal"
    assert s.stream_for_priority(JobPriority.low) == "jobs:stream:low"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `ImportError: cannot import name 'JobPriority'` / `AttributeError: 'Settings' object has no attribute 'stream_high'`.

- [ ] **Step 3: Add the `JobPriority` enum**

In `app/schemas/enums.py`, add below `JobStatus`:

```python
class JobPriority(str, Enum):
    high = "high"
    normal = "normal"
    low = "low"
```

- [ ] **Step 4: Add stream settings + mapping to `Settings`**

In `app/core/config.py`, add the import at the top (after the existing imports):

```python
from app.schemas.enums import JobPriority
```

Add the three stream fields next to `jobs_stream` (leave `jobs_stream` in place for now):

```python
    jobs_stream: str = "jobs:stream"
    stream_high: str = "jobs:stream:high"
    stream_normal: str = "jobs:stream:normal"
    stream_low: str = "jobs:stream:low"
```

Add these methods to the `Settings` class body (below the fields, above nothing-else-needed):

```python
    @property
    def ordered_streams(self) -> list[str]:
        return [self.stream_high, self.stream_normal, self.stream_low]

    @property
    def priority_streams(self) -> list[tuple[JobPriority, str]]:
        return [
            (JobPriority.high, self.stream_high),
            (JobPriority.normal, self.stream_normal),
            (JobPriority.low, self.stream_low),
        ]

    def stream_for_priority(self, priority: JobPriority) -> str:
        return dict(self.priority_streams)[priority]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS (all tests, including the pre-existing `test_settings_defaults`).

- [ ] **Step 6: Lint, format, commit**

```bash
uv run ruff check --fix
uv run ruff format
git add app/schemas/enums.py app/core/config.py tests/unit/test_config.py
git commit -m "feat: add JobPriority enum and priority-stream mapping in Settings"
```

---

### Task 2: `priority` column on the `Job` model + migration `0003`

**Files:**
- Modify: `app/models/job.py`
- Create: `alembic/versions/0003_add_priority.py`
- Test: `tests/unit/test_job_model.py`, `tests/integration/test_migration.py`

**Interfaces:**
- Consumes: `JobPriority` (Task 1).
- Produces: `Job.priority: Mapped[JobPriority]` (column `priority job_priority NOT NULL DEFAULT 'normal'`, indexed `ix_jobs_priority`); `Job(...)` defaults `priority` to `JobPriority.normal` in memory.

- [ ] **Step 1: Write the failing model tests**

In `tests/unit/test_job_model.py`, update the expected column set in `test_job_table_and_columns` to include `"priority"`:

```python
    assert cols == {
        "id",
        "type",
        "payload",
        "status",
        "result",
        "error",
        "created_at",
        "started_at",
        "completed_at",
        "scheduled_at",
        "is_synced_to_redis",
        "priority",
    }
```

Append a default test:

```python
def test_job_defaults_priority_normal():
    from app.schemas.enums import JobPriority

    j = Job(type=JobType.email, payload={"to": "a@b.com", "subject": "Hi"})
    assert j.priority is JobPriority.normal
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_job_model.py -v`
Expected: FAIL — column set mismatch (`priority` missing) / `AttributeError: priority`.

- [ ] **Step 3: Add the column to the model**

In `app/models/job.py`, extend the enums import:

```python
from app.schemas.enums import JobPriority, JobStatus, JobType
```

Add the mapped column after `status` (keep it near the other classification columns):

```python
    priority: Mapped[JobPriority] = mapped_column(
        SAEnum(JobPriority, name="job_priority"),
        default=JobPriority.normal,
        server_default="normal",
        nullable=False,
        index=True,
    )
```

In `Job.__init__`, add a `setdefault` next to the existing ones:

```python
    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("id", uuid.uuid4())
        kwargs.setdefault("status", JobStatus.pending)
        kwargs.setdefault("priority", JobPriority.normal)
        super().__init__(**kwargs)
```

- [ ] **Step 4: Run model tests to verify they pass**

Run: `uv run pytest tests/unit/test_job_model.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing migration test**

Append to `tests/integration/test_migration.py`:

```python
def test_priority_column_and_index(pg_engine):
    insp = inspect(pg_engine)
    cols = {c["name"] for c in insp.get_columns("jobs")}
    assert "priority" in cols
    index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
    assert "ix_jobs_priority" in index_names
```

- [ ] **Step 6: Run the migration test to verify it fails**

Run: `uv run pytest tests/integration/test_migration.py -v`
Expected: FAIL — `priority` not in columns (migration not written yet).

- [ ] **Step 7: Write migration `0003`**

Create `alembic/versions/0003_add_priority.py`:

```python
"""add priority

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-01
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

JOB_PRIORITY = postgresql.ENUM("high", "normal", "low", name="job_priority")


def upgrade() -> None:
    bind = op.get_bind()
    JOB_PRIORITY.create(bind, checkfirst=True)

    job_priority_ref = postgresql.ENUM(
        "high", "normal", "low", name="job_priority", create_type=False
    )
    op.add_column(
        "jobs",
        sa.Column(
            "priority", job_priority_ref, nullable=False, server_default="normal"
        ),
    )
    op.create_index("ix_jobs_priority", "jobs", ["priority"])


def downgrade() -> None:
    op.drop_index("ix_jobs_priority", table_name="jobs")
    op.drop_column("jobs", "priority")
    JOB_PRIORITY.drop(op.get_bind(), checkfirst=True)
```

- [ ] **Step 8: Run the migration test to verify it passes**

Run: `uv run pytest tests/integration/test_migration.py -v`
Expected: PASS. (The `pg_engine` session fixture runs `alembic upgrade head`, which now applies `0003`.)

- [ ] **Step 9: Lint, format, commit**

```bash
uv run ruff check --fix
uv run ruff format
git add app/models/job.py alembic/versions/0003_add_priority.py tests/unit/test_job_model.py tests/integration/test_migration.py
git commit -m "feat: add priority column to jobs table (migration 0003)"
```

---

### Task 3: Repository — priority on create, filter on list, batched lookup

**Files:**
- Modify: `app/repository.py`
- Test: `tests/integration/test_repository.py`

**Interfaces:**
- Consumes: `Job.priority` (Task 2), `JobPriority` (Task 1).
- Produces:
  - `create_job(session, job_type, payload, *, status=JobStatus.pending, scheduled_at=None, priority=JobPriority.normal) -> Job`.
  - `list_jobs(session, *, status=None, job_type=None, priority=None, limit=50, cursor=None) -> tuple[list[Job], str | None]`.
  - `get_priorities(session, job_ids: list[UUID]) -> dict[UUID, JobPriority]`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/integration/test_repository.py`:

```python
def test_create_job_defaults_priority_normal(db_session):
    from app.schemas.enums import JobPriority

    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert job.priority is JobPriority.normal


def test_create_job_sets_priority(db_session):
    from app.schemas.enums import JobPriority

    job = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    db_session.refresh(job)
    assert job.priority is JobPriority.high


def test_list_filters_by_priority(db_session):
    from app.schemas.enums import JobPriority

    repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    highs, _ = repo.list_jobs(db_session, priority=JobPriority.high)
    assert len(highs) == 1
    assert highs[0].priority is JobPriority.high


def test_get_priorities_batched(db_session):
    from app.schemas.enums import JobPriority

    a = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        priority=JobPriority.high,
    )
    b = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})

    result = repo.get_priorities(db_session, [a.id, b.id])
    assert result == {a.id: JobPriority.high, b.id: JobPriority.normal}


def test_get_priorities_empty_returns_empty(db_session):
    assert repo.get_priorities(db_session, []) == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_repository.py -v -k "priority or priorities"`
Expected: FAIL — `create_job` has no `priority` kwarg / `list_jobs` has no `priority` kwarg / `get_priorities` undefined.

- [ ] **Step 3: Implement the repository changes**

In `app/repository.py`, extend the enums import:

```python
from app.schemas.enums import JobPriority, JobStatus, JobType
```

Update `create_job` signature and body:

```python
def create_job(
    session: Session,
    job_type: JobType,
    payload: dict,
    *,
    status: JobStatus = JobStatus.pending,
    scheduled_at: datetime | None = None,
    priority: JobPriority = JobPriority.normal,
) -> Job:
    job = Job(
        type=job_type,
        payload=payload,
        status=status,
        scheduled_at=scheduled_at,
        priority=priority,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job
```

Add the `priority` filter to `list_jobs` (new parameter + one `where`):

```python
def list_jobs(
    session: Session,
    *,
    status: JobStatus | None = None,
    job_type: JobType | None = None,
    priority: JobPriority | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> tuple[list[Job], str | None]:
    stmt = select(Job)
    if status is not None:
        stmt = stmt.where(Job.status == status)
    if job_type is not None:
        stmt = stmt.where(Job.type == job_type)
    if priority is not None:
        stmt = stmt.where(Job.priority == priority)
    if cursor is not None:
        c_created, c_id = decode_cursor(cursor)
        stmt = stmt.where(tuple_(Job.created_at, Job.id) < (c_created, c_id))
    stmt = stmt.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit + 1)

    rows = list(session.execute(stmt).scalars())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = encode_cursor(last.created_at, last.id)
    return rows, next_cursor
```

Add `get_priorities` (place it near `list_unsynced`):

```python
def get_priorities(
    session: Session, job_ids: list[UUID]
) -> dict[UUID, JobPriority]:
    if not job_ids:
        return {}
    rows = session.execute(
        select(Job.id, Job.priority).where(Job.id.in_(job_ids))
    ).all()
    return {row.id: row.priority for row in rows}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: PASS (new tests + all pre-existing repository tests).

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix
uv run ruff format
git add app/repository.py tests/integration/test_repository.py
git commit -m "feat: repository priority create/filter and batched get_priorities"
```

---

### Task 4: Consumer `read_priority` strict-drain primitive

**Files:**
- Modify: `app/queue/consumer.py`
- Test: `tests/integration/test_queue.py`

**Interfaces:**
- Consumes: nothing new (takes an ordered list of stream names, e.g. `settings.ordered_streams`).
- Produces: `read_priority(client, streams: list[str], group: str, consumer: str, block_ms: int) -> list[tuple[str, str, dict]]` — returns `(stream, message_id, fields)` tuples, highest-priority stream first. `ack`, `ensure_group`, `enqueue`, `read_one` unchanged (`read_one` removed later in Task 8).

- [ ] **Step 1: Write the failing tests**

Append to `tests/integration/test_queue.py` (the file already imports `ack`, `ensure_group`, `read_one` and defines `STREAM`/`GROUP`; add `read_priority` to the import line):

```python
from app.queue.consumer import ack, ensure_group, read_one, read_priority
```

Then append:

```python
PRIO_STREAMS = ["s:high", "s:normal", "s:low"]


def _ensure_prio_groups(redis_client):
    for s in PRIO_STREAMS:
        ensure_group(redis_client, s, GROUP)


def test_read_priority_prefers_higher_stream(redis_client):
    _ensure_prio_groups(redis_client)
    enqueue(redis_client, "s:low", "low-1")
    enqueue(redis_client, "s:high", "high-1")

    batch = read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=100)

    assert [(s, f["job_id"]) for s, _mid, f in batch] == [("s:high", "high-1")]
    # Strict: the low stream was never read, so its entry is still undelivered.
    assert redis_client.xlen("s:low") == 1
    assert redis_client.xpending("s:low", GROUP)["pending"] == 0


def test_read_priority_falls_through_to_lower(redis_client):
    _ensure_prio_groups(redis_client)
    enqueue(redis_client, "s:low", "low-1")

    batch = read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=100)

    assert [(s, f["job_id"]) for s, _mid, f in batch] == [("s:low", "low-1")]


def test_read_priority_empty_returns_empty_list(redis_client):
    _ensure_prio_groups(redis_client)
    assert read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=50) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_queue.py -v -k read_priority`
Expected: FAIL — `ImportError: cannot import name 'read_priority'`.

- [ ] **Step 3: Implement `read_priority`**

In `app/queue/consumer.py`, add below `read_one`:

```python
def _flatten(resp) -> list[tuple[str, str, dict]]:
    out: list[tuple[str, str, dict]] = []
    if not resp:
        return out
    for stream, messages in resp:
        for message_id, fields in messages:
            out.append((stream, message_id, fields))
    return out


def read_priority(
    client: redis.Redis,
    streams: list[str],
    group: str,
    consumer: str,
    block_ms: int,
) -> list[tuple[str, str, dict]]:
    # Strict priority: probe each stream highest-first, non-blocking. The first
    # non-empty stream's messages are returned immediately, so a higher-priority
    # backlog is fully drained before a lower stream is even checked.
    for stream in streams:
        msgs = _flatten(
            client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=1,
                block=None,
            )
        )
        if msgs:
            return msgs
    # All empty: block across every stream at once (priority order preserved in
    # the reply). Reached only when idle, so it cannot reorder a real backlog.
    return _flatten(
        client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">" for stream in streams},
            count=1,
            block=block_ms,
        )
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/integration/test_queue.py -v`
Expected: PASS (new `read_priority` tests + the existing `read_one`/ack tests).

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix
uv run ruff format
git add app/queue/consumer.py tests/integration/test_queue.py
git commit -m "feat: add read_priority strict-drain consumer primitive"
```

---

### Task 5: Worker — strict-drain loop across three streams

**Files:**
- Modify: `app/worker/runner.py`
- Test: `tests/integration/test_worker.py`

**Interfaces:**
- Consumes: `read_priority` (Task 4), `ack`/`ensure_group` (existing), `Settings.ordered_streams`/`priority_streams` (Task 1), `repo.create_job(priority=...)` (Task 3), `process_job` (unchanged).
- Produces: `run_forever` now ensures the group on all three streams and processes messages priority-first; `process_job(session, job_id)` signature unchanged.

- [ ] **Step 1: Update the failing integration tests**

In `tests/integration/test_worker.py`, replace `test_run_forever_processes_one_then_stops` with a priority-stream version and add a strict-drain test. Replace lines 52–77 (the whole `test_run_forever_processes_one_then_stops` function) with:

```python
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

    run_forever(test_settings, stop=stop)

    with factory() as s:
        refreshed = repo.get_job(s, job.id)
    assert refreshed.status is JobStatus.completed


def test_run_forever_drains_high_before_low(test_settings, redis_client, pg_engine):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.schemas.enums import JobPriority
    from app.worker.runner import run_forever

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        low = repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            priority=JobPriority.low,
        )
        high = repo.create_job(
            s,
            JobType.email,
            {"to": "a@b.com", "subject": "Hi"},
            priority=JobPriority.high,
        )
    # Low is enqueued first, but high must be processed first.
    enqueue(redis_client, test_settings.stream_low, str(low.id))
    enqueue(redis_client, test_settings.stream_high, str(high.id))

    calls = {"n": 0}

    def stop() -> bool:
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    run_forever(test_settings, stop=stop)  # exactly one processing pass

    with factory() as s:
        assert repo.get_job(s, high.id).status is JobStatus.completed
        assert repo.get_job(s, low.id).status is JobStatus.pending  # untouched
    # High was acked on its own stream; low is still queued.
    assert (
        redis_client.xpending(test_settings.stream_high, test_settings.consumer_group)[
            "pending"
        ]
        == 0
    )
    assert redis_client.xlen(test_settings.stream_low) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_worker.py -v -k run_forever`
Expected: FAIL — `run_forever` still reads the single `jobs_stream`; the high/low test fails (low not left pending, or high never read) and the ordered-streams enqueue lands in an unread stream.

- [ ] **Step 3: Rewrite the worker loop**

Replace `app/worker/runner.py` entirely with:

```python
import signal
from collections.abc import Callable
from uuid import UUID

import structlog
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.redis import create_redis_client
from app.jobs.registry import run_handler
from app.queue.consumer import CONSUMER_NAME, ack, ensure_group, read_priority
from app.schemas.payloads import validate_payload

log = structlog.get_logger("worker")


def process_job(session: Session, job_id: UUID) -> None:
    if not repo.claim_job(session, job_id):
        log.info("job.skipped", reason="not_pending")
        return

    job = repo.get_job(session, job_id)
    try:
        payload = validate_payload(job.type, job.payload)
        result = run_handler(job.type, payload)
    except Exception as exc:  # noqa: BLE001 — any handler/validation error fails the job
        repo.fail_job(
            session, job_id, {"type": type(exc).__name__, "message": str(exc)}
        )
        log.info("job.failed", error_type=type(exc).__name__)
        return

    repo.complete_job(session, job_id, result)
    log.info("job.completed")


def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> None:
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
                    process_job(session, job_id)
                # Ack on the message's own stream, after the PG commit (at-least-once).
                ack(client, stream, settings.consumer_group, message_id)

    log.info("worker.stopped")
    client.close()
    engine.dispose()
```

- [ ] **Step 4: Run the worker tests to verify they pass**

Run: `uv run pytest tests/integration/test_worker.py -v`
Expected: PASS (both `run_forever` tests + the unchanged `process_job` tests).

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix
uv run ruff format
git add app/worker/runner.py tests/integration/test_worker.py
git commit -m "feat: worker drains three priority streams high-first"
```

---

### Task 6: Ticker + reconciler route by priority (batched)

**Files:**
- Modify: `app/queue/delayed.py`
- Modify: `app/ticker/runner.py`
- Test: `tests/integration/test_delayed.py`, `tests/integration/test_ticker.py`

**Interfaces:**
- Consumes: `repo.get_priorities` (Task 3), `Settings.stream_for_priority`/`ordered_streams` (Task 1), `enqueue`/`ensure_group` (existing).
- Produces: `delayed.promote(client, zset: str, routed: list[tuple[str, str]], all_ids: list[str]) -> None` (new signature: `routed` = `(stream, job_id)` pairs to XADD; `all_ids` = ids to ZREM). `promote_due`/`reconcile_orphans` route each job to its priority stream.

- [ ] **Step 1: Update the failing `delayed.promote` tests**

In `tests/integration/test_delayed.py`, replace `test_promote_moves_ids_to_stream_and_removes` and `test_promote_empty_is_noop` (lines 23–33) with the new signature:

```python
def test_promote_moves_ids_to_stream_and_removes(redis_client):
    delayed.schedule(redis_client, ZSET, "a", time.time() - 1)
    delayed.schedule(redis_client, ZSET, "b", time.time() - 1)
    delayed.promote(
        redis_client,
        ZSET,
        [(STREAM, "a"), (STREAM, "b")],
        ["a", "b"],
    )
    assert redis_client.xlen(STREAM) == 2
    assert redis_client.zcard(ZSET) == 0


def test_promote_routes_to_multiple_streams(redis_client):
    delayed.schedule(redis_client, ZSET, "a", time.time() - 1)
    delayed.schedule(redis_client, ZSET, "b", time.time() - 1)
    delayed.promote(
        redis_client,
        ZSET,
        [("s:high", "a"), ("s:low", "b")],
        ["a", "b"],
    )
    assert redis_client.xlen("s:high") == 1
    assert redis_client.xlen("s:low") == 1
    assert redis_client.zcard(ZSET) == 0


def test_promote_empty_is_noop(redis_client):
    delayed.promote(redis_client, ZSET, [], [])
    assert redis_client.xlen(STREAM) == 0
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/integration/test_delayed.py -v`
Expected: FAIL — `promote()` still takes `(client, stream, zset, job_ids)`; new calls raise `TypeError`.

- [ ] **Step 3: Change `delayed.promote` signature**

Replace the `promote` function in `app/queue/delayed.py` with:

```python
def promote(
    client: redis.Redis,
    zset: str,
    routed: list[tuple[str, str]],
    all_ids: list[str],
) -> None:
    if not all_ids:
        return
    # XADD every routed id to its target stream BEFORE removing any from the ZSET,
    # so a crash mid-promotion leaves the ids in the ZSET to be retried next tick.
    # Duplicate stream entries are absorbed by the worker's idempotent claim guard.
    pipe = client.pipeline(transaction=False)
    for stream, job_id in routed:
        pipe.xadd(stream, {"job_id": job_id})
    pipe.execute()
    client.zrem(zset, *all_ids)
```

- [ ] **Step 4: Run the delayed tests to verify they pass**

Run: `uv run pytest tests/integration/test_delayed.py -v`
Expected: PASS.

- [ ] **Step 5: Update the failing ticker tests**

In `tests/integration/test_ticker.py`, make these edits (all `jobs_stream` references become the routed priority stream — default-priority jobs route to `stream_normal`):

- `test_promote_due_moves_mature_job` — change the stream assertion:
  ```python
      assert redis_client.xlen(test_settings.stream_normal) == 1
  ```
- `test_promote_due_skips_future` — change the stream assertion:
  ```python
      assert redis_client.xlen(test_settings.stream_normal) == 0
  ```
- `test_reconcile_reenqueues_pending_orphan` — change the stream assertion:
  ```python
      assert redis_client.xlen(test_settings.stream_normal) == 1
  ```
- `test_reconcile_noop_when_synced` — change the stream assertion:
  ```python
      assert redis_client.xlen(test_settings.stream_normal) == 0
  ```
- `test_run_forever_promotes_then_stops` — change the final assertion:
  ```python
      assert redis_client.xlen(settings.stream_normal) == 1
  ```
- `test_end_to_end_scheduled_job_completes` and `test_duplicate_promotion_second_claim_is_noop` — replace the single `ensure_group(redis_client, test_settings.jobs_stream, test_settings.consumer_group)` line in each with:
  ```python
      for stream in test_settings.ordered_streams:
          ensure_group(redis_client, stream, test_settings.consumer_group)
  ```

Append a new routing test:

```python
def test_promote_due_routes_by_priority(db_session, redis_client, test_settings):
    from app.schemas.enums import JobPriority

    when = datetime(2020, 1, 1, tzinfo=timezone.utc)
    high = repo.create_job(
        db_session,
        JobType.email,
        {"to": "a@b.com", "subject": "Hi"},
        status=JobStatus.scheduled,
        scheduled_at=when,
        priority=JobPriority.high,
    )
    delayed.schedule(
        redis_client, test_settings.delayed_zset, str(high.id), when.timestamp()
    )

    promoted = promote_due(db_session, redis_client, test_settings)

    assert promoted == 1
    assert redis_client.xlen(test_settings.stream_high) == 1
    assert redis_client.xlen(test_settings.stream_normal) == 0
    db_session.refresh(high)
    assert high.status is JobStatus.pending
```

- [ ] **Step 6: Run the ticker tests to verify they fail**

Run: `uv run pytest tests/integration/test_ticker.py -v`
Expected: FAIL — `promote_due`/`reconcile_orphans` still call the old `delayed.promote`/`enqueue(jobs_stream)`, so routed streams are empty and `run_forever`'s `ensure_group(settings.jobs_stream, ...)` no longer matches.

- [ ] **Step 7: Route promotion and reconciliation by priority**

In `app/ticker/runner.py`, update the imports to add `repo` usage of `get_priorities` (repo is already imported) and keep `enqueue`/`ensure_group`. Replace `promote_due` with:

```python
def promote_due(session: Session, client: redis.Redis, settings: Settings) -> int:
    now_epoch = time.time()
    ids = delayed.due_job_ids(
        client, settings.delayed_zset, now_epoch, settings.ticker_batch_size
    )
    if not ids:
        return 0
    priorities = repo.get_priorities(session, [UUID(i) for i in ids])
    routed: list[tuple[str, str]] = []
    for i in ids:
        prio = priorities.get(UUID(i))
        if prio is None:
            # No scheduled row (cancelled/deleted): drop it — do not enqueue —
            # but it is still ZREM'd below so it can't re-accumulate.
            continue
        routed.append((settings.stream_for_priority(prio), i))
    delayed.promote(client, settings.delayed_zset, routed, ids)
    repo.promote_scheduled_to_pending(session, [UUID(i) for i in ids])
    log.info("ticker.promoted", enqueued=len(routed), pulled=len(ids))
    return len(ids)
```

In `reconcile_orphans`, change the `else` (pending) branch from `enqueue(client, settings.jobs_stream, str(job.id))` to route by priority:

```python
            else:
                enqueue(
                    client,
                    settings.stream_for_priority(job.priority),
                    str(job.id),
                )
```

In `run_forever`, replace the single `ensure_group(...)` call:

```python
    for stream in settings.ordered_streams:
        ensure_group(client, stream, settings.consumer_group)
```

and update the start log line to not reference `jobs_stream`:

```python
    log.info("ticker.started", zset=settings.delayed_zset, streams=settings.ordered_streams)
```

- [ ] **Step 8: Run the ticker + delayed tests to verify they pass**

Run: `uv run pytest tests/integration/test_ticker.py tests/integration/test_delayed.py -v`
Expected: PASS.

- [ ] **Step 9: Lint, format, commit**

```bash
uv run ruff check --fix
uv run ruff format
git add app/queue/delayed.py app/ticker/runner.py tests/integration/test_delayed.py tests/integration/test_ticker.py
git commit -m "feat: ticker and reconciler route jobs to priority streams (batched lookup)"
```

---

### Task 7: API — accept, echo, and filter by priority

**Files:**
- Modify: `app/schemas/api.py`
- Modify: `app/api/routes.py`
- Modify: `app/main.py`
- Test: `tests/integration/test_api.py`

**Interfaces:**
- Consumes: `JobPriority` (Task 1), `repo.create_job(priority=...)`/`repo.list_jobs(priority=...)` (Task 3), `Settings.stream_for_priority`/`ordered_streams` (Task 1).
- Produces: `POST /jobs` accepts optional `priority` (default `normal`) and routes to the matching stream; `JobAccepted`/`JobOut` include `priority`; `GET /jobs?priority=` filters; API lifespan ensures the group on all three streams.

- [ ] **Step 1: Update the failing API tests**

In `tests/integration/test_api.py`:

- `test_submit_creates_job_and_enqueues` — replace the tail (lines 19–22, the stream lookup) with a priority-aware version:
  ```python
      # Default priority is normal, echoed and routed to the normal stream.
      assert body["priority"] == "normal"
      redis_client = client.app.state.redis
      settings = client.app.state.settings
      assert redis_client.xlen(settings.stream_normal) == 1
  ```
- `test_submit_scheduled_job_parks_in_zset` — change the last stream assertion:
  ```python
      assert redis_client.xlen(settings.stream_normal) == 0
  ```
- `test_submit_past_scheduled_at_runs_immediately` — change the stream assertion:
  ```python
      assert redis_client.xlen(settings.stream_normal) == 1
  ```

Append two new tests:

```python
def test_submit_high_priority_routes_to_high_stream(client):
    client.app.state.redis.flushdb()
    resp = client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.com", "subject": "Hi"},
            "priority": "high",
        },
    )
    assert resp.status_code == 202
    assert resp.json()["priority"] == "high"

    redis_client = client.app.state.redis
    settings = client.app.state.settings
    assert redis_client.xlen(settings.stream_high) == 1
    assert redis_client.xlen(settings.stream_normal) == 0


def test_list_filters_by_priority(client):
    client.post(
        "/jobs",
        json={
            "type": "email",
            "payload": {"to": "a@b.com", "subject": "Hi"},
            "priority": "high",
        },
    )
    client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    resp = client.get("/jobs", params={"priority": "high"})
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["priority"] == "high"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/integration/test_api.py -v`
Expected: FAIL — response has no `priority` field; submit ignores `priority` and enqueues to `jobs_stream`; `?priority=` is not a recognized query param.

- [ ] **Step 3: Add `priority` to the API schemas**

In `app/schemas/api.py`, extend the enums import and add the fields:

```python
from app.schemas.enums import JobPriority, JobStatus, JobType
```

`JobSubmission` gains a default field:

```python
class JobSubmission(BaseModel):
    type: JobType
    payload: dict
    priority: JobPriority = JobPriority.normal
    scheduled_at: datetime | None = None
    ...
```

`JobAccepted` gains `priority`:

```python
class JobAccepted(BaseModel):
    id: UUID
    type: JobType
    status: JobStatus
    priority: JobPriority
    created_at: datetime
    scheduled_at: datetime | None = None
```

`JobOut` gains `priority`:

```python
class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: JobType
    status: JobStatus
    priority: JobPriority
    payload: dict
    result: dict | None
    error: dict | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    scheduled_at: datetime | None
```

- [ ] **Step 4: Route by priority + expose the filter in `routes.py`**

In `app/api/routes.py`, extend the enums import:

```python
from app.schemas.enums import JobPriority, JobStatus, JobType
```

In `submit_job`, pass `priority` on both paths and route the immediate enqueue. Replace the branch body (the `if scheduled_at ... else ...` block and the `return`) with:

```python
    if scheduled_at is not None and scheduled_at > datetime.now(timezone.utc):
        # Scheduled path: persist SCHEDULED + park in the delayed ZSET.
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            status=JobStatus.scheduled,
            scheduled_at=scheduled_at,
            priority=submission.priority,
        )
        schedule(client, settings.delayed_zset, str(job.id), scheduled_at.timestamp())
    else:
        # Immediate path: persist PENDING + push to the priority stream.
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            priority=submission.priority,
        )
        enqueue(
            client, settings.stream_for_priority(submission.priority), str(job.id)
        )
    # Handoff confirmed → flip the flag so the reconciler ignores this row.
    repo.mark_synced(session, job.id)
    return JobAccepted(
        id=job.id,
        type=job.type,
        status=job.status,
        priority=job.priority,
        created_at=job.created_at,
        scheduled_at=job.scheduled_at,
    )
```

In `list_jobs`, add the `priority` query param and pass it through:

```python
@router.get("/jobs", response_model=JobList)
def list_jobs(
    session: Session = Depends(get_db),
    status: JobStatus | None = Query(default=None),
    type: JobType | None = Query(default=None),
    priority: JobPriority | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None),
) -> JobList:
    try:
        jobs, next_cursor = repo.list_jobs(
            session,
            status=status,
            job_type=type,
            priority=priority,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc
    return JobList(
        items=[JobOut.model_validate(j) for j in jobs], next_cursor=next_cursor
    )
```

- [ ] **Step 5: Ensure the group on all three streams at API startup**

In `app/main.py`, replace the single `ensure_group(...)` line inside `lifespan` with:

```python
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        for stream in settings.ordered_streams:
            ensure_group(redis_client, stream, settings.consumer_group)
        yield
        redis_client.close()
        engine.dispose()
```

- [ ] **Step 6: Run the API tests to verify they pass**

Run: `uv run pytest tests/integration/test_api.py -v`
Expected: PASS (updated + new priority tests; `test_list_filters_by_type` and the rest still pass).

- [ ] **Step 7: Lint, format, commit**

```bash
uv run ruff check --fix
uv run ruff format
git add app/schemas/api.py app/api/routes.py app/main.py tests/integration/test_api.py
git commit -m "feat: API accepts, echoes, and filters jobs by priority"
```

---

### Task 8: Retire the single-stream setting and `read_one`

**Files:**
- Modify: `app/core/config.py`
- Modify: `app/queue/consumer.py`
- Test: `tests/unit/test_config.py`, `tests/integration/test_queue.py`

**Interfaces:**
- Consumes: `read_priority`, `ack`, `ensure_group`, `enqueue` (all existing).
- Produces: `Settings` no longer has `jobs_stream`; `read_one` is removed. No behavior change — only dead-code/config cleanup.

- [ ] **Step 1: Confirm nothing still references the old names**

Run: `uv run python -c "import subprocess,sys; sys.exit(0)"` then search:
Run: `git grep -n "jobs_stream" -- app tests` and `git grep -n "read_one" -- app tests`
Expected: `jobs_stream` appears only in `app/core/config.py` (the field) and `tests/unit/test_config.py` (the assertion); `read_one` appears only in `app/queue/consumer.py` (definition) and `tests/integration/test_queue.py`. If anything else appears, that file was missed in an earlier task — fix it before continuing.

- [ ] **Step 2: Update the config test**

In `tests/unit/test_config.py`, edit `test_settings_defaults` to drop the `jobs_stream` assertion (keep the others):

```python
def test_settings_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.consumer_group == "workers"
    assert s.block_ms == 5000
    assert s.log_level == "INFO"
```

- [ ] **Step 3: Rewrite the `read_one` tests to use `read_priority`**

In `tests/integration/test_queue.py`, update the import line to drop `read_one`:

```python
from app.queue.consumer import ack, ensure_group, enqueue, read_priority
```

Replace `test_enqueue_read_ack_cycle` and `test_read_returns_none_when_empty` (the two `read_one` tests) with a single ack-cycle test built on `read_priority`:

```python
def test_enqueue_read_ack_cycle(redis_client):
    ensure_group(redis_client, STREAM, GROUP)
    enqueue(redis_client, STREAM, "job-123")

    batch = read_priority(redis_client, [STREAM], GROUP, "consumer-a", block_ms=1000)
    assert len(batch) == 1
    stream, message_id, fields = batch[0]
    assert fields["job_id"] == "job-123"

    # Still pending until acked.
    assert redis_client.xpending(STREAM, GROUP)["pending"] == 1

    ack(redis_client, stream, GROUP, message_id)
    assert redis_client.xpending(STREAM, GROUP)["pending"] == 0
```

(The pre-existing `from app.queue.producer import enqueue` line at the top of the file is now redundant with the consolidated import — remove whichever duplicate remains so `enqueue` is imported exactly once.)

- [ ] **Step 4: Remove `jobs_stream` and `read_one`**

In `app/core/config.py`, delete the line:

```python
    jobs_stream: str = "jobs:stream"
```

In `app/queue/consumer.py`, delete the entire `read_one` function (the `def read_one(...)` block), keeping `ensure_group`, `read_priority`, `_flatten`, and `ack`.

- [ ] **Step 5: Run the touched tests to verify they pass**

Run: `uv run pytest tests/unit/test_config.py tests/integration/test_queue.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite + lint**

Run: `uv run pytest`
Expected: PASS (entire suite).
Run: `uv run ruff check`
Expected: clean (no errors).

- [ ] **Step 7: Commit**

```bash
uv run ruff format
git add app/core/config.py app/queue/consumer.py tests/unit/test_config.py tests/integration/test_queue.py
git commit -m "refactor: remove single-stream jobs_stream setting and unused read_one"
```

---

## Self-Review

**1. Spec coverage** (each spec section → task):
- §4 Data model & migration → Task 2 (column, `job_priority` enum, `ix_jobs_priority`, `server_default='normal'` backfill).
- §5 Enum & stream mapping → Task 1 (`JobPriority`, three stream settings, `priority_streams`, `ordered_streams`, `stream_for_priority`); group-on-all-streams appears in Tasks 5/6/7.
- §6 Producer & routing → Task 7 (immediate route) + Task 6 (promotion/reconcile route); `enqueue` stays generic.
- §7 Worker strict drain → Tasks 4 (`read_priority`) + 5 (loop, list processing, per-stream ack).
- §8 Ticker & reconciler batched routing → Task 6 (`get_priorities` in Task 3; grouping + `delayed.promote` new signature; reconciler branch).
- §9 API → Task 7 (submission default, echo in `JobAccepted`/`JobOut`, `?priority=` filter).
- §10 Failure modes → covered: missing/cancelled due id dropped (Task 6 `promote_due`); default normal (Tasks 3/7); XADD-before-ZREM (Task 6 `delayed.promote`); ack-on-own-stream (Task 5); cutover of `jobs_stream` (Task 8).
- §11 Docker Compose → no change required (services unchanged); no task needed.
- §12 Testing plan → routing (Tasks 6/7), strict drain (Task 5), ack correctness (Tasks 4/5), ticker batched promote (Task 6), reconciler (Task 6), API echo/filter (Task 7), existing-test migration (Tasks 5/6/7/8).

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to". Every code step shows full code; every test step shows the assertions.

**3. Type consistency:** `create_job(..., priority: JobPriority = JobPriority.normal)`, `list_jobs(..., priority=None)`, `get_priorities(session, list[UUID]) -> dict[UUID, JobPriority]`, `read_priority(client, streams: list[str], group, consumer, block_ms) -> list[tuple[str,str,dict]]`, `delayed.promote(client, zset, routed: list[tuple[str,str]], all_ids: list[str])`, and `Settings.stream_for_priority`/`ordered_streams`/`priority_streams` are used identically across the API, worker, ticker, and consumer tasks.
