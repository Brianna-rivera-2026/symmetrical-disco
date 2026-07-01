# Cancellation, Batch Jobs, Progress & Idempotency — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add job cancellation (pre-execution + cooperative in-flight), a `batch` job type with a live progress percentage, and idempotency-key deduplication on submission — on top of the existing FastAPI + Postgres + Redis-Streams job system.

**Architecture:** Postgres stays the single source of truth. Cancellation of a `pending`/`scheduled` job is a guarded status flip; a `processing` job is cancelled *cooperatively* — the endpoint sets a `cancel_requested_at` flag, a batch handler polls it through an injected `JobContext` (throttled, change-only DB writes that also carry the progress %), stops early by raising `JobCancelled`, and the worker performs the guarded `processing → cancelled`. Idempotency compares a canonical SHA-256 of the *submitted* payload, never the DB-normalized JSONB.

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy 2.0, Alembic, Pydantic v2, `redis-py` (Streams), PostgreSQL 16, structlog, pytest + testcontainers, `uv`.

**Source spec:** `docs/superpowers/specs/2026-07-01-cancellation-batch-progress-design.md`

## Global Constraints

- **Package/tooling:** always `uv` — `uv run pytest`, `uv add <pkg>`. Never raw `pip`/`venv`/`poetry`.
- **Lint/format:** `uv run ruff check --fix` and `uv run ruff format` before each commit.
- **Logging:** structured logging only (`structlog`); never `print`.
- **Core safety invariant (do not break):** every transition *out of* `status='processing'` is a single guarded `UPDATE … WHERE id=:id AND status='processing'`; any Redis side-effect runs only if `rowcount == 1`.
- **Handler purity:** email/webhook/report handlers stay pure (sleep + return a dict, no DB). Only the batch handler touches the DB, and only through the injected `JobContext`.
- **At-least-once + commit-then-handoff + `is_synced_to_redis` reconciliation** must be preserved.
- **Migrations** run via Alembic against PostgreSQL 16 (testcontainers). Enum name is `job_type`.

---

## File Structure

**Create:**
- `alembic/versions/0005_add_cancellation_batch_progress.py` — migration: 4 columns + `batch` enum value + partial unique index.
- `app/worker/context.py` — `JobContext` Protocol + `PgJobContext` (throttled, change-only progress/cancel polling).
- `app/idempotency.py` — `canonical_hash(job_type, payload)`.
- `tests/unit/test_context.py`, `tests/unit/test_batch_handler.py`, `tests/unit/test_idempotency.py`, `tests/unit/test_batch_payload.py`
- `tests/integration/test_cancel.py`, `tests/integration/test_batch.py`, `tests/integration/test_idempotency_api.py`

**Modify:**
- `app/schemas/enums.py` — add `JobType.batch`.
- `app/models/job.py` — 4 mapped columns + partial-unique `__table_args__`.
- `app/core/config.py` — `cancel_poll_interval_s`, `batch_timeout_safety_factor`.
- `app/schemas/payloads.py` — `MAX_BATCH_ITEMS`, `BatchPayload`, union member, `validate_payload(..., context=…)`.
- `app/jobs/handlers.py` — `JobCancelled`, `handle_batch`, `_process_item`; `(payload, ctx)` on the three existing handlers.
- `app/jobs/registry.py` — `run_handler(job_type, payload, ctx)` + `batch` entry.
- `app/repository.py` — `init_progress`, `cancel_job`, `cancel_pending_or_scheduled`, `request_cancel`, `get_by_idempotency_key`; `complete_job(progress=…)`; `create_job(idempotency_key=…, idempotency_hash=…)`.
- `app/worker/runner.py` — `process_job` gains `session_factory`, builds ctx, `init_progress` for batch, `JobCancelled` branch, `complete_job(progress=…)`.
- `app/schemas/api.py` — `JobSubmission.idempotency_key`; `JobOut.progress` + `JobOut.cancel_requested_at`.
- `app/api/routes.py` — idempotency in `POST /jobs`; new `POST /jobs/{id}/cancel`.
- `tests/unit/test_handlers.py`, `tests/integration/test_worker.py` — migrate to `(payload, ctx)` / `process_job(..., session_factory)`.

---

## Task 1: Data model — enum, columns, migration

**Files:**
- Modify: `app/schemas/enums.py`
- Modify: `app/models/job.py`
- Create: `alembic/versions/0005_add_cancellation_batch_progress.py`
- Test: `tests/integration/test_migration.py` (append)

**Interfaces:**
- Produces: `JobType.batch`; `Job.progress: int | None`, `Job.cancel_requested_at: datetime | None`, `Job.idempotency_key: str | None`, `Job.idempotency_hash: str | None`; a partial unique index `uq_jobs_idempotency_key`.

- [ ] **Step 1: Write the failing test** — append to `tests/integration/test_migration.py`:

```python
import pytest
from sqlalchemy.exc import IntegrityError

from app import repository as repo
from app.models.job import Job
from app.schemas.enums import JobType


def test_batch_type_and_new_columns_persist(db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    db_session.refresh(job)
    assert job.progress is None
    assert job.cancel_requested_at is None
    assert job.idempotency_key is None
    assert job.idempotency_hash is None


def test_idempotency_key_partial_unique(db_session):
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}, idempotency_key="k1"))
    db_session.commit()
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}, idempotency_key="k1"))
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_null_idempotency_keys_do_not_collide(db_session):
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}))
    db_session.add(Job(type=JobType.email, payload={"to": "a", "subject": "b"}))
    db_session.commit()  # two NULL keys must not violate the partial index
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/integration/test_migration.py -k "batch_type or partial_unique or do_not_collide" -v`
Expected: FAIL — `JobType` has no `batch`, columns don't exist.

- [ ] **Step 3a: Add the enum value** — `app/schemas/enums.py`:

```python
class JobType(str, Enum):
    email = "email"
    webhook = "webhook"
    report = "report"
    batch = "batch"
```

- [ ] **Step 3b: Add the columns + index** to `app/models/job.py`. Add imports and columns:

```python
from sqlalchemy import Index, Text
```

Inside `class Job(Base)`, after the existing `is_synced_to_redis` column add:

```python
    progress: Mapped[int | None] = mapped_column(sa.Integer, nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_hash: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index(
            "uq_jobs_idempotency_key",
            "idempotency_key",
            unique=True,
            postgresql_where=sa.text("idempotency_key IS NOT NULL"),
        ),
    )
```

(No change to `__init__` — the extra nullable columns default to `None` when unset.)

- [ ] **Step 3c: Write the migration** — `alembic/versions/0005_add_cancellation_batch_progress.py`:

```python
"""add cancellation, batch, progress & idempotency

Revision ID: 0005
Revises: 0004
Create Date: 2026-07-01
"""

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("progress", sa.Integer(), nullable=True))
    op.add_column(
        "jobs",
        sa.Column("cancel_requested_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column("jobs", sa.Column("idempotency_key", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("idempotency_hash", sa.Text(), nullable=True))
    # PostgreSQL 12+ allows ADD VALUE in a transaction as long as the value is not
    # USED in the same transaction (it isn't here). IF NOT EXISTS keeps re-runs safe.
    op.execute("ALTER TYPE job_type ADD VALUE IF NOT EXISTS 'batch'")
    op.create_index(
        "uq_jobs_idempotency_key",
        "jobs",
        ["idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_jobs_idempotency_key", table_name="jobs")
    op.drop_column("jobs", "idempotency_hash")
    op.drop_column("jobs", "idempotency_key")
    op.drop_column("jobs", "cancel_requested_at")
    op.drop_column("jobs", "progress")
    # Enum value 'batch' is intentionally left in place (Postgres cannot drop an
    # enum value cleanly; it is inert if unused).
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/integration/test_migration.py -v`
Expected: PASS (the `pg_engine` fixture runs `alembic upgrade head`, applying 0005).

- [ ] **Step 5: Lint & commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/schemas/enums.py app/models/job.py alembic/versions/0005_add_cancellation_batch_progress.py tests/integration/test_migration.py
git commit -m "feat: add batch type, progress/cancel/idempotency columns (migration 0005)"
```

---

## Task 2: Config + `BatchPayload` with the timeout-budget validator

**Files:**
- Modify: `app/core/config.py`
- Modify: `app/schemas/payloads.py`
- Test: `tests/unit/test_batch_payload.py`

**Interfaces:**
- Consumes: `JobType.batch` (Task 1).
- Produces: `Settings.cancel_poll_interval_s: float`, `Settings.batch_timeout_safety_factor: float`; `payloads.MAX_BATCH_ITEMS: int`; `BatchPayload(type, items: list[dict], item_delay_ms: int)`; `validate_payload(job_type, raw, *, context: dict | None = None)`.

- [ ] **Step 1: Write the failing test** — `tests/unit/test_batch_payload.py`:

```python
import pytest
from pydantic import ValidationError

from app.schemas.enums import JobType
from app.schemas.payloads import MAX_BATCH_ITEMS, BatchPayload, validate_payload


def test_batch_payload_defaults():
    p = validate_payload(JobType.batch, {"items": [{"x": 1}]})
    assert isinstance(p, BatchPayload)
    assert p.item_delay_ms == 50
    assert p.items == [{"x": 1}]


def test_batch_rejects_too_many_items():
    with pytest.raises(ValidationError):
        BatchPayload(items=[{} for _ in range(MAX_BATCH_ITEMS + 1)])


def test_budget_validator_rejects_doomed_batch():
    ctx = {"handler_timeout_s": 45.0, "safety_factor": 0.8}
    # 1000 items * 50ms = 50s >= 45*0.8 = 36s -> reject
    with pytest.raises(ValueError):
        validate_payload(JobType.batch, {"items": [{} for _ in range(1000)], "item_delay_ms": 50}, context=ctx)


def test_budget_validator_accepts_under_budget():
    ctx = {"handler_timeout_s": 45.0, "safety_factor": 0.8}
    p = validate_payload(JobType.batch, {"items": [{} for _ in range(10)], "item_delay_ms": 50}, context=ctx)
    assert len(p.items) == 10


def test_budget_check_skipped_without_context():
    # 1000 * 50ms would be doomed, but no context -> only the size cap applies
    p = validate_payload(JobType.batch, {"items": [{} for _ in range(100)], "item_delay_ms": 50})
    assert len(p.items) == 100
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_batch_payload.py -v`
Expected: FAIL — `MAX_BATCH_ITEMS`/`BatchPayload` don't exist; `validate_payload` has no `context` kwarg.

- [ ] **Step 3a: Add config settings** — in `app/core/config.py`, add to `Settings` (next to the other job settings):

```python
    cancel_poll_interval_s: float = 2.0
    batch_timeout_safety_factor: float = 0.8
```

- [ ] **Step 3b: Add the payload** — rewrite `app/schemas/payloads.py`:

```python
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter, ValidationInfo, model_validator

from app.schemas.enums import JobType

MAX_BATCH_ITEMS = 500


class EmailPayload(BaseModel):
    type: Literal[JobType.email] = JobType.email
    to: str
    subject: str
    body: str | None = None


class WebhookPayload(BaseModel):
    type: Literal[JobType.webhook] = JobType.webhook
    url: str
    method: str = "POST"


class ReportPayload(BaseModel):
    type: Literal[JobType.report] = JobType.report
    report_type: str
    params: dict | None = None


class BatchPayload(BaseModel):
    type: Literal[JobType.batch] = JobType.batch
    items: list[dict] = Field(max_length=MAX_BATCH_ITEMS)
    item_delay_ms: int = Field(default=50, ge=0)

    @model_validator(mode="after")
    def _fits_timeout_budget(self, info: ValidationInfo) -> "BatchPayload":
        context = info.context or {}
        budget = context.get("handler_timeout_s")
        if budget is not None:
            est_s = (len(self.items) * self.item_delay_ms) / 1000
            if est_s >= budget * context.get("safety_factor", 0.8):
                raise ValueError(
                    "estimated batch duration exceeds worker timeout budget"
                )
        return self


JobPayload = Annotated[
    Union[EmailPayload, WebhookPayload, ReportPayload, BatchPayload],
    Field(discriminator="type"),
]

_ADAPTER: TypeAdapter = TypeAdapter(JobPayload)


def validate_payload(
    job_type: JobType | str, raw: dict, *, context: dict | None = None
) -> EmailPayload | WebhookPayload | ReportPayload | BatchPayload:
    job_type = JobType(job_type)  # raises ValueError on unknown type
    return _ADAPTER.validate_python({**raw, "type": job_type.value}, context=context)
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/test_batch_payload.py -v`
Expected: PASS.

- [ ] **Step 5: Lint & commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/core/config.py app/schemas/payloads.py tests/unit/test_batch_payload.py
git commit -m "feat: BatchPayload with size cap and timeout-budget validator"
```

---

## Task 3: Batch handler, `JobCancelled`, uniform `(payload, ctx)` dispatch

**Files:**
- Modify: `app/jobs/handlers.py`
- Modify: `app/jobs/registry.py`
- Test: `tests/unit/test_batch_handler.py`
- Test (migrate): `tests/unit/test_handlers.py`

**Interfaces:**
- Consumes: `BatchPayload` (Task 2).
- Produces: `JobCancelled(Exception)` with `.summary: dict`; `handle_batch(payload, ctx) -> dict`; `run_handler(job_type, payload, ctx) -> dict`; all handlers now take `(payload, ctx)`. A `ctx` must provide `.cancelled() -> bool` and `.set_progress(int) -> None` (Task 5 supplies the real one; tests use a fake).

- [ ] **Step 1: Write the failing test** — `tests/unit/test_batch_handler.py`:

```python
import pytest

from app.jobs import handlers
from app.jobs.handlers import JobCancelled, handle_batch
from app.schemas.payloads import BatchPayload


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


class _FakeCtx:
    def __init__(self, cancel_after=None):
        self.cancel_after = cancel_after
        self.calls = 0
        self.progress = []

    def cancelled(self) -> bool:
        hit = self.cancel_after is not None and self.calls >= self.cancel_after
        self.calls += 1
        return hit

    def set_progress(self, pct: int) -> None:
        self.progress.append(pct)


def test_batch_summarizes_success_and_failure():
    payload = BatchPayload(items=[{"ok": 1}, {"fail": True}, {"ok": 2}])
    out = handle_batch(payload, _FakeCtx())
    assert out["total"] == 3
    assert out["succeeded"] == 2
    assert out["failed"] == 1
    assert out["errors"][0]["index"] == 1
    assert "progress" not in out  # progress lives on the row, not in the summary


def test_batch_completes_even_if_all_fail():
    payload = BatchPayload(items=[{"fail": True}, {"fail": True}])
    out = handle_batch(payload, _FakeCtx())
    assert out == {
        "total": 2,
        "succeeded": 0,
        "failed": 2,
        "errors": [{"index": 0, "error": "item failed"}, {"index": 1, "error": "item failed"}],
    }


def test_batch_raises_jobcancelled_with_partial_summary():
    ctx = _FakeCtx(cancel_after=2)  # first two items processed, then cancel
    payload = BatchPayload(items=[{}, {}, {}, {}])
    with pytest.raises(JobCancelled) as exc:
        handle_batch(payload, ctx)
    assert exc.value.summary["succeeded"] == 2
    assert exc.value.summary["total"] == 4


def test_batch_reports_progress_per_item():
    ctx = _FakeCtx()
    handle_batch(BatchPayload(items=[{}, {}, {}, {}]), ctx)
    assert ctx.progress == [25, 50, 75, 100]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_batch_handler.py -v`
Expected: FAIL — `JobCancelled`/`handle_batch` don't exist.

- [ ] **Step 3a: Add exception, batch handler, and `(payload, ctx)`** — edit `app/jobs/handlers.py`:

```python
import random
import time
import uuid

from app.schemas.payloads import (
    BatchPayload,
    EmailPayload,
    ReportPayload,
    WebhookPayload,
)


class WebhookFailedError(Exception):
    """Raised when the simulated webhook call fails."""


class JobCancelled(Exception):
    """Raised by a cooperative handler when cancellation was requested.

    Carries the partial summary so the worker can persist it on the cancelled row.
    """

    def __init__(self, summary: dict) -> None:
        super().__init__("job cancelled")
        self.summary = summary


def handle_email(payload: EmailPayload, ctx) -> dict:
    time.sleep(random.uniform(1, 3))
    return {"message_id": f"msg_{uuid.uuid4().hex[:12]}"}


def handle_webhook(payload: WebhookPayload, ctx) -> dict:
    time.sleep(random.uniform(1, 2))
    if random.random() < 0.8:
        raise WebhookFailedError(f"webhook call to {payload.url} failed")
    return {"status": 200}


def handle_report(payload: ReportPayload, ctx) -> dict:
    time.sleep(random.uniform(3, 5))
    return {"file_url": f"https://reports.local/{uuid.uuid4().hex[:12]}.pdf"}


def _process_item(item: dict, delay_ms: int) -> None:
    time.sleep(delay_ms / 1000)
    if item.get("fail"):
        raise RuntimeError(item.get("error", "item failed"))


def handle_batch(payload: BatchPayload, ctx) -> dict:
    n = len(payload.items)
    summary = {"total": n, "succeeded": 0, "failed": 0, "errors": []}
    for i, item in enumerate(payload.items):
        if ctx.cancelled():
            raise JobCancelled(summary)
        try:
            _process_item(item, payload.item_delay_ms)
            summary["succeeded"] += 1
        except Exception as exc:  # noqa: BLE001 — per-item, collected not raised
            summary["failed"] += 1
            summary["errors"].append({"index": i, "error": str(exc)})
        ctx.set_progress(int((i + 1) / n * 100) if n else 100)
    return summary
```

- [ ] **Step 3b: Update the registry** — `app/jobs/registry.py`:

```python
from collections.abc import Callable

from app.jobs.handlers import handle_batch, handle_email, handle_report, handle_webhook
from app.schemas.enums import JobType

HANDLERS: dict[JobType, Callable[[object, object], dict]] = {
    JobType.email: handle_email,
    JobType.webhook: handle_webhook,
    JobType.report: handle_report,
    JobType.batch: handle_batch,
}


def run_handler(job_type: JobType, payload, ctx) -> dict:
    return HANDLERS[job_type](payload, ctx)
```

- [ ] **Step 3c: Migrate existing handler unit tests** — in `tests/unit/test_handlers.py`, pass `None` for `ctx`:

```python
def test_email_returns_message_id():
    out = handlers.handle_email(EmailPayload(to="a@b.com", subject="Hi"), None)
    assert "message_id" in out


def test_report_returns_file_url():
    out = handlers.handle_report(ReportPayload(report_type="sales"), None)
    assert out["file_url"].startswith("https://")


def test_webhook_success_branch(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.5)
    out = handlers.handle_webhook(WebhookPayload(url="https://x.test"), None)
    assert out == {"status": 200}


def test_webhook_failure_branch(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)
    with pytest.raises(handlers.WebhookFailedError):
        handlers.handle_webhook(WebhookPayload(url="https://x.test"), None)


def test_run_handler_dispatches_by_type():
    out = run_handler(JobType.email, EmailPayload(to="a@b.com", subject="Hi"), None)
    assert "message_id" in out
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/test_batch_handler.py tests/unit/test_handlers.py -v`
Expected: PASS.

- [ ] **Step 5: Lint & commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/jobs/handlers.py app/jobs/registry.py tests/unit/test_batch_handler.py tests/unit/test_handlers.py
git commit -m "feat: batch handler + JobCancelled + uniform (payload, ctx) dispatch"
```

---

## Task 4: Repository helpers (guarded transitions + idempotency lookup)

**Files:**
- Modify: `app/repository.py`
- Test: `tests/integration/test_repository.py` (append)

**Interfaces:**
- Consumes: `Job` columns (Task 1).
- Produces: `init_progress(session, job_id) -> bool`; `cancel_job(session, job_id, summary) -> bool`; `cancel_pending_or_scheduled(session, job_id) -> bool`; `request_cancel(session, job_id) -> bool`; `get_by_idempotency_key(session, key) -> Job | None`; `complete_job(session, job_id, result, progress=None) -> bool`; `create_job(..., idempotency_key=None, idempotency_hash=None) -> Job`.

- [ ] **Step 1: Write the failing test** — append to `tests/integration/test_repository.py`:

```python
from app.schemas.enums import JobStatus, JobType


def test_init_progress_only_when_processing(db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    assert repo.init_progress(db_session, job.id) is False  # pending, not processing
    repo.claim_job(db_session, job.id)
    assert repo.init_progress(db_session, job.id) is True
    db_session.refresh(job)
    assert job.progress == 0


def test_complete_job_sets_progress_for_batch(db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    repo.claim_job(db_session, job.id)
    assert repo.complete_job(db_session, job.id, {"total": 0}, progress=100) is True
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.progress == 100


def test_complete_job_leaves_progress_null_for_non_batch(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    repo.claim_job(db_session, job.id)
    repo.complete_job(db_session, job.id, {"message_id": "m"})
    db_session.refresh(job)
    assert job.progress is None


def test_cancel_pending_or_scheduled_guard(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    assert repo.cancel_pending_or_scheduled(db_session, job.id) is True
    db_session.refresh(job)
    assert job.status is JobStatus.cancelled
    # second call is a no-op (already cancelled)
    assert repo.cancel_pending_or_scheduled(db_session, job.id) is False


def test_cancel_pending_or_scheduled_rejects_processing(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    repo.claim_job(db_session, job.id)
    assert repo.cancel_pending_or_scheduled(db_session, job.id) is False


def test_request_cancel_only_when_processing(db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    assert repo.request_cancel(db_session, job.id) is False  # pending
    repo.claim_job(db_session, job.id)
    assert repo.request_cancel(db_session, job.id) is True
    db_session.refresh(job)
    assert job.cancel_requested_at is not None


def test_cancel_job_guarded_terminal(db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    repo.claim_job(db_session, job.id)
    assert repo.cancel_job(db_session, job.id, {"total": 3, "succeeded": 1}) is True
    db_session.refresh(job)
    assert job.status is JobStatus.cancelled
    assert job.result == {"total": 3, "succeeded": 1}


def test_get_by_idempotency_key(db_session):
    key = "abc"
    assert repo.get_by_idempotency_key(db_session, key) is None
    job = repo.create_job(
        db_session, JobType.email, {"to": "a", "subject": "b"},
        idempotency_key=key, idempotency_hash="h1",
    )
    found = repo.get_by_idempotency_key(db_session, key)
    assert found is not None and found.id == job.id
    assert found.idempotency_hash == "h1"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/integration/test_repository.py -k "progress or cancel or idempotency" -v`
Expected: FAIL — helpers / new params don't exist.

- [ ] **Step 3a: Extend `create_job` and `complete_job`** in `app/repository.py`:

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
    idempotency_key: str | None = None,
    idempotency_hash: str | None = None,
) -> Job:
    job = Job(
        type=job_type,
        payload=payload,
        status=status,
        scheduled_at=scheduled_at,
        priority=priority,
        max_attempts=max_attempts,
        idempotency_key=idempotency_key,
        idempotency_hash=idempotency_hash,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return job
```

Update `complete_job` to accept an optional terminal progress:

```python
def complete_job(
    session: Session, job_id: UUID, result: dict, progress: int | None = None
) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(
            status=JobStatus.completed,
            result=result,
            completed_at=_now(),
            attempts=Job.attempts + 1,
            progress=progress if progress is not None else Job.progress,
        )
    )
    session.commit()
    return res.rowcount == 1
```

- [ ] **Step 3b: Add the new helpers** (append to `app/repository.py`):

```python
def init_progress(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(progress=0)
    )
    session.commit()
    return res.rowcount == 1


def cancel_pending_or_scheduled(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
        )
        .values(status=JobStatus.cancelled, completed_at=_now())
    )
    session.commit()
    return res.rowcount == 1


def request_cancel(session: Session, job_id: UUID) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(cancel_requested_at=_now())
    )
    session.commit()
    return res.rowcount == 1


def cancel_job(session: Session, job_id: UUID, summary: dict) -> bool:
    res = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.processing)
        .values(status=JobStatus.cancelled, result=summary, completed_at=_now())
    )
    session.commit()
    return res.rowcount == 1


def get_by_idempotency_key(session: Session, key: str) -> Job | None:
    return session.execute(
        select(Job).where(Job.idempotency_key == key)
    ).scalar_one_or_none()
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: PASS.

- [ ] **Step 5: Lint & commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/repository.py tests/integration/test_repository.py
git commit -m "feat: repo helpers for progress, cooperative cancel, idempotency lookup"
```

---

## Task 5: `PgJobContext` — throttled, change-only cancel/progress polling

**Files:**
- Create: `app/worker/context.py`
- Test: `tests/unit/test_context.py`
- Test: `tests/integration/test_batch.py` (create; one context integration test here)

**Interfaces:**
- Consumes: `repo.claim_job`, `repo.request_cancel` (Task 4).
- Produces: `JobContext` Protocol; `PgJobContext(job_id, session_factory, poll_interval_s, now=time.monotonic)` with `set_progress(int)`, `cancelled() -> bool`, and overridable `_write(pct) -> tuple[bool, bool]` / `_read() -> tuple[bool, bool]` (each returns `(alive, cancel_requested)`).

- [ ] **Step 1: Write the failing unit test** — `tests/unit/test_context.py`:

```python
from app.worker.context import PgJobContext


class _FakeCtx(PgJobContext):
    def __init__(self, interval, now_fn):
        super().__init__("jid", None, interval, now=now_fn)
        self.writes = []
        self.reads = 0
        self.alive = True
        self.flag = False

    def _write(self, pct):
        self.writes.append(pct)
        return (self.alive, self.flag)

    def _read(self):
        self.reads += 1
        return (self.alive, self.flag)


def test_first_call_writes_pending_progress():
    ctx = _FakeCtx(2.0, lambda: 0.0)
    ctx.set_progress(10)
    assert ctx.cancelled() is False
    assert ctx.writes == [10]


def test_skips_poll_within_interval():
    t = [0.0]
    ctx = _FakeCtx(2.0, lambda: t[0])
    ctx.set_progress(10)
    ctx.cancelled()          # polls at t=0, writes [10]
    ctx.set_progress(20)
    t[0] = 1.0               # < interval -> no poll
    ctx.cancelled()
    assert ctx.writes == [10]


def test_change_only_reads_when_pct_unchanged():
    ctx = _FakeCtx(0.0, lambda: 0.0)  # always past interval
    ctx.set_progress(10)
    ctx.cancelled()          # writes [10]
    ctx.cancelled()          # pct unchanged -> read, no write
    assert ctx.writes == [10]
    assert ctx.reads == 1


def test_cancel_flag_is_detected():
    ctx = _FakeCtx(0.0, lambda: 0.0)
    ctx.flag = True
    ctx.set_progress(5)
    assert ctx.cancelled() is True


def test_row_gone_stops_the_loop():
    ctx = _FakeCtx(0.0, lambda: 0.0)
    ctx.alive = False
    ctx.set_progress(5)
    assert ctx.cancelled() is True
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_context.py -v`
Expected: FAIL — module `app.worker.context` does not exist.

- [ ] **Step 3: Implement the context** — `app/worker/context.py`:

```python
import time
from collections.abc import Callable
from typing import Protocol
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session


class JobContext(Protocol):
    def set_progress(self, pct: int) -> None: ...
    def cancelled(self) -> bool: ...


class PgJobContext:
    """Postgres-backed cancel/progress channel for a running handler.

    Polls at most once per `poll_interval_s` (cached between ticks). On a poll it
    writes progress only when the percent changed (a coalesced UPDATE … RETURNING
    that also reads the cancel flag and confirms the row is still 'processing');
    otherwise it does a flag-only SELECT. Opens its own short-lived session per
    poll because it runs inside the worker's timeout thread.
    """

    def __init__(
        self,
        job_id: UUID | str,
        session_factory: Callable[[], Session] | None,
        poll_interval_s: float,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._job_id = job_id
        self._sf = session_factory
        self._interval = poll_interval_s
        self._now = now
        self._pending_pct = 0
        self._last_written_pct: int | None = None
        self._last_poll: float | None = None
        self._cached = False

    def set_progress(self, pct: int) -> None:
        self._pending_pct = pct

    def cancelled(self) -> bool:
        now = self._now()
        if self._last_poll is None or now - self._last_poll >= self._interval:
            if self._pending_pct != self._last_written_pct:
                alive, requested = self._write(self._pending_pct)
                self._last_written_pct = self._pending_pct
            else:
                alive, requested = self._read()
            self._cached = requested or not alive
            self._last_poll = now
        return self._cached

    def _write(self, pct: int) -> tuple[bool, bool]:
        with self._sf() as session:
            row = session.execute(
                text(
                    "UPDATE jobs SET progress = :pct "
                    "WHERE id = :id AND status = 'processing' "
                    "RETURNING cancel_requested_at"
                ),
                {"pct": pct, "id": self._job_id},
            ).first()
            session.commit()
        if row is None:
            return (False, False)  # no longer processing
        return (True, row[0] is not None)

    def _read(self) -> tuple[bool, bool]:
        with self._sf() as session:
            row = session.execute(
                text("SELECT cancel_requested_at, status FROM jobs WHERE id = :id"),
                {"id": self._job_id},
            ).first()
        if row is None or row[1] != "processing":
            return (False, False)
        return (True, row[0] is not None)
```

- [ ] **Step 4: Run the unit test to verify it passes**

Run: `uv run pytest tests/unit/test_context.py -v`
Expected: PASS.

- [ ] **Step 5: Write the integration test** — create `tests/integration/test_batch.py`:

```python
from app import repository as repo
from app.core.db import make_session_factory
from app.schemas.enums import JobType
from app.worker.context import PgJobContext


def test_pg_context_writes_progress_and_reads_cancel(db_session, pg_engine):
    sf = make_session_factory(pg_engine)
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    repo.claim_job(db_session, job.id)  # -> processing

    ctx = PgJobContext(job.id, sf, poll_interval_s=0.0)
    ctx.set_progress(42)
    assert ctx.cancelled() is False
    db_session.refresh(job)
    assert job.progress == 42

    repo.request_cancel(db_session, job.id)
    ctx.set_progress(43)  # change so the next poll writes + re-reads the flag
    assert ctx.cancelled() is True
```

- [ ] **Step 6: Run to verify it passes**

Run: `uv run pytest tests/integration/test_batch.py -v`
Expected: PASS.

- [ ] **Step 7: Lint & commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/worker/context.py tests/unit/test_context.py tests/integration/test_batch.py
git commit -m "feat: PgJobContext throttled change-only cancel/progress polling"
```

---

## Task 6: Worker wiring — inject context, init progress, cancel branch

**Files:**
- Modify: `app/worker/runner.py`
- Test: `tests/integration/test_batch.py` (append), `tests/integration/test_worker.py` (migrate call site)

**Interfaces:**
- Consumes: `run_handler(job_type, payload, ctx)` (Task 3), `PgJobContext` (Task 5), `repo.init_progress`/`cancel_job`/`complete_job(progress=…)` (Task 4).
- Produces: `process_job(session, client, settings, job_id, session_factory=None) -> Outcome` (label `"cancelled"` added).

- [ ] **Step 1: Write the failing test** — append to `tests/integration/test_batch.py`:

```python
from datetime import datetime, timezone

from sqlalchemy import update

from app.models.job import Job
from app.schemas.enums import JobStatus
from app.worker.runner import process_job


def test_batch_completes_with_progress_100(db_session, redis_client, test_settings, pg_engine):
    sf = make_session_factory(pg_engine)
    job = repo.create_job(db_session, JobType.batch, {"items": [{}, {}], "item_delay_ms": 0})
    outcome = process_job(db_session, redis_client, test_settings, job.id, sf)
    assert outcome.label == "completed"
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.progress == 100
    assert job.result["total"] == 2


def test_tiny_batch_reaches_progress_100_without_polling(db_session, redis_client, test_settings, pg_engine):
    # poll interval is huge, so the poll loop never writes progress; completion must
    # still land it on 100 (type-driven, not IS NOT NULL).
    settings = test_settings.model_copy(update={"cancel_poll_interval_s": 999.0})
    sf = make_session_factory(pg_engine)
    job = repo.create_job(db_session, JobType.batch, {"items": [{}], "item_delay_ms": 0})
    process_job(db_session, redis_client, settings, job.id, sf)
    db_session.refresh(job)
    assert job.progress == 100


def test_batch_cooperative_cancel(db_session, redis_client, test_settings, pg_engine):
    sf = make_session_factory(pg_engine)
    job = repo.create_job(db_session, JobType.batch, {"items": [{}, {}, {}], "item_delay_ms": 0})
    # Simulate "cancel arrived just as processing begins": stamp the flag on the row
    # so the handler's first poll sees it. (claim_job leaves cancel_requested_at intact.)
    db_session.execute(
        update(Job).where(Job.id == job.id).values(cancel_requested_at=datetime.now(timezone.utc))
    )
    db_session.commit()
    outcome = process_job(db_session, redis_client, test_settings, job.id, sf)
    assert outcome.label == "cancelled"
    db_session.refresh(job)
    assert job.status is JobStatus.cancelled
    assert job.result == {"total": 3, "succeeded": 0, "failed": 0, "errors": []}
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/integration/test_batch.py -k "completes_with_progress or tiny_batch or cooperative" -v`
Expected: FAIL — `process_job` doesn't accept `session_factory`; no cancel branch.

- [ ] **Step 3: Rewrite `process_job` and its call site** in `app/worker/runner.py`. Add imports at top:

```python
from app.jobs.handlers import JobCancelled
from app.schemas.enums import JobType
from app.worker.context import PgJobContext
```

Replace `process_job` with:

```python
def process_job(
    session: Session,
    client: redis.Redis,
    settings: Settings,
    job_id: UUID,
    session_factory: Callable[[], Session] | None = None,
) -> Outcome:
    if not repo.claim_job(session, job_id):
        log.info("job.skipped", reason="not_claimable")
        return Outcome(ack=True, recycle=False, label="skipped")

    job = repo.get_job(session, job_id)
    is_batch = job.type == JobType.batch
    if is_batch:
        repo.init_progress(session, job.id)
    ctx = PgJobContext(job.id, session_factory, settings.cancel_poll_interval_s)
    try:
        payload = validate_payload(job.type, job.payload)
        result = run_with_timeout(
            lambda: run_handler(job.type, payload, ctx), settings.job_handler_timeout_s
        )
    except JobCancelled as cancelled:
        won = repo.cancel_job(session, job.id, cancelled.summary)
        log.info("job.cancelled", won=won)
        return Outcome(ack=won, recycle=False, label="cancelled")
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

    won = repo.complete_job(
        session, job.id, result, progress=100 if is_batch else None
    )
    if not won:
        log.critical("job.complete_lost_to_reaper")
        return Outcome(ack=False, recycle=False, label="lost")
    log.info("job.completed")
    return Outcome(ack=True, recycle=False, label="completed")
```

In `run_forever`, update the call site (inside the `for stream, message_id, fields in batch:` loop):

```python
                with session_factory() as session:
                    outcome = process_job(
                        session, client, settings, job_id, session_factory
                    )
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `uv run pytest tests/integration/test_batch.py -v`
Expected: PASS.

- [ ] **Step 5: Verify the existing worker tests still pass** (they call `process_job` with 4 args; `session_factory` defaults to `None`, and non-batch handlers never touch it):

Run: `uv run pytest tests/integration/test_worker.py -v`
Expected: PASS (no changes required).

- [ ] **Step 6: Lint & commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/worker/runner.py tests/integration/test_batch.py
git commit -m "feat: worker injects JobContext, inits progress, honors JobCancelled"
```

---

## Task 7: Idempotency on `POST /jobs`

**Files:**
- Create: `app/idempotency.py`
- Modify: `app/schemas/api.py`
- Modify: `app/api/routes.py`
- Test: `tests/unit/test_idempotency.py`, `tests/integration/test_idempotency_api.py`

**Interfaces:**
- Consumes: `repo.get_by_idempotency_key`, `repo.create_job(idempotency_key=…, idempotency_hash=…)` (Task 4).
- Produces: `canonical_hash(job_type, payload) -> str`; `JobSubmission.idempotency_key: str | None`; `POST /jobs` returns `200` on replay, `409` on key-reuse mismatch, `202` on create.

- [ ] **Step 1: Write the failing unit test** — `tests/unit/test_idempotency.py`:

```python
from app.idempotency import canonical_hash
from app.schemas.enums import JobType


def test_hash_is_stable_across_key_order():
    a = canonical_hash(JobType.email, {"to": "x", "subject": "y"})
    b = canonical_hash(JobType.email, {"subject": "y", "to": "x"})
    assert a == b


def test_hash_differs_for_different_payloads():
    a = canonical_hash(JobType.email, {"to": "x", "subject": "y"})
    b = canonical_hash(JobType.email, {"to": "z", "subject": "y"})
    assert a != b


def test_hash_differs_for_different_type():
    a = canonical_hash(JobType.email, {"k": 1})
    b = canonical_hash(JobType.webhook, {"k": 1})
    assert a != b
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_idempotency.py -v`
Expected: FAIL — `app.idempotency` doesn't exist.

- [ ] **Step 3a: Implement `canonical_hash`** — `app/idempotency.py`:

```python
import hashlib
import json

from app.schemas.enums import JobType


def canonical_hash(job_type: JobType, payload: dict) -> str:
    """Stable SHA-256 of a *submitted* payload (pre-JSONB), for idempotency reuse
    detection. `default=str` is defensive only — `payload` is a JSON-parsed dict
    and already contains no non-serializable objects."""
    blob = json.dumps(
        {"type": job_type.value, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(blob).hexdigest()
```

- [ ] **Step 3b: Add the field** — in `app/schemas/api.py`, add to `JobSubmission`:

```python
    idempotency_key: str | None = None
```

- [ ] **Step 3c: Rewrite the submit route** — in `app/api/routes.py`. Update imports:

```python
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy.exc import IntegrityError

from app.idempotency import canonical_hash
```

Replace `submit_job` and add two module-level helpers above it:

```python
def _create_and_handoff(
    session, client, settings, submission, key, req_hash
):
    scheduled_at = submission.scheduled_at
    if scheduled_at is not None and scheduled_at > datetime.now(timezone.utc):
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            status=JobStatus.scheduled,
            scheduled_at=scheduled_at,
            priority=submission.priority,
            max_attempts=settings.max_attempts,
            idempotency_key=key,
            idempotency_hash=req_hash,
        )
        schedule(client, settings.delayed_zset, str(job.id), scheduled_at.timestamp())
    else:
        job = repo.create_job(
            session,
            submission.type,
            submission.payload,
            priority=submission.priority,
            max_attempts=settings.max_attempts,
            idempotency_key=key,
            idempotency_hash=req_hash,
        )
        enqueue(client, settings.stream_for_priority(submission.priority), str(job.id))
    repo.mark_synced(session, job.id)
    return job


def _accepted(job) -> JobAccepted:
    return JobAccepted(
        id=job.id,
        type=job.type,
        status=job.status,
        priority=job.priority,
        created_at=job.created_at,
        scheduled_at=job.scheduled_at,
    )


def _replay_or_conflict(existing, req_hash, response) -> JobAccepted:
    if existing is not None and existing.idempotency_hash == req_hash:
        response.status_code = 200
        return _accepted(existing)
    raise HTTPException(
        status_code=409, detail="idempotency key reused with a different payload"
    )


@router.post("/jobs", response_model=JobAccepted, status_code=202)
def submit_job(
    submission: JobSubmission,
    request: Request,
    response: Response,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobAccepted:
    settings = request.app.state.settings
    try:
        validate_payload(
            submission.type,
            submission.payload,
            context={
                "handler_timeout_s": settings.job_handler_timeout_s,
                "safety_factor": settings.batch_timeout_safety_factor,
            },
        )
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    key = submission.idempotency_key
    if key is None:
        job = _create_and_handoff(session, client, settings, submission, None, None)
        return _accepted(job)

    req_hash = canonical_hash(submission.type, submission.payload)
    existing = repo.get_by_idempotency_key(session, key)
    if existing is not None:
        return _replay_or_conflict(existing, req_hash, response)
    try:
        job = _create_and_handoff(session, client, settings, submission, key, req_hash)
    except IntegrityError:
        session.rollback()
        existing = repo.get_by_idempotency_key(session, key)
        return _replay_or_conflict(existing, req_hash, response)
    return _accepted(job)
```

- [ ] **Step 4: Run the unit test to verify it passes**

Run: `uv run pytest tests/unit/test_idempotency.py -v`
Expected: PASS.

- [ ] **Step 5: Write the integration test** — `tests/integration/test_idempotency_api.py`:

```python
from app import repository as repo
from app.api import routes
from app.idempotency import canonical_hash
from app.schemas.enums import JobType

_EMAIL = {"to": "a@b.com", "subject": "Hi"}


def test_replay_returns_200_and_same_job(client):
    body = {"type": "email", "payload": _EMAIL, "idempotency_key": "k1"}
    r1 = client.post("/jobs", json=body)
    assert r1.status_code == 202
    r2 = client.post("/jobs", json=body)
    assert r2.status_code == 200
    assert r2.json()["id"] == r1.json()["id"]
    settings = client.app.state.settings
    assert client.app.state.redis.xlen(settings.stream_normal) == 1  # only one enqueue


def test_same_key_different_payload_returns_409(client):
    client.post("/jobs", json={"type": "email", "payload": _EMAIL, "idempotency_key": "k2"})
    resp = client.post(
        "/jobs",
        json={"type": "email", "payload": {"to": "z@z.com", "subject": "Diff"}, "idempotency_key": "k2"},
    )
    assert resp.status_code == 409


def test_no_key_always_creates(client):
    r1 = client.post("/jobs", json={"type": "email", "payload": _EMAIL})
    r2 = client.post("/jobs", json={"type": "email", "payload": _EMAIL})
    assert r1.json()["id"] != r2.json()["id"]


def test_race_path_different_payload_conflicts(client, db_session, monkeypatch):
    # Pre-create the "winner" row with key "race".
    repo.create_job(
        db_session, JobType.email, _EMAIL,
        idempotency_key="race", idempotency_hash=canonical_hash(JobType.email, _EMAIL),
    )
    # Force the first lookup to miss so the route takes the create -> IntegrityError
    # -> rollback -> re-lookup branch (the concurrent-race path).
    real = repo.get_by_idempotency_key
    calls = {"n": 0}

    def flaky(session, key):
        calls["n"] += 1
        return None if calls["n"] == 1 else real(session, key)

    monkeypatch.setattr(routes.repo, "get_by_idempotency_key", flaky)
    resp = client.post(
        "/jobs",
        json={"type": "email", "payload": {"to": "z@z.com", "subject": "Diff"}, "idempotency_key": "race"},
    )
    assert resp.status_code == 409  # loser does NOT receive the winner's job
```

- [ ] **Step 6: Run to verify it passes**

Run: `uv run pytest tests/integration/test_idempotency_api.py -v`
Expected: PASS.

- [ ] **Step 7: Lint & commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/idempotency.py app/schemas/api.py app/api/routes.py tests/unit/test_idempotency.py tests/integration/test_idempotency_api.py
git commit -m "feat: idempotency-key replay/conflict on POST /jobs"
```

---

## Task 8: Cancel endpoint + `JobOut` fields

**Files:**
- Modify: `app/schemas/api.py`
- Modify: `app/api/routes.py`
- Test: `tests/integration/test_cancel.py`

**Interfaces:**
- Consumes: `repo.cancel_pending_or_scheduled`, `repo.request_cancel`, `repo.get_job` (Task 4).
- Produces: `JobOut.progress: int | None`, `JobOut.cancel_requested_at: datetime | None`; `POST /jobs/{job_id}/cancel -> JobOut` with `200`/`202`/`409`/`404`.

- [ ] **Step 1: Write the failing test** — `tests/integration/test_cancel.py`:

```python
import uuid
from datetime import datetime, timedelta, timezone

from app import repository as repo
from app.schemas.enums import JobStatus, JobType

_EMAIL = {"to": "a@b.com", "subject": "Hi"}


def test_cancel_pending_returns_200(client):
    jid = client.post("/jobs", json={"type": "email", "payload": _EMAIL}).json()["id"]
    resp = client.post(f"/jobs/{jid}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


def test_cancel_scheduled_zrems_from_delayed(client):
    when = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    jid = client.post(
        "/jobs", json={"type": "email", "payload": _EMAIL, "scheduled_at": when}
    ).json()["id"]
    settings = client.app.state.settings
    assert client.app.state.redis.zcard(settings.delayed_zset) == 1
    resp = client.post(f"/jobs/{jid}/cancel")
    assert resp.status_code == 200
    assert client.app.state.redis.zcard(settings.delayed_zset) == 0


def test_cancel_processing_returns_202_and_sets_flag(client, db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    repo.claim_job(db_session, job.id)  # -> processing
    resp = client.post(f"/jobs/{job.id}/cancel")
    assert resp.status_code == 202
    db_session.refresh(job)
    assert job.cancel_requested_at is not None
    assert job.status is JobStatus.processing  # endpoint does NOT flip status


def test_cancel_completed_returns_409(client, db_session):
    job = repo.create_job(db_session, JobType.email, _EMAIL)
    repo.claim_job(db_session, job.id)
    repo.complete_job(db_session, job.id, {"message_id": "m"})
    resp = client.post(f"/jobs/{job.id}/cancel")
    assert resp.status_code == 409


def test_cancel_already_cancelled_is_idempotent_200(client):
    jid = client.post("/jobs", json={"type": "email", "payload": _EMAIL}).json()["id"]
    client.post(f"/jobs/{jid}/cancel")
    resp = client.post(f"/jobs/{jid}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


def test_cancel_unknown_returns_404(client):
    assert client.post(f"/jobs/{uuid.uuid4()}/cancel").status_code == 404


def test_job_out_exposes_progress_field(client, db_session):
    job = repo.create_job(db_session, JobType.batch, {"items": []})
    got = client.get(f"/jobs/{job.id}").json()
    assert "progress" in got
    assert got["progress"] is None
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/integration/test_cancel.py -v`
Expected: FAIL — no cancel route; `JobOut` lacks `progress`.

- [ ] **Step 3a: Add `JobOut` fields** — in `app/schemas/api.py`, add to `JobOut` (after `scheduled_at`):

```python
    progress: int | None = None
    cancel_requested_at: datetime | None = None
```

- [ ] **Step 3b: Add the cancel route** — in `app/api/routes.py`, add after `retry_job`:

```python
@router.post("/jobs/{job_id}/cancel", response_model=JobOut)
def cancel_job_route(
    job_id: UUID,
    request: Request,
    response: Response,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobOut:
    settings = request.app.state.settings
    for _ in range(3):  # bounded re-resolve for the legal processing<->pending flap
        if repo.cancel_pending_or_scheduled(session, job_id):
            client.zrem(settings.delayed_zset, str(job_id))  # harmless no-op if absent
            return JobOut.model_validate(repo.get_job(session, job_id))
        if repo.request_cancel(session, job_id):
            response.status_code = 202
            return JobOut.model_validate(repo.get_job(session, job_id))
        job = repo.get_job(session, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status is JobStatus.cancelled:
            return JobOut.model_validate(job)  # idempotent 200
        if job.status in (JobStatus.completed, JobStatus.failed):
            raise HTTPException(
                status_code=409, detail="job cannot be cancelled in its current state"
            )
        # pending/scheduled/processing again → loop and retry the guarded transitions
    raise HTTPException(status_code=409, detail="job state is changing; retry")
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/integration/test_cancel.py -v`
Expected: PASS.

- [ ] **Step 5: Full suite + lint**

Run: `uv run pytest`
Expected: PASS (all tasks green).

```bash
uv run ruff check --fix && uv run ruff format
git add app/schemas/api.py app/api/routes.py tests/integration/test_cancel.py
git commit -m "feat: POST /jobs/{id}/cancel + progress in JobOut"
```

---

## Self-Review

**Spec coverage:**
- Cancellation (pre-execution + cooperative in-flight): Tasks 4, 6, 8. ✅
- Batch job type + summary (collect & continue, always complete): Tasks 2, 3, 6. ✅
- Progress % (0-init, type-driven 100, change-only writes, throttle): Tasks 4, 5, 6. ✅
- Idempotency (replay 200 / conflict 409 / race path / canonical hash): Tasks 4, 7. ✅
- Migration + enum + partial unique index: Task 1. ✅
- Config (`cancel_poll_interval_s`, `batch_timeout_safety_factor`, `MAX_BATCH_ITEMS`): Task 2. ✅
- Gateway `422` for doomed batches: Task 2. ✅
- Guarded-transition invariant preserved (init/cancel/complete all `WHERE status='processing'`): Task 4. ✅
- Edge cases from spec §11 with tests: cancel of every state (Task 8), tiny batch → 100 (Task 6), all-items-fail → completed summary (Task 3), race-path 409 (Task 7). ✅

**Type consistency:** `run_handler(job_type, payload, ctx)` and all handlers use `(payload, ctx)` (Tasks 3, 6). `PgJobContext` exposes `set_progress`/`cancelled`, consumed by `handle_batch` (Tasks 3, 5, 6). `complete_job(..., progress=None)` defined in Task 4, called in Task 6. `create_job(..., idempotency_key, idempotency_hash)` defined in Task 4, called in Tasks 4/7. `_replay_or_conflict`/`_create_and_handoff`/`_accepted` all defined and used within Task 7.

**Placeholder scan:** No TBD/TODO; every code step has complete code; every test has real assertions.

**Note on `session_factory` default:** `process_job(..., session_factory=None)` keeps the five existing `test_worker.py` call sites working unchanged (non-batch handlers never touch the context), while `run_forever` and the batch tests pass a real factory. Batch execution always receives one.
