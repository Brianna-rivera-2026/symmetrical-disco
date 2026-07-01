# Batch Items as Real Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the batch job type's opaque `dict` items with real sub-jobs — each item is an email, webhook, or report payload, validated at submission and dispatched to the actual handler.

**Architecture:** `BatchItemPayload` (a `email | webhook | report` discriminated union, no batch nesting) replaces `list[dict]`. `JobPayload` and `BatchItemPayload` both derive from one shared `_BaseItemPayload` union (DRY). `handle_batch` dispatches each item through the existing `registry.run_handler` (deferred import to avoid a circular import) instead of a dummy simulation. The upfront timeout-budget check and everything that only existed to support it (`item_delay_ms`, the model validator, `Settings.batch_timeout_safety_factor`, `validate_payload`'s `context` param) are removed — a too-long batch now surfaces via the existing `HandlerTimeout` → retry path instead.

**Tech Stack:** Python 3.11, FastAPI, Pydantic v2 discriminated unions, pytest + testcontainers (real PostgreSQL 16 + Redis 7), `uv`.

**Source spec:** `docs/superpowers/specs/2026-07-01-batch-items-as-real-jobs-design.md` (amends `docs/superpowers/specs/2026-07-01-cancellation-batch-progress-design.md` §5/§8).

## Global Constraints

- **Package/tooling:** always `uv` — `uv run pytest`, `uv add <pkg>`. Never raw `pip`/`venv`/`poetry`.
- **Lint/format:** `uv run ruff check --fix` and `uv run ruff format` before each commit.
- **Logging:** structured logging only (`structlog`); never `print`.
- **No nested batches:** `BatchItemPayload` must be a union of exactly `EmailPayload | WebhookPayload | ReportPayload` — never include `BatchPayload`.
- **DRY:** `JobPayload` and `BatchItemPayload` must both derive from one shared `_BaseItemPayload` union — the three payload types are listed exactly once in the module.
- **Single dispatch table:** `handle_batch` must reuse `app.jobs.registry.run_handler` — no second, duplicate handler map.
- **Full suite must stay green at every commit in this task** — this is one cohesive change (schema, handler, and the tests that exercise both are tightly coupled and cannot be split across separate green checkpoints without an artificial red window).

---

## File Structure

**Modify:**
- `app/schemas/payloads.py` — `BatchItemPayload`, DRY `JobPayload`, `BatchPayload.items` retyped, `item_delay_ms` and the budget `model_validator` removed, `validate_payload` reverts to its 2-arg signature.
- `app/jobs/handlers.py` — `handle_batch` rewritten to dispatch via `run_handler`; `_process_item` deleted.
- `app/core/config.py` — `batch_timeout_safety_factor` setting removed.
- `app/api/routes.py` — the `context={...}` argument to `validate_payload` in `submit_job` removed.
- `tests/unit/test_batch_payload.py` — rewritten for the new schema.
- `tests/unit/test_batch_handler.py` — rewritten for real per-type dispatch.
- `tests/integration/test_batch.py` — real typed items in the three batch-processing tests; adds a `_no_sleep` fixture (real handlers now genuinely sleep).

---

## Task 1: Batch items as real jobs — schema, handler, config/route cleanup, and all affected tests

**Files:**
- Modify: `app/schemas/payloads.py` (full-file rewrite, ~60 lines)
- Modify: `app/jobs/handlers.py:45-64` (delete `_process_item`, rewrite `handle_batch`)
- Modify: `app/core/config.py:34` (delete `batch_timeout_safety_factor`)
- Modify: `app/api/routes.py:87-94` (simplify the `validate_payload` call)
- Test: `tests/unit/test_batch_payload.py` (full-file rewrite)
- Test: `tests/unit/test_batch_handler.py` (full-file rewrite)
- Test: `tests/integration/test_batch.py` (add fixture; update 3 tests' fixtures/assertions)

**Interfaces:**
- Consumes: `app.jobs.registry.run_handler(job_type, payload, ctx) -> dict` (pre-existing, unchanged), `JobCancelled(summary: dict)` (pre-existing, unchanged), `EmailPayload`/`WebhookPayload`/`ReportPayload` (pre-existing, unchanged).
- Produces: `app.schemas.payloads.BatchItemPayload` (new type alias); `BatchPayload.items: list[BatchItemPayload]`; `handle_batch(payload, ctx) -> dict` returning `{"total": int, "succeeded": int, "failed": int, "results": list[{"index": int, "result": dict}], "errors": list[{"index": int, "error": str}]}`; `validate_payload(job_type, raw) -> EmailPayload | WebhookPayload | ReportPayload | BatchPayload` (2-arg, `context` kwarg removed).

- [ ] **Step 1: Write the failing schema tests** — replace the full contents of `tests/unit/test_batch_payload.py`:

```python
import pytest
from pydantic import ValidationError

from app.schemas.enums import JobType
from app.schemas.payloads import (
    MAX_BATCH_ITEMS,
    BatchPayload,
    EmailPayload,
    ReportPayload,
    WebhookPayload,
    validate_payload,
)


def test_batch_payload_accepts_heterogeneous_items():
    p = validate_payload(
        JobType.batch,
        {
            "items": [
                {"type": "email", "to": "a@b.com", "subject": "Hi"},
                {"type": "webhook", "url": "https://x.test"},
                {"type": "report", "report_type": "sales"},
            ]
        },
    )
    assert isinstance(p, BatchPayload)
    assert isinstance(p.items[0], EmailPayload)
    assert isinstance(p.items[1], WebhookPayload)
    assert isinstance(p.items[2], ReportPayload)


def test_batch_rejects_item_with_unknown_type():
    with pytest.raises(ValidationError):
        BatchPayload(items=[{"type": "sms", "to": "+1"}])


def test_batch_rejects_item_missing_required_field():
    with pytest.raises(ValidationError):
        BatchPayload(items=[{"type": "webhook"}])  # missing required 'url'


def test_batch_rejects_nested_batch_item():
    with pytest.raises(ValidationError):
        BatchPayload(items=[{"type": "batch", "items": []}])


def test_batch_rejects_too_many_items():
    item = {"type": "email", "to": "a@b.com", "subject": "Hi"}
    with pytest.raises(ValidationError):
        BatchPayload(items=[item for _ in range(MAX_BATCH_ITEMS + 1)])
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_batch_payload.py -v`
Expected: FAIL — `BatchPayload.items` is still `list[dict]`, so heterogeneous/invalid-type checks don't apply yet, and the old `item_delay_ms`-based tests are gone (import errors or assertion mismatches against the current schema).

- [ ] **Step 3: Rewrite the schema** — replace the full contents of `app/schemas/payloads.py`:

```python
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter

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


_BaseItemPayload = Union[EmailPayload, WebhookPayload, ReportPayload]

BatchItemPayload = Annotated[_BaseItemPayload, Field(discriminator="type")]


class BatchPayload(BaseModel):
    type: Literal[JobType.batch] = JobType.batch
    items: list[BatchItemPayload] = Field(max_length=MAX_BATCH_ITEMS)


JobPayload = Annotated[
    Union[_BaseItemPayload, BatchPayload],
    Field(discriminator="type"),
]

_ADAPTER: TypeAdapter = TypeAdapter(JobPayload)


def validate_payload(
    job_type: JobType | str, raw: dict
) -> EmailPayload | WebhookPayload | ReportPayload | BatchPayload:
    job_type = JobType(job_type)  # raises ValueError on unknown type
    return _ADAPTER.validate_python({**raw, "type": job_type.value})
```

- [ ] **Step 4: Run to verify the schema tests pass**

Run: `uv run pytest tests/unit/test_batch_payload.py -v`
Expected: PASS (5/5).

- [ ] **Step 5: Write the failing handler tests** — replace the full contents of `tests/unit/test_batch_handler.py`:

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


def test_batch_dispatches_real_handlers_mixed_success_and_failure(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # forces webhook < 0.2 -> fail
    payload = BatchPayload(
        items=[
            {"type": "email", "to": "a@b.com", "subject": "Hi"},
            {"type": "webhook", "url": "https://x.test"},
            {"type": "report", "report_type": "sales"},
        ]
    )
    out = handle_batch(payload, _FakeCtx())
    assert out["total"] == 3
    assert out["succeeded"] == 2
    assert out["failed"] == 1
    assert [r["index"] for r in out["results"]] == [0, 2]
    assert "message_id" in out["results"][0]["result"]
    assert "file_url" in out["results"][1]["result"]
    assert out["errors"] == [
        {"index": 1, "error": "webhook call to https://x.test failed"}
    ]


def test_batch_all_fail_still_completes(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # every webhook fails
    payload = BatchPayload(
        items=[
            {"type": "webhook", "url": "https://a.test"},
            {"type": "webhook", "url": "https://b.test"},
        ]
    )
    out = handle_batch(payload, _FakeCtx())
    assert out["succeeded"] == 0
    assert out["failed"] == 2
    assert out["results"] == []
    assert [e["index"] for e in out["errors"]] == [0, 1]


def test_batch_raises_jobcancelled_with_partial_summary():
    ctx = _FakeCtx(cancel_after=2)  # first two items processed, then cancel
    payload = BatchPayload(
        items=[
            {"type": "email", "to": "a@b.com", "subject": "1"},
            {"type": "email", "to": "a@b.com", "subject": "2"},
            {"type": "report", "report_type": "sales"},
            {"type": "report", "report_type": "ops"},
        ]
    )
    with pytest.raises(JobCancelled) as exc:
        handle_batch(payload, ctx)
    assert exc.value.summary["succeeded"] == 2
    assert exc.value.summary["total"] == 4
    assert len(exc.value.summary["results"]) == 2


def test_batch_reports_progress_per_item():
    ctx = _FakeCtx()
    payload = BatchPayload(
        items=[{"type": "email", "to": "a@b.com", "subject": str(i)} for i in range(4)]
    )
    handle_batch(payload, ctx)
    assert ctx.progress == [25, 50, 75, 100]
```

- [ ] **Step 6: Run to verify it fails**

Run: `uv run pytest tests/unit/test_batch_handler.py -v`
Expected: FAIL — `handle_batch` still calls the now-nonexistent `_process_item`/`payload.item_delay_ms`.

- [ ] **Step 7: Rewrite the handler** — replace lines 45-64 of `app/jobs/handlers.py` (deletes `_process_item`, rewrites `handle_batch`):

```python
def handle_batch(payload: BatchPayload, ctx) -> dict:
    from app.jobs.registry import run_handler  # deferred: registry imports this module

    n = len(payload.items)
    summary = {"total": n, "succeeded": 0, "failed": 0, "results": [], "errors": []}
    for i, item in enumerate(payload.items):
        if ctx.cancelled():
            raise JobCancelled(summary)
        try:
            result = run_handler(item.type, item, ctx)
            summary["succeeded"] += 1
            summary["results"].append({"index": i, "result": result})
        except Exception as exc:  # noqa: BLE001 — per-item, collected not raised
            summary["failed"] += 1
            summary["errors"].append({"index": i, "error": str(exc)})
        ctx.set_progress(int((i + 1) / n * 100) if n else 100)
    return summary
```

The rest of `app/jobs/handlers.py` (imports, `WebhookFailedError`, `JobCancelled`, `handle_email`, `handle_webhook`, `handle_report`) is unchanged.

- [ ] **Step 8: Run to verify the handler tests pass**

Run: `uv run pytest tests/unit/test_batch_handler.py -v`
Expected: PASS (4/4).

- [ ] **Step 9: Remove the now-dead config setting** — in `app/core/config.py`, delete line 34:

```python
    batch_timeout_safety_factor: float = 0.8
```

- [ ] **Step 10: Simplify the route's validate_payload call** — in `app/api/routes.py`, replace lines 87-94:

```python
        validate_payload(
            submission.type,
            submission.payload,
            context={
                "handler_timeout_s": settings.job_handler_timeout_s,
                "safety_factor": settings.batch_timeout_safety_factor,
            },
        )
```

with:

```python
        validate_payload(submission.type, submission.payload)
```

(The `settings = request.app.state.settings` line directly above stays — it's still used later in the function for `_create_and_handoff`.)

- [ ] **Step 11: Run the full unit suite to verify no regressions so far**

Run: `uv run pytest tests/unit -q`
Expected: PASS, 0 failed.

- [ ] **Step 12: Commit the schema + handler + config/route changes**

```bash
git add app/schemas/payloads.py app/jobs/handlers.py app/core/config.py app/api/routes.py tests/unit/test_batch_payload.py tests/unit/test_batch_handler.py
git commit -m "feat: batch items are real email/webhook/report jobs"
```

- [ ] **Step 13: Update the integration tests** — replace the full contents of `tests/integration/test_batch.py`:

```python
from datetime import datetime, timezone

import pytest
from sqlalchemy import update

from app import repository as repo
from app.core.db import make_session_factory
from app.jobs import handlers
from app.models.job import Job
from app.schemas.enums import JobStatus, JobType
from app.worker.context import PgJobContext
from app.worker.runner import process_job


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


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


def test_batch_completes_with_progress_100(
    db_session, redis_client, test_settings, pg_engine
):
    sf = make_session_factory(pg_engine)
    job = repo.create_job(
        db_session,
        JobType.batch,
        {
            "items": [
                {"type": "email", "to": "a@b.com", "subject": "Hi"},
                {"type": "report", "report_type": "sales"},
            ]
        },
    )
    outcome = process_job(db_session, redis_client, test_settings, job.id, sf)
    assert outcome.label == "completed"
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.progress == 100
    assert job.result["total"] == 2
    assert job.result["succeeded"] == 2


def test_tiny_batch_reaches_progress_100_without_polling(
    db_session, redis_client, test_settings, pg_engine
):
    # poll interval is huge, so the poll loop never writes progress; completion must
    # still land it on 100 (type-driven, not IS NOT NULL).
    settings = test_settings.model_copy(update={"cancel_poll_interval_s": 999.0})
    sf = make_session_factory(pg_engine)
    job = repo.create_job(
        db_session,
        JobType.batch,
        {"items": [{"type": "email", "to": "a@b.com", "subject": "Hi"}]},
    )
    process_job(db_session, redis_client, settings, job.id, sf)
    db_session.refresh(job)
    assert job.progress == 100


def test_batch_cooperative_cancel(db_session, redis_client, test_settings, pg_engine):
    sf = make_session_factory(pg_engine)
    job = repo.create_job(
        db_session,
        JobType.batch,
        {
            "items": [
                {"type": "email", "to": "a@b.com", "subject": "1"},
                {"type": "email", "to": "a@b.com", "subject": "2"},
                {"type": "email", "to": "a@b.com", "subject": "3"},
            ]
        },
    )
    # Simulate "cancel arrived just as processing begins": stamp the flag on the row
    # so the handler's first poll sees it. (claim_job leaves cancel_requested_at intact.)
    db_session.execute(
        update(Job)
        .where(Job.id == job.id)
        .values(cancel_requested_at=datetime.now(timezone.utc))
    )
    db_session.commit()
    outcome = process_job(db_session, redis_client, test_settings, job.id, sf)
    assert outcome.label == "cancelled"
    db_session.refresh(job)
    assert job.status is JobStatus.cancelled
    assert job.result == {
        "total": 3,
        "succeeded": 0,
        "failed": 0,
        "results": [],
        "errors": [],
    }
```

- [ ] **Step 14: Run the integration tests to verify they pass against real Postgres/Redis**

Run: `uv run pytest tests/integration/test_batch.py -v`
Expected: PASS (4/4).

- [ ] **Step 15: Run the full suite to verify no regressions anywhere**

Run: `uv run pytest -q`
Expected: PASS, 0 failed (151 total minus the 3 removed budget-validator tests, plus the new ones — exact count will differ slightly from before; the only requirement is 0 failed).

- [ ] **Step 16: Lint, format, and commit the integration test update**

```bash
uv run ruff check --fix
uv run ruff format
git add tests/integration/test_batch.py
git commit -m "test: exercise batch items as real email/webhook/report jobs"
```

---

## Self-Review

**Spec coverage:**
- Mixed types allowed, no nested batch: `BatchItemPayload = Annotated[_BaseItemPayload, ...]` excludes `BatchPayload`; covered by `test_batch_rejects_nested_batch_item`. ✅
- Validate every item at submission, reject whole batch on any invalid item: Pydantic's discriminated union raises `ValidationError` for the whole `BatchPayload` if any item fails; `submit_job` already catches `ValidationError` → `422` (unchanged code path). Covered by `test_batch_rejects_item_with_unknown_type` / `test_batch_rejects_item_missing_required_field`. ✅
- Drop the budget check and everything supporting it (`item_delay_ms`, model validator, `batch_timeout_safety_factor`, `context` param): removed in Steps 3, 9, 10. ✅
- Summary gains `results`: `handle_batch`'s new shape, covered by every handler test. ✅
- Reuse `run_handler`, no duplicate dispatch table: Step 7's `handle_batch` calls `run_handler` via a deferred import; no new dict defined. ✅
- DRY `JobPayload`/`BatchItemPayload` via `_BaseItemPayload`: Step 3. ✅
- `MAX_BATCH_ITEMS` cap still enforced (unchanged from before, now tested with genuinely valid items): `test_batch_rejects_too_many_items`. ✅
- Integration coverage with real Postgres/Redis, no dummy dict items, `_no_sleep` fixture added: Step 13. ✅

**Type consistency:** `run_handler(job_type, payload, ctx)` (pre-existing, `app/jobs/registry.py`) is called with `item.type` (a `JobType` enum member on the parsed sub-payload) and `item` itself (an `EmailPayload`/`WebhookPayload`/`ReportPayload` instance) — matches `HANDLERS[job_type](payload, ctx)`'s existing dispatch signature exactly, no changes needed to `registry.py`. `JobCancelled(summary: dict)` unchanged. `BatchPayload.items: list[BatchItemPayload]` used consistently in both `handlers.py` (iterated as typed objects with `.type`) and all rewritten tests (constructed from raw dicts, which Pydantic parses into the union automatically).

**Placeholder scan:** No TBD/TODO; every step has complete, runnable code; every test has real, executable assertions (no `assert True`/empty bodies).
