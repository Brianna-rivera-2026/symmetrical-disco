# Batch items as real jobs — Design (amendment)

**Date:** 2026-07-01
**Status:** Approved (pre-implementation)
**Amends:** `docs/superpowers/specs/2026-07-01-cancellation-batch-progress-design.md` §5, §8
(already implemented and merged to `master`) — this document supersedes those two
sections; the rest of that spec (cancellation, progress, idempotency) is unaffected
and unchanged.

## 1. Context & motivation

The shipped `batch` job type treats `items` as opaque `list[dict]`, processed by a
dummy `_process_item` (sleep + a `{"fail": true}` flag) — a simulation stand-in with
no relationship to the three real job types. This revision makes each batch item a
real sub-job: its own `type` (`email`/`webhook`/`report`) plus that type's real
payload fields, dispatched to the actual handler (`handle_email`/`handle_webhook`/
`handle_report`).

## 2. Decisions (locked)

1. **Mixed types allowed per batch** — each item carries its own `type`; items may
   freely combine email/webhook/report within one batch. No nested batch items:
   `BatchItemPayload` is a union of the three real types only, excluding `BatchPayload`.
2. **Validate every item at submission time.** Each item is checked against its
   declared type's schema when the batch is submitted (`POST /jobs`); any invalid
   item (unknown/missing `type`, or a missing required field for its type) rejects
   the **whole submission** with `422` — nothing is created. This matches how a
   single-job submission already behaves today.
3. **Drop the upfront timeout-budget check**, and everything that existed solely to
   support it: `item_delay_ms`, `BatchPayload`'s `model_validator`,
   `Settings.batch_timeout_safety_factor`, `validate_payload`'s `context` parameter,
   and the route's `context={...}` argument. A batch that is genuinely too long for
   the worker's handler timeout now surfaces via the existing `HandlerTimeout` →
   retry (re-runs the whole batch from scratch) → eventual `FAILED` after
   `max_attempts` path — the same safety net every other handler already has, just
   at run time instead of submission time.
4. **Summary gains captured per-item results.** Alongside `errors`, a `results` list
   records each successful item's actual handler return value (e.g. email's
   `message_id`, report's `file_url`) — otherwise a batch of reports would complete
   with no way to retrieve what was generated.
5. **Batch dispatch reuses the single existing dispatch table** —
   `app.jobs.registry.run_handler` / `HANDLERS` — rather than a second, duplicate
   map. `handle_batch` imports `run_handler` **inside the function body** (a
   deferred import), because `registry.py` imports `handlers.py` at module scope;
   a module-level import in the other direction would be circular.
6. **`JobPayload` and `BatchItemPayload` are both derived from one shared union**
   (`_BaseItemPayload = Union[EmailPayload, WebhookPayload, ReportPayload]`) instead
   of listing the three payload types twice. `typing.Union` flattens nested unions,
   so `Union[_BaseItemPayload, BatchPayload]` is exactly the four-member union
   Pydantic's discriminator sees today — this is a pure DRY refactor, not a
   behavior change to `JobPayload`.

## 3. Schema changes (`app/schemas/payloads.py`)

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

`validate_payload` reverts to its original 2-argument signature (the `context`
kwarg is removed entirely, not merely defaulted — it has no remaining caller).

## 4. Handler changes (`app/jobs/handlers.py`)

`_process_item` is deleted. `handle_batch` dispatches each item through the
existing registry:

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

Cancellation and progress-reporting semantics (checked before each item, reported
after each item, `JobCancelled` carrying the partial summary) are unchanged from
the shipped design — only what happens *between* those two calls changes (a real
handler dispatch instead of a sleep-and-maybe-raise simulation).

Failure now comes entirely from real handler behavior: `webhook` fails ~20% of the
time on its own (`handle_webhook`'s existing `random.random() < 0.2` check);
`email`/`report` succeed deterministically barring a genuine bug. There is no more
`{"fail": true}` test hook — tests needing a deterministic failure use the existing
`monkeypatch.setattr(handlers.random, "random", ...)` pattern already established
for `handle_webhook`'s own unit tests.

## 5. Config & route cleanup

- `Settings.batch_timeout_safety_factor` — removed (`app/core/config.py`).
- `app/api/routes.py`'s `submit_job` — the `context={"handler_timeout_s": ...,
  "safety_factor": ...}` argument to `validate_payload` is removed; the call
  reverts to `validate_payload(submission.type, submission.payload)`.

## 6. Failure modes & edge cases

| Scenario | Outcome |
|----------|---------|
| Batch item with unknown/missing `type` | `422` at submission, nothing created |
| Batch item missing a required field for its declared type (e.g. webhook without `url`) | `422` at submission, nothing created |
| Batch item with `type: "batch"` (nested batch) | `422` at submission — `BatchItemPayload` has no `batch` member |
| Batch exceeding `MAX_BATCH_ITEMS` (500) | `422` at submission, unchanged from today |
| A genuinely too-long batch (e.g. 50 report items) | No longer rejected upfront; runs until `HandlerTimeout`, retried with backoff (re-running the whole batch from scratch each attempt), eventually `FAILED` at `max_attempts` — same path as any other handler timeout |
| Webhook item fails its own ~20% check | Recorded in `errors`, loop continues, batch still completes |
| Email/report items | Succeed deterministically; recorded in `results` with their real return value |
| Cancellation mid-batch | Unchanged: `JobCancelled` raised before the next item's dispatch, partial `results`/`errors` preserved in the summary |

## 7. Testing plan (`uv run pytest`)

**Unit — `tests/unit/test_batch_payload.py`** (replaces the budget-validator tests):
- A heterogeneous batch (email + webhook + report) parses; `items` contains
  correctly-typed instances (`isinstance(p.items[0], EmailPayload)`, etc.).
- An item with an unknown `type` → `ValidationError`.
- An item missing a required field for its declared type → `ValidationError`.
- An item with `type: "batch"` → `ValidationError` (no nesting).
- `MAX_BATCH_ITEMS` cap still enforced — using genuinely valid items (not bare
  `{}`), so the failure is actually the length cap, not an incidental
  discriminator-mismatch on an empty dict.

**Unit — `tests/unit/test_batch_handler.py`** (rewritten):
- Mixed success/failure: e.g. `[email, webhook(monkeypatched to fail), report]` →
  `succeeded=2`, `failed=1`, `results` has 2 entries, `errors` has 1.
- All-fail: all-webhook items with `random.random` monkeypatched below `0.2` →
  `succeeded=0`, `failed=n`, batch still returns a summary (not raised).
- Cancellation mid-batch: unchanged mechanism, partial `results`/`errors` at the
  cancellation point.
- Progress-per-item: unchanged mechanism (uses deterministic-success items so the
  percentages aren't entangled with webhook's randomness).

**Integration — `tests/integration/test_batch.py`** (updated):
- Add an autouse `_no_sleep` fixture patching `handlers.time.sleep`, matching the
  existing pattern in `tests/integration/test_worker.py` and
  `tests/unit/test_batch_handler.py` — real handlers now genuinely sleep 1–5s
  without it.
- Replace dummy `{"items": [{}, {}], "item_delay_ms": 0}` fixtures with real typed
  items (email/report, avoiding webhook where a test needs deterministic success).
- `test_batch_cooperative_cancel`'s expected summary gains the new `results: []` key.

**Existing-test migration:** any remaining reference to `item_delay_ms` or
`batch_timeout_safety_factor` is removed.
