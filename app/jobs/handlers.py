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
    if random.random() < 0.2:
        raise WebhookFailedError(f"webhook call to {payload.url} failed")
    return {"status": 200}


def handle_report(payload: ReportPayload, ctx) -> dict:
    time.sleep(random.uniform(3, 5))
    return {"file_url": f"https://reports.local/{uuid.uuid4().hex[:12]}.pdf"}


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
