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
