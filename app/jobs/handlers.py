import random
import time
import uuid

from app.schemas.payloads import EmailPayload, ReportPayload, WebhookPayload


class WebhookFailedError(Exception):
    """Raised when the simulated webhook call fails."""


def handle_email(payload: EmailPayload) -> dict:
    time.sleep(random.uniform(1, 3))
    return {"message_id": f"msg_{uuid.uuid4().hex[:12]}"}


def handle_webhook(payload: WebhookPayload) -> dict:
    time.sleep(random.uniform(1, 2))
    if random.random() < 0.2:
        raise WebhookFailedError(f"webhook call to {payload.url} failed")
    return {"status": 200}


def handle_report(payload: ReportPayload) -> dict:
    time.sleep(random.uniform(3, 5))
    return {"file_url": f"https://reports.local/{uuid.uuid4().hex[:12]}.pdf"}
