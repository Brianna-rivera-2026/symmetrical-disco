from unittest.mock import AsyncMock

import pytest

from app.jobs import handlers
from app.jobs.registry import run_handler
from app.schemas.enums import JobType
from app.schemas.payloads import EmailPayload, ReportPayload, WebhookPayload


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.asyncio, "sleep", AsyncMock())


@pytest.mark.asyncio
async def test_email_returns_message_id():
    out = await handlers.handle_email(EmailPayload(to="a@b.com", subject="Hi"), None)
    assert "message_id" in out


@pytest.mark.asyncio
async def test_report_returns_file_url():
    out = await handlers.handle_report(ReportPayload(report_type="sales"), None)
    assert out["file_url"].startswith("https://")


@pytest.mark.asyncio
async def test_webhook_success_branch(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.5)
    out = await handlers.handle_webhook(WebhookPayload(url="https://x.test"), None)
    assert out == {"status": 200}


@pytest.mark.asyncio
async def test_webhook_failure_branch(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)
    with pytest.raises(handlers.WebhookFailedError):
        await handlers.handle_webhook(WebhookPayload(url="https://x.test"), None)


@pytest.mark.asyncio
async def test_run_handler_dispatches_by_type():
    out = await run_handler(JobType.email, EmailPayload(to="a@b.com", subject="Hi"), None)
    assert "message_id" in out
