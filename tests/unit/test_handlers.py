import pytest

from app.jobs import handlers
from app.jobs.registry import run_handler
from app.schemas.enums import JobType
from app.schemas.payloads import EmailPayload, ReportPayload, WebhookPayload


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def test_email_returns_message_id():
    out = handlers.handle_email(EmailPayload(to="a@b.com", subject="Hi"))
    assert "message_id" in out


def test_report_returns_file_url():
    out = handlers.handle_report(ReportPayload(report_type="sales"))
    assert out["file_url"].startswith("https://")


def test_webhook_success_branch(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.5)  # >= 0.2 → success
    out = handlers.handle_webhook(WebhookPayload(url="https://x.test"))
    assert out == {"status": 200}


def test_webhook_failure_branch(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # < 0.2 → failure
    with pytest.raises(handlers.WebhookFailedError):
        handlers.handle_webhook(WebhookPayload(url="https://x.test"))


def test_run_handler_dispatches_by_type():
    out = run_handler(JobType.email, EmailPayload(to="a@b.com", subject="Hi"))
    assert "message_id" in out
