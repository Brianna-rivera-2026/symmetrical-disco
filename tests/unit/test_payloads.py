import pytest
from pydantic import ValidationError

from app.schemas.enums import JobType
from app.schemas.payloads import EmailPayload, WebhookPayload, validate_payload


def test_validate_email_payload():
    p = validate_payload(JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert isinstance(p, EmailPayload)
    assert p.to == "a@b.com"
    assert p.body is None


def test_validate_webhook_defaults_method():
    p = validate_payload("webhook", {"url": "https://x.test"})
    assert isinstance(p, WebhookPayload)
    assert p.method == "POST"


def test_validate_webhook_rejects_http_scheme():
    with pytest.raises(ValidationError):
        validate_payload("webhook", {"url": "http://x.test"})


def test_validate_webhook_rejects_non_url():
    with pytest.raises(ValidationError):
        validate_payload("webhook", {"url": "not a url"})


def test_validate_rejects_missing_required_field():
    with pytest.raises(ValidationError):
        validate_payload(JobType.email, {"subject": "no recipient"})


def test_validate_rejects_unknown_type():
    with pytest.raises(ValueError):
        validate_payload("translate", {"foo": "bar"})


def test_email_rejects_invalid_address():
    with pytest.raises(ValidationError):
        validate_payload(JobType.email, {"to": "not-an-email", "subject": "Hi"})


def test_email_rejects_empty_subject():
    with pytest.raises(ValidationError):
        validate_payload(JobType.email, {"to": "a@b.com", "subject": ""})


def test_webhook_rejects_unknown_method():
    with pytest.raises(ValidationError):
        validate_payload("webhook", {"url": "https://x.test", "method": "DELETE"})


def test_report_rejects_unknown_report_type():
    with pytest.raises(ValidationError):
        validate_payload("report", {"report_type": "espionage"})


def test_report_rejects_too_many_params_keys():
    params = {f"k{i}": 1 for i in range(51)}
    with pytest.raises(ValidationError):
        validate_payload("report", {"report_type": "sales", "params": params})


def test_report_rejects_oversized_params():
    with pytest.raises(ValidationError):
        validate_payload(
            "report", {"report_type": "sales", "params": {"k": "x" * 9000}}
        )


def test_payload_rejects_unknown_keys():
    with pytest.raises(ValidationError):
        validate_payload(
            JobType.email, {"to": "a@b.com", "subject": "Hi", "bcc": "spy@evil.test"}
        )
