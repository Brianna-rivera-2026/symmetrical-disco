import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.schemas.enums import JobType
from app.schemas.payloads import (
    EmailPayload,
    PayloadPolicyError,
    WebhookPayload,
    validate_payload,
)


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


def _settings(**overrides):
    return Settings(
        database_url="postgresql+psycopg://u:p@h/db",
        redis_url="redis://h:6379/0",
        **overrides,
    )


def test_webhook_host_suffix_match_allowed():
    s = _settings(webhook_allowed_hosts=["hooks.example.com"])
    p = validate_payload("webhook", {"url": "https://a.hooks.example.com/x"}, s)
    assert isinstance(p, WebhookPayload)


def test_webhook_host_not_allowlisted_rejected():
    s = _settings(webhook_allowed_hosts=["hooks.example.com"])
    with pytest.raises(PayloadPolicyError):
        validate_payload("webhook", {"url": "https://evil.test/x"}, s)


def test_webhook_empty_allowlist_denies_all():
    with pytest.raises(PayloadPolicyError):
        validate_payload("webhook", {"url": "https://x.test"}, _settings())


def test_webhook_suffix_match_requires_label_boundary():
    s = _settings(webhook_allowed_hosts=["hooks.example.com"])
    with pytest.raises(PayloadPolicyError):
        validate_payload(
            "webhook", {"url": "https://evilhooks.example.com.attacker.test"}, s
        )
    with pytest.raises(PayloadPolicyError):
        validate_payload("webhook", {"url": "https://xhooks.example.com"}, s)


def test_email_domain_allowed_case_insensitive():
    s = _settings(email_allowed_domains=["Example.COM"])
    p = validate_payload(JobType.email, {"to": "a@example.com", "subject": "Hi"}, s)
    assert isinstance(p, EmailPayload)


def test_email_domain_not_allowlisted_rejected():
    s = _settings(email_allowed_domains=["example.com"])
    with pytest.raises(PayloadPolicyError):
        validate_payload(JobType.email, {"to": "a@other.com", "subject": "Hi"}, s)


def test_batch_items_are_policy_checked():
    s = _settings(email_allowed_domains=["example.com"], webhook_allowed_hosts=[])
    with pytest.raises(PayloadPolicyError):
        validate_payload(
            "batch",
            {
                "items": [
                    {"type": "email", "to": "a@example.com", "subject": "ok"},
                    {"type": "webhook", "url": "https://x.test"},
                ]
            },
            s,
        )


def test_no_settings_skips_policy():
    p = validate_payload("webhook", {"url": "https://x.test"})
    assert isinstance(p, WebhookPayload)
