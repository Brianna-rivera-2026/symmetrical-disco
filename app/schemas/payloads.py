import json
from typing import Annotated, Literal, Union

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    TypeAdapter,
    UrlConstraints,
    field_validator,
)

from app.schemas.enums import JobType, ReportType

MAX_BATCH_ITEMS = 500
MAX_REPORT_PARAMS_KEYS = 50
MAX_REPORT_PARAMS_BYTES = 8192

# https-only: worker egress allows TCP 443 exclusively, and plaintext
# delivery would leak payloads in transit.
HttpsUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["https"], max_length=2048)]


class EmailPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal[JobType.email] = JobType.email
    to: EmailStr = Field(max_length=320)
    subject: str = Field(min_length=1, max_length=500)
    body: str | None = Field(default=None, max_length=20_000)


class WebhookPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal[JobType.webhook] = JobType.webhook
    url: HttpsUrl
    method: Literal["GET", "POST"] = "POST"


class ReportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal[JobType.report] = JobType.report
    report_type: ReportType
    params: dict | None = None

    @field_validator("params")
    @classmethod
    def _bound_params(cls, v: dict | None) -> dict | None:
        if v is None:
            return v
        if len(v) > MAX_REPORT_PARAMS_KEYS:
            raise ValueError(f"params exceeds {MAX_REPORT_PARAMS_KEYS} keys")
        size = len(json.dumps(v, separators=(",", ":"), default=str))
        if size > MAX_REPORT_PARAMS_BYTES:
            raise ValueError(
                f"params exceeds {MAX_REPORT_PARAMS_BYTES} bytes serialized"
            )
        return v


_BaseItemPayload = Union[EmailPayload, WebhookPayload, ReportPayload]

BatchItemPayload = Annotated[_BaseItemPayload, Field(discriminator="type")]


class BatchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal[JobType.batch] = JobType.batch
    items: list[BatchItemPayload] = Field(max_length=MAX_BATCH_ITEMS)


JobPayload = Annotated[
    Union[_BaseItemPayload, BatchPayload],
    Field(discriminator="type"),
]

_ADAPTER: TypeAdapter = TypeAdapter(JobPayload)


class PayloadPolicyError(ValueError):
    """Payload passed schema validation but violates a configured allowlist.

    Subclasses ValueError so the API's existing 422 path catches it; the
    worker treats it as non-retryable."""


def _host_allowed(host: str, allowed: list[str]) -> bool:
    host = host.lower().rstrip(".")
    for entry in allowed:
        entry = entry.lower().strip(".")
        if host == entry or host.endswith("." + entry):
            return True
    return False


def _check_policy(payload, settings) -> None:
    if isinstance(payload, WebhookPayload):
        host = payload.url.host or ""
        if not _host_allowed(host, settings.webhook_allowed_hosts):
            raise PayloadPolicyError(f"webhook host {host!r} is not allowlisted")
    elif isinstance(payload, EmailPayload):
        domain = payload.to.rsplit("@", 1)[1].lower()
        if domain not in {d.lower() for d in settings.email_allowed_domains}:
            raise PayloadPolicyError(f"email domain {domain!r} is not allowlisted")
    elif isinstance(payload, BatchPayload):
        for item in payload.items:
            _check_policy(item, settings)


def validate_payload(
    job_type: JobType | str, raw: dict, settings=None
) -> EmailPayload | WebhookPayload | ReportPayload | BatchPayload:
    job_type = JobType(job_type)  # raises ValueError on unknown type
    payload = _ADAPTER.validate_python({**raw, "type": job_type.value})
    if settings is not None:
        _check_policy(payload, settings)
    return payload
