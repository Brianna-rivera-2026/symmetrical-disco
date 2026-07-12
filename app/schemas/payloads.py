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


def validate_payload(
    job_type: JobType | str, raw: dict
) -> EmailPayload | WebhookPayload | ReportPayload | BatchPayload:
    job_type = JobType(job_type)  # raises ValueError on unknown type
    return _ADAPTER.validate_python({**raw, "type": job_type.value})
