from typing import Annotated, Literal, Union

from pydantic import AnyUrl, BaseModel, Field, TypeAdapter, UrlConstraints

from app.schemas.enums import JobType

MAX_BATCH_ITEMS = 500

# https-only: worker egress allows TCP 443 exclusively, and plaintext
# delivery would leak payloads in transit.
HttpsUrl = Annotated[AnyUrl, UrlConstraints(allowed_schemes=["https"], max_length=2048)]


class EmailPayload(BaseModel):
    type: Literal[JobType.email] = JobType.email
    to: str = Field(max_length=320)
    subject: str = Field(max_length=500)
    body: str | None = Field(default=None, max_length=20_000)


class WebhookPayload(BaseModel):
    type: Literal[JobType.webhook] = JobType.webhook
    url: HttpsUrl
    method: str = Field(default="POST", max_length=10)


class ReportPayload(BaseModel):
    type: Literal[JobType.report] = JobType.report
    report_type: str = Field(max_length=100)
    params: dict | None = Field(default=None, max_length=50)


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
