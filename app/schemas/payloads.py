from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter

from app.schemas.enums import JobType

MAX_BATCH_ITEMS = 500


class EmailPayload(BaseModel):
    type: Literal[JobType.email] = JobType.email
    to: str
    subject: str
    body: str | None = None


class WebhookPayload(BaseModel):
    type: Literal[JobType.webhook] = JobType.webhook
    url: str
    method: str = "POST"


class ReportPayload(BaseModel):
    type: Literal[JobType.report] = JobType.report
    report_type: str
    params: dict | None = None


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
