from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter, ValidationInfo, model_validator

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


class BatchPayload(BaseModel):
    type: Literal[JobType.batch] = JobType.batch
    items: list[dict] = Field(max_length=MAX_BATCH_ITEMS)
    item_delay_ms: int = Field(default=50, ge=0)

    @model_validator(mode="after")
    def _fits_timeout_budget(self, info: ValidationInfo) -> "BatchPayload":
        context = info.context or {}
        budget = context.get("handler_timeout_s")
        if budget is not None:
            est_s = (len(self.items) * self.item_delay_ms) / 1000
            if est_s >= budget * context.get("safety_factor", 0.8):
                raise ValueError(
                    "estimated batch duration exceeds worker timeout budget"
                )
        return self


JobPayload = Annotated[
    Union[EmailPayload, WebhookPayload, ReportPayload, BatchPayload],
    Field(discriminator="type"),
]

_ADAPTER: TypeAdapter = TypeAdapter(JobPayload)


def validate_payload(
    job_type: JobType | str, raw: dict, *, context: dict | None = None
) -> EmailPayload | WebhookPayload | ReportPayload | BatchPayload:
    job_type = JobType(job_type)  # raises ValueError on unknown type
    return _ADAPTER.validate_python({**raw, "type": job_type.value}, context=context)
