from pydantic import BaseModel


class EmailResult(BaseModel):
    message_id: str


class WebhookResult(BaseModel):
    status: int


class ReportResult(BaseModel):
    file_url: str
