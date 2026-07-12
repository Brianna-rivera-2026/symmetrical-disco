from enum import Enum


class JobType(str, Enum):
    email = "email"
    webhook = "webhook"
    report = "report"
    batch = "batch"


class JobStatus(str, Enum):
    scheduled = "scheduled"
    pending = "pending"
    processing = "processing"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class JobPriority(str, Enum):
    high = "high"
    normal = "normal"
    low = "low"


class ReportType(str, Enum):
    sales = "sales"
    ops = "ops"
    weekly_summary = "weekly_summary"
