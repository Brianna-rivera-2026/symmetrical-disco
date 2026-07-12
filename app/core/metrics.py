"""Job-pipeline instruments. Created against the global MeterProvider; with
OTel disabled these are proxy no-ops, so emission points never need guards."""

from opentelemetry import metrics

_meter = metrics.get_meter("app.jobs")

jobs_submitted = _meter.create_counter(
    "jobs.submitted", description="Jobs accepted by the API"
)
jobs_processed = _meter.create_counter(
    "jobs.processed", description="Worker outcomes per delivery"
)
jobs_failed = _meter.create_counter(
    "jobs.failed", description="Jobs that exhausted max_attempts"
)
job_processing_duration = _meter.create_histogram(
    "job.processing.duration", unit="s", description="process_job wall time"
)
job_queue_wait = _meter.create_histogram(
    "job.queue.wait", unit="s", description="XADD-to-delivery latency"
)
ticker_promoted = _meter.create_counter(
    "ticker.promoted", description="Scheduled jobs promoted to streams"
)
ticker_reaped = _meter.create_counter(
    "ticker.reaped", description="Stale in-flight messages reclaimed"
)
ticker_reconciled = _meter.create_counter(
    "ticker.reconciled", description="Unsynced jobs re-handed to Redis"
)
auth_validations = _meter.create_counter(
    "auth.validations", description="API key auth attempts by result and source"
)
jobs_dropped_ownerless = _meter.create_counter(
    "jobs.dropped_ownerless", description="Jobs dropped by the worker ownerless guard"
)
worker_recycles = _meter.create_counter(
    "worker.recycles", description="Workers that self-recycled on memory breach"
)
