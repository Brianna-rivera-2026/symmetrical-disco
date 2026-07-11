import redis.asyncio as redis
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind

_tracer = trace.get_tracer("app.queue.producer")


def message_fields(
    stream: str, job_id: str, carrier: dict | None = None
) -> dict[str, str]:
    """XADD fields carrying W3C trace context. With `carrier` (a job's stored
    {'traceparent': ...}) the send joins that original trace; without it, the
    currently active context is used. A malformed carrier degrades to a new
    root trace — this never raises."""
    context = extract(carrier) if carrier else None
    with _tracer.start_as_current_span(
        f"send {stream}",
        context=context,
        kind=SpanKind.PRODUCER,
        attributes={
            "messaging.system": "redis",
            "messaging.destination.name": stream,
            "job.id": job_id,
        },
    ):
        fields = {"job_id": job_id}
        inject(fields)
    return fields


async def enqueue(
    client: redis.Redis, stream: str, job_id: str, carrier: dict | None = None
) -> str:
    return await client.xadd(stream, message_fields(stream, job_id, carrier))
