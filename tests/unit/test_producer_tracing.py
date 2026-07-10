from opentelemetry import trace
from opentelemetry.trace import SpanKind

from app.queue.producer import message_fields


def test_fields_carry_active_context(span_exporter):
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("api-request") as parent:
        fields = message_fields("jobs:stream:high", "j1")
    trace_id = format(parent.get_span_context().trace_id, "032x")
    assert fields["job_id"] == "j1"
    assert trace_id in fields["traceparent"]
    producer_spans = [
        s for s in span_exporter.get_finished_spans()
        if s.name == "send jobs:stream:high"
    ]
    assert len(producer_spans) == 1
    assert producer_spans[0].kind is SpanKind.PRODUCER
    assert format(producer_spans[0].context.trace_id, "032x") == trace_id
    assert producer_spans[0].attributes["messaging.system"] == "redis"


def test_fields_from_stored_carrier_join_original_trace(span_exporter):
    stored = {"traceparent": f"00-{'ab' * 16}-{'cd' * 8}-01"}
    fields = message_fields("jobs:stream:low", "j2", carrier=stored)
    assert "ab" * 16 in fields["traceparent"]


def test_fields_with_malformed_carrier_do_not_raise(span_exporter):
    fields = message_fields("jobs:stream:normal", "j3", carrier={"traceparent": "garbage"})
    assert fields["job_id"] == "j3"
    assert "traceparent" in fields  # new root trace, not an error
