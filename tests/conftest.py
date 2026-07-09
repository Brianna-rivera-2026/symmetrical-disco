import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

# Installed at import time: global providers may only be set once per process,
# and conftest imports before any test or app module emits telemetry.
_SPAN_EXPORTER = InMemorySpanExporter()
_METRIC_READER = InMemoryMetricReader()

_tracer_provider = TracerProvider()
_tracer_provider.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
trace.set_tracer_provider(_tracer_provider)
metrics.set_meter_provider(MeterProvider(metric_readers=[_METRIC_READER]))


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    _SPAN_EXPORTER.clear()
    return _SPAN_EXPORTER


@pytest.fixture
def metric_reader() -> InMemoryMetricReader:
    # Cumulative across the session; tests assert on attribute sets / deltas.
    return _METRIC_READER
