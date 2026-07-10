import logging

from opentelemetry import trace

from app.core.config import Settings
from app.core.telemetry import (
    configure_telemetry,
    current_trace_carrier,
    shutdown_telemetry,
)


def _settings(**overrides) -> Settings:
    return Settings(
        database_url="postgresql+psycopg://u:p@localhost/x",
        redis_url="redis://localhost:6379/0",
        **overrides,
    )


def test_settings_defaults():
    settings = _settings()
    assert settings.otel_enabled is False
    assert settings.otel_exporter_otlp_endpoint == "http://localhost:4317"


def test_disabled_is_noop():
    settings = _settings(otel_enabled=False)
    provider_before = trace.get_tracer_provider()
    handlers_before = logging.getLogger().handlers[:]
    configure_telemetry(settings, "test-service")
    assert trace.get_tracer_provider() is provider_before
    assert logging.getLogger().handlers == handlers_before
    shutdown_telemetry()  # must be safe when nothing was configured


def test_current_trace_carrier_inside_span(span_exporter):
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("request") as span:
        carrier = current_trace_carrier()
    assert carrier is not None
    trace_id = format(span.get_span_context().trace_id, "032x")
    assert trace_id in carrier["traceparent"]


def test_current_trace_carrier_outside_span_is_none():
    assert current_trace_carrier() is None


def test_configure_telemetry_swallows_setup_errors(monkeypatch, caplog):
    import app.core.telemetry as telemetry_module

    def _boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        telemetry_module,
        "Resource",
        type("BrokenResource", (), {"create": staticmethod(_boom)}),
    )
    settings = _settings(otel_enabled=True)
    with caplog.at_level("WARNING"):
        configure_telemetry(settings, "test-service")  # must not raise
    assert any("telemetry.configure_failed" in r.message for r in caplog.records)
