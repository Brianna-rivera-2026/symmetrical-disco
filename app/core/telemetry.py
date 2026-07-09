import logging

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.propagate import inject
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app.core.config import Settings
from app.core.logging import ContextFilter

_state: dict[str, list] = {"providers": [], "handlers": []}


def configure_telemetry(
    settings: Settings, service_name: str, instance_id: str | None = None
) -> None:
    """Set up OTLP traces/metrics/logs and auto-instrumentation.

    No-op when settings.otel_enabled is False. Must run BEFORE the service
    creates its SQLAlchemy engine so the instrumentation hooks it. Any setup
    failure is logged and swallowed — telemetry must never block startup.
    """
    if not settings.otel_enabled:
        return

    try:
        attributes: dict[str, str] = {"service.name": service_name}
        if instance_id is not None:
            attributes["service.instance.id"] = instance_id
        resource = Resource.create(attributes)
        endpoint = settings.otel_exporter_otlp_endpoint

        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
        )
        trace.set_tracer_provider(tracer_provider)

        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(endpoint=endpoint, insecure=True)
                )
            ],
        )
        metrics.set_meter_provider(meter_provider)

        logger_provider = LoggerProvider(resource=resource)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint, insecure=True))
        )
        set_logger_provider(logger_provider)
        otel_handler = LoggingHandler(logger_provider=logger_provider)
        otel_handler.addFilter(ContextFilter())
        logging.getLogger().addHandler(otel_handler)
        _state["handlers"].append(otel_handler)

        LoggingInstrumentor().instrument(set_logging_format=False)
        SQLAlchemyInstrumentor().instrument()
        RedisInstrumentor().instrument()

        _state["providers"] = [tracer_provider, meter_provider, logger_provider]
    except Exception:  # noqa: BLE001 — telemetry setup must never block startup
        logging.getLogger("app.telemetry").warning(
            "telemetry.configure_failed", exc_info=True
        )


def shutdown_telemetry() -> None:
    """Flush and shut down providers. Safe (and a no-op) when disabled."""
    for handler in _state["handlers"]:
        logging.getLogger().removeHandler(handler)
    _state["handlers"].clear()
    for provider in _state["providers"]:
        provider.shutdown()
    _state["providers"].clear()


def current_trace_carrier() -> dict[str, str] | None:
    """W3C carrier ({'traceparent': ...}) for the active context, or None
    when there is no active span (e.g. OTel disabled)."""
    carrier: dict[str, str] = {}
    inject(carrier)
    return carrier or None
