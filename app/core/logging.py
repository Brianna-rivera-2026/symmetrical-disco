import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone

_log_context: ContextVar[dict] = ContextVar("log_context", default={})

# Attribute names present on every LogRecord; anything else came in via
# `extra` (or the ContextFilter) and belongs in the JSON output.
_RESERVED = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | {"message", "asctime", "taskName"}


@contextmanager
def bind_log_context(**fields: object) -> Iterator[None]:
    """Bind fields onto every log record emitted inside the block."""
    token = _log_context.set({**_log_context.get(), **fields})
    try:
        yield
    finally:
        _log_context.reset(token)


def bind_static_log_context(**fields: object) -> None:
    """Bind fields for the remaining lifetime of the current context
    (process-constant values like the worker's consumer name)."""
    _log_context.set({**_log_context.get(), **fields})


class ContextFilter(logging.Filter):
    """Merge the bound context into each record; explicit `extra` fields win."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _log_context.get().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("otel"):
                continue
            out[key] = value
        # Injected by opentelemetry-instrumentation-logging; "0" means no span.
        trace_id = getattr(record, "otelTraceID", "0")
        if trace_id != "0":
            out["trace_id"] = trace_id
            out["span_id"] = getattr(record, "otelSpanID", "0")
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(ContextFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    # Third-party loggers inherit WARNING from root; only app.* speaks at the
    # configured level (keeps noisy libraries out of stdout and OTLP alike).
    root.setLevel(logging.WARNING)
    logging.getLogger("app").setLevel(level)

    # Hijack uvicorn loggers so their records flow through the same handler,
    # pinned to WARNING because uvicorn otherwise sets its own levels.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
        lg.setLevel(logging.WARNING)
