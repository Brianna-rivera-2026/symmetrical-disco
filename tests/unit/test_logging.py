import json
import logging
import threading

import pytest

from app.core.logging import bind_log_context, configure_logging


@pytest.fixture(autouse=True)
def _restore_logging():
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    app_logger = logging.getLogger("app")
    saved_app_level = app_logger.level
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    app_logger.setLevel(saved_app_level)


def _last_record(capsys) -> dict:
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_emits_json_with_extra_and_bound_context(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")
    with bind_log_context(job_id="abc"):
        log.info("job.received", extra={"job_type": "email"})
    record = _last_record(capsys)
    assert record["event"] == "job.received"
    assert record["job_id"] == "abc"
    assert record["job_type"] == "email"
    assert record["level"] == "info"
    assert record["logger"] == "app.test"
    assert "timestamp" in record


def test_context_cleared_after_block(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")
    with bind_log_context(job_id="abc"):
        pass
    log.info("after")
    assert "job_id" not in _last_record(capsys)


def test_bind_log_context_nests(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")
    with bind_log_context(consumer="w1"):
        with bind_log_context(job_id="abc"):
            log.info("inner")
        record_inner = _last_record(capsys)
        log.info("outer")
        record_outer = _last_record(capsys)
    assert record_inner["consumer"] == "w1" and record_inner["job_id"] == "abc"
    assert record_outer["consumer"] == "w1" and "job_id" not in record_outer


def test_context_not_inherited_by_threads(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")

    def emit():
        log.info("from-thread")

    with bind_log_context(job_id="abc"):
        thread = threading.Thread(target=emit)
        thread.start()
        thread.join()
    assert "job_id" not in _last_record(capsys)


def test_third_party_suppressed_app_passes(capsys):
    configure_logging("INFO")
    logging.getLogger("sqlalchemy.engine").info("third-party info")
    logging.getLogger("app.worker").info("app info")
    lines = [line for line in capsys.readouterr().out.strip().splitlines() if line]
    events = [json.loads(line)["event"] for line in lines]
    assert "app info" in events
    assert "third-party info" not in events


def test_exception_rendered(capsys):
    configure_logging("INFO")
    log = logging.getLogger("app.test")
    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("ticker.tick_failed")
    record = _last_record(capsys)
    assert record["event"] == "ticker.tick_failed"
    assert record["level"] == "error"
    assert "ValueError: boom" in record["exception"]


def test_no_trace_ids_without_instrumentation(capsys):
    configure_logging("INFO")
    logging.getLogger("app.test").info("plain")
    assert "trace_id" not in _last_record(capsys)
