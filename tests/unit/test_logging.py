import json

import pytest
import structlog

from app.core.logging import configure_logging


@pytest.fixture(autouse=True)
def _reset_structlog():
    structlog.reset_defaults()


def test_configure_logging_emits_json_with_context(capsys):
    configure_logging("INFO")
    log = structlog.get_logger("test")
    with structlog.contextvars.bound_contextvars(job_id="abc"):
        log.info("job.received", job_type="email")
    out = capsys.readouterr().out.strip().splitlines()[-1]
    record = json.loads(out)
    assert record["event"] == "job.received"
    assert record["job_id"] == "abc"
    assert record["job_type"] == "email"


def test_contextvars_cleared_after_block(capsys):
    configure_logging("INFO")
    log = structlog.get_logger("test")
    with structlog.contextvars.bound_contextvars(job_id="abc"):
        pass
    log.info("after")
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "job_id" not in record
