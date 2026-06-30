# Distributed Job Processing — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the basic flow of a distributed background job processing system — submit a job over an API, queue it in Redis Streams, process it with multiple concurrent worker processes, and persist its state and result in PostgreSQL.

**Architecture:** FastAPI API writes each job to Postgres (source of truth), commits, then `XADD`s the job id to a single Redis Stream consumer group. Identical worker processes each pull one job at a time, run a mock handler, update Postgres, and ack. Concurrency comes from running multiple worker replicas on the shared consumer group. Delivery is at-least-once (claim-guard + ack-after-commit).

**Tech Stack:** Python 3.11+, FastAPI, uvicorn, SQLAlchemy 2.0 (sync) + Alembic, psycopg 3, redis-py (sync), Pydantic 2 + pydantic-settings, structlog, pytest + fakeredis + testcontainers, Docker Compose. Managed with `uv`.

**Spec:** `docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md`

## Global Constraints

- **Python:** `requires-python >= 3.11`.
- **Tooling:** `uv` only. Run everything via `uv run …`; add deps via `uv add …` / `uv add --dev …`. Never `pip`, `venv`, or `poetry`.
- **No `print`** anywhere — structured logging via structlog with job context only.
- **Synchronous stack** throughout: sync SQLAlchemy engine/sessions, sync redis-py, sync FastAPI route handlers. No async/await.
- **Source of truth:** PostgreSQL. Redis carries only the job id pointer.
- **Enqueue invariant:** `XADD` happens strictly *after* the Postgres `INSERT` has committed.
- **Delivery:** at-least-once — claim-guard (`pending → processing` conditional update) + `XACK` only after the Postgres state update commits.
- **Consumer name:** unique per process — `f"worker_{os.getenv('HOSTNAME','local')}_{uuid.uuid4().hex[:6]}"`.
- **Status enum** (defined fully up front, Phase 1 uses only `pending/processing/completed/failed`): `scheduled, pending, processing, completed, failed, cancelled`.
- **Job types:** `email`, `webhook`, `report`.
- **Test profiles:** unit tests use `fakeredis` + in-memory/no DB; integration tests use real Postgres + Redis via `testcontainers`.

## File Structure

```
app/
  __init__.py
  main.py              # create_app() factory + module-level app  (api entrypoint)
  core/
    __init__.py
    config.py          # Settings (pydantic-settings) + get_settings()
    logging.py         # configure_logging() — structlog + stdlib/uvicorn hijack
    db.py              # create_engine + sessionmaker helpers
    redis.py           # create_redis_client(settings)
  models/
    __init__.py
    base.py            # DeclarativeBase
    job.py             # Job ORM model
  schemas/             # SHARED package (imported by API and worker)
    __init__.py
    enums.py           # JobType, JobStatus
    payloads.py        # tagged-union payload models + validate_payload()
    results.py         # per-type result models
    api.py             # request/response DTOs
  cursor.py            # encode_cursor / decode_cursor (keyset pagination)
  repository.py        # job data-access: create/get/list/claim/complete/fail
  queue/
    __init__.py
    producer.py        # enqueue(redis, stream, job_id)
    consumer.py        # CONSUMER_NAME, ensure_group, read_one, ack
  jobs/
    __init__.py
    handlers.py        # email/webhook/report handlers + WebhookFailedError
    registry.py        # JobType -> handler, run_handler()
  api/
    __init__.py
    deps.py            # get_db, get_redis FastAPI dependencies
    routes.py          # POST /jobs, GET /jobs/{id}, GET /jobs, GET /health
  worker/
    __init__.py
    runner.py          # process_job(), run_forever()
    __main__.py        # worker process entrypoint  (python -m app.worker)
alembic/
  env.py
  versions/0001_create_jobs.py
alembic.ini
tests/
  unit/                # fakeredis + pure-python
  integration/         # testcontainers Postgres + Redis
  integration/conftest.py
docker-compose.yml
Dockerfile
.dockerignore
pyproject.toml
README.md · DECISIONS.md · AI_USAGE.md
```

The root `main.py` stub is deleted; the real entrypoints are `app.main:app` (uvicorn) and `python -m app.worker`.

---

### Task 1: Project bootstrap & configuration

**Files:**
- Modify: `pyproject.toml` (add dependencies)
- Delete: `main.py` (root stub)
- Create: `app/__init__.py`, `app/core/__init__.py`, `app/core/config.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces:
  - `class Settings(BaseSettings)` with fields `database_url: str`, `redis_url: str`, `jobs_stream: str = "jobs:stream"`, `consumer_group: str = "workers"`, `block_ms: int = 5000`, `log_level: str = "INFO"`.
  - `get_settings() -> Settings` (lru-cached).

- [ ] **Step 1: Add dependencies**

```bash
uv add fastapi "uvicorn[standard]" "sqlalchemy>=2.0" alembic "psycopg[binary]" redis "pydantic>=2" pydantic-settings structlog
uv add --dev pytest fakeredis testcontainers httpx ruff
```

- [ ] **Step 2: Create package markers and delete the stub**

```bash
rm main.py
mkdir -p app/core tests/unit tests/integration
touch app/__init__.py app/core/__init__.py tests/__init__.py tests/unit/__init__.py
```

- [ ] **Step 3: Write the failing test**

`tests/unit/test_config.py`:
```python
from app.core.config import Settings


def test_settings_defaults():
    s = Settings(database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0")
    assert s.jobs_stream == "jobs:stream"
    assert s.consumer_group == "workers"
    assert s.block_ms == 5000
    assert s.log_level == "INFO"


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@h/db")
    monkeypatch.setenv("REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("BLOCK_MS", "1000")
    s = Settings()
    assert s.block_ms == 1000
```

- [ ] **Step 4: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.core.config'`

- [ ] **Step 5: Implement `app/core/config.py`**

```python
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    jobs_stream: str = "jobs:stream"
    consumer_group: str = "workers"
    block_ms: int = 5000
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS (2 passed)

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock app tests
git rm main.py
git commit -m "chore: bootstrap uv deps, package skeleton, and Settings"
```

---

### Task 2: Structured logging

**Files:**
- Create: `app/core/logging.py`
- Test: `tests/unit/test_logging.py`

**Interfaces:**
- Produces: `configure_logging(log_level: str) -> None` — configures structlog to emit JSON, routes stdlib + uvicorn loggers through structlog's `ProcessorFormatter`, and enables `structlog.contextvars` merging so `bound_contextvars` fields appear in output.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_logging.py`:
```python
import json
import logging

import structlog

from app.core.logging import configure_logging


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.core.logging'`

- [ ] **Step 3: Implement `app/core/logging.py`**

```python
import logging
import sys

import structlog

_SHARED_PROCESSORS = [
    structlog.contextvars.merge_contextvars,
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_SHARED_PROCESSORS,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Hijack uvicorn loggers so their records flow through the same formatter.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add app/core/logging.py tests/unit/test_logging.py
git commit -m "feat: structlog JSON logging with stdlib/uvicorn hijack"
```

---

### Task 3: Shared schemas (enums, tagged-union payloads, results, DTOs)

**Files:**
- Create: `app/schemas/__init__.py`, `app/schemas/enums.py`, `app/schemas/payloads.py`, `app/schemas/results.py`, `app/schemas/api.py`
- Test: `tests/unit/test_payloads.py`

**Interfaces:**
- Produces:
  - `enums.JobType(str, Enum)` = `email|webhook|report`; `enums.JobStatus(str, Enum)` = `scheduled|pending|processing|completed|failed|cancelled`.
  - `payloads.EmailPayload|WebhookPayload|ReportPayload` (each has `type: Literal[...]` discriminator); `payloads.JobPayload` annotated union; `payloads.validate_payload(job_type, raw: dict) -> EmailPayload|WebhookPayload|ReportPayload` (raises `pydantic.ValidationError` on mismatch).
  - `results.EmailResult{message_id:str}`, `results.WebhookResult{status:int}`, `results.ReportResult{file_url:str}`.
  - `api.JobSubmission{type:JobType, payload:dict}`, `api.JobAccepted{id,type,status,created_at}`, `api.JobOut{...full job...}`, `api.JobList{items:list[JobOut], next_cursor:str|None}`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_payloads.py`:
```python
import pytest
from pydantic import ValidationError

from app.schemas.enums import JobType
from app.schemas.payloads import EmailPayload, WebhookPayload, validate_payload


def test_validate_email_payload():
    p = validate_payload(JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert isinstance(p, EmailPayload)
    assert p.to == "a@b.com"
    assert p.body is None


def test_validate_webhook_defaults_method():
    p = validate_payload("webhook", {"url": "https://x.test"})
    assert isinstance(p, WebhookPayload)
    assert p.method == "POST"


def test_validate_rejects_missing_required_field():
    with pytest.raises(ValidationError):
        validate_payload(JobType.email, {"subject": "no recipient"})


def test_validate_rejects_unknown_type():
    with pytest.raises(ValueError):
        validate_payload("translate", {"foo": "bar"})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_payloads.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.schemas'`

- [ ] **Step 3: Create `app/schemas/__init__.py`** (empty) and **`app/schemas/enums.py`**

```python
from enum import Enum


class JobType(str, Enum):
    email = "email"
    webhook = "webhook"
    report = "report"


class JobStatus(str, Enum):
    scheduled = "scheduled"
    pending = "pending"
    processing = "processing"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"
```

- [ ] **Step 4: Implement `app/schemas/payloads.py`**

```python
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter

from app.schemas.enums import JobType


class EmailPayload(BaseModel):
    type: Literal[JobType.email] = JobType.email
    to: str
    subject: str
    body: str | None = None


class WebhookPayload(BaseModel):
    type: Literal[JobType.webhook] = JobType.webhook
    url: str
    method: str = "POST"


class ReportPayload(BaseModel):
    type: Literal[JobType.report] = JobType.report
    report_type: str
    params: dict | None = None


JobPayload = Annotated[
    Union[EmailPayload, WebhookPayload, ReportPayload],
    Field(discriminator="type"),
]

_ADAPTER: TypeAdapter = TypeAdapter(JobPayload)


def validate_payload(
    job_type: JobType | str, raw: dict
) -> EmailPayload | WebhookPayload | ReportPayload:
    job_type = JobType(job_type)  # raises ValueError on unknown type
    return _ADAPTER.validate_python({**raw, "type": job_type.value})
```

- [ ] **Step 5: Implement `app/schemas/results.py`**

```python
from pydantic import BaseModel


class EmailResult(BaseModel):
    message_id: str


class WebhookResult(BaseModel):
    status: int


class ReportResult(BaseModel):
    file_url: str
```

- [ ] **Step 6: Implement `app/schemas/api.py`**

```python
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.schemas.enums import JobStatus, JobType


class JobSubmission(BaseModel):
    type: JobType
    payload: dict


class JobAccepted(BaseModel):
    id: UUID
    type: JobType
    status: JobStatus
    created_at: datetime


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: JobType
    status: JobStatus
    payload: dict
    result: dict | None
    error: dict | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class JobList(BaseModel):
    items: list[JobOut]
    next_cursor: str | None
```

- [ ] **Step 7: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_payloads.py -v`
Expected: PASS (4 passed)

- [ ] **Step 8: Commit**

```bash
git add app/schemas tests/unit/test_payloads.py
git commit -m "feat: shared schemas with discriminated-union payload validation"
```

---

### Task 4: Job ORM model & database session helpers

**Files:**
- Create: `app/models/__init__.py`, `app/models/base.py`, `app/models/job.py`, `app/core/db.py`
- Test: `tests/unit/test_job_model.py`

**Interfaces:**
- Consumes: `app.schemas.enums.JobType`, `JobStatus`.
- Produces:
  - `models.base.Base` (DeclarativeBase).
  - `models.job.Job` ORM mapped class — columns `id: UUID` (pk, default `uuid4`), `type: JobType`, `payload: dict`, `status: JobStatus` (default `pending`), `result: dict|None`, `error: dict|None`, `created_at: datetime` (server default `now()`), `started_at: datetime|None`, `completed_at: datetime|None`. `__tablename__ = "jobs"`.
  - `core.db.make_engine(database_url) -> Engine`; `core.db.make_session_factory(engine) -> sessionmaker[Session]`.

- [ ] **Step 1: Write the failing test**

`tests/unit/test_job_model.py`:
```python
import uuid

from app.models.job import Job
from app.schemas.enums import JobStatus, JobType


def test_job_table_and_columns():
    assert Job.__tablename__ == "jobs"
    cols = set(Job.__table__.columns.keys())
    assert cols == {
        "id", "type", "payload", "status", "result", "error",
        "created_at", "started_at", "completed_at",
    }


def test_job_defaults_when_instantiated():
    j = Job(type=JobType.email, payload={"to": "a@b.com", "subject": "Hi"})
    assert isinstance(j.id, uuid.UUID)
    assert j.status is JobStatus.pending
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_job_model.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.models.job'`

- [ ] **Step 3: Create `app/models/__init__.py`** (empty) and **`app/models/base.py`**

```python
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
```

- [ ] **Step 4: Implement `app/models/job.py`**

```python
import uuid
from datetime import datetime

from sqlalchemy import Enum as SAEnum
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base
from app.schemas.enums import JobStatus, JobType


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    type: Mapped[JobType] = mapped_column(SAEnum(JobType, name="job_type"))
    payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus, name="job_status"),
        default=JobStatus.pending,
        index=True,
    )
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
```

> Note: `default=JobStatus.pending` and `default=uuid.uuid4` are Python-side defaults, so a freshly instantiated `Job()` already has them set (the unit test relies on this — no DB round-trip needed).

- [ ] **Step 5: Implement `app/core/db.py`**

```python
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def make_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True, future=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_job_model.py -v`
Expected: PASS (2 passed)

- [ ] **Step 7: Commit**

```bash
git add app/models app/core/db.py tests/unit/test_job_model.py
git commit -m "feat: Job ORM model and DB session helpers"
```

---

### Task 5: Alembic setup, initial migration & Postgres test fixtures

**Files:**
- Create: `alembic.ini`, `alembic/env.py`, `alembic/script.py.mako`, `alembic/versions/0001_create_jobs.py`
- Create: `tests/integration/__init__.py`, `tests/integration/conftest.py`
- Test: `tests/integration/test_migration.py`

**Interfaces:**
- Consumes: `app.models.base.Base`, `app.models.job.Job`, `app.core.config.Settings`.
- Produces (fixtures, in `tests/integration/conftest.py`):
  - `postgres_container` (session) → running Postgres.
  - `database_url` (session) → `postgresql+psycopg://…` URL of that container.
  - `pg_engine` (session) → `Engine` after `alembic upgrade head` ran against it.
  - `db_session` (function) → `Session`; truncates `jobs` after each test.

- [ ] **Step 1: Scaffold alembic**

```bash
uv run alembic init -t generic alembic
```
This creates `alembic.ini`, `alembic/env.py`, `alembic/script.py.mako`, `alembic/versions/`.

- [ ] **Step 2: Replace `alembic/env.py`** with a version that reads the URL from env/config and uses our metadata

```python
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.models.base import Base
from app.models.job import Job  # noqa: F401  (ensure model is imported/registered)

config = context.config

db_url = os.getenv("DATABASE_URL") or config.get_main_option("sqlalchemy.url")
config.set_main_option("sqlalchemy.url", db_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 3: Write the initial migration `alembic/versions/0001_create_jobs.py`**

```python
"""create jobs table

Revision ID: 0001
Revises:
Create Date: 2026-06-30
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

JOB_TYPE = postgresql.ENUM("email", "webhook", "report", name="job_type")
JOB_STATUS = postgresql.ENUM(
    "scheduled", "pending", "processing", "completed", "failed", "cancelled",
    name="job_status",
)


def upgrade() -> None:
    bind = op.get_bind()
    JOB_TYPE.create(bind, checkfirst=True)
    JOB_STATUS.create(bind, checkfirst=True)

    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("type", JOB_TYPE, nullable=False),
        sa.Column("payload", postgresql.JSONB, nullable=False),
        sa.Column("status", JOB_STATUS, nullable=False),
        sa.Column("result", postgresql.JSONB, nullable=True),
        sa.Column("error", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_jobs_status", "jobs", ["status"])
    op.create_index("ix_jobs_type", "jobs", ["type"])
    op.create_index("ix_jobs_created_at_id", "jobs", ["created_at", "id"])


def downgrade() -> None:
    op.drop_index("ix_jobs_created_at_id", table_name="jobs")
    op.drop_index("ix_jobs_type", table_name="jobs")
    op.drop_index("ix_jobs_status", table_name="jobs")
    op.drop_table("jobs")
    JOB_STATUS.drop(op.get_bind(), checkfirst=True)
    JOB_TYPE.drop(op.get_bind(), checkfirst=True)
```

- [ ] **Step 4: Write `tests/integration/conftest.py`**

```python
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

from app.core.db import make_engine, make_session_factory


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16", driver="psycopg") as pg:
        yield pg


@pytest.fixture(scope="session")
def database_url(postgres_container) -> str:
    return postgres_container.get_connection_url()


@pytest.fixture(scope="session")
def pg_engine(database_url):
    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "alembic")
    cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(cfg, "head")
    engine = make_engine(database_url)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(pg_engine):
    factory = make_session_factory(pg_engine)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        with pg_engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE jobs"))
```

- [ ] **Step 5: Write the failing test `tests/integration/test_migration.py`**

```python
from sqlalchemy import inspect


def test_jobs_table_exists_with_indexes(pg_engine):
    insp = inspect(pg_engine)
    assert "jobs" in insp.get_table_names()
    index_names = {ix["name"] for ix in insp.get_indexes("jobs")}
    assert "ix_jobs_created_at_id" in index_names
```

- [ ] **Step 6: Run test to verify it fails, then passes**

Run: `uv run pytest tests/integration/test_migration.py -v`
Expected first run before files complete: FAIL. After Steps 2–4 are in place: PASS (1 passed). (Requires Docker running.)

- [ ] **Step 7: Commit**

```bash
git add alembic alembic.ini tests/integration
git commit -m "feat: alembic setup, initial jobs migration, Postgres test fixtures"
```

---

### Task 6: Cursor codec (keyset pagination)

**Files:**
- Create: `app/cursor.py`
- Test: `tests/unit/test_cursor.py`

**Interfaces:**
- Produces: `encode_cursor(created_at: datetime, job_id: UUID) -> str` (opaque base64); `decode_cursor(cursor: str) -> tuple[datetime, UUID]` (raises `ValueError` on malformed input).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_cursor.py`:
```python
import uuid
from datetime import datetime, timezone

import pytest

from app.cursor import decode_cursor, encode_cursor


def test_cursor_round_trip():
    created = datetime(2026, 6, 30, 12, 0, 0, tzinfo=timezone.utc)
    jid = uuid.uuid4()
    token = encode_cursor(created, jid)
    got_created, got_id = decode_cursor(token)
    assert got_created == created
    assert got_id == jid


def test_decode_rejects_garbage():
    with pytest.raises(ValueError):
        decode_cursor("not-a-real-cursor")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_cursor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.cursor'`

- [ ] **Step 3: Implement `app/cursor.py`**

```python
import base64
import json
from datetime import datetime
from uuid import UUID


def encode_cursor(created_at: datetime, job_id: UUID) -> str:
    raw = json.dumps({"c": created_at.isoformat(), "i": str(job_id)}).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        data = json.loads(raw)
        return datetime.fromisoformat(data["c"]), UUID(data["i"])
    except (ValueError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError("invalid cursor") from exc
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_cursor.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add app/cursor.py tests/unit/test_cursor.py
git commit -m "feat: opaque keyset pagination cursor codec"
```

---

### Task 7: Job repository (data access)

**Files:**
- Create: `app/repository.py`
- Test: `tests/integration/test_repository.py`

**Interfaces:**
- Consumes: `app.models.job.Job`, `app.schemas.enums.JobType/JobStatus`, `app.cursor.encode_cursor/decode_cursor`, `db_session` fixture.
- Produces:
  - `create_job(session, job_type: JobType, payload: dict) -> Job` (commits, status `pending`).
  - `get_job(session, job_id: UUID) -> Job | None`.
  - `list_jobs(session, *, status: JobStatus|None=None, job_type: JobType|None=None, limit: int=50, cursor: str|None=None) -> tuple[list[Job], str|None]` (ordered `created_at DESC, id DESC`; returns page + `next_cursor`).
  - `claim_job(session, job_id: UUID) -> bool` (conditional `pending → processing`, sets `started_at`, commits; `True` iff a row was claimed).
  - `complete_job(session, job_id: UUID, result: dict) -> None` (status `completed`, sets `result`, `completed_at`, commits).
  - `fail_job(session, job_id: UUID, error: dict) -> None` (status `failed`, sets `error`, `completed_at`, commits).

- [ ] **Step 1: Write the failing test**

`tests/integration/test_repository.py`:
```python
import uuid

from app import repository as repo
from app.schemas.enums import JobStatus, JobType


def test_create_and_get(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert job.status is JobStatus.pending
    fetched = repo.get_job(db_session, job.id)
    assert fetched.id == job.id


def test_get_missing_returns_none(db_session):
    assert repo.get_job(db_session, uuid.uuid4()) is None


def test_claim_guard_only_succeeds_once(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    assert repo.claim_job(db_session, job.id) is True
    assert repo.claim_job(db_session, job.id) is False  # already processing
    db_session.refresh(job)
    assert job.status is JobStatus.processing
    assert job.started_at is not None


def test_complete_and_fail(db_session):
    j1 = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.claim_job(db_session, j1.id)
    repo.complete_job(db_session, j1.id, {"message_id": "m-1"})
    db_session.refresh(j1)
    assert j1.status is JobStatus.completed
    assert j1.result == {"message_id": "m-1"}
    assert j1.completed_at is not None

    j2 = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    repo.claim_job(db_session, j2.id)
    repo.fail_job(db_session, j2.id, {"type": "WebhookFailedError", "message": "boom"})
    db_session.refresh(j2)
    assert j2.status is JobStatus.failed
    assert j2.error["type"] == "WebhookFailedError"


def test_list_filters_and_cursor(db_session):
    for _ in range(3):
        repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    repo.create_job(db_session, JobType.report, {"report_type": "sales"})

    emails, _ = repo.list_jobs(db_session, job_type=JobType.email)
    assert len(emails) == 3

    page1, cursor = repo.list_jobs(db_session, limit=2)
    assert len(page1) == 2
    assert cursor is not None
    page2, cursor2 = repo.list_jobs(db_session, limit=2, cursor=cursor)
    assert len(page2) == 2
    assert cursor2 is None
    ids = {j.id for j in page1} | {j.id for j in page2}
    assert len(ids) == 4
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.repository'`

- [ ] **Step 3: Implement `app/repository.py`**

```python
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import and_, or_, select, tuple_, update
from sqlalchemy.orm import Session

from app.cursor import decode_cursor, encode_cursor
from app.models.job import Job
from app.schemas.enums import JobStatus, JobType


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_job(session: Session, job_type: JobType, payload: dict) -> Job:
    job = Job(type=job_type, payload=payload, status=JobStatus.pending)
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def get_job(session: Session, job_id: UUID) -> Job | None:
    return session.get(Job, job_id)


def list_jobs(
    session: Session,
    *,
    status: JobStatus | None = None,
    job_type: JobType | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> tuple[list[Job], str | None]:
    stmt = select(Job)
    if status is not None:
        stmt = stmt.where(Job.status == status)
    if job_type is not None:
        stmt = stmt.where(Job.type == job_type)
    if cursor is not None:
        c_created, c_id = decode_cursor(cursor)
        stmt = stmt.where(tuple_(Job.created_at, Job.id) < (c_created, c_id))
    stmt = stmt.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit + 1)

    rows = list(session.execute(stmt).scalars())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = encode_cursor(last.created_at, last.id)
    return rows, next_cursor


def claim_job(session: Session, job_id: UUID) -> bool:
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.pending)
        .values(status=JobStatus.processing, started_at=_now())
    )
    result = session.execute(stmt)
    session.commit()
    return result.rowcount == 1


def complete_job(session: Session, job_id: UUID, result: dict) -> None:
    session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(status=JobStatus.completed, result=result, completed_at=_now())
    )
    session.commit()


def fail_job(session: Session, job_id: UUID, error: dict) -> None:
    session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(status=JobStatus.failed, error=error, completed_at=_now())
    )
    session.commit()
```

> Note: imports `and_`, `or_` are unused — remove them so `ruff` passes. (Keep `select`, `tuple_`, `update`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Lint & commit**

```bash
uv run ruff check --fix app/repository.py
git add app/repository.py tests/integration/test_repository.py
git commit -m "feat: job repository with claim guard and keyset listing"
```

---

### Task 8: Redis client, producer & consumer + Redis test fixtures

**Files:**
- Create: `app/core/redis.py`, `app/queue/__init__.py`, `app/queue/producer.py`, `app/queue/consumer.py`
- Modify: `tests/integration/conftest.py` (add Redis fixtures)
- Test: `tests/integration/test_queue.py`

**Interfaces:**
- Consumes: `redis.Redis`.
- Produces:
  - `core.redis.create_redis_client(redis_url: str) -> redis.Redis` (`decode_responses=True`).
  - `queue.producer.enqueue(client, stream: str, job_id: str) -> str` (`XADD`, returns message id).
  - `queue.consumer.CONSUMER_NAME: str` (module-level, unique per process).
  - `queue.consumer.ensure_group(client, stream: str, group: str) -> None` (idempotent `XGROUP CREATE … MKSTREAM`).
  - `queue.consumer.read_one(client, stream, group, consumer, block_ms) -> tuple[str, dict] | None` (`XREADGROUP COUNT 1`).
  - `queue.consumer.ack(client, stream, group, message_id) -> None` (`XACK`).
- New fixtures: `redis_container` (session), `redis_client` (function, flushes db after each test).

- [ ] **Step 1: Implement `app/core/redis.py`**

```python
import redis


def create_redis_client(redis_url: str) -> redis.Redis:
    return redis.Redis.from_url(redis_url, decode_responses=True)
```

- [ ] **Step 2: Implement `app/queue/producer.py`**

```python
import redis


def enqueue(client: redis.Redis, stream: str, job_id: str) -> str:
    return client.xadd(stream, {"job_id": job_id})
```

- [ ] **Step 3: Implement `app/queue/consumer.py`**

```python
import os
import uuid

import redis

CONSUMER_NAME = f"worker_{os.getenv('HOSTNAME', 'local')}_{uuid.uuid4().hex[:6]}"


def ensure_group(client: redis.Redis, stream: str, group: str) -> None:
    try:
        client.xgroup_create(name=stream, groupname=group, id="$", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def read_one(
    client: redis.Redis, stream: str, group: str, consumer: str, block_ms: int
) -> tuple[str, dict] | None:
    resp = client.xreadgroup(
        groupname=group,
        consumername=consumer,
        streams={stream: ">"},
        count=1,
        block=block_ms,
    )
    if not resp:
        return None
    _stream, messages = resp[0]
    message_id, fields = messages[0]
    return message_id, fields


def ack(client: redis.Redis, stream: str, group: str, message_id: str) -> None:
    client.xack(stream, group, message_id)
```

- [ ] **Step 4: Add Redis fixtures to `tests/integration/conftest.py`**

```python
from testcontainers.redis import RedisContainer  # add to imports

from app.core.redis import create_redis_client  # add to imports


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7") as rc:
        yield rc


@pytest.fixture
def redis_client(redis_container):
    url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    client = create_redis_client(url)
    yield client
    client.flushdb()
    client.close()
```

- [ ] **Step 5: Write the failing test `tests/integration/test_queue.py`**

```python
from app.queue.consumer import ack, ensure_group, read_one
from app.queue.producer import enqueue

STREAM = "jobs:stream"
GROUP = "workers"


def test_ensure_group_is_idempotent(redis_client):
    ensure_group(redis_client, STREAM, GROUP)
    ensure_group(redis_client, STREAM, GROUP)  # must not raise


def test_enqueue_read_ack_cycle(redis_client):
    ensure_group(redis_client, STREAM, GROUP)
    enqueue(redis_client, STREAM, "job-123")

    msg = read_one(redis_client, STREAM, GROUP, "consumer-a", block_ms=1000)
    assert msg is not None
    message_id, fields = msg
    assert fields["job_id"] == "job-123"

    # Still pending until acked.
    pending = redis_client.xpending(STREAM, GROUP)
    assert pending["pending"] == 1

    ack(redis_client, STREAM, GROUP, message_id)
    pending_after = redis_client.xpending(STREAM, GROUP)
    assert pending_after["pending"] == 0


def test_read_returns_none_when_empty(redis_client):
    ensure_group(redis_client, STREAM, GROUP)
    assert read_one(redis_client, STREAM, GROUP, "consumer-a", block_ms=100) is None
```

- [ ] **Step 6: Run test to verify it fails, then passes**

Run: `uv run pytest tests/integration/test_queue.py -v`
Expected: after implementation, PASS (3 passed). (Requires Docker.)

- [ ] **Step 7: Commit**

```bash
git add app/core/redis.py app/queue tests/integration/conftest.py tests/integration/test_queue.py
git commit -m "feat: redis client, stream producer/consumer, and PEL-verified tests"
```

---

### Task 9: Job handlers & registry

**Files:**
- Create: `app/jobs/__init__.py`, `app/jobs/handlers.py`, `app/jobs/registry.py`
- Test: `tests/unit/test_handlers.py`

**Interfaces:**
- Consumes: `app.schemas.payloads.EmailPayload/WebhookPayload/ReportPayload`, `app.schemas.enums.JobType`.
- Produces:
  - `handlers.WebhookFailedError(Exception)`.
  - `handlers.handle_email(payload: EmailPayload) -> dict` (sleeps 1–3s, returns `{"message_id": ...}`).
  - `handlers.handle_webhook(payload: WebhookPayload) -> dict` (sleeps 1–2s, 80% `{"status": 200}`, 20% raises `WebhookFailedError`).
  - `handlers.handle_report(payload: ReportPayload) -> dict` (sleeps 3–5s, returns `{"file_url": ...}`).
  - `registry.run_handler(job_type: JobType, payload) -> dict` (looks up handler, calls it).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_handlers.py`:
```python
import pytest

from app.jobs import handlers
from app.jobs.registry import run_handler
from app.schemas.enums import JobType
from app.schemas.payloads import EmailPayload, ReportPayload, WebhookPayload


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def test_email_returns_message_id():
    out = handlers.handle_email(EmailPayload(to="a@b.com", subject="Hi"))
    assert "message_id" in out


def test_report_returns_file_url():
    out = handlers.handle_report(ReportPayload(report_type="sales"))
    assert out["file_url"].startswith("https://")


def test_webhook_success_branch(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.5)  # >= 0.2 → success
    out = handlers.handle_webhook(WebhookPayload(url="https://x.test"))
    assert out == {"status": 200}


def test_webhook_failure_branch(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # < 0.2 → failure
    with pytest.raises(handlers.WebhookFailedError):
        handlers.handle_webhook(WebhookPayload(url="https://x.test"))


def test_run_handler_dispatches_by_type():
    out = run_handler(JobType.email, EmailPayload(to="a@b.com", subject="Hi"))
    assert "message_id" in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_handlers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.jobs'`

- [ ] **Step 3: Implement `app/jobs/handlers.py`**

```python
import random
import time
import uuid

from app.schemas.payloads import EmailPayload, ReportPayload, WebhookPayload


class WebhookFailedError(Exception):
    """Raised when the simulated webhook call fails."""


def handle_email(payload: EmailPayload) -> dict:
    time.sleep(random.uniform(1, 3))
    return {"message_id": f"msg_{uuid.uuid4().hex[:12]}"}


def handle_webhook(payload: WebhookPayload) -> dict:
    time.sleep(random.uniform(1, 2))
    if random.random() < 0.2:
        raise WebhookFailedError(f"webhook call to {payload.url} failed")
    return {"status": 200}


def handle_report(payload: ReportPayload) -> dict:
    time.sleep(random.uniform(3, 5))
    return {"file_url": f"https://reports.local/{uuid.uuid4().hex[:12]}.pdf"}
```

- [ ] **Step 4: Implement `app/jobs/registry.py`**

```python
from typing import Callable

from app.jobs.handlers import handle_email, handle_report, handle_webhook
from app.schemas.enums import JobType

HANDLERS: dict[JobType, Callable[[object], dict]] = {
    JobType.email: handle_email,
    JobType.webhook: handle_webhook,
    JobType.report: handle_report,
}


def run_handler(job_type: JobType, payload) -> dict:
    return HANDLERS[job_type](payload)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_handlers.py -v`
Expected: PASS (5 passed)

- [ ] **Step 6: Commit**

```bash
git add app/jobs tests/unit/test_handlers.py
git commit -m "feat: mock job handlers and dispatch registry"
```

---

### Task 10: API service (routes, deps, app factory)

**Files:**
- Create: `app/api/__init__.py`, `app/api/deps.py`, `app/api/routes.py`, `app/main.py`
- Modify: `tests/integration/conftest.py` (add `test_settings` + `client` fixtures)
- Test: `tests/integration/test_api.py`

**Interfaces:**
- Consumes: `app.repository`, `app.queue.producer.enqueue`, `app.queue.consumer.ensure_group`, `app.schemas.api`, `app.core.*`.
- Produces:
  - `api.deps.get_db(request) -> Iterator[Session]`; `api.deps.get_redis(request) -> redis.Redis`.
  - `api.routes.router` with `POST /jobs`, `GET /jobs/{job_id}`, `GET /jobs`, `GET /health`.
  - `main.create_app(settings: Settings | None = None) -> FastAPI`; module-level `main.app`.

- [ ] **Step 1: Implement `app/api/deps.py`**

```python
from collections.abc import Iterator

import redis
from fastapi import Request
from sqlalchemy.orm import Session


def get_db(request: Request) -> Iterator[Session]:
    factory = request.app.state.session_factory
    with factory() as session:
        yield session


def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis
```

- [ ] **Step 2: Implement `app/api/routes.py`**

```python
from uuid import UUID

import redis
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app import repository as repo
from app.api.deps import get_db, get_redis
from app.queue.producer import enqueue
from app.schemas.api import JobAccepted, JobList, JobOut, JobSubmission
from app.schemas.enums import JobStatus, JobType
from app.schemas.payloads import validate_payload

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.post("/jobs", response_model=JobAccepted, status_code=202)
def submit_job(
    submission: JobSubmission,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobAccepted:
    try:
        validate_payload(submission.type, submission.payload)
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    job = repo.create_job(session, submission.type, submission.payload)
    # Enqueue invariant: XADD only after the INSERT has committed (create_job commits).
    enqueue(client, client.connection_pool.connection_kwargs and "jobs:stream" or "jobs:stream", str(job.id))
    return JobAccepted(id=job.id, type=job.type, status=job.status, created_at=job.created_at)


@router.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: UUID, session: Session = Depends(get_db)) -> JobOut:
    job = repo.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobOut.model_validate(job)


@router.get("/jobs", response_model=JobList)
def list_jobs(
    session: Session = Depends(get_db),
    status: JobStatus | None = Query(default=None),
    type: JobType | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None),
) -> JobList:
    try:
        jobs, next_cursor = repo.list_jobs(
            session, status=status, job_type=type, limit=limit, cursor=cursor
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc
    return JobList(items=[JobOut.model_validate(j) for j in jobs], next_cursor=next_cursor)
```

> Fix before committing: the `enqueue(...)` stream argument above is deliberately wrong to force you to wire the real stream name. Replace that line with the clean version in Step 3.

- [ ] **Step 3: Fix the enqueue call to use settings**

Replace the `enqueue(...)` line in `submit_job` with a settings-driven stream name. Update the function to read the stream from app state:

```python
@router.post("/jobs", response_model=JobAccepted, status_code=202)
def submit_job(
    submission: JobSubmission,
    request: Request,
    session: Session = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
) -> JobAccepted:
    try:
        validate_payload(submission.type, submission.payload)
    except (ValidationError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    job = repo.create_job(session, submission.type, submission.payload)
    # Enqueue invariant: XADD only after create_job() committed the INSERT.
    enqueue(client, request.app.state.settings.jobs_stream, str(job.id))
    return JobAccepted(id=job.id, type=job.type, status=job.status, created_at=job.created_at)
```

Add `from fastapi import Request` to the imports.

- [ ] **Step 4: Implement `app/main.py`**

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import Settings, get_settings
from app.core.db import make_engine, make_session_factory
from app.core.logging import configure_logging
from app.core.redis import create_redis_client
from app.queue.consumer import ensure_group


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    redis_client = create_redis_client(settings.redis_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        ensure_group(redis_client, settings.jobs_stream, settings.consumer_group)
        yield
        redis_client.close()
        engine.dispose()

    app = FastAPI(title="Job Processor", lifespan=lifespan)
    app.state.settings = settings
    app.state.session_factory = session_factory
    app.state.redis = redis_client
    app.include_router(router)
    return app


app = create_app()
```

> `app = create_app()` runs at import with real env settings; for uvicorn (`app.main:app`) that's correct. Tests build their own app via `create_app(test_settings)` and never import this module-level `app`, so no DB/Redis connection is attempted at import time during tests.

- [ ] **Step 5: Add `test_settings` + `client` fixtures to `tests/integration/conftest.py`**

```python
from fastapi.testclient import TestClient  # add to imports

from app.core.config import Settings  # add to imports
from app.main import create_app  # add to imports


@pytest.fixture
def test_settings(database_url, redis_container) -> Settings:
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    return Settings(database_url=database_url, redis_url=redis_url)


@pytest.fixture
def client(pg_engine, test_settings):
    app = create_app(test_settings)
    with TestClient(app) as c:
        yield c
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE jobs"))
```

> `pg_engine` is included so migrations are applied before the client runs. The `text` import already exists in conftest from Task 5.

- [ ] **Step 6: Write the failing test `tests/integration/test_api.py`**

```python
def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_submit_creates_job_and_enqueues(client):
    resp = client.post("/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}})
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "pending"
    assert body["type"] == "email"

    # Job is fetchable.
    got = client.get(f"/jobs/{body['id']}")
    assert got.status_code == 200
    assert got.json()["payload"]["to"] == "a@b.com"

    # And a message was enqueued on the stream.
    redis_client = client.app.state.redis
    stream = client.app.state.settings.jobs_stream
    assert redis_client.xlen(stream) == 1


def test_submit_rejects_bad_payload(client):
    resp = client.post("/jobs", json={"type": "email", "payload": {"subject": "no recipient"}})
    assert resp.status_code == 422


def test_submit_rejects_unknown_type(client):
    resp = client.post("/jobs", json={"type": "translate", "payload": {}})
    assert resp.status_code == 422


def test_get_missing_returns_404(client):
    import uuid

    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404


def test_list_filters_by_type(client):
    client.post("/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}})
    client.post("/jobs", json={"type": "report", "payload": {"report_type": "sales"}})
    resp = client.get("/jobs", params={"type": "email"})
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["type"] == "email"
```

- [ ] **Step 7: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_api.py -v`
Expected: PASS (6 passed). (Requires Docker.)

- [ ] **Step 8: Lint & commit**

```bash
uv run ruff check --fix app/api app/main.py
git add app/api app/main.py tests/integration/conftest.py tests/integration/test_api.py
git commit -m "feat: FastAPI submit/get/list/health endpoints"
```

---

### Task 11: Worker (job processing loop)

**Files:**
- Create: `app/worker/__init__.py`, `app/worker/runner.py`, `app/worker/__main__.py`
- Test: `tests/integration/test_worker.py`

**Interfaces:**
- Consumes: `app.repository`, `app.jobs.registry.run_handler`, `app.schemas.payloads.validate_payload`, `app.queue.consumer.*`, `app.queue.producer.enqueue`.
- Produces:
  - `worker.runner.process_job(session, job_id: UUID) -> None` — claim guard; if not claimed, returns (duplicate/no-op); else validates payload, runs handler, then `complete_job` on success or `fail_job` on any exception.
  - `worker.runner.run_forever(settings, *, stop: Callable[[], bool] = ...) -> None` — XREADGROUP loop with per-iteration `bound_contextvars`, `ack` after the Postgres update commits, and SIGTERM-driven graceful shutdown.

- [ ] **Step 1: Write the failing test**

`tests/integration/test_worker.py`:
```python
import pytest

from app import repository as repo
from app.jobs import handlers
from app.schemas.enums import JobStatus, JobType
from app.worker.runner import process_job


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


def test_process_job_completes_email(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    process_job(db_session, job.id)
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert "message_id" in job.result


def test_process_job_marks_failure(db_session, monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # force webhook failure
    job = repo.create_job(db_session, JobType.webhook, {"url": "https://x.test"})
    process_job(db_session, job.id)
    db_session.refresh(job)
    assert job.status is JobStatus.failed
    assert job.error["type"] == "WebhookFailedError"


def test_duplicate_delivery_is_noop(db_session):
    job = repo.create_job(db_session, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    process_job(db_session, job.id)
    db_session.refresh(job)
    first_result = job.result
    # Second delivery: already completed, claim guard fails → no change.
    process_job(db_session, job.id)
    db_session.refresh(job)
    assert job.status is JobStatus.completed
    assert job.result == first_result


def test_invalid_payload_fails_job(db_session):
    job = repo.create_job(db_session, JobType.email, {"missing": "recipient"})
    process_job(db_session, job.id)
    db_session.refresh(job)
    assert job.status is JobStatus.failed
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_worker.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.worker'`

- [ ] **Step 3: Implement `app/worker/runner.py`**

```python
import signal
from collections.abc import Callable
from uuid import UUID

import structlog
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_engine, make_session_factory
from app.core.redis import create_redis_client
from app.jobs.registry import run_handler
from app.queue.consumer import CONSUMER_NAME, ack, ensure_group, read_one
from app.schemas.payloads import validate_payload

log = structlog.get_logger("worker")


def process_job(session: Session, job_id: UUID) -> None:
    if not repo.claim_job(session, job_id):
        log.info("job.skipped", reason="not_pending")
        return

    job = repo.get_job(session, job_id)
    try:
        payload = validate_payload(job.type, job.payload)
        result = run_handler(job.type, payload)
    except Exception as exc:  # noqa: BLE001 — any handler/validation error fails the job
        repo.fail_job(session, job_id, {"type": type(exc).__name__, "message": str(exc)})
        log.info("job.failed", error_type=type(exc).__name__)
        return

    repo.complete_job(session, job_id, result)
    log.info("job.completed")


def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> None:
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    client = create_redis_client(settings.redis_url)
    ensure_group(client, settings.jobs_stream, settings.consumer_group)

    shutting_down = {"flag": False}

    def _request_stop(*_):
        shutting_down["flag"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    structlog.contextvars.bind_contextvars(consumer=CONSUMER_NAME)
    log.info("worker.started", stream=settings.jobs_stream, group=settings.consumer_group)

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

    while not _should_stop():
        msg = read_one(
            client, settings.jobs_stream, settings.consumer_group, CONSUMER_NAME, settings.block_ms
        )
        if msg is None:
            continue
        message_id, fields = msg
        job_id = UUID(fields["job_id"])
        with structlog.contextvars.bound_contextvars(job_id=str(job_id), message_id=message_id):
            log.info("job.received")
            with session_factory() as session:
                process_job(session, job_id)
            # Ack only after the Postgres state update has committed (at-least-once).
            ack(client, settings.jobs_stream, settings.consumer_group, message_id)

    log.info("worker.stopped")
    client.close()
    engine.dispose()
```

> The `with bound_contextvars(...)` block scopes `job_id`/`message_id` per iteration so they never leak into the next job's logs, even if an exception fires during cleanup.

- [ ] **Step 4: Implement `app/worker/__main__.py`**

```python
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.worker.runner import run_forever


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    run_forever(settings)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_worker.py -v`
Expected: PASS (4 passed). (Requires Docker.)

- [ ] **Step 6: Add an end-to-end loop test using `stop`**

Append to `tests/integration/test_worker.py`:
```python
def test_run_forever_processes_one_then_stops(test_settings, redis_client, pg_engine):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.worker.runner import run_forever

    ensure_group(redis_client, test_settings.jobs_stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        job = repo.create_job(s, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    enqueue(redis_client, test_settings.jobs_stream, str(job.id))

    calls = {"n": 0}

    def stop() -> bool:
        # Allow exactly one processing pass, then stop.
        if calls["n"] >= 1:
            return True
        calls["n"] += 1
        return False

    run_forever(test_settings, stop=stop)

    with factory() as s:
        refreshed = repo.get_job(s, job.id)
    assert refreshed.status is JobStatus.completed
```

Run: `uv run pytest tests/integration/test_worker.py -v`
Expected: PASS (5 passed).

- [ ] **Step 7: Lint & commit**

```bash
uv run ruff check --fix app/worker
git add app/worker tests/integration/test_worker.py
git commit -m "feat: worker processing loop with at-least-once delivery"
```

---

### Task 12: Docker Compose, Dockerfile & migrate service

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`

**Interfaces:**
- Produces: a `docker compose` stack — `postgres`, `redis`, one-shot `migrate`, `api` (:8000), and scalable `worker`. API and worker run from one image.

- [ ] **Step 1: Write `Dockerfile`**

```dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

EXPOSE 8000
CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Write `.dockerignore`**

```
.git
.venv
__pycache__
*.pyc
tests
docs
.pytest_cache
.ruff_cache
```

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: jobs
      POSTGRES_PASSWORD: jobs
      POSTGRES_DB: jobs
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U jobs"]
      interval: 3s
      timeout: 3s
      retries: 10
    ports:
      - "5432:5432"

  redis:
    image: redis:7
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 3s
      timeout: 3s
      retries: 10
    ports:
      - "6379:6379"

  migrate:
    build: .
    command: uv run alembic upgrade head
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    depends_on:
      postgres:
        condition: service_healthy

  api:
    build: .
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    ports:
      - "8000:8000"
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy

  worker:
    build: .
    command: uv run python -m app.worker
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
```

- [ ] **Step 4: Validate compose config**

Run: `docker compose config`
Expected: prints the resolved configuration with no errors.

- [ ] **Step 5: Smoke test the stack (manual)**

```bash
docker compose up --build -d
# wait for api healthy, then:
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.com","subject":"Hi"}}'
# capture the returned id, then:
curl -s localhost:8000/jobs/<id>
docker compose down -v
```
Expected: submit returns `202` with `status: pending`; after a few seconds the job shows `status: completed` with a `message_id` result.

- [ ] **Step 6: Commit**

```bash
git add Dockerfile .dockerignore docker-compose.yml
git commit -m "feat: docker compose stack with one-shot migrate, api, and scalable worker"
```

---

### Task 13: Deliverable docs (README, DECISIONS, AI_USAGE)

**Files:**
- Modify: `README.md`, `DECISIONS.md`, `AI_USAGE.md`

**Interfaces:** None (documentation).

- [ ] **Step 1: Fill in `README.md`** — replace the outline with real content

Sections to write (with the actual commands):
- **Run:** `docker compose up --build`; scale workers `docker compose up --build --scale worker=3`.
- **Test:** `uv run pytest` (note: integration tests need Docker running for testcontainers; `uv run pytest tests/unit` for the fast subset).
- **Submit a test job:**
  ```bash
  curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
    -d '{"type":"email","payload":{"to":"a@b.com","subject":"Hi"}}'
  curl localhost:8000/jobs/<id>
  curl "localhost:8000/jobs?type=email&limit=20"
  ```
- **Architecture overview:** API → Postgres (commit) → Redis Stream consumer group → worker replicas → Postgres update; at-least-once via claim-guard + ack-after-commit. Link to the spec.

- [ ] **Step 2: Fill in `DECISIONS.md`**

- **1. Job Pickup Strategy:** Redis Streams + one consumer group; each worker process is a competing consumer pulling one job at a time (`XREADGROUP COUNT 1`). Why: built-in load balancing and a per-consumer pending list (PEL) for at-least-once. Trade-off: one job per process means concurrency = replica count.
- **2. Worker Crash Recovery (describe-only):** On crash, the in-flight message stays unacked in the group's PEL. A future reaper would `XAUTOCLAIM` stale messages and reset stuck `processing` jobs; because consumer names are unique per process (prefix + UUID), the reaper must also `XGROUP DELCONSUMER` dead consumers. The claim-guard makes redelivery safe (a completed job re-delivered is a no-op). Also documents the commit-then-`XADD` orphan gap (job committed but process died before `XADD`) and its future mitigation (transactional outbox / pending-sweeper). Not built in Phase 1.
- **3. Priority Queue:** Deferred to Phase 2.
- **4. Retry Backoff:** Deferred to Phase 2 (Phase 1 marks failures terminal).
- **5. One thing I'd do differently:** e.g. add the transactional outbox + reaper, or move to async-concurrent workers (Approach C) for throughput.

- [ ] **Step 3: Fill in `AI_USAGE.md`** — tools used, where AI helped (schema/boilerplate/test scaffolding), what needed fixing (especially concurrency: claim-guard correctness, ack-after-commit ordering, unique consumer names), and what AI struggled with.

- [ ] **Step 4: Run the full unit suite and lint as a final check**

```bash
uv run pytest tests/unit -v
uv run ruff check
uv run ruff format
```
Expected: unit tests pass; ruff reports no issues.

- [ ] **Step 5: Commit**

```bash
git add README.md DECISIONS.md AI_USAGE.md
git commit -m "docs: fill in README, DECISIONS, and AI_USAGE"
```

---

## Self-Review

**Spec coverage:**
- §2 Architecture → Tasks 7/8/10/11/12 ✓
- §3 Data model + indexes → Tasks 4, 5 ✓
- §4 API contract (submit/get/list+cursor/health) → Tasks 6, 10 ✓
- §5 Shared tagged-union schemas, validated by API and worker → Task 3 (schemas), Task 10 (API validation), Task 11 (worker validation) ✓
- §6 Enqueue invariant (commit-then-XADD) → Task 10 submit route ✓
- §7 Worker dispatch, claim-guard, ack-after-commit, unique consumer name, describe-only recovery → Tasks 8, 11, 13 (DECISIONS) ✓
- §8 Handlers (email/webhook 80-20/report) → Task 9 ✓
- §9 Error handling + structlog + contextvars + uvicorn hijack → Tasks 2, 10, 11 ✓
- §10 Project structure → all tasks ✓
- §11 Config → Task 1 ✓
- §12 Test profiles (unit fakeredis / integration testcontainers) → unit Tasks 1-4,6,9; integration Tasks 5,7,8,10,11 ✓
- §13 Orchestration → Task 12 ✓
- §14 Deliverables → Task 13 ✓

**Placeholder scan:** No TBD/TODO. The one intentionally-wrong line (Task 10 Step 2 `enqueue(...)`) is explicitly corrected in Step 3 with a stated reason — kept as a deliberate two-step to make the settings-wiring explicit.

**Type consistency:** `validate_payload(job_type, raw)`, `claim_job(session, job_id) -> bool`, `complete_job/fail_job(session, job_id, …)`, `enqueue(client, stream, job_id)`, `read_one(...) -> tuple[str, dict] | None`, `run_handler(job_type, payload) -> dict`, `process_job(session, job_id)` are used identically across producer, consumer, repository, API, and worker tasks.

**Note on fakeredis:** unit tasks (1–4, 6, 9) need neither Redis nor a DB. The spec's "unit uses fakeredis" applies only where a unit test touches Redis; in this plan all Redis-touching tests are integration (real Redis), so `fakeredis` is available as a declared dev dep but is only needed if a future unit test stubs Redis. This is consistent with the spec's intent (real Redis for stream/PEL fidelity).
