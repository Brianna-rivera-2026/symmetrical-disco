# API Security Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-user rate limiting, webhook/email allowlists, request size caps, stricter payload validation, and a least-privilege Postgres role split, per `docs/superpowers/specs/2026-07-13-api-security-design.md`.

**Architecture:** All request-shaping controls live in the API layer (ASGI middleware + FastAPI dependencies + pydantic validators); allowlist policy is enforced in `validate_payload` so both the API (submit) and the worker (execute) apply it; the DB role split is pure deploy config (SQL init script + DSN wiring in compose and Helm).

**Tech Stack:** FastAPI, pydantic v2, `fastapi-limiter` (Redis-backed), `email-validator`, uvicorn `ProxyHeadersMiddleware`, PostgreSQL roles, Helm/OpenShift.

**Already done (do NOT redo):** `WebhookPayload.url` is already `HttpsUrl` (https-only, `app/schemas/payloads.py`); the `worker-internet-egress` NetworkPolicy already exists in the chart.

## Global Constraints

- Package management is **uv only**: `uv add <pkg>`, `uv run pytest`, `uv run ruff check --fix`, `uv run ruff format`. Never pip/venv/poetry.
- No `print`; use `logging.getLogger("app.<component>")`.
- TDD for all Python code: failing test → minimal implementation → pass → commit.
- **No pytest for infra config** (compose/Helm/SQL init) — verify manually (project convention).
- Defaults from the spec, verbatim: submit 20/min, control 30/min, read 120/min, stats 30/min; `max_request_body_bytes = 262144`; params ≤ 50 keys and ≤ 8 KB serialized; `scheduled_at` ≤ 365 days ahead; `forwarded_allow_ips = "127.0.0.1"`; empty allowlist ⇒ deny all for that job type.
- Roles are named exactly `jobs_migrator` and `jobs_app`.

---

### Task 1: Settings fields

**Files:**
- Modify: `app/core/config.py`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces (used by every later task): `Settings.rate_limit_enabled: bool`, `Settings.submit_rate_limit_per_min: int`, `Settings.control_rate_limit_per_min: int`, `Settings.read_rate_limit_per_min: int`, `Settings.stats_rate_limit_per_min: int`, `Settings.forwarded_allow_ips: str`, `Settings.webhook_allowed_hosts: list[str]`, `Settings.email_allowed_domains: list[str]`, `Settings.max_request_body_bytes: int`.

- [ ] **Step 1: Write the failing test** — append to `tests/unit/test_config.py`:

```python
def test_security_settings_defaults():
    s = Settings(
        database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h:6379/0"
    )
    assert s.rate_limit_enabled is True
    assert s.submit_rate_limit_per_min == 20
    assert s.control_rate_limit_per_min == 30
    assert s.read_rate_limit_per_min == 120
    assert s.stats_rate_limit_per_min == 30
    assert s.forwarded_allow_ips == "127.0.0.1"
    assert s.webhook_allowed_hosts == []
    assert s.email_allowed_domains == []
    assert s.max_request_body_bytes == 262144
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_config.py::test_security_settings_defaults -v`
Expected: FAIL (ValidationError / AttributeError — fields don't exist).

- [ ] **Step 3: Implement** — in `app/core/config.py`, add to `Settings` after `api_user_keys_file`:

```python
    rate_limit_enabled: bool = True
    submit_rate_limit_per_min: int = 20
    control_rate_limit_per_min: int = 30
    read_rate_limit_per_min: int = 120
    stats_rate_limit_per_min: int = 30
    forwarded_allow_ips: str = "127.0.0.1"
    webhook_allowed_hosts: list[str] = []
    email_allowed_domains: list[str] = []
    max_request_body_bytes: int = 262144
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_config.py -v` — Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add app/core/config.py tests/unit/test_config.py
git commit -m "feat: security settings (rate limits, allowlists, size cap, proxy trust)"
```

---

### Task 2: Stricter payload and submission types

**Files:**
- Modify: `app/schemas/enums.py`, `app/schemas/payloads.py`, `app/schemas/api.py`, `app/api/routes.py` (cursor param only)
- Test: `tests/unit/test_payloads.py`, `tests/unit/test_schemas_scheduling.py`

**Interfaces:**
- Produces: `ReportType(str, Enum)` with `sales`, `ops`, `weekly_summary`; all payload models and `JobSubmission` reject unknown keys; `EmailPayload.to: EmailStr`.
- Consumes: nothing new.

- [ ] **Step 1: Add dependency**

Run: `uv add "pydantic[email]"`

- [ ] **Step 2: Write the failing tests** — append to `tests/unit/test_payloads.py`:

```python
def test_email_rejects_invalid_address():
    with pytest.raises(ValidationError):
        validate_payload(JobType.email, {"to": "not-an-email", "subject": "Hi"})


def test_email_rejects_empty_subject():
    with pytest.raises(ValidationError):
        validate_payload(JobType.email, {"to": "a@b.com", "subject": ""})


def test_webhook_rejects_unknown_method():
    with pytest.raises(ValidationError):
        validate_payload("webhook", {"url": "https://x.test", "method": "DELETE"})


def test_report_rejects_unknown_report_type():
    with pytest.raises(ValidationError):
        validate_payload("report", {"report_type": "espionage"})


def test_report_rejects_too_many_params_keys():
    params = {f"k{i}": 1 for i in range(51)}
    with pytest.raises(ValidationError):
        validate_payload("report", {"report_type": "sales", "params": params})


def test_report_rejects_oversized_params():
    with pytest.raises(ValidationError):
        validate_payload("report", {"report_type": "sales", "params": {"k": "x" * 9000}})


def test_payload_rejects_unknown_keys():
    with pytest.raises(ValidationError):
        validate_payload(JobType.email, {"to": "a@b.com", "subject": "Hi", "bcc": "spy@evil.test"})
```

And append to `tests/unit/test_schemas_scheduling.py` (imports at top of file already include `JobSubmission`; add `from datetime import datetime, timedelta, timezone` and `import pytest` / `from pydantic import ValidationError` if missing):

```python
def test_submission_rejects_far_future_schedule():
    far = datetime.now(timezone.utc) + timedelta(days=366)
    with pytest.raises(ValidationError):
        JobSubmission(type="email", payload={}, scheduled_at=far)


def test_submission_rejects_empty_idempotency_key():
    with pytest.raises(ValidationError):
        JobSubmission(type="email", payload={}, idempotency_key="")


def test_submission_rejects_unknown_keys():
    with pytest.raises(ValidationError):
        JobSubmission(type="email", payload={}, surprise=True)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_payloads.py tests/unit/test_schemas_scheduling.py -v`
Expected: the new tests FAIL; existing ones PASS.

- [ ] **Step 4: Implement enums** — in `app/schemas/enums.py`, add:

```python
class ReportType(str, Enum):
    sales = "sales"
    ops = "ops"
    weekly_summary = "weekly_summary"
```

- [ ] **Step 5: Implement payload changes** — `app/schemas/payloads.py`. Add imports (`json`, `ConfigDict`, `EmailStr`, `field_validator`, `ReportType`) and constants; the file already has `HttpsUrl`:

```python
import json
from typing import Annotated, Literal, Union

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    TypeAdapter,
    UrlConstraints,
    field_validator,
)

from app.schemas.enums import JobType, ReportType

MAX_BATCH_ITEMS = 500
MAX_REPORT_PARAMS_KEYS = 50
MAX_REPORT_PARAMS_BYTES = 8192
```

Replace the model definitions (keep `HttpsUrl` as is):

```python
class EmailPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal[JobType.email] = JobType.email
    to: EmailStr = Field(max_length=320)
    subject: str = Field(min_length=1, max_length=500)
    body: str | None = Field(default=None, max_length=20_000)


class WebhookPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal[JobType.webhook] = JobType.webhook
    url: HttpsUrl
    method: Literal["GET", "POST"] = "POST"


class ReportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal[JobType.report] = JobType.report
    report_type: ReportType
    params: dict | None = None

    @field_validator("params")
    @classmethod
    def _bound_params(cls, v: dict | None) -> dict | None:
        if v is None:
            return v
        if len(v) > MAX_REPORT_PARAMS_KEYS:
            raise ValueError(f"params exceeds {MAX_REPORT_PARAMS_KEYS} keys")
        size = len(json.dumps(v, separators=(",", ":"), default=str))
        if size > MAX_REPORT_PARAMS_BYTES:
            raise ValueError(f"params exceeds {MAX_REPORT_PARAMS_BYTES} bytes serialized")
        return v
```

`BatchPayload` gains `model_config = ConfigDict(extra="forbid")` as its first line; everything else unchanged.

- [ ] **Step 6: Implement JobSubmission changes** — `app/schemas/api.py`:

```python
class JobSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: JobType
    payload: dict
    priority: JobPriority = JobPriority.normal
    scheduled_at: datetime | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)

    @field_validator("scheduled_at")
    @classmethod
    def _normalize_utc(cls, v: datetime | None) -> datetime | None:
        if v is None:
            return None
        v = v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
        if v > datetime.now(timezone.utc) + timedelta(days=365):
            raise ValueError("scheduled_at more than 365 days in the future")
        return v
```

Add `timedelta` to the `datetime` import and `Field` to the pydantic import.

- [ ] **Step 7: Cursor cap** — `app/api/routes.py`, `list_jobs`:

```python
    cursor: str | None = Query(default=None, max_length=512),
```

- [ ] **Step 8: Run the full unit suite**

Run: `uv run pytest tests/unit -v`
Expected: all PASS. If a test constructs a report payload with a `report_type` not in the enum, change the test data to `"sales"`.

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml uv.lock app/schemas tests/unit app/api/routes.py
git commit -m "feat: stricter payload types (EmailStr, ReportType enum, extra=forbid, bounds)"
```

---

### Task 3: Allowlist policy in validate_payload (submit + worker, non-retryable)

**Files:**
- Modify: `app/schemas/payloads.py`, `app/api/routes.py:150-154`, `app/worker/runner.py:110-120,164`, `tests/integration/conftest.py:106`, `docs/superpowers/specs/2026-07-13-api-security-design.md` (§2 wording)
- Test: `tests/unit/test_payloads.py`, `tests/integration/test_worker.py`

**Interfaces:**
- Produces: `PayloadPolicyError(ValueError)` in `app/schemas/payloads.py`; `validate_payload(job_type, raw, settings: Settings | None = None)` — when `settings` is provided, allowlists are enforced (including inside batch items); when `None`, schema-only (unit-test convenience).
- Consumes: `Settings.webhook_allowed_hosts`, `Settings.email_allowed_domains` (Task 1).

- [ ] **Step 1: Write the failing unit tests** — append to `tests/unit/test_payloads.py`:

```python
from app.core.config import Settings
from app.schemas.payloads import PayloadPolicyError


def _settings(**overrides):
    return Settings(
        database_url="postgresql+psycopg://u:p@h/db",
        redis_url="redis://h:6379/0",
        **overrides,
    )


def test_webhook_host_suffix_match_allowed():
    s = _settings(webhook_allowed_hosts=["hooks.example.com"])
    p = validate_payload("webhook", {"url": "https://a.hooks.example.com/x"}, s)
    assert isinstance(p, WebhookPayload)


def test_webhook_host_not_allowlisted_rejected():
    s = _settings(webhook_allowed_hosts=["hooks.example.com"])
    with pytest.raises(PayloadPolicyError):
        validate_payload("webhook", {"url": "https://evil.test/x"}, s)


def test_webhook_empty_allowlist_denies_all():
    with pytest.raises(PayloadPolicyError):
        validate_payload("webhook", {"url": "https://x.test"}, _settings())


def test_webhook_suffix_match_requires_label_boundary():
    s = _settings(webhook_allowed_hosts=["hooks.example.com"])
    with pytest.raises(PayloadPolicyError):
        validate_payload("webhook", {"url": "https://evilhooks.example.com.attacker.test"}, s)
    with pytest.raises(PayloadPolicyError):
        validate_payload("webhook", {"url": "https://xhooks.example.com"}, s)


def test_email_domain_allowed_case_insensitive():
    s = _settings(email_allowed_domains=["Example.COM"])
    p = validate_payload(JobType.email, {"to": "a@example.com", "subject": "Hi"}, s)
    assert isinstance(p, EmailPayload)


def test_email_domain_not_allowlisted_rejected():
    s = _settings(email_allowed_domains=["example.com"])
    with pytest.raises(PayloadPolicyError):
        validate_payload(JobType.email, {"to": "a@other.com", "subject": "Hi"}, s)


def test_batch_items_are_policy_checked():
    s = _settings(email_allowed_domains=["example.com"], webhook_allowed_hosts=[])
    with pytest.raises(PayloadPolicyError):
        validate_payload(
            "batch",
            {"items": [
                {"type": "email", "to": "a@example.com", "subject": "ok"},
                {"type": "webhook", "url": "https://x.test"},
            ]},
            s,
        )


def test_no_settings_skips_policy():
    p = validate_payload("webhook", {"url": "https://x.test"})
    assert isinstance(p, WebhookPayload)
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/test_payloads.py -v` — Expected: new tests FAIL (no `PayloadPolicyError`).

- [ ] **Step 3: Implement** — append to `app/schemas/payloads.py` (after `_ADAPTER`), and extend `validate_payload`:

```python
class PayloadPolicyError(ValueError):
    """Payload passed schema validation but violates a configured allowlist.

    Subclasses ValueError so the API's existing 422 path catches it; the
    worker treats it as non-retryable."""


def _host_allowed(host: str, allowed: list[str]) -> bool:
    host = host.lower().rstrip(".")
    for entry in allowed:
        entry = entry.lower().strip(".")
        if host == entry or host.endswith("." + entry):
            return True
    return False


def _check_policy(payload, settings) -> None:
    if isinstance(payload, WebhookPayload):
        host = payload.url.host or ""
        if not _host_allowed(host, settings.webhook_allowed_hosts):
            raise PayloadPolicyError(f"webhook host {host!r} is not allowlisted")
    elif isinstance(payload, EmailPayload):
        domain = payload.to.rsplit("@", 1)[1].lower()
        if domain not in {d.lower() for d in settings.email_allowed_domains}:
            raise PayloadPolicyError(f"email domain {domain!r} is not allowlisted")
    elif isinstance(payload, BatchPayload):
        for item in payload.items:
            _check_policy(item, settings)


def validate_payload(
    job_type: JobType | str, raw: dict, settings=None
) -> EmailPayload | WebhookPayload | ReportPayload | BatchPayload:
    job_type = JobType(job_type)  # raises ValueError on unknown type
    payload = _ADAPTER.validate_python({**raw, "type": job_type.value})
    if settings is not None:
        _check_policy(payload, settings)
    return payload
```

(No type annotation on `settings` to avoid importing `Settings` into the schema module; document in the docstring if desired.)

- [ ] **Step 4: Wire the API route** — `app/api/routes.py` `submit_job`: change the call to

```python
        validate_payload(submission.type, submission.payload, settings)
```

- [ ] **Step 5: Wire the worker (non-retryable)** — `app/worker/runner.py`:

Add import: `from app.schemas.payloads import PayloadPolicyError` and change line 111 to pass settings:

```python
        payload = validate_payload(job.type, job.payload, settings)
```

Add a new except branch **before** the generic `except Exception` (after the `asyncio.CancelledError` branch):

```python
    except PayloadPolicyError as exc:
        # Non-retryable: retrying can never make a policy violation pass.
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, "PayloadPolicyError"))
        won = await repo.fail_job(
            session, job.id, {"type": "PayloadPolicyError", "message": str(exc)}
        )
        if won:
            app_metrics.jobs_failed.add(
                1, {"type": job.type.value, "priority": job.priority.value}
            )
        log.warning("job.policy_rejected", extra={"won": won})
        _record_outcome(job, "policy_rejected", started)
        return Outcome(ack=won, label="policy_rejected")
```

- [ ] **Step 6: Keep existing integration tests green** — `tests/integration/conftest.py:106`, the `test_settings` fixture becomes:

```python
    return Settings(
        database_url=database_url,
        redis_url=redis_url,
        rate_limit_enabled=False,  # rate-limit tests opt in explicitly
        webhook_allowed_hosts=["x.test"],
        email_allowed_domains=["b.com"],
    )
```

(`rate_limit_enabled=False` is consumed by Task 5; harmless before it.)

- [ ] **Step 7: Write the failing worker integration test** — append to `tests/integration/test_worker.py`, following that file's existing fixture pattern for running one job through the worker (reuse its helpers for enqueuing and running a cycle; the key assertions):

```python
async def test_policy_violation_fails_without_retry(...existing fixtures...):
    # enqueue a webhook job whose host is NOT in webhook_allowed_hosts
    # (insert the row directly via repo.create_job with payload
    #  {"url": "https://evil.test/x"}, then enqueue and run one worker cycle)
    job = await repo.get_job(session, job_id)
    assert job.status is JobStatus.failed
    assert job.attempts <= 1                      # no retry ladder
    assert job.error["type"] == "PayloadPolicyError"
```

Adapt the setup lines to the file's existing conventions (look at the nearest test that drives a job to `failed`). The assertion block above is the contract.

- [ ] **Step 8: Run tests**

Run: `uv run pytest tests/unit/test_payloads.py tests/integration/test_worker.py tests/integration/test_api.py -v`
Expected: all PASS. Existing API tests keep passing because the conftest allowlists cover `x.test` / `b.com` used in test data.

- [ ] **Step 9: Sync the spec** — in `docs/superpowers/specs/2026-07-13-api-security-design.md` §2, replace the sentence beginning "**Worker:** `handle_webhook` re-checks the host before" with:

```
2. **Worker:** the runner's `validate_payload(job.type, job.payload, settings)`
   call re-checks policy on every execution attempt, so jobs enqueued before
   the list was tightened (or injected via DB) fail **non-retryably**
   (`PayloadPolicyError`) — enforcement lives in one place instead of per-handler.
```

- [ ] **Step 10: Commit**

```bash
git add app/schemas/payloads.py app/api/routes.py app/worker/runner.py tests docs/superpowers/specs/2026-07-13-api-security-design.md
git commit -m "feat: webhook host + email domain allowlists, non-retryable at worker"
```

---

### Task 4: Request body size limit middleware

**Files:**
- Create: `app/api/middleware.py`
- Modify: `app/main.py`
- Test: `tests/unit/test_middleware.py` (new)

**Interfaces:**
- Produces: `BodySizeLimitMiddleware(app, max_bytes: int)` — pure ASGI, 413 on oversize.
- Consumes: `Settings.max_request_body_bytes` (Task 1).

- [ ] **Step 1: Write the failing tests** — create `tests/unit/test_middleware.py`:

```python
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.middleware import BodySizeLimitMiddleware


def make_client(max_bytes: int = 100) -> TestClient:
    app = FastAPI()

    @app.post("/echo")
    async def echo(payload: dict) -> dict:
        return payload

    app.add_middleware(BodySizeLimitMiddleware, max_bytes=max_bytes)
    return TestClient(app)


def test_under_limit_passes():
    r = make_client().post("/echo", json={"a": 1})
    assert r.status_code == 200


def test_content_length_over_limit_rejected():
    r = make_client().post("/echo", content=b"x" * 200, headers={"content-type": "application/json"})
    assert r.status_code == 413


def test_chunked_body_over_limit_rejected():
    def chunks():
        for _ in range(20):
            yield b"y" * 50  # no Content-Length: httpx sends chunked

    r = make_client().post("/echo", content=chunks(), headers={"content-type": "application/json"})
    assert r.status_code == 413
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/test_middleware.py -v` — Expected: FAIL (module doesn't exist).

- [ ] **Step 3: Implement** — create `app/api/middleware.py`:

```python
"""ASGI middleware for request body size limits (spec §3)."""

import json


class _BodyTooLarge(Exception):
    pass


async def _send_413(send) -> None:
    body = json.dumps({"detail": "request body too large"}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class BodySizeLimitMiddleware:
    """Rejects oversize requests with 413: via Content-Length when declared,
    otherwise by counting streamed bytes and aborting once the cap is crossed."""

    def __init__(self, app, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        for name, value in scope["headers"]:
            if name == b"content-length":
                try:
                    if int(value) > self.max_bytes:
                        await _send_413(send)
                        return
                except ValueError:
                    pass  # malformed header: fall through to counting

        received = 0
        response_started = False

        async def wrapped_send(message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        async def wrapped_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise _BodyTooLarge
            return message

        try:
            await self.app(scope, wrapped_receive, wrapped_send)
        except _BodyTooLarge:
            if response_started:
                raise  # too late for a clean 413; let the server drop the connection
            await _send_413(send)
```

- [ ] **Step 4: Register in create_app** — `app/main.py`, after `app = FastAPI(...)`:

```python
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=settings.max_request_body_bytes)
```

with import `from app.api.middleware import BodySizeLimitMiddleware`.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/unit/test_middleware.py tests/integration/test_api.py -v` — Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/api/middleware.py app/main.py tests/unit/test_middleware.py
git commit -m "feat: 413 request body size limit middleware"
```

---

### Task 5: ProxyHeadersMiddleware + rate limiting

**Files:**
- Create: `app/api/ratelimit.py`
- Modify: `app/main.py`, `app/api/routes.py`
- Test: `tests/unit/test_ratelimit.py` (new), `tests/integration/test_ratelimit.py` (new)

**Interfaces:**
- Produces: `user_or_ip_identifier(request) -> str` and `rate_limit(group: str)` dependency factory in `app/api/ratelimit.py`. Valid groups: `"submit"`, `"control"`, `"read"`, `"stats"` — resolved to `Settings.<group>_rate_limit_per_min`.
- Consumes: Task 1 settings; `hash_key` from `app/users/keys.py`; app Redis client from `app.state.redis`.

- [ ] **Step 1: Add dependency**

Run: `uv add fastapi-limiter`

- [ ] **Step 2: Write the failing unit test** — create `tests/unit/test_ratelimit.py`:

```python
import pytest
from starlette.requests import Request

from app.api.ratelimit import user_or_ip_identifier
from app.users.keys import hash_key


def make_request(headers: list = None, client=("10.0.0.9", 1234)) -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/jobs",
        "headers": headers or [],
        "client": client,
        "query_string": b"",
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_identifier_keys_on_api_key_hash():
    req = make_request(headers=[(b"x-api-key", b"secret-key")])
    assert await user_or_ip_identifier(req) == "u:" + hash_key("secret-key")


@pytest.mark.asyncio
async def test_identifier_falls_back_to_client_ip():
    assert await user_or_ip_identifier(make_request()) == "ip:10.0.0.9"
```

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/unit/test_ratelimit.py -v` — Expected: FAIL (module doesn't exist).

- [ ] **Step 4: Implement** — create `app/api/ratelimit.py`:

```python
"""Per-user rate limiting (spec §1): fastapi-limiter dependencies keyed by
API-key hash, falling back to client IP for unauthenticated requests."""

from fastapi import Request, Response
from fastapi_limiter.depends import RateLimiter

from app.users.keys import hash_key


async def user_or_ip_identifier(request: Request) -> str:
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return "u:" + hash_key(api_key)
    host = request.client.host if request.client else "unknown"
    return "ip:" + host


def rate_limit(group: str):
    """Route dependency for one limit group; no-op when disabled in settings."""

    async def dependency(request: Request, response: Response) -> None:
        settings = request.app.state.settings
        if not settings.rate_limit_enabled:
            return
        times = getattr(settings, f"{group}_rate_limit_per_min")
        await RateLimiter(times=times, seconds=60)(request, response)

    return dependency
```

- [ ] **Step 5: Initialize in create_app** — `app/main.py`:

Imports:

```python
from fastapi_limiter import FastAPILimiter
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from app.api.ratelimit import user_or_ip_identifier
```

In `lifespan`, before `yield`:

```python
        if settings.rate_limit_enabled:
            await FastAPILimiter.init(redis_client, identifier=user_or_ip_identifier)
```

After the middleware registration from Task 4:

```python
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=settings.forwarded_allow_ips)
```

(Do not call `FastAPILimiter.close()` on shutdown — it closes the shared Redis client, which the lifespan already closes itself.)

- [ ] **Step 6: Wire the routes** — `app/api/routes.py`. Import `from app.api.ratelimit import rate_limit`, then add a `dependencies=[...]` to each protected route decorator (`/health` and `/ready` get none):

```python
@router.get("/stats", response_model=StatsResponse, dependencies=[Depends(rate_limit("stats"))])
...
@router.post("/jobs", response_model=JobAccepted, status_code=202, dependencies=[Depends(rate_limit("submit"))])
...
@router.get("/jobs/{job_id}", response_model=JobOut, dependencies=[Depends(rate_limit("read"))])
...
@router.post("/jobs/{job_id}/retry", response_model=JobOut, dependencies=[Depends(rate_limit("control"))])
...
@router.post("/jobs/{job_id}/cancel", response_model=JobOut, dependencies=[Depends(rate_limit("control"))])
...
@router.get("/jobs", response_model=JobList, dependencies=[Depends(rate_limit("read"))])
```

- [ ] **Step 7: Write the failing integration test** — create `tests/integration/test_ratelimit.py` (mirrors `client` fixture in `tests/integration/conftest.py`, but with limits on and a tiny threshold):

```python
import pytest
from fastapi.testclient import TestClient

from app import repository as repo
from app.core.config import Settings
from app.core.db import make_session_factory
from app.main import create_app
from app.users.keys import hash_key

from .conftest import DEFAULT_TEST_KEY, SECOND_TEST_KEY


@pytest.fixture
async def limited_client(pg_engine, database_url, redis_url):
    factory = make_session_factory(pg_engine)
    async with factory() as session:
        await repo.upsert_user(session, "default-user", hash_key(DEFAULT_TEST_KEY))
        await repo.upsert_user(session, "second-user", hash_key(SECOND_TEST_KEY))
        await session.commit()
    settings = Settings(
        database_url=database_url,
        redis_url=redis_url,
        rate_limit_enabled=True,
        read_rate_limit_per_min=3,
        webhook_allowed_hosts=["x.test"],
        email_allowed_domains=["b.com"],
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def test_over_limit_returns_429_with_retry_after(limited_client):
    headers = {"X-API-Key": DEFAULT_TEST_KEY}
    for _ in range(3):
        assert limited_client.get("/jobs", headers=headers).status_code == 200
    r = limited_client.get("/jobs", headers=headers)
    assert r.status_code == 429
    assert "retry-after" in {k.lower() for k in r.headers}


def test_other_user_unaffected(limited_client):
    for _ in range(4):
        limited_client.get("/jobs", headers={"X-API-Key": DEFAULT_TEST_KEY})
    r = limited_client.get("/jobs", headers={"X-API-Key": SECOND_TEST_KEY})
    assert r.status_code == 200


def test_disabled_flag_bypasses_limits(client):
    # `client` fixture has rate_limit_enabled=False
    for _ in range(10):
        assert client.get("/jobs").status_code == 200
```

Adapt fixture names (`redis_url`, `database_url`, `pg_engine`) to the exact names in `tests/integration/conftest.py` — check how the existing `client` fixture gets them and reuse the same ones. If `SECOND_TEST_KEY` has no user by default, the `upsert_user` above covers it.

- [ ] **Step 8: Run tests**

Run: `uv run pytest tests/unit/test_ratelimit.py tests/integration/test_ratelimit.py tests/integration/test_api.py -v`
Expected: all PASS (other integration tests stay green because conftest sets `rate_limit_enabled=False`).

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml uv.lock app/api/ratelimit.py app/main.py app/api/routes.py tests
git commit -m "feat: per-user Redis rate limiting + trusted proxy headers"
```

---

### Task 6: Postgres role split — init script + docker-compose

**Files:**
- Create: `deploy/chart/jobprocessor/files/db-init/01-roles.sh` (single source; compose bind-mounts it, Helm embeds it via `.Files.Get` in Task 7)
- Modify: `docker-compose.yml`
- Modify: `docs/superpowers/specs/2026-07-13-api-security-design.md` (path changed from `deploy/db-init/01-roles.sql` to the chart `files/` dir — update §5 first bullet)

**Interfaces:**
- Produces: roles `jobs_migrator` (owns schema, DDL) and `jobs_app` (DML only); script env contract: `APP_DB_PASSWORD`, `MIGRATOR_DB_PASSWORD` required; DB/superuser resolved from `POSTGRES_*` (postgres:16) or `POSTGRESQL_*` (sclorg) env.
- No pytest — manual verification (project convention).

- [ ] **Step 1: Write the script** — create `deploy/chart/jobprocessor/files/db-init/01-roles.sh`:

```bash
#!/usr/bin/env bash
# Creates the least-privilege role pair (spec §5). Idempotent — safe to run
# on every start. Runs under two images:
#   - postgres:16 (compose):   /docker-entrypoint-initdb.d/, fresh init only
#   - sclorg postgresql (Helm): /opt/app-root/src/postgresql-start/, EVERY
#     start — which is what migrates existing volumes automatically.
set -euo pipefail

DB="${POSTGRES_DB:-${POSTGRESQL_DATABASE:?no database name in env}}"
SUPERUSER="${POSTGRES_USER:-postgres}"
: "${APP_DB_PASSWORD:?}" "${MIGRATOR_DB_PASSWORD:?}"

psql -v ON_ERROR_STOP=1 --username "$SUPERUSER" --dbname "$DB" \
     -v db="$DB" -v app_pw="$APP_DB_PASSWORD" -v mig_pw="$MIGRATOR_DB_PASSWORD" <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'jobs_migrator') THEN
    CREATE ROLE jobs_migrator LOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'jobs_app') THEN
    CREATE ROLE jobs_app LOGIN;
  END IF;
END
$$;

-- psql var interpolation does not reach inside dollar-quoted DO bodies,
-- so passwords are set here instead.
ALTER ROLE jobs_migrator PASSWORD :'mig_pw';
ALTER ROLE jobs_app PASSWORD :'app_pw';

GRANT CONNECT ON DATABASE :"db" TO jobs_migrator, jobs_app;
ALTER SCHEMA public OWNER TO jobs_migrator;
GRANT USAGE, CREATE ON SCHEMA public TO jobs_migrator;
GRANT USAGE ON SCHEMA public TO jobs_app;

-- Adopt tables/sequences created before the role split (no-op on fresh DBs).
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT c.relname, c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind IN ('r', 'p', 'S')
      AND pg_get_userbyid(c.relowner) <> 'jobs_migrator'
  LOOP
    IF r.relkind = 'S' THEN
      EXECUTE format('ALTER SEQUENCE public.%I OWNER TO jobs_migrator', r.relname);
    ELSE
      EXECUTE format('ALTER TABLE public.%I OWNER TO jobs_migrator', r.relname);
    END IF;
  END LOOP;
END
$$;

-- Explicit grants: zero rows on fresh init; they are what makes re-runs
-- against existing databases pick up pre-existing tables.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO jobs_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO jobs_app;

-- Future objects created by migrations get granted automatically.
ALTER DEFAULT PRIVILEGES FOR ROLE jobs_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO jobs_app;
ALTER DEFAULT PRIVILEGES FOR ROLE jobs_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO jobs_app;
SQL
```

- [ ] **Step 2: Wire docker-compose** — `docker-compose.yml`:

`postgres` service — add the two password envs (dev-only inline values, matching the repo's dev convention) and the mount:

```yaml
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: jobs
      POSTGRES_PASSWORD: jobs
      POSTGRES_DB: jobs
      APP_DB_PASSWORD: jobs_app
      MIGRATOR_DB_PASSWORD: jobs_migrator
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./deploy/chart/jobprocessor/files/db-init/01-roles.sh:/docker-entrypoint-initdb.d/01-roles.sh:ro
```

`migrate` service: `DATABASE_URL: postgresql+psycopg://jobs_migrator:jobs_migrator@postgres:5432/jobs`

`api`, `worker`, `ticker`, `users-sync` services: `DATABASE_URL: postgresql+psycopg://jobs_app:jobs_app@postgres:5432/jobs`

Also add to the `api`, `worker` and `ticker` services' `environment` the dev allowlists (JSON-encoded lists, how pydantic-settings parses list envs):

```yaml
      WEBHOOK_ALLOWED_HOSTS: '["example.com"]'
      EMAIL_ALLOWED_DOMAINS: '["example.com"]'
```

- [ ] **Step 3: Manual verification** (no pytest for infra config):

```bash
docker compose down -v          # fresh datadir so initdb.d runs
docker compose up -d --build
docker compose exec postgres psql -U jobs_app -d jobs -c "CREATE TABLE hack(x int)"
# Expected: ERROR:  permission denied for schema public
docker compose exec postgres psql -U jobs_app -d jobs -c "SELECT count(*) FROM jobs"
# Expected: a number (DML works)
docker compose logs migrate | tail -5
# Expected: alembic upgrade ran to head as jobs_migrator without errors
curl -s -X POST localhost:8000/jobs -H "X-API-Key: dev-alice-local-only" -H "content-type: application/json" \
  -d '{"type":"email","payload":{"to":"a@example.com","subject":"hi"}}'
# Expected: 202 (submission works end-to-end with jobs_app + allowlists)
```

- [ ] **Step 4: Update spec §5** — change the first bullet's path from `deploy/db-init/01-roles.sql` to `deploy/chart/jobprocessor/files/db-init/01-roles.sh` with a note: single source shared by compose (bind-mount) and Helm (`.Files.Get`); sclorg's `postgresql-start/` runs it every start, which handles existing volumes automatically (the manual `REASSIGN` path is only needed for compose volumes predating the split — or just `docker compose down -v` in dev).

- [ ] **Step 5: Commit**

```bash
git add deploy/chart/jobprocessor/files/db-init/01-roles.sh docker-compose.yml docs/superpowers/specs/2026-07-13-api-security-design.md
git commit -m "feat: jobs_migrator/jobs_app role split for docker-compose"
```

---

### Task 7: Postgres role split + security env — Helm chart

**Files:**
- Create: `deploy/chart/jobprocessor/templates/db-init-configmap.yaml`
- Modify: `deploy/chart/jobprocessor/templates/credentials-secret.yaml`, `postgres-statefulset.yaml`, `_helpers.tpl`, `pgbouncer-deployment.yaml`, `pgbouncer-ini-configmap.yaml`, `migrate-job.yaml`, `users-sync-job.yaml`, `api-deployment.yaml`, `values.yaml`
- No pytest — verify with `helm lint` + `helm template` grep.

**Interfaces:**
- Consumes: `files/db-init/01-roles.sh` (Task 6); env contract `APP_DB_PASSWORD`/`MIGRATOR_DB_PASSWORD`.
- Produces: secret keys `db-app-password`, `db-migrator-password`; helpers `jobprocessor.appDatabaseUrl` (now `jobs_app` via pgbouncer), `jobprocessor.appDirectDatabaseUrl` (new, `jobs_app` direct), `jobprocessor.migratorDatabaseUrl` (new, `jobs_migrator` direct).

- [ ] **Step 1: credentials-secret.yaml** — extend generation/persistence (same lookup pattern as the existing two):

```yaml
{{- $appDbPass := randAlphaNum 32 }}
{{- $migratorDbPass := randAlphaNum 32 }}
{{- if $existing }}
{{- $appDbPass = index $existing.data "db-app-password" | b64dec }}
{{- $migratorDbPass = index $existing.data "db-migrator-password" | b64dec }}
{{- end }}
```

and in `stringData:` add `db-app-password: {{ $appDbPass | quote }}` and `db-migrator-password: {{ $migratorDbPass | quote }}`.

**Upgrade caveat:** on an existing release the `lookup` finds a secret *without* the two new keys — `index $existing.data "db-app-password"` returns nil and `b64dec` of nil renders empty. Guard each key individually:

```yaml
{{- if $existing }}
{{- $dbPass = index $existing.data "db-password" | b64dec }}
{{- $redisPass = index $existing.data "redis-password" | b64dec }}
{{- with index $existing.data "db-app-password" }}{{ $appDbPass = . | b64dec }}{{ end }}
{{- with index $existing.data "db-migrator-password" }}{{ $migratorDbPass = . | b64dec }}{{ end }}
{{- end }}
```

- [ ] **Step 2: db-init-configmap.yaml** — new template:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "jobprocessor.fullname" . }}-db-init
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: postgres
data:
  01-roles.sh: |
{{ .Files.Get "files/db-init/01-roles.sh" | indent 4 }}
```

- [ ] **Step 3: postgres-statefulset.yaml** — add env (after `POSTGRESQL_MAX_CONNECTIONS`):

```yaml
            - name: APP_DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: db-app-password
            - name: MIGRATOR_DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: db-migrator-password
```

add volumeMount + volume (sclorg runs `postgresql-start/*.sh` on every start):

```yaml
            - name: db-init
              mountPath: /opt/app-root/src/postgresql-start
              readOnly: true
```
```yaml
        - name: db-init
          configMap:
            name: {{ include "jobprocessor.fullname" . }}-db-init
```

- [ ] **Step 4: _helpers.tpl** — replace the DSN helpers:

```yaml
{{- define "jobprocessor.appDatabaseUrl" -}}
postgresql+psycopg://jobs_app:$(DB_APP_PASSWORD)@{{ include "jobprocessor.pgbouncerHost" . }}:6432/{{ .Values.postgres.database }}{{- if .Values.tls.appToPgbouncer -}}?sslmode=verify-full&sslrootcert=/etc/pki/service-ca/ca.crt{{- end -}}
{{- end }}

{{- define "jobprocessor.appDirectDatabaseUrl" -}}
postgresql+psycopg://jobs_app:$(DB_APP_PASSWORD)@{{ include "jobprocessor.postgresHost" . }}:5432/{{ .Values.postgres.database }}?sslmode=verify-full&sslrootcert=/etc/pki/service-ca/ca.crt
{{- end }}

{{- define "jobprocessor.migratorDatabaseUrl" -}}
postgresql+psycopg://jobs_migrator:$(DB_MIGRATOR_PASSWORD)@{{ include "jobprocessor.postgresHost" . }}:5432/{{ .Values.postgres.database }}?sslmode=verify-full&sslrootcert=/etc/pki/service-ca/ca.crt
{{- end }}
```

In `jobprocessor.appEnv`, replace the `DB_PASSWORD` secretKeyRef block with:

```yaml
- name: DB_APP_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "jobprocessor.fullname" . }}-credentials
      key: db-app-password
```

and append the security env (values-driven, JSON so pydantic-settings parses the lists):

```yaml
- name: WEBHOOK_ALLOWED_HOSTS
  value: {{ .Values.security.webhookAllowedHosts | toJson | quote }}
- name: EMAIL_ALLOWED_DOMAINS
  value: {{ .Values.security.emailAllowedDomains | toJson | quote }}
```

Grep the templates for any other `directDatabaseUrl` reference and migrate it to the appropriate new helper.

- [ ] **Step 5: migrate-job.yaml** — swap env to the migrator pair:

```yaml
            - name: DB_MIGRATOR_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: db-migrator-password
            - name: DATABASE_URL
              value: {{ include "jobprocessor.migratorDatabaseUrl" . | quote }}
```

- [ ] **Step 6: users-sync-job.yaml** — swap env to the app pair (DML only):

```yaml
            - name: DB_APP_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: db-app-password
            - name: DATABASE_URL
              value: {{ include "jobprocessor.appDirectDatabaseUrl" . | quote }}
```

- [ ] **Step 7: pgbouncer** — clients now connect as `jobs_app`, so pgbouncer's userlist must hold `jobs_app`. In `pgbouncer-deployment.yaml`: `DB_USER` value becomes `jobs_app`, and `DB_PASSWORD`'s secretKeyRef key becomes `db-app-password`. In `pgbouncer-ini-configmap.yaml`: drop `auth_user={{ .Values.postgres.user }}` from the `[databases]` line (auth comes from userlist.txt, which the image entrypoint writes from `DB_USER`/`DB_PASSWORD`), and change `admin_users = {{ .Values.postgres.user }}` to `admin_users = jobs_app`.

- [ ] **Step 8: api-deployment.yaml** — after the `DB_POOL_SIZE` env:

```yaml
            - name: FORWARDED_ALLOW_IPS
              value: "*"   # safe: api-ingress netpol only admits the router on :8000
```

- [ ] **Step 9: values.yaml** — add:

```yaml
security:
  # Empty lists = deny all webhook/email jobs (secure default). Set per env.
  webhookAllowedHosts: []
  emailAllowedDomains: []
```

- [ ] **Step 10: Verify rendering**

```bash
helm lint deploy/chart/jobprocessor
helm template t deploy/chart/jobprocessor | grep -E "jobs_app|jobs_migrator|db-app-password|db-migrator-password|FORWARDED_ALLOW_IPS|WEBHOOK_ALLOWED_HOSTS|postgresql-start"
```

Expected: lint passes; migrate job renders the migrator DSN; api/worker/ticker render `jobs_app` DSNs; postgres mounts `postgresql-start`; no template still references the removed `directDatabaseUrl`/`db-password` in app containers (postgres's own `POSTGRESQL_PASSWORD` keeps `db-password`).

- [ ] **Step 11: Commit**

```bash
git add deploy/chart/jobprocessor
git commit -m "feat: role-split DSNs, db-init configmap, security env in Helm chart"
```

---

### Task 8: Full verification pass

- [ ] **Step 1: Full test suite** — `uv run pytest` — Expected: all pass (261 pre-existing + new).
- [ ] **Step 2: Lint/format** — `uv run ruff check --fix && uv run ruff format` — Expected: clean; commit any formatting deltas.
- [ ] **Step 3: Compose end-to-end** — run the Task 6 Step 3 manual checklist if not already done on the final code.
- [ ] **Step 4: Rate limit smoke vs running compose stack:**

```bash
for i in $(seq 1 25); do curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST localhost:8000/jobs -H "X-API-Key: dev-alice-local-only" -H "content-type: application/json" \
  -d '{"type":"email","payload":{"to":"a@example.com","subject":"hi"}}'; done | sort | uniq -c
# Expected: 202s then 429s after the 20th within the same minute
```

- [ ] **Step 5: Commit any remaining changes**

```bash
git add -A && git commit -m "chore: api-security verification fixes"
```
