# Async Worker and Database Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the DB layer, queue, API, worker, and ticker to asyncio; run up to `worker_concurrency` jobs concurrently per worker via `asyncio.TaskGroup` + `asyncio.Semaphore`; replace thread-based handler timeouts (and process recycling) with `asyncio.wait_for` task cancellation.

**Architecture:** SQLAlchemy `create_async_engine`/`async_sessionmaker` (psycopg3 async — same `postgresql+psycopg` URL), `redis.asyncio.Redis`, async FastAPI routes, one asyncio event loop per process (worker/ticker). Health endpoints move from `http.server` threads to FastAPI+Uvicorn served as a task on the same loop, so a wedged loop makes the liveness probe hang/fail and the orchestrator restarts the pod. Alembic migrations stay synchronous.

**Tech Stack:** FastAPI, SQLAlchemy 2.0 async, `redis.asyncio`, Uvicorn, pytest + pytest-asyncio, testcontainers.

## Global Constraints

- Python `>=3.11` (required for `asyncio.TaskGroup`).
- Package management via **uv** only: `uv add`, `uv run pytest`, `uv run ruff check --fix`, `uv run ruff format`. Never pip/venv/poetry.
- No print statements; use `logging.getLogger("app.<component>")` and `app.core.logging.bind_log_context`.
- New setting: `worker_concurrency: int = 10` (env `WORKER_CONCURRENCY`).
- Recycle behavior is REMOVED: delete `max_handler_timeouts_before_recycle`, the `Outcome.recycle` field, and worker exit-code-1 recycling.
- Alembic migrations remain synchronous (they run once at deploy/init).
- OTel metric gauge callbacks run on exporter threads and MUST NOT await — where a callback needs DB/Redis, give it a dedicated **sync** engine/client (observability-only).
- Run the full suite with `uv run pytest` before declaring any task complete. Integration tests need Docker (testcontainers); if Docker is unavailable, run unit tests and say so explicitly.

---

### Task 1: Async foundation — engine/session, Redis client, settings, test harness

**Files:**
- Modify: `app/core/db.py`
- Modify: `app/core/redis.py`
- Modify: `app/core/config.py`
- Modify: `pyproject.toml` (add `pytest-asyncio` dev dep + config)
- Modify: `tests/integration/conftest.py` (async `pg_engine`, `db_session`, `redis_client` fixtures)
- Test: `tests/unit/test_config.py` (add `worker_concurrency` test)

**Interfaces:**
- Produces: `make_engine(database_url: str) -> AsyncEngine`, `make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]`, `create_redis_client(redis_url: str) -> redis.asyncio.Redis`, `Settings.worker_concurrency: int` (default 10). All later tasks consume these.
- Note: `Settings.max_handler_timeouts_before_recycle` is deleted in Task 7; leave it in place for now so the old worker keeps compiling.

- [ ] **Step 1: Add pytest-asyncio and configure asyncio mode**

```bash
uv add --dev pytest-asyncio
```

Append to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
```

(`auto` mode: plain `async def test_*` functions and async fixtures just work. Session loop scope lets session-scoped async fixtures share one loop.)

- [ ] **Step 2: Write failing test for the new setting**

Append to `tests/unit/test_config.py` (match the file's existing style for constructing `Settings`; it already builds `Settings(database_url=..., redis_url=...)`):

```python
def test_worker_concurrency_default_and_env(monkeypatch):
    s = Settings(database_url="postgresql+psycopg://x/y", redis_url="redis://x")
    assert s.worker_concurrency == 10
    monkeypatch.setenv("WORKER_CONCURRENCY", "3")
    s2 = Settings(database_url="postgresql+psycopg://x/y", redis_url="redis://x")
    assert s2.worker_concurrency == 3
```

- [ ] **Step 3: Run it to verify it fails**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `AttributeError`/`ValidationError`-free but `worker_concurrency` missing (AttributeError).

- [ ] **Step 4: Add the setting**

In `app/core/config.py`, after `cancel_poll_interval_s: float = 2.0` add:

```python
    worker_concurrency: int = 10
```

- [ ] **Step 5: Convert engine/session factory to async**

Replace `app/core/db.py` entirely:

```python
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def make_engine(database_url: str) -> AsyncEngine:
    # pool_timeout=5: a saturated pool turns into a fast 503 on /ready
    # instead of a 30s hang (also bounds app-side checkout waits).
    return create_async_engine(database_url, pool_pre_ping=True, pool_timeout=5)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=engine, expire_on_commit=False)
```

(`postgresql+psycopg://` URLs work for both sync and async engines with psycopg3 — no URL change anywhere.)

- [ ] **Step 6: Convert the Redis client factory to async**

Replace `app/core/redis.py` entirely:

```python
import redis.asyncio as redis


def create_redis_client(redis_url: str) -> redis.Redis:
    return redis.Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=10,
    )
```

- [ ] **Step 7: Convert shared test fixtures to async**

In `tests/integration/conftest.py`:

- `pg_engine`: keep Alembic upgrade sync (it builds its own sync engine from `alembic.ini` config), but the yielded engine is now async:

```python
@pytest.fixture(scope="session")
async def pg_engine(database_url):
    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "alembic")
    cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(cfg, "head")
    engine = make_engine(database_url)
    yield engine
    await engine.dispose()
```

- `db_session`:

```python
@pytest.fixture
async def db_session(pg_engine):
    factory = make_session_factory(pg_engine)
    session = factory()
    try:
        yield session
    finally:
        await session.close()
        async with pg_engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE jobs, users"))
```

- `redis_client`:

```python
@pytest.fixture
async def redis_client(redis_container):
    url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"
    client = create_redis_client(url)
    yield client
    await client.flushdb()
    await client.aclose()
```

- Every other fixture that opens a repo session (`client`, `default_user_id`, `unauth_client`, `second_user`, `owner_id`) becomes `async def` and awaits, e.g.:

```python
@pytest.fixture
async def client(pg_engine, test_settings):
    factory = _make_session_factory(pg_engine)
    async with factory() as session:
        await repo.upsert_user(session, "default-user", hash_key(DEFAULT_TEST_KEY))
        await session.commit()
    app = create_app(test_settings)
    with TestClient(app) as c:
        c.headers.update({"X-API-Key": DEFAULT_TEST_KEY})
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs, users"))
    real_redis = create_redis_client(test_settings.redis_url)
    try:
        await real_redis.flushdb()
    finally:
        await real_redis.aclose()
```

Keep `TestClient` (it drives async apps through its own portal loop — API tests stay synchronous call-style).

- [ ] **Step 8: Verify unit config tests pass; expect broad integration breakage until later tasks land**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS. (The full suite is red mid-migration; Tasks 2–9 restore it. This is a migration-wide refactor, so the usual "all tests green before commit" gate applies to the *unit* scope per task and to the *whole suite* at Task 10.)

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml uv.lock app/core/db.py app/core/redis.py app/core/config.py tests/integration/conftest.py tests/unit/test_config.py
git commit -m "feat: async engine/session/redis foundation, worker_concurrency setting"
```

---

### Task 2: Repository layer → async

**Files:**
- Modify: `app/repository.py` (all functions)
- Modify: `tests/integration/test_repository.py` (tests become `async def`, awaits added)

**Interfaces:**
- Consumes: `AsyncSession` from Task 1.
- Produces: every public function keeps its exact name/params/returns but becomes `async def` taking `session: AsyncSession`. E.g. `async def create_job(session, job_type, payload, *, status=..., ...) -> Job`, `async def claim_job(session, job_id) -> bool`, `async def upsert_user(session, name, key_hash) -> UUID`, `async def get_user_by_key_hash(session, key_hash) -> User | None`, etc. All later tasks call these with `await repo.<fn>(...)`.

- [ ] **Step 1: Convert every function in `app/repository.py`**

Mechanical, uniform rules — apply to all functions in the file:
- `from sqlalchemy.ext.asyncio import AsyncSession` replaces the `Session` import; every signature becomes `async def f(session: AsyncSession, ...)`.
- `session.execute(...)` → `await session.execute(...)`; `session.commit()` → `await session.commit()`; `session.refresh(job)` → `await session.refresh(job)`; `session.get(Job, job_id)` → `await session.get(Job, job_id)`.

Two representative conversions (repeat the same shape for the rest):

```python
async def create_job(
    session: AsyncSession,
    job_type: JobType,
    payload: dict,
    *,
    status: JobStatus = JobStatus.pending,
    scheduled_at: datetime | None = None,
    priority: JobPriority = JobPriority.normal,
    max_attempts: int = 4,
    idempotency_key: str | None = None,
    idempotency_hash: str | None = None,
    trace_context: dict | None = None,
    user_id: UUID | None = None,
) -> Job:
    job = Job(
        type=job_type,
        payload=payload,
        status=status,
        scheduled_at=scheduled_at,
        priority=priority,
        max_attempts=max_attempts,
        idempotency_key=idempotency_key,
        idempotency_hash=idempotency_hash,
        trace_context=trace_context,
        user_id=user_id,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def claim_job(session: AsyncSession, job_id: UUID) -> bool:
    stmt = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status.in_([JobStatus.pending, JobStatus.scheduled]),
        )
        .values(status=JobStatus.processing, started_at=_now())
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount == 1
```

In `list_jobs`, the scalars iteration becomes `rows = list((await session.execute(stmt)).scalars())`. In `get_job`, the scoped form becomes `return (await session.execute(select(Job).where(Job.id == job_id, Job.user_id == user_id))).scalar_one_or_none()`. `count_by_status` returns `(await session.execute(...)).all()`. `upsert_user` returns `(await session.execute(stmt)).scalar_one()` and still does NOT commit.

- [ ] **Step 2: Convert `tests/integration/test_repository.py`**

Every test becomes `async def` and awaits repo calls, e.g.:

```python
async def test_claim_job_transitions_pending_to_processing(db_session, owner_id):
    job = await repo.create_job(db_session, JobType.email, {"to": "a@b.c", "subject": "s", "body": "b"}, user_id=owner_id)
    assert await repo.claim_job(db_session, job.id) is True
    assert await repo.claim_job(db_session, job.id) is False
```

(Keep each existing test's assertions exactly as they are — only add `async`/`await`.)

- [ ] **Step 3: Run repository tests**

Run: `uv run pytest tests/integration/test_repository.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/repository.py tests/integration/test_repository.py
git commit -m "feat: convert repository layer to async sessions"
```

---

### Task 3: Queue producer, consumer, delayed → async

**Files:**
- Modify: `app/queue/producer.py`, `app/queue/consumer.py`, `app/queue/delayed.py`
- Modify: `tests/integration/test_queue.py`, `tests/integration/test_delayed.py`, `tests/unit/test_producer_tracing.py`

**Interfaces:**
- Consumes: `redis.asyncio.Redis` from Task 1.
- Produces: `async def enqueue(client, stream, job_id, carrier=None) -> str`; `message_fields(...)` stays **sync** (pure trace-context work, no I/O); `async def ensure_group(client, stream, group) -> None`; `async def ack(client, stream, group, message_id) -> None`; `async def read_priority(client, streams, group, consumer, block_ms, count=1) -> list[tuple[str, str, dict]]` (new `count` param — worker reads up to its free slots); `async def schedule(client, zset, job_id, score) -> None`; `async def due_job_ids(client, zset, now_epoch, limit) -> list[str]`; `async def promote(client, zset, routed, all_ids) -> None`.

- [ ] **Step 1: Convert the three queue modules**

`app/queue/producer.py`: import `redis.asyncio as redis`; `enqueue` becomes:

```python
async def enqueue(
    client: redis.Redis, stream: str, job_id: str, carrier: dict | None = None
) -> str:
    return await client.xadd(stream, message_fields(stream, job_id, carrier))
```

`app/queue/consumer.py`: `ensure_group`/`ack` gain `async`/`await`; `read_priority` gains `count`:

```python
async def read_priority(
    client: redis.Redis,
    streams: list[str],
    group: str,
    consumer: str,
    block_ms: int,
    count: int = 1,
) -> list[tuple[str, str, dict]]:
    for stream in streams:
        msgs = _flatten(
            await client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=count,
                block=None,
            )
        )
        if msgs:
            return msgs
    return _flatten(
        await client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">" for stream in streams},
            count=count,
            block=block_ms,
        )
    )
```

(Keep the strict-priority comments; `_flatten` stays sync.)

`app/queue/delayed.py`:

```python
import redis.asyncio as redis


async def schedule(client: redis.Redis, zset: str, job_id: str, score: float) -> None:
    await client.zadd(zset, {job_id: score})


async def due_job_ids(
    client: redis.Redis, zset: str, now_epoch: float, limit: int
) -> list[str]:
    return await client.zrangebyscore(zset, min=0, max=now_epoch, start=0, num=limit)


async def promote(
    client: redis.Redis,
    zset: str,
    routed: list[tuple[str, dict]],
    all_ids: list[str],
) -> None:
    if not all_ids:
        return
    # XADD every routed message to its target stream BEFORE removing any id
    # from the ZSET, so a crash mid-promotion leaves the ids in the ZSET to be
    # retried next tick. Duplicate stream entries are absorbed by the worker's
    # idempotent claim guard.
    async with client.pipeline(transaction=False) as pipe:
        for stream, fields in routed:
            pipe.xadd(stream, fields)
        await pipe.execute()
    await client.zrem(zset, *all_ids)
```

- [ ] **Step 2: Convert the queue tests**

`tests/integration/test_queue.py` and `test_delayed.py`: tests become `async def`, all queue-module calls and direct `redis_client` calls awaited (fixture is already async from Task 1). `tests/unit/test_producer_tracing.py`: if it tests `message_fields` it stays sync-unchanged; any `enqueue` test becomes async with a fake client whose `xadd` is an `async def`.

- [ ] **Step 3: Run**

Run: `uv run pytest tests/integration/test_queue.py tests/integration/test_delayed.py tests/unit/test_producer_tracing.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/queue tests/integration/test_queue.py tests/integration/test_delayed.py tests/unit/test_producer_tracing.py
git commit -m "feat: convert queue producer/consumer/delayed to async redis"
```

---

### Task 4: Retry + observability helpers → async

**Files:**
- Modify: `app/retry.py`, `app/observability.py`
- Modify: `tests/integration/test_retry.py`, `tests/unit/test_observability.py`

**Interfaces:**
- Produces: `async def schedule_retry_or_fail(session, client, settings, job, error, carrier=None) -> bool` (same semantics); `async def check_readiness(session, client) -> dict[str, str]`; `async def gather_stats(session, client, settings) -> StatsResponse`. Pure helpers (`backoff_delay`, `live_worker_count`, `zero_fill_status_counts`, `pending_age_seconds`, `_tolerate_nogroup`) stay sync.

- [ ] **Step 1: Convert `app/retry.py`**

`schedule_retry_or_fail` becomes `async def`; every `repo.*` call, `enqueue(...)`, and `delayed.schedule(...)` gets `await`. Signature types: `session: AsyncSession`, `client: redis.asyncio.Redis`. No logic changes.

- [ ] **Step 2: Convert `app/observability.py`**

```python
async def check_readiness(session: AsyncSession, client: redis.Redis) -> dict[str, str]:
    checks: dict[str, str] = {}
    try:
        await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except SQLAlchemyError:
        checks["postgres"] = "error"
    try:
        await client.ping()
        checks["redis"] = "ok"
    except redis.RedisError:
        checks["redis"] = "error"
    return checks
```

`gather_stats`: `async def`; the pipeline block becomes

```python
    async with client.pipeline(transaction=False) as pipe:
        for stream in stream_names:
            pipe.xinfo_groups(stream)
        for stream in stream_names:
            pipe.xinfo_consumers(stream, settings.consumer_group)
        pipe.zcard(settings.delayed_zset)
        results = await pipe.execute(raise_on_error=False)
```

and the two DB reads become `status_rows = (await session.execute(...)).all()` / `min_created = (await session.execute(...)).scalar_one()`. Imports switch to `redis.asyncio as redis` and `AsyncSession`.

- [ ] **Step 3: Convert tests and run**

`tests/integration/test_retry.py`: async/await conversion (assertions unchanged). `tests/unit/test_observability.py`: pure-helper tests unchanged; `check_readiness`/`gather_stats` tests become async with async fakes.

Run: `uv run pytest tests/integration/test_retry.py tests/unit/test_observability.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/retry.py app/observability.py tests/integration/test_retry.py tests/unit/test_observability.py
git commit -m "feat: convert retry and observability helpers to async"
```

---

### Task 5: API service → async (deps, routes, app factory, users sync)

**Files:**
- Modify: `app/api/deps.py`, `app/api/routes.py`, `app/main.py`, `app/users/sync.py`
- Modify: `tests/integration/test_users_sync.py` (async conversion); API integration tests (`test_api.py`, `test_auth_api.py`, `test_auth_e2e.py`, `test_idempotency_api.py`, `test_health_stats.py`, `test_metrics.py`, `test_cancel.py` route parts) keep their synchronous `TestClient` call style — only fixture/db-seeding awaits change (done in Task 1) plus any direct `repo.*` calls inside tests gain `await`.

**Interfaces:**
- Consumes: async repo (Task 2), async queue (Task 3), async observability (Task 4), `make_engine`/`make_session_factory`/`create_redis_client` (Task 1).
- Produces: `get_db(request) -> AsyncIterator[AsyncSession]`; all route handlers `async def`; `create_app(settings) -> FastAPI` unchanged signature; `app/users/sync.py`: `async def sync_users(session, keys) -> int`, `def run(settings) -> int` (sync wrapper calling `asyncio.run(_run_async(settings))`), `main()` unchanged.

- [ ] **Step 1: Convert `app/api/deps.py`**

```python
async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    factory = request.app.state.session_factory
    async with factory() as session:
        yield session
```

`get_redis` stays sync (returns the client object). `get_current_user` is already async: change `session: AsyncSession = Depends(get_db)` typing and `row = await repo.get_user_by_key_hash(session, key_hash)`. Update the docstring (no more threadpool copy — endpoints are native async now).

- [ ] **Step 2: Convert `app/api/routes.py`**

Every handler becomes `async def`; every `repo.*`, `enqueue`, `schedule`, `check_readiness`, `gather_stats`, `client.zrem`, `session.rollback()`, `session.refresh(job)` call gets `await`. `_create_and_handoff` and `_replay_or_conflict` become `async def` (call sites `await` them); `_accepted` stays sync. Concretely for the trickiest route:

```python
@router.post("/jobs/{job_id}/cancel", response_model=JobOut)
async def cancel_job_route(
    job_id: UUID,
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_db),
    client: redis.Redis = Depends(get_redis),
    user: AuthedUser = Depends(get_current_user),
) -> JobOut:
    settings = request.app.state.settings
    if await repo.get_job(session, job_id, user_id=user.id) is None:
        raise HTTPException(status_code=404, detail="job not found")
    for _ in range(3):  # bounded re-resolve for the legal processing<->pending flap
        if await repo.cancel_pending_or_scheduled(session, job_id):
            await client.zrem(settings.delayed_zset, str(job_id))  # harmless no-op if absent
            return JobOut.model_validate(await repo.get_job(session, job_id))
        if await repo.request_cancel(session, job_id):
            response.status_code = 202
            return JobOut.model_validate(await repo.get_job(session, job_id))
        job = await repo.get_job(session, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status is JobStatus.cancelled:
            return JobOut.model_validate(job)  # idempotent 200
        if job.status in (JobStatus.completed, JobStatus.failed):
            raise HTTPException(
                status_code=409, detail="job cannot be cancelled in its current state"
            )
    raise HTTPException(status_code=409, detail="job state is changing; retry")
```

Imports: `redis.asyncio as redis` replaces `redis`; `AsyncSession` replaces `Session`.

- [ ] **Step 3: Convert `app/main.py` lifespan**

`ensure_group` and shutdown become awaited inside the (already async) lifespan:

```python
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        for stream in settings.ordered_streams:
            await ensure_group(redis_client, stream, settings.consumer_group)
        yield
        await redis_client.aclose()
        await engine.dispose()
        shutdown_telemetry()
```

Everything else in `create_app` is unchanged (the async engine/factory come from the already-converted `app/core/db.py`).

- [ ] **Step 4: Convert `app/users/sync.py`**

`sync_users` becomes async (`await repo.upsert_user(...)`, `await session.commit()`). `run(settings)` keeps returning `int` but does the work in a private coroutine:

```python
async def _run_async(settings: Settings) -> int:
    configure_telemetry(settings, "users-sync")
    tracer = trace.get_tracer("app.users.sync")
    engine = make_engine(settings.database_url)
    exit_code = 0
    try:
        with tracer.start_as_current_span("users.sync") as span:
            keys = load_keys(settings.api_user_keys_file)
            session_factory = make_session_factory(engine)
            async with session_factory() as session:
                count = await sync_users(session, keys)
            span.set_attribute("users.synced_count", count)
            log.info("users.synced", extra={"count": count, "names": sorted(keys)})
    except (OSError, ValueError, SQLAlchemyError) as exc:
        # (keep the existing key-hash-redaction comment verbatim)
        log.error("users.sync_failed", extra={"error_type": type(exc).__name__})
        exit_code = 1
    finally:
        await engine.dispose()
        shutdown_telemetry()
    return exit_code


def run(settings: Settings) -> int:
    return asyncio.run(_run_async(settings))
```

- [ ] **Step 5: Convert affected tests and run**

`tests/integration/test_users_sync.py`: tests calling `sync_users` directly become async; tests calling `run(...)` stay sync. API tests: only in-test `repo.*` seeding calls gain `await` (making those tests `async def`); `client.post(...)`-style calls stay as-is.

Run: `uv run pytest tests/integration/test_api.py tests/integration/test_auth_api.py tests/integration/test_auth_e2e.py tests/integration/test_idempotency_api.py tests/integration/test_health_stats.py tests/integration/test_metrics.py tests/integration/test_users_sync.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add app/api app/main.py app/users/sync.py tests/integration
git commit -m "feat: convert API service and users sync to async"
```

---

### Task 6: Job handlers, registry, and cooperative cancellation context → async

**Files:**
- Modify: `app/jobs/handlers.py`, `app/jobs/registry.py`, `app/worker/context.py`
- Modify: `tests/unit/test_handlers.py`, `tests/unit/test_batch_handler.py`, `tests/unit/test_context.py`

**Interfaces:**
- Consumes: `async_sessionmaker[AsyncSession]` (Task 1).
- Produces: `JobContext` protocol: `def set_progress(self, pct: int) -> None`, `async def cancelled(self) -> bool`. All handlers `async def handle_*(payload, ctx) -> dict`. `async def run_handler(job_type, payload, ctx) -> dict`. `PgJobContext(job_id, session_factory: async_sessionmaker | None, poll_interval_s, now=time.monotonic)` with `async def cancelled(self) -> bool`.

- [ ] **Step 1: Convert handlers and registry**

`app/jobs/handlers.py`: `import asyncio` replaces `time`; each handler becomes `async def` and `time.sleep(x)` → `await asyncio.sleep(x)`. Batch handler:

```python
async def handle_batch(payload: BatchPayload, ctx) -> dict:
    from app.jobs.registry import run_handler  # deferred: registry imports this module

    n = len(payload.items)
    summary = {"total": n, "succeeded": 0, "failed": 0, "results": [], "errors": []}
    for i, item in enumerate(payload.items):
        if await ctx.cancelled():
            raise JobCancelled(summary)
        try:
            result = await run_handler(item.type, item, ctx)
            summary["succeeded"] += 1
            summary["results"].append({"index": i, "result": result})
        except Exception as exc:  # noqa: BLE001 — per-item, collected not raised
            summary["failed"] += 1
            summary["errors"].append({"index": i, "error": str(exc)})
        ctx.set_progress(int((i + 1) / n * 100) if n else 100)
    return summary
```

`app/jobs/registry.py`:

```python
from collections.abc import Awaitable, Callable

HANDLERS: dict[JobType, Callable[[object, object], Awaitable[dict]]] = { ... same map ... }


async def run_handler(job_type: JobType, payload, ctx) -> dict:
    return await HANDLERS[job_type](payload, ctx)
```

- [ ] **Step 2: Convert `app/worker/context.py`**

`cancelled()` and the `_write`/`_read` helpers become async; sessions come from the async factory:

```python
class JobContext(Protocol):
    def set_progress(self, pct: int) -> None: ...
    async def cancelled(self) -> bool: ...
```

```python
    async def cancelled(self) -> bool:
        now = self._now()
        if self._last_poll is None or now - self._last_poll >= self._interval:
            if self._pending_pct != self._last_written_pct:
                alive, requested = await self._write(self._pending_pct)
                self._last_written_pct = self._pending_pct
            else:
                alive, requested = await self._read()
            self._cached = requested or not alive
            self._last_poll = now
        return self._cached

    async def _write(self, pct: int) -> tuple[bool, bool]:
        async with self._sf() as session:
            row = (
                await session.execute(
                    text(
                        "UPDATE jobs SET progress = :pct "
                        "WHERE id = :id AND status = 'processing' "
                        "RETURNING cancel_requested_at"
                    ),
                    {"pct": pct, "id": self._job_id},
                )
            ).first()
            await session.commit()
        if row is None:
            return (False, False)  # no longer processing
        return (True, row[0] is not None)

    async def _read(self) -> tuple[bool, bool]:
        async with self._sf() as session:
            row = (
                await session.execute(
                    text("SELECT cancel_requested_at, status FROM jobs WHERE id = :id"),
                    {"id": self._job_id},
                )
            ).first()
        if row is None or row[1] != "processing":
            return (False, False)
        return (True, row[0] is not None)
```

Update the class docstring: it no longer runs "inside the worker's timeout thread" — it opens short-lived async sessions on the worker loop. Type hint for `session_factory` becomes `async_sessionmaker[AsyncSession] | None`.

- [ ] **Step 3: Convert the unit tests and run**

`tests/unit/test_handlers.py`, `test_batch_handler.py`: `async def` tests, `await` handler calls; fake contexts implement `async def cancelled()`. `tests/unit/test_context.py`: fake session factories become async context managers, tests await `ctx.cancelled()`.

Run: `uv run pytest tests/unit/test_handlers.py tests/unit/test_batch_handler.py tests/unit/test_context.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/jobs app/worker/context.py tests/unit/test_handlers.py tests/unit/test_batch_handler.py tests/unit/test_context.py
git commit -m "feat: convert job handlers and cancellation context to async"
```

---

### Task 7: Timeout via asyncio cancellation

**Files:**
- Modify: `app/worker/timeout.py`
- Modify: `tests/unit/test_timeout.py`
- Modify: `app/core/config.py` (delete `max_handler_timeouts_before_recycle`)

**Interfaces:**
- Produces: `class HandlerTimeout(Exception)` (unchanged); `async def run_with_timeout(coro: Awaitable[T], timeout_s: float) -> T` — takes an already-created awaitable, awaits it under `asyncio.wait_for`, raises `HandlerTimeout` on timeout. Task 8's worker calls `await run_with_timeout(run_handler(job.type, payload, ctx), settings.job_handler_timeout_s)`.

- [ ] **Step 1: Write the failing tests**

Replace `tests/unit/test_timeout.py`:

```python
import asyncio

import pytest

from app.worker.timeout import HandlerTimeout, run_with_timeout


async def test_returns_value_when_fast():
    async def fast():
        return 21 * 2

    assert await run_with_timeout(fast(), timeout_s=1.0) == 42


async def test_raises_handler_timeout_and_cancels_when_slow():
    cancelled = asyncio.Event()

    async def slow():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with pytest.raises(HandlerTimeout):
        await run_with_timeout(slow(), timeout_s=0.05)
    assert cancelled.is_set()


async def test_propagates_handler_exception():
    async def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        await run_with_timeout(boom(), timeout_s=1.0)
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/test_timeout.py -v`
Expected: FAIL (old sync signature).

- [ ] **Step 3: Implement**

Replace `app/worker/timeout.py`:

```python
import asyncio
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


class HandlerTimeout(Exception):
    """Raised when a job handler exceeds its allotted execution time."""


async def run_with_timeout(coro: Awaitable[T], timeout_s: float) -> T:
    """Await `coro` for up to `timeout_s`, then cancel it.

    Cancellation is native asyncio: the handler task is cancelled and awaited
    to completion, so nothing is orphaned and no process recycle is needed.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_s)
    except TimeoutError as exc:
        raise HandlerTimeout(f"handler exceeded {timeout_s}s") from exc
```

- [ ] **Step 4: Delete the recycle setting**

Remove `max_handler_timeouts_before_recycle: int = 1` from `app/core/config.py` and delete any test in `tests/unit/test_config.py` that references it. (`worker/runner.py` still references it until Task 8 — that file is mid-rewrite; Task 8 removes the reference. If Task 8 is executed by a different engineer, warn them the worker doesn't import cleanly between these two commits — or fold this step's commit into Task 8's if strict bisectability is required.)

- [ ] **Step 5: Run and commit**

Run: `uv run pytest tests/unit/test_timeout.py tests/unit/test_config.py -v`
Expected: PASS

```bash
git add app/worker/timeout.py tests/unit/test_timeout.py app/core/config.py tests/unit/test_config.py
git commit -m "feat: replace thread timeout with asyncio.wait_for cancellation"
```

---

### Task 8: Worker runner — TaskGroup concurrency, no recycle, rollback on cancel

**Files:**
- Modify: `app/worker/runner.py`, `app/worker/__main__.py`
- Modify: `tests/integration/test_worker.py`, `tests/integration/test_batch.py`, `tests/integration/test_cancel.py` (worker-side parts), `tests/integration/test_reaper.py` if it drives `process_job`

**Interfaces:**
- Consumes: async repo/queue/retry/context/timeout (Tasks 2–7), `Settings.worker_concurrency` (Task 1), `HealthServer` async API from Task 9 — see note below.
- Produces: `async def process_job(session, client, settings, job_id, session_factory=None) -> Outcome` where `Outcome` is now `@dataclass class Outcome: ack: bool; label: str` (**`recycle` field deleted**); `async def handle_message(session_factory, client, settings, stream, message_id, fields) -> Outcome`; `async def run_forever(settings, *, stop=None) -> int`; `__main__.main()` runs `sys.exit(asyncio.run(run_forever(settings)))`.
- **Ordering note:** Task 9 rewrites `HealthServer` with `async def start/stop`. Execute Task 9 before this task's final full-run, or temporarily keep the worker's health-server block commented consistently. Recommended execution order: 9 then 8, or accept that `run_forever` integration tests pass only after both land.

- [ ] **Step 1: Rewrite `process_job` (async, cancellation-safe)**

Same control flow, with these exact changes:
- Signature: `async def process_job(session: AsyncSession, client: redis.Redis, settings: Settings, job_id: UUID, session_factory: async_sessionmaker[AsyncSession] | None = None) -> Outcome`; all `repo.*` and `schedule_retry_or_fail` calls awaited.
- Handler invocation replaces the lambda/thread version:

```python
    try:
        payload = validate_payload(job.type, job.payload)
        result = await run_with_timeout(
            run_handler(job.type, payload, ctx), settings.job_handler_timeout_s
        )
    except JobCancelled as cancelled:
        won = await repo.cancel_job(session, job.id, cancelled.summary)
        log.info("job.cancelled", extra={"won": won})
        _record_outcome(job, "cancelled", started)
        return Outcome(ack=won, label="cancelled")
    except HandlerTimeout as timeout_exc:
        await session.rollback()  # discard any state the cancelled handler left mid-flight
        span.record_exception(timeout_exc)
        span.set_status(Status(StatusCode.ERROR, "HandlerTimeout"))
        won = await schedule_retry_or_fail(
            session, client, settings, job,
            {"type": "HandlerTimeout", "message": f">{settings.job_handler_timeout_s}s"},
        )
        log.warning("job.timeout", extra={"won": won})
        _record_outcome(job, "timeout", started)
        return Outcome(ack=won, label="timeout")
    except asyncio.CancelledError:
        # External cancellation (shutdown): roll back before propagating so the
        # job row isn't left with a half-written transaction; the reaper will
        # reclaim the message.
        await session.rollback()
        raise
    except Exception as exc:  # noqa: BLE001 — any handler/validation error is retryable
        ...same as today with awaits, returning Outcome(ack=won, label="retried")
```

- All other `Outcome(...)` constructions drop `recycle=`.

- [ ] **Step 2: Rewrite `handle_message` and the run loop**

`handle_message`: `async def`; `async with session_factory() as session:` around `await process_job(...)`; `await ack(...)`.

`run_forever` core (replacing the sync while-loop and recycle logic):

```python
async def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> int:
    configure_telemetry(settings, "jobs-worker", instance_id=CONSUMER_NAME)
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    client = create_redis_client(settings.redis_url)

    heartbeat = Heartbeat()
    health_server: HealthServer | None = None
    if settings.health_port is not None:
        health_server = HealthServer(
            port=settings.health_port,
            heartbeat=heartbeat,
            max_heartbeat_age_s=worker_heartbeat_threshold_s(settings),
            engine=engine,
            redis_client=client,
        )
        await health_server.start()

    for stream in settings.ordered_streams:
        await ensure_group(client, stream, settings.consumer_group)

    if settings.otel_enabled:
        register_worker_resource_gauges()

    shutting_down = {"flag": False}

    def _request_stop(*_):
        shutting_down["flag"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    bind_static_log_context(consumer=CONSUMER_NAME)
    log.info(
        "worker.started",
        extra={
            "streams": settings.ordered_streams,
            "group": settings.consumer_group,
            "concurrency": settings.worker_concurrency,
        },
    )

    def _should_stop() -> bool:
        return shutting_down["flag"] or (stop() if stop else False)

    sem = asyncio.Semaphore(settings.worker_concurrency)

    async def _run_one(stream: str, message_id: str, fields: dict) -> None:
        try:
            await handle_message(session_factory, client, settings, stream, message_id, fields)
        finally:
            sem.release()

    async with asyncio.TaskGroup() as tg:
        while not _should_stop():
            heartbeat.beat()
            await sem.acquire()  # wait for a free slot before pulling work
            try:
                batch = await read_priority(
                    client,
                    settings.ordered_streams,
                    settings.consumer_group,
                    CONSUMER_NAME,
                    settings.block_ms,
                    count=1,
                )
            except BaseException:
                sem.release()
                raise
            if not batch:
                sem.release()
                continue
            stream, message_id, fields = batch[0]
            tg.create_task(_run_one(stream, message_id, fields))
        # Exiting the `async with` waits for in-flight jobs to finish draining.

    log.info("worker.stopped")
    if health_server is not None:
        await health_server.stop()
    shutdown_telemetry()
    await client.aclose()
    await engine.dispose()
    return 0
```

Notes locked in: one message per read keeps strict priority exact; the semaphore is acquired *before* reading so a full worker blocks on XREADGROUP without over-fetching; `_run_one` swallows nothing — `handle_message` already converts handler errors to outcomes, and a raised exception fails the TaskGroup (crash → compose restart, which is the intended fail-fast). Return code is always 0 (graceful stop); recycle exit code 1 is gone. Heartbeat still beats once per loop iteration; the liveness threshold formula in `worker_heartbeat_threshold_s` still bounds it (block_ms + handler timeout + slack) because semaphore waits are bounded by the handler timeout.

`app/worker/__main__.py`:

```python
def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    sys.exit(asyncio.run(run_forever(settings)))
```

- [ ] **Step 3: Add a concurrency test**

Add to `tests/integration/test_worker.py`:

```python
async def test_jobs_run_concurrently(test_settings, pg_engine, redis_client, owner_id, monkeypatch):
    """With concurrency N, two slow jobs overlap instead of running serially."""
    import app.jobs.registry as registry

    running = 0
    peak = 0

    async def slow_handler(payload, ctx):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.3)
        running -= 1
        return {"ok": True}

    monkeypatch.setitem(registry.HANDLERS, JobType.email, slow_handler)
    factory = make_session_factory(pg_engine)
    async with factory() as session:
        jobs = [
            await repo.create_job(
                session, JobType.email,
                {"to": "a@b.c", "subject": "s", "body": "b"}, user_id=owner_id,
            )
            for _ in range(2)
        ]
    for job in jobs:
        await enqueue(redis_client, test_settings.stream_for_priority(job.priority), str(job.id))

    settings = test_settings.model_copy(update={"worker_concurrency": 2, "block_ms": 100})
    processed = {"n": 0}

    # run the worker loop until both jobs complete, with a hard deadline
    async def _stop_when_done():
        return processed["n"] >= 2  # placeholder replaced by polling job status below

    async def _poll_done() -> bool:
        async with factory() as session:
            done = [
                (await repo.get_job(session, j.id)).status is JobStatus.completed
                for j in jobs
            ]
        processed["n"] = sum(done)
        return all(done)

    stop_flag = {"stop": False}
    worker = asyncio.create_task(run_forever(settings, stop=lambda: stop_flag["stop"]))
    try:
        async with asyncio.timeout(15):
            while not await _poll_done():
                await asyncio.sleep(0.05)
    finally:
        stop_flag["stop"] = True
        await worker
    assert peak == 2
```

Adapt existing worker tests: `process_job`/`handle_message`/`run_forever` calls awaited; drop every assertion on `outcome.recycle` and on recycle exit codes (a timeout now asserts `outcome.label == "timeout"` and worker keeps running); timeout tests assert the worker processes a subsequent job after a timeout instead of exiting.

- [ ] **Step 4: Run**

Run: `uv run pytest tests/integration/test_worker.py tests/integration/test_batch.py tests/integration/test_cancel.py tests/integration/test_reaper.py -v`
Expected: PASS (requires Task 9's HealthServer if `health_port` is set in any test; default settings leave it `None`).

- [ ] **Step 5: Commit**

```bash
git add app/worker tests/integration/test_worker.py tests/integration/test_batch.py tests/integration/test_cancel.py tests/integration/test_reaper.py
git commit -m "feat: TaskGroup worker with configurable concurrency, recycle removed"
```

---

### Task 9: HealthServer → FastAPI + Uvicorn on the shared loop

**Files:**
- Modify: `app/core/healthcheck.py`
- Modify: `tests/integration/test_healthcheck.py`, `tests/unit/test_healthcheck_thresholds.py`

**Interfaces:**
- Consumes: `AsyncEngine`, `redis.asyncio.Redis` (Task 1).
- Produces: `Heartbeat` unchanged (`beat()`, `age_seconds()` — keep, now called from one loop; drop the threading.Lock and its docstring, plain attribute suffices). `worker_heartbeat_threshold_s`/`ticker_heartbeat_threshold_s` unchanged. New `HealthServer`:
  - `HealthServer(port: int, heartbeat: Heartbeat, max_heartbeat_age_s: float, engine: AsyncEngine, redis_client: redis.Redis)`
  - `async def start(self) -> None` (serves; sets `self.port` to the bound port — supports port 0 in tests)
  - `async def stop(self) -> None`
  - Endpoints: `GET /health` (liveness: heartbeat age; and *because the server shares the worker loop*, a permanently blocked loop means the probe never answers → probe timeout → restart), `GET /ready` (async `SELECT 1` + `PING`, PING bounded by `asyncio.wait_for(..., 2.0)`).

- [ ] **Step 1: Rewrite `app/core/healthcheck.py`**

```python
import asyncio
import time

import redis.asyncio as redis
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.config import Settings


def worker_heartbeat_threshold_s(settings: Settings) -> float:
    """A worker legitimately blocks on XREADGROUP then awaits a job slot; busy
    is healthy, hung is not."""
    return settings.block_ms / 1000 + settings.job_handler_timeout_s + 10.0


def ticker_heartbeat_threshold_s(settings: Settings) -> float:
    return max(10.0, 5 * settings.ticker_interval_s)


class Heartbeat:
    """Last-beat tracker for the main loop (single event loop: plain attribute)."""

    def __init__(self) -> None:
        self._last = time.monotonic()

    def beat(self) -> None:
        self._last = time.monotonic()

    def age_seconds(self) -> float:
        return time.monotonic() - self._last


_REDIS_PROBE_TIMEOUT_S = 2.0  # /ready must fail fast despite the client's generous 5s/10s socket timeouts


class HealthServer:
    """Uvicorn server task on the shared event loop: /health = liveness (loop
    heartbeat — a blocked loop also simply never answers, so the probe times
    out and the orchestrator restarts the pod), /ready = readiness probing the
    app's own async engine pool and Redis client."""

    def __init__(
        self,
        port: int,
        heartbeat: Heartbeat,
        max_heartbeat_age_s: float,
        engine: AsyncEngine,
        redis_client: redis.Redis,
    ) -> None:
        self._heartbeat = heartbeat
        self._max_age = max_heartbeat_age_s
        self._engine = engine
        self._redis = redis_client
        self._task: asyncio.Task | None = None

        app = FastAPI()

        @app.get("/health")
        async def health() -> JSONResponse:
            age = self._heartbeat.age_seconds()
            if age <= self._max_age:
                return JSONResponse({"status": "ok", "checks": {"loop": "ok"}})
            return JSONResponse(
                {
                    "status": "unavailable",
                    "checks": {"loop": f"stale ({age:.0f}s > {self._max_age:.0f}s)"},
                },
                status_code=503,
            )

        @app.get("/ready")
        async def ready() -> JSONResponse:
            checks: dict[str, str] = {}
            try:
                async with self._engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                checks["postgres"] = "ok"
            except Exception:  # noqa: BLE001 — any failure means not ready
                checks["postgres"] = "error"
            try:
                await asyncio.wait_for(self._redis.ping(), _REDIS_PROBE_TIMEOUT_S)
                checks["redis"] = "ok"
            except (redis.RedisError, TimeoutError):
                checks["redis"] = "error"
            ok = all(value == "ok" for value in checks.values())
            return JSONResponse(
                {"status": "ok" if ok else "unavailable", "checks": checks},
                status_code=200 if ok else 503,
            )

        config = uvicorn.Config(
            app, host="0.0.0.0", port=port, log_level="warning", access_log=False
        )
        self._server = uvicorn.Server(config)
        self.port = port

    async def start(self) -> None:
        self._task = asyncio.create_task(self._server.serve())
        while not self._server.started:
            if self._task.done():
                self._task.result()  # surface bind errors: fail fast, compose restarts
                raise RuntimeError("health server exited before startup")
            await asyncio.sleep(0.01)
        self.port = self._server.servers[0].sockets[0].getsockname()[1]

    async def stop(self) -> None:
        self._server.should_exit = True
        if self._task is not None:
            await self._task
```

Also disable uvicorn's own signal handling so the worker's SIGTERM handler stays in charge: add `self._server.install_signal_handlers = lambda: None` right after constructing the server (uvicorn's `serve()` inside an existing loop skips signal installation in recent versions, but pin the behavior explicitly).

- [ ] **Step 2: Update tests and run**

`tests/unit/test_healthcheck_thresholds.py`: unchanged (pure functions). `tests/integration/test_healthcheck.py`: tests become async; construct with async engine + async redis fixtures, `await server.start()` (port 0), hit `http://127.0.0.1:{server.port}/health` and `/ready` with `httpx.AsyncClient`, `await server.stop()`. Keep the same assertions (200/503 bodies, stale-heartbeat 503, broken-backend readiness 503).

Run: `uv run pytest tests/integration/test_healthcheck.py tests/unit/test_healthcheck_thresholds.py -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add app/core/healthcheck.py tests/integration/test_healthcheck.py tests/unit/test_healthcheck_thresholds.py
git commit -m "feat: serve health probes via FastAPI/uvicorn on the shared event loop"
```

---

### Task 10: Ticker → async loop

**Files:**
- Modify: `app/ticker/runner.py`, `app/ticker/__main__.py`
- Modify: `tests/integration/test_ticker.py`

**Interfaces:**
- Consumes: async repo/queue/retry (Tasks 2–4), async `HealthServer` (Task 9).
- Produces: `async def promote_due(session, client, settings) -> int`, `async def reconcile_orphans(session, client, settings) -> int`, `async def reap_stale(session, client, settings) -> int`, `async def _reap_one(...)`, `async def run_forever(settings, *, stop=None) -> None`. Gauge callbacks stay **sync** and get dedicated sync clients (see below); their helper signatures change to `queue_depth_observations(sync_client, settings)`, `queue_scheduled_observations(sync_client, settings)`, `job_status_observations(sync_session_factory, settings)` where `sync_client` is a `redis.Redis` (sync) and `sync_session_factory` a classic `sessionmaker`.

- [ ] **Step 1: Convert tick functions**

`promote_due` / `reconcile_orphans` / `_reap_one` / `reap_stale`: `async def`, awaiting every `delayed.*`, `repo.*`, `enqueue`, `schedule_retry_or_fail`, `client.xack`, `client.xautoclaim`, `ensure_group` call. Logic, spans, and comments unchanged.

- [ ] **Step 2: Keep OTel gauges on sync clients**

Metric reader threads call gauge callbacks synchronously — they cannot await. In `run_forever`, when `settings.otel_enabled`, build observability-only sync resources:

```python
import redis as sync_redis
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def _make_sync_observability_resources(settings: Settings):
    """Gauge callbacks run on the OTel exporter thread and cannot await; give
    them their own tiny sync engine/client, used for nothing else."""
    engine = create_engine(settings.database_url, pool_pre_ping=True, pool_timeout=5, pool_size=1)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    client = sync_redis.Redis.from_url(
        settings.redis_url, decode_responses=True,
        socket_connect_timeout=5, socket_timeout=10,
    )
    return engine, factory, client
```

`register_ticker_gauges(sync_client, sync_session_factory, settings)` and the three `*_observations` helpers keep their current bodies (they were already sync) — only the import changes so they type against the sync `redis`/`Session`. Dispose the sync engine and close the sync client in the shutdown block.

- [ ] **Step 3: Convert the run loop**

```python
async def run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> None:
    ...setup identical to today but: engine/session_factory/client from the async factories,
       `await health_server.start()`, `await ensure_group(...)` per stream,
       sync observability resources per Step 2 when otel_enabled...

    while not _should_stop():
        heartbeat.beat()
        try:
            async with session_factory() as session:
                promoted = await promote_due(session, client, settings)
            now = time.time()
            if now - last_reconcile >= settings.reconcile_interval_s:
                async with session_factory() as session:
                    recovered = await reconcile_orphans(session, client, settings)
                last_reconcile = now
                if recovered:
                    log.info("ticker.reconciled", extra={"count": recovered})
            if now - last_reap >= settings.reaper_interval_s:
                async with session_factory() as session:
                    await reap_stale(session, client, settings)
                last_reap = now
            if promoted >= settings.ticker_batch_size:
                continue
            await asyncio.sleep(settings.ticker_interval_s)
        except Exception:  # noqa: BLE001
            log.exception("ticker.tick_failed")
            await asyncio.sleep(settings.ticker_interval_s)

    log.info("ticker.stopped")
    if health_server is not None:
        await health_server.stop()
    shutdown_telemetry()
    await client.aclose()
    await engine.dispose()
    # plus sync observability engine.dispose()/client.close() when created
```

`app/ticker/__main__.py`: `asyncio.run(run_forever(settings))`.

- [ ] **Step 4: Convert `tests/integration/test_ticker.py` and run**

Tests become async; `promote_due`/`reconcile_orphans`/`reap_stale` awaited with async `db_session`/`redis_client` fixtures; assertions unchanged.

Run: `uv run pytest tests/integration/test_ticker.py -v`
Expected: PASS

- [ ] **Step 5: Full suite, lint, commit**

Run: `uv run pytest`
Expected: ALL PASS (this is the migration-complete gate).
Run: `uv run ruff check --fix && uv run ruff format`

```bash
git add app/ticker tests/integration/test_ticker.py
git commit -m "feat: convert ticker to async loop with sync observability sidecar"
```

---

### Task 11: Manual verification (Docker Compose)

**Files:** none (verification only; per user preference, infra/config is verified manually, not via pytest).

- [ ] **Step 1: Bring the stack up**

Run: `docker compose up --build -d` then `docker compose ps`
Expected: api, worker, ticker, postgres, redis all healthy (healthchecks green).

- [ ] **Step 2: Concurrency check**

Submit ~20 jobs quickly via `POST /jobs` (X-API-Key from the mounted secret). Watch worker logs: multiple `job.received` before the first `job.completed`, and with `WORKER_CONCURRENCY=2` on the worker service, at most 2 in flight.

- [ ] **Step 3: Timeout + cancellation check**

Submit a job that exceeds `JOB_HANDLER_TIMEOUT_S` (temporarily lower it, e.g. `JOB_HANDLER_TIMEOUT_S=2`, `VISIBILITY_TIMEOUT_S=10`): expect `job.timeout` log, job retried/failed per backoff, and — critically — **no worker restart** (`docker compose ps` shows 0 restarts). Cancel a processing batch job via `POST /jobs/{id}/cancel`: expect 202 then status `cancelled` with partial summary.

- [ ] **Step 4: Probe check**

`curl localhost:<health_port>/health` and `/ready` on worker and ticker: 200 with `{"status":"ok"}`. Stop redis (`docker compose stop redis`): `/ready` flips to 503 with `"redis": "error"`; start it again and confirm recovery.

- [ ] **Step 5: Report results to the user** (evidence, not assertions — paste the observed log lines/status codes).
