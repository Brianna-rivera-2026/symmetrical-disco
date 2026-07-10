# Design Specification: Asynchronous Worker and Database Layer

## Goal / Problem Statement
Migrate the background job processing system's database, queue consumer, API, Worker, and Ticker components to be asynchronous. 
Additionally, run multiple worker jobs concurrently up to a configurable limit (`worker_concurrency`), managed via `asyncio.TaskGroup` and `asyncio.Semaphore`. Using native asyncio task cancellation for timeout management eliminates the need to recycle worker processes to reclaim resources.

---

## Approved Decisions (user-reviewed 2026-07-10)
- **Config Setting**: A new config parameter `worker_concurrency` (default: `10`) is added to control the concurrency limit of the worker. It is configured via environment variables (e.g. `WORKER_CONCURRENCY`). — *Approved.*
- **Recycle Removal**: Worker processes will no longer exit and restart after job timeouts, as cancellation is natively handled by asyncio and leaves no orphaned threads. — *Approved: drop recycling entirely.*

---

## Proposed Changes

### 1. Database and Repository Layer
- **Async Engine & Session**: Replace `create_engine` and `sessionmaker` with `create_async_engine` and `async_sessionmaker` utilizing the `AsyncSession` class.
- **Repository Methods**: Convert all database helper functions in `app/repository.py` to `async def` and ensure queries are run asynchronously.
- **Alembic / Migrations**: Keep Alembic migrations running synchronously as part of the startup flow (they run once during deploy/init, which is clean).

### 2. API Service
- **Endpoints**: Transition all HTTP route handlers in `app/api/routes.py` to `async def`.
- **Dependencies**: Update database dependencies (`get_db`) to yield `AsyncSession` using `async with`.
- **Redis Client**: Instantiate an asynchronous Redis client (`redis.asyncio.Redis`) and await all enqueue/schedule operations.

### 3. Queue Consumer & Producer
- **Producer / Consumer API**: Transition functions in `app/queue/producer.py` and `app/queue/consumer.py` to async.
- **Pipeline Operations**: Convert the pipeline implementation in `promote` (`app/queue/delayed.py`) to use `async with client.pipeline(...)`.

### 4. Worker Service
- **Worker Concurrency**: Load `worker_concurrency` from environment or config.
- **Async Handlers**: Change all handlers in `app/jobs/handlers.py` to `async def` and replace `time.sleep` with `await asyncio.sleep`.
- **Cooperative Cancellation**: Adapt `PgJobContext.cancelled()` to be an async method that queries the database asynchronously.
- **Loop Concurrency**: Build the runner around `asyncio.TaskGroup` and `asyncio.Semaphore`.
- **Timeout Management**: Replace `ThreadPoolExecutor` and manual thread timeout handling with `asyncio.wait_for()`, triggering task cancellation immediately upon timeout.
- **Transactional Rollback on Cancellation**: The worker's base execution will use an async context manager that explicitly catches `asyncio.CancelledError`. If a job times out and is cancelled, the context manager performs an `await session.rollback()` to ensure database integrity before letting the exception bubble up.

### 5. Health Server & Ticker Service
- **Worker Health & Liveness Verification**: Use `FastAPI` + `Uvicorn` inside `HealthServer` to run health checks asynchronously on the worker and ticker processes. The Liveness Probe (`/health` endpoint) performs a deep check verifying that the TaskGroup event loop is actually ticking by checking the main loop's heartbeat. If the shared event loop gets permanently blocked, the Liveness Probe will natively time out, allowing OpenShift to kill and restart the degraded pod.
- **Ticker Loop**: Convert the Ticker runner loop to run asynchronously using `await asyncio.sleep` for interval pauses.
- **Async Ticker Methods**: Make `promote_due`, `reconcile_orphans`, and `reap_stale` async.

---

## Verification Plan

### Automated Tests
- Adapt the integration and unit tests to run with an async testing setup.
- Convert tests that mock database or Redis sessions to utilize async sessions.
- Run `uv run pytest` to verify both unit and integration tests.

### Manual Verification
- Deploy the Docker compose setup and verify that the API, Worker, and Ticker start up successfully.
- Trigger high-concurrency job submission to verify that jobs are processed concurrently.
- Trigger job cancellations and timeouts to ensure that they are gracefully aborted via asyncio task cancellation and that transactions are correctly rolled back.
