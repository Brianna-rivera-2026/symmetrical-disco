# Production Hardening — Persistence & Zero-Loss Redeploys Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the job-processing stack survive container recreation without losing data, and make redeploys drain in-flight work instead of killing it — per `docs/superpowers/specs/2026-07-01-production-hardening-design.md`.

**Architecture:** Two named Docker volumes (`pgdata`, `redisdata`) + Redis AOF close the persistence gap. `stop_grace_period` tuned to each service's worst-case drain time (already-implemented SIGTERM handling in `worker`/`ticker`, uvicorn in `api`) closes the redeploy gap. Explicit image tags give a rollback path. Total Redis loss recovery stays a documented manual playbook — no new application code for that case (approved design decision).

**Tech Stack:** Docker Compose, Redis 7 (AOF), PostgreSQL 16, pytest + testcontainers (integration tests), PyYAML (new dev dependency, for a compose-config-reading test).

## Global Constraints

- Run all Python/tooling via `uv run ...` (e.g. `uv run pytest`, `uv run ruff check --fix`) — never raw `python`/`pip`/`venv`. (CLAUDE.md)
- Add new dependencies via `uv add --dev <package>`, never edit `pyproject.toml`'s dependency list by hand.
- No `print()` in application or test code — structured logging (`structlog`) only, and tests use `assert`, not prints. (CLAUDE.md)
- This plan makes **no schema/migration change** and **no job-processing application-code change** — only `docker-compose.yml` config, two new/extended test files, and docs (per the approved spec, §5).
- Redis image: `redis:7`. Postgres image: `postgres:16`. Python: `>=3.11`. (existing `docker-compose.yml` / `pyproject.toml`)
- Follow existing test conventions: integration tests live in `tests/integration/`, use the `testcontainers`-based fixtures already in `tests/integration/conftest.py` (`redis_container`, `pg_engine`, etc.) where applicable, and use `app.core.redis.create_redis_client` (not a bare `redis.Redis(...)`) for any new Redis client construction, matching `tests/integration/conftest.py:56-61`.

---

## File Structure

- **Modify:** `docker-compose.yml` — add `pgdata`/`redisdata` named volumes + Redis AOF `command` (Task 2); add `stop_grace_period` per service + shared `image:` tag (Task 4).
- **Create:** `tests/integration/test_persistence.py` — static compose-config assertions (volumes + AOF present) and a dynamic Redis-container-recreation test proving Stream/ZSET data survives (Task 1).
- **Modify:** `tests/integration/test_worker.py` — add a real-SIGTERM graceful-drain characterization test (Task 3).
- **Create:** `docs/runbooks/redis-total-loss-recovery.md` — the operator playbook for a total Redis wipe, adapted from spec §3.3 (Task 5).
- **Modify:** `DECISIONS.md` — new "§6 Persistence, Redeploys & Recovery" section (Task 6).
- **Modify:** `README.md` — new "Persistence & Deployment" section + link to the runbook and the production-hardening spec (Task 6).
- **Modify:** `pyproject.toml` / `uv.lock` — add `pyyaml` as a dev dependency, via `uv add --dev pyyaml` (Task 1, step 1).

---

### Task 1: Persistence integration test (RED)

**Files:**
- Create: `tests/integration/test_persistence.py`
- Modify: `pyproject.toml`, `uv.lock` (via `uv add --dev pyyaml`)

**Interfaces:**
- Consumes: `app.core.redis.create_redis_client(redis_url: str) -> redis.Redis` (existing, `app/core/redis.py:4`).
- Produces: nothing new for later tasks — this is a leaf test file. Later tasks (2) make it pass.

This test reads the *actual* `docker-compose.yml` file (not a hardcoded copy of the config), so it goes RED now and GREEN only once Task 2 edits the real file — genuine regression protection, not a demonstration test.

- [ ] **Step 1: Add the `pyyaml` dev dependency**

Run: `uv add --dev pyyaml`
Expected: `pyproject.toml`'s `[dependency-groups] dev` list gains a `pyyaml>=...` entry and `uv.lock` updates.

- [ ] **Step 2: Write the failing test**

Create `tests/integration/test_persistence.py`:

```python
import time
import uuid
from pathlib import Path

import docker
import pytest
import yaml
from testcontainers.redis import RedisContainer

from app.core.redis import create_redis_client

COMPOSE_PATH = Path(__file__).resolve().parents[2] / "docker-compose.yml"


def _load_compose_services() -> dict:
    compose = yaml.safe_load(COMPOSE_PATH.read_text())
    return compose["services"]


def test_compose_services_declare_persistent_volumes():
    services = _load_compose_services()

    postgres_volumes = services["postgres"].get("volumes", [])
    assert any(v.endswith(":/var/lib/postgresql/data") for v in postgres_volumes), (
        "postgres service must mount a named volume at /var/lib/postgresql/data"
    )

    redis_service = services["redis"]
    redis_volumes = redis_service.get("volumes", [])
    assert any(v.endswith(":/data") for v in redis_volumes), (
        "redis service must mount a named volume at /data for persistence"
    )
    command = redis_service.get("command", "")
    assert "--appendonly yes" in command, "redis must run with AOF enabled"


def test_redis_state_survives_container_recreation_with_compose_config():
    redis_command = _load_compose_services()["redis"]["command"]
    docker_client = docker.from_env()
    volume_name = f"test-redis-persist-{uuid.uuid4().hex[:8]}"
    docker_client.volumes.create(name=volume_name)
    try:
        first = RedisContainer("redis:7").with_volume_mapping(
            volume_name, "/data", mode="rw"
        )
        first.with_command(redis_command)
        first.start()
        try:
            url = (
                f"redis://{first.get_container_host_ip()}:"
                f"{first.get_exposed_port(6379)}/0"
            )
            client = create_redis_client(url)
            client.xadd("teststream", {"job_id": "abc"})
            client.zadd("jobs:delayed", {"job-2": 123456.0})
            # appendfsync everysec flushes on a ~1s cycle; wait past it before
            # we pull the container out from under the writes.
            time.sleep(1.5)
            client.close()
        finally:
            first.stop()  # stops + removes the container; the named volume is untouched

        second = RedisContainer("redis:7").with_volume_mapping(
            volume_name, "/data", mode="rw"
        )
        second.with_command(redis_command)
        second.start()
        try:
            url = (
                f"redis://{second.get_container_host_ip()}:"
                f"{second.get_exposed_port(6379)}/0"
            )
            client = create_redis_client(url)
            entries = client.xrange("teststream", "-", "+")
            assert len(entries) == 1
            assert entries[0][1] == {"job_id": "abc"}
            assert client.zscore("jobs:delayed", "job-2") == 123456.0
            client.close()
        finally:
            second.stop()
    finally:
        docker_client.volumes.get(volume_name).remove(force=True)
```

- [ ] **Step 2b: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_persistence.py -v`
Expected: FAIL — both tests fail against today's `docker-compose.yml`:
`test_compose_services_declare_persistent_volumes` fails on the postgres-volume assertion (no `volumes:` key exists on either service yet); `test_redis_state_survives_container_recreation_with_compose_config` fails with a `KeyError: 'command'` (redis service has no `command` key yet), since the container starts with default (non-AOF) settings and the second container — pointed at an empty-by-default `/data` mount with no AOF — has nothing to read.

- [ ] **Step 3: Commit the RED test**

```bash
git add tests/integration/test_persistence.py pyproject.toml uv.lock
git commit -m "test: add failing persistence coverage for compose volumes + AOF"
```

---

### Task 2: Add named volumes + Redis AOF to docker-compose.yml (GREEN)

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing new.
- Produces: the `pgdata`/`redisdata` volumes and Redis `command` that Task 1's test reads and Task 4 builds on.

- [ ] **Step 1: Edit `docker-compose.yml`** — add `volumes:` to `postgres`, add `command:` + `volumes:` to `redis`, and a top-level `volumes:` block:

```yaml
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: jobs
      POSTGRES_PASSWORD: jobs
      POSTGRES_DB: jobs
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U jobs"]
      interval: 3s
      timeout: 3s
      retries: 10
    ports:
      - "5432:5432"

  redis:
    image: redis:7
    command: redis-server --appendonly yes --appendfsync everysec
    volumes:
      - redisdata:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 3s
      timeout: 3s
      retries: 10
    ports:
      - "6379:6379"
```

And at the end of the file (new top-level key, sibling of `services:`):

```yaml
volumes:
  pgdata:
  redisdata:
```

- [ ] **Step 2: Run the persistence test to verify it passes**

Run: `uv run pytest tests/integration/test_persistence.py -v`
Expected: PASS (2 passed). Note: the second test builds two real `redis:7` containers and sleeps 1.5s, so expect it to take several seconds — this is normal.

- [ ] **Step 3: Manual verification against the real compose stack**

Run:
```bash
docker compose up -d postgres redis
docker compose exec redis redis-cli set smoke-test hello
docker compose down
docker compose up -d postgres redis
docker compose exec redis redis-cli get smoke-test
```
Expected: the final command prints `hello` — proving the real `docker-compose.yml` (not just the test's ad hoc containers) persists Redis data across a `down`/`up` cycle. Clean up with `docker compose down`.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: persist postgres and redis data via named volumes + AOF"
```

---

### Task 3: Worker graceful-drain characterization test

**Files:**
- Modify: `tests/integration/test_worker.py`

**Interfaces:**
- Consumes: `app.worker.runner.run_forever(settings: Settings, *, stop: Callable[[], bool] | None = None) -> int` (existing, `app/worker/runner.py:75`) — already installs real `signal.signal(signal.SIGTERM, ...)` handlers at `app/worker/runner.py:87-88` and only checks `_should_stop()` at the top of its loop (`app/worker/runner.py:102`), so with `read_priority`'s `count=1` (`app/queue/consumer.py:48`) at most one job is ever in flight.
- Produces: nothing new for later tasks.

This test does **not** require new application code — the drain behavior already exists (`worker/runner.py:84-88`, `97-98`). It is a regression guard proving a *real* OS SIGTERM (not the injected `stop` callable every other test in this file uses) is honored only after the in-flight job completes. Because `signal.signal()` only works on the main thread, the test sends the signal from a background `threading.Timer` while `run_forever` blocks the main thread — this mirrors how Docker actually delivers SIGTERM to a running process.

- [ ] **Step 1: Write the test**

Add to `tests/integration/test_worker.py` (needs `import os`, `import signal`, `import threading` at the top of the file alongside the existing `import time`):

```python
def test_run_forever_drains_in_flight_job_on_real_sigterm(
    test_settings, redis_client, pg_engine, monkeypatch
):
    from app.core.db import make_session_factory
    from app.queue.consumer import ensure_group
    from app.queue.producer import enqueue
    from app.worker.runner import run_forever

    # Let the handler run for real (not the autouse no-sleep patch) so the
    # SIGTERM arrives while the job is still in flight.
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: _real_sleep(0.5))

    for stream in test_settings.ordered_streams:
        ensure_group(redis_client, stream, test_settings.consumer_group)
    factory = make_session_factory(pg_engine)
    with factory() as s:
        job = repo.create_job(s, JobType.email, {"to": "a@b.com", "subject": "Hi"})
    enqueue(redis_client, test_settings.stream_normal, str(job.id))

    # Fire mid-handler: the worker has already claimed the job (well under
    # 0.5s) but the handler's sleep is not yet done.
    timer = threading.Timer(0.15, lambda: os.kill(os.getpid(), signal.SIGTERM))
    timer.start()
    try:
        exit_code = run_forever(test_settings)
    finally:
        timer.cancel()

    assert exit_code == 0
    with factory() as s:
        assert repo.get_job(s, job.id).status is JobStatus.completed
    assert (
        redis_client.xpending(test_settings.stream_normal, test_settings.consumer_group)[
            "pending"
        ]
        == 0
    )
```

- [ ] **Step 2: Run test to verify it already passes**

Run: `uv run pytest tests/integration/test_worker.py::test_run_forever_drains_in_flight_job_on_real_sigterm -v`
Expected: PASS immediately. This is expected and correct — the drain logic predates this plan (spec §4.1: "No worker code change"); this step exists to add permanent regression coverage for a real-signal path the suite didn't previously exercise (existing tests only use the injected `stop` callable, e.g. `test_worker.py:80-104`).

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_worker.py
git commit -m "test: cover worker graceful drain on a real SIGTERM"
```

---

### Task 4: stop_grace_period + image tags for zero-loss redeploys

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing new.
- Produces: nothing consumed by later tasks (Task 6 docs reference these values by their literal numbers, not by import).

- [ ] **Step 1: Edit `docker-compose.yml`** — add `image:` to each buildable service and `stop_grace_period:` to `api`, `worker`, `ticker`:

```yaml
  migrate:
    build: .
    image: jobprocessor-app:${APP_TAG:-dev}
    command: alembic upgrade head
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    depends_on:
      postgres:
        condition: service_healthy

  api:
    build: .
    image: jobprocessor-app:${APP_TAG:-dev}
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    ports:
      - "8000:8000"
    stop_grace_period: 30s
    healthcheck:
      test: ["CMD", "python", "-c",
             "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').status==200 else 1)"]
      interval: 15s
      timeout: 12s
      retries: 5
      start_period: 10s
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy

  worker:
    build: .
    image: jobprocessor-app:${APP_TAG:-dev}
    command: python -m app.worker
    restart: on-failure
    stop_grace_period: 50s
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy

  ticker:
    build: .
    image: jobprocessor-app:${APP_TAG:-dev}
    command: python -m app.ticker
    stop_grace_period: 15s
    environment:
      DATABASE_URL: postgresql+psycopg://jobs:jobs@postgres:5432/jobs
      REDIS_URL: redis://redis:6379/0
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
```

(`worker`'s `50s` = `job_handler_timeout_s` (45s, `app/core/config.py:26`) + 5s margin. `api`'s `30s` covers uvicorn's HTTP drain. `ticker`'s `15s` comfortably covers one promote/reconcile/reap pass.)

- [ ] **Step 2: Validate the compose file parses**

Run: `docker compose config --quiet`
Expected: no output, exit code 0 (confirms valid YAML + valid Compose schema, including the new `${APP_TAG:-dev}` interpolation).

- [ ] **Step 3: Manual verification of the drain behavior**

Run:
```bash
docker compose up -d --build
docker compose exec api curl -s -X POST http://localhost:8000/jobs -H 'content-type: application/json' -d '{"type":"report","payload":{"report_type":"x"}}'
docker compose stop -t 50 worker
docker compose logs worker --tail 20
```
Expected: the worker logs show `job.completed` (or `job.received` immediately followed by a normal finish) before `worker.stopped exit_code=0` — i.e., the container exits on its own well before the 50s timeout, rather than being killed. Clean up with `docker compose down`.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: graceful stop_grace_period + explicit image tags for redeploys"
```

---

### Task 5: Total Redis loss recovery playbook

**Files:**
- Create: `docs/runbooks/redis-total-loss-recovery.md`

**Interfaces:**
- Consumes: nothing (pure documentation).
- Produces: a path referenced by Task 6's README/DECISIONS updates.

- [ ] **Step 1: Write the playbook**

Create `docs/runbooks/redis-total-loss-recovery.md`:

```markdown
# Runbook: Total Redis Loss Recovery

**When to use this:** Redis has lost all data (e.g., the `redisdata` volume was
deleted, or the AOF file was corrupted and Redis started with an empty
dataset) while Postgres still holds job history. This is a manual procedure —
there is no automated self-heal for this case (see
`docs/superpowers/specs/2026-07-01-production-hardening-design.md`, §3.3, for
why).

## Symptoms

- Redis has no streams, no `jobs:delayed` ZSET, and no consumer group.
- Postgres still has `pending`/`scheduled`/`processing` rows.
- `pending`/`scheduled` rows are flagged `is_synced_to_redis = TRUE` even
  though Redis has nothing for them — `reconcile_orphans` only acts on rows
  flagged `FALSE`, so it will not pick these up on its own.
- `processing` rows have lost their PEL entry, so the reaper (which reads the
  PEL via `XAUTOCLAIM`) is blind to them too.

## Steps

1. **Confirm scope.** Connect to Redis and Postgres; verify Redis has no
   streams/ZSET/consumer group while Postgres still holds
   `pending`/`scheduled`/`processing` rows.

2. **Recreate the consumer group.** Bring the services up (or restart them) —
   `ensure_group` runs on `api`, `worker`, and `ticker` startup and recreates
   the consumer group on each stream. No manual action needed unless you want
   it immediately, in which case:
   ```
   redis-cli XGROUP CREATE jobs:stream:high workers $ MKSTREAM
   redis-cli XGROUP CREATE jobs:stream:normal workers $ MKSTREAM
   redis-cli XGROUP CREATE jobs:stream:low workers $ MKSTREAM
   ```

3. **Re-arm reconcile.** Run once against Postgres:
   ```sql
   UPDATE jobs SET is_synced_to_redis = FALSE
   WHERE status IN ('pending', 'scheduled');
   ```
   The ticker's `reconcile_orphans` loop (runs every `reconcile_interval_s`,
   default 60s) will then re-derive each row: `pending` jobs go to their
   priority stream, `scheduled` jobs go into `jobs:delayed` at
   `scheduled_at`. Any duplicate delivery is absorbed by the worker's
   idempotent claim-guard — this step is safe to run even if some rows
   already have a live message.

4. **Recover stuck `processing` jobs.** For `processing` rows older than
   `visibility_timeout_s` (default 60s) whose worker process is confirmed
   dead, re-enqueue them:
   ```sql
   UPDATE jobs SET status = 'pending', is_synced_to_redis = FALSE
   WHERE id = '<job-id>';
   ```
   **Do not** touch a `processing` row if its worker might still be alive —
   that worker will finish and commit normally; resetting it risks the job
   running twice.

5. **Verify.** Watch the ticker's logs for `ticker.reconciled count=…` and
   confirm affected jobs transition to `completed`/`failed` via
   `GET /jobs/{id}` or `GET /jobs?status=...`.
```

- [ ] **Step 2: Manually validate the playbook once**

This exercises the runbook itself end-to-end against the real stack, per the
design spec's "Manual (playbook validation)" testing requirement (§6). Not an
automated test — there is no code path to assert against (§3.3's non-goal).

Run:
```bash
docker compose up -d --build
curl -X POST http://localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.com","subject":"Hi"}}'
curl -X POST http://localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"type":"email","payload":{"to":"a@b.com","subject":"Hi"},"scheduled_at":"2099-01-01T00:00:00Z"}'
docker compose exec redis redis-cli FLUSHALL
docker compose exec postgres psql -U jobs -d jobs -c \
  "UPDATE jobs SET is_synced_to_redis = FALSE WHERE status IN ('pending','scheduled');"
docker compose logs -f ticker
```
Expected: within `reconcile_interval_s` (60s), the ticker logs
`ticker.reconciled count=2`; the immediate job then completes normally
(worker logs `job.completed`), and the scheduled job sits correctly in
`jobs:delayed` until its `scheduled_at` (verify with
`docker compose exec redis redis-cli ZSCORE jobs:delayed <job-id>`). Clean up
with `docker compose down`.

- [ ] **Step 3: Commit**

```bash
git add docs/runbooks/redis-total-loss-recovery.md
git commit -m "docs: add total-Redis-loss recovery playbook"
```

---

### Task 6: DECISIONS.md and README.md updates

**Files:**
- Modify: `DECISIONS.md`
- Modify: `README.md`

**Interfaces:** none — pure documentation, no code.

- [ ] **Step 1: Append a new section to `DECISIONS.md`**

Add after the existing "## 5. One Thing I Would Do Differently With More Time" section (i.e., at the end of the file):

```markdown

---

## 6. Persistence, Redeploys & Recovery

**Approach chosen:** Named Docker volumes for Postgres (`pgdata`) and Redis
(`redisdata`) + Redis AOF (`appendfsync everysec`); `stop_grace_period` tuned
per service to the existing (already-implemented) SIGTERM drain behavior;
explicit `image:` tags for rollback; total-Redis-loss recovery is a
**documented manual playbook**, not new automation.

**Why:**
- Volumes + AOF close the biggest gap: without them, any container
  recreation (including a routine `docker compose up --build`) wiped all job
  and queue state.
- The worker already traps SIGTERM and finishes its one in-flight job before
  exiting (`app/worker/runner.py`); the only missing piece was compose's
  `stop_grace_period`, which defaulted to Docker's 10s and forced a SIGKILL on
  any `report` job (45s handler timeout) during a redeploy.
- We deliberately did **not** build an automated Redis-rebuild-on-startup
  path. `reconcile_orphans` only re-syncs rows flagged
  `is_synced_to_redis = FALSE`, so a *total* Redis wipe (not just a restart —
  AOF + the volume already cover restarts) strands already-synced
  `pending`/`scheduled` jobs. An automated fix would need new startup code,
  new reset semantics on `is_synced_to_redis`, and a race-safe sentinel — real
  complexity for a failure mode the volume + AOF already make rare. See
  `docs/runbooks/redis-total-loss-recovery.md` for the manual procedure.

**Trade-offs:**
- **Deploy latency:** the worker's `stop_grace_period: 50s` means a redeploy
  can take up to 50s per worker if a long job is in flight. Accepted — bounded
  to one handler, and it avoids redone work and redelivery churn.
- **Manual recovery step:** a total Redis loss requires an operator to run the
  runbook; it does not self-heal. Accepted given how rare a total loss is once
  the volume + AOF are in place.
- **API downtime, not just data-loss, on recreate:** a single `api` container
  briefly stops accepting requests during `up --build`; "zero-loss" (no
  dropped jobs — `commit-then-XADD` already makes this safe) holds, but
  "zero-downtime" would need multiple `api` replicas behind a proxy, which is
  out of scope here.
- **Migration discipline:** because `api`/`worker`/`ticker` are recreated
  independently during a redeploy, old and new code briefly run against the
  same schema. Migrations in this project follow **expand/contract**:
  additive-only within a release (new nullable columns, new tables); a column
  or constraint the previous release's code still reads is only dropped in a
  *later* release, after that code is fully retired. Combined with explicit
  `image:` tags, this means rolling back to the previous image is always safe
  — it runs against the newer, backward-compatible schema without needing a
  down-migration.
```

- [ ] **Step 2: Add a new section to `README.md`**

Insert this new section between the job-types bullet list and the closing
"For the full design, see..." line (i.e., right after the `**report:**` bullet,
before the final paragraph):

```markdown

## Persistence & Deployment

Postgres (`pgdata`) and Redis (`redisdata`) both use named Docker volumes, and
Redis runs with AOF persistence (`appendfsync everysec`). Job history and
queue state (streams, the consumer-group PEL, the delayed-jobs ZSET) survive
`docker compose down` / `up --build` and container restarts. Only an explicit
`docker compose down -v` destroys them.

**Redeploys drain gracefully.** `worker`, `api`, and `ticker` all trap SIGTERM
and finish in-flight work before exiting; each service's `stop_grace_period`
in `docker-compose.yml` is set above its worst-case drain time (the worker's
50s covers the 45s handler timeout) so Docker's SIGKILL never arrives mid-job
during a normal redeploy.

**Rollback:** each buildable service shares an explicit `image:
jobprocessor-app:${APP_TAG:-dev}` tag. Tag a release with
`APP_TAG=v1.2.0 docker compose build`, deploy it with
`APP_TAG=v1.2.0 docker compose up -d`, and roll back by re-running `up -d`
with the previous `APP_TAG`. This is safe because migrations in this project
are additive-only within a release (see `DECISIONS.md` §6) — an older image
never breaks against a newer schema.

**If Redis ever loses all its data** (e.g., the volume itself is deleted),
recovery is a manual procedure — see
[`docs/runbooks/redis-total-loss-recovery.md`](docs/runbooks/redis-total-loss-recovery.md).
```

Then update the final line of the file from:
```markdown
For the full design, see [`docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md`](docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md).
```
to:
```markdown
For the full design, see [`docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md`](docs/superpowers/specs/2026-06-30-job-processing-phase1-design.md), and for the persistence/redeploy hardening described above, see [`docs/superpowers/specs/2026-07-01-production-hardening-design.md`](docs/superpowers/specs/2026-07-01-production-hardening-design.md).
```

- [ ] **Step 3: Commit**

```bash
git add DECISIONS.md README.md
git commit -m "docs: document persistence, redeploy drain, and migration discipline"
```

---

## Final verification

- [ ] **Run the full suite**

Run: `uv run pytest`
Expected: all tests pass, including the two new/extended files
(`tests/integration/test_persistence.py`,
`tests/integration/test_worker.py::test_run_forever_drains_in_flight_job_on_real_sigterm`).

- [ ] **Lint**

Run: `uv run ruff check --fix && uv run ruff format`
Expected: no remaining issues.
