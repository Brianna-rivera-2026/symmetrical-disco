# Production Hardening — Persistence & Zero-Loss Redeploys Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the job-processing stack survive container recreation without losing data, and make redeploys drain in-flight work instead of killing it — per `docs/superpowers/specs/2026-07-01-production-hardening-design.md`.

**Architecture:** Two named Docker volumes (`pgdata`, `redisdata`) + Redis AOF close the persistence gap. `stop_grace_period` tuned to each service's worst-case drain time (already-implemented SIGTERM handling in `worker`/`ticker`, uvicorn in `api`) closes the redeploy gap. Explicit image tags give a rollback path. Total Redis loss recovery stays a documented manual playbook — no new application code for that case (approved design decision).

**Tech Stack:** Docker Compose, Redis 7 (AOF), PostgreSQL 16.

## Global Constraints

- Run all Python/tooling via `uv run ...` (e.g. `uv run pytest`, `uv run ruff check --fix`) — never raw `python`/`pip`/`venv`. (CLAUDE.md)
- No `print()` in application code — structured logging (`structlog`) only. (CLAUDE.md)
- This plan makes **no schema/migration change** and **no job-processing application-code change** — only `docker-compose.yml` config and docs (per the approved spec, §5).
- **Config-only changes (Docker Compose, env vars, infra flags) are verified manually — a documented command + expected output — not via automated pytest tests.** Automated tests are reserved for actual application code behavior. (Project convention; see also: a real-OS-SIGTERM test was tried for the drain behavior below and dropped after hitting a Windows-only trap — `os.kill(pid, SIGTERM)` hard-kills via `TerminateProcess` on Windows instead of invoking the registered handler — reinforcing why manual verification is the right tool for infra config.)
- Redis image: `redis:7`. Postgres image: `postgres:16`. Python: `>=3.11`. (existing `docker-compose.yml` / `pyproject.toml`)

---

## File Structure

- **Modify:** `docker-compose.yml` — add `pgdata`/`redisdata` named volumes + Redis AOF `command` (Task 1); add `stop_grace_period` per service + shared `image:` tag (Task 2).
- **Create:** `docs/runbooks/redis-total-loss-recovery.md` — the operator playbook for a total Redis wipe, adapted from spec §3.3 (Task 3).
- **Modify:** `DECISIONS.md` — new "§6 Persistence, Redeploys & Recovery" section (Task 4).
- **Modify:** `README.md` — new "Persistence & Deployment" section + link to the runbook and the production-hardening spec (Task 4).

---

### Task 1: Add named volumes + Redis AOF to docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing new.
- Produces: the `pgdata`/`redisdata` volumes and Redis `command` that Task 2 builds on.

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

- [ ] **Step 2: Manual verification against the real compose stack**

Run:
```bash
docker compose up -d postgres redis
docker compose exec redis redis-cli set smoke-test hello
docker compose down
docker compose up -d postgres redis
docker compose exec redis redis-cli get smoke-test
```
Expected: the final command prints `hello` — proving `docker-compose.yml` persists Redis data across a `down`/`up` cycle. Clean up with `docker compose down`.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: persist postgres and redis data via named volumes + AOF"
```

---

### Task 2: stop_grace_period + image tags for zero-loss redeploys

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: nothing new.
- Produces: nothing consumed by later tasks (Task 4 docs reference these values by their literal numbers, not by import).

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

This manual check is the only verification for graceful drain in this plan — an automated real-OS-SIGTERM test was tried and dropped (see Global Constraints) after hitting a Windows-only signal-delivery trap. The worker's SIGTERM handling itself is pre-existing application code (`app/worker/runner.py:84-88`, unchanged by this plan) — this task only adds the compose-level `stop_grace_period` that lets that existing behavior take effect during a real redeploy.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: graceful stop_grace_period + explicit image tags for redeploys"
```

---

### Task 3: Total Redis loss recovery playbook

**Files:**
- Create: `docs/runbooks/redis-total-loss-recovery.md`

**Interfaces:**
- Consumes: nothing (pure documentation).
- Produces: a path referenced by Task 4's README/DECISIONS updates.

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

### Task 4: DECISIONS.md and README.md updates

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
- We also deliberately did **not** write automated tests asserting the
  `docker-compose.yml` config itself (volumes, AOF, `stop_grace_period`) —
  config-only changes are verified manually. An early attempt at an automated
  real-OS-SIGTERM regression test hit a Windows-only trap
  (`os.kill(pid, SIGTERM)` hard-kills via `TerminateProcess` on Windows
  instead of invoking the registered handler) that a manual check never would
  have hit.

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
before the final paragraph). **Check the current `README.md` content first** —
it may already have local, uncommitted edits (e.g. new `/health`, `/stats`,
`/cancel` endpoint docs); place this section sensibly relative to whatever
structure is actually there rather than assuming the exact anchor text below
still matches verbatim:

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

Then update the design-spec link line at/near the end of the file (wherever it
currently points only at the Phase 1 design doc) to also link the production-
hardening spec, e.g. appending:
```markdown
 and for the persistence/redeploy hardening described above, see [`docs/superpowers/specs/2026-07-01-production-hardening-design.md`](docs/superpowers/specs/2026-07-01-production-hardening-design.md).
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
Expected: all tests pass (this plan adds no new tests — 161 passed is the
baseline this plan should still show at the end).

- [ ] **Lint**

Run: `uv run ruff check --fix && uv run ruff format`
Expected: no remaining issues.
