# OpenShift Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the job processor to OpenShift via a Helm chart: TLS everywhere, NetworkPolicy isolation, edge-TLS Route gateway, probes, PgBouncer + KEDA scaling with enforced connection math, memory-threshold worker self-recycling, OTel via the Red Hat operator.

**Architecture:** One Helm chart (`deploy/chart/jobprocessor/`) with in-chart Postgres/PgBouncer/Redis, hook Jobs for migrate/users-sync, and operator CRs behind values flags. Small app changes: engine pool options for PgBouncer, a memory recycler in the worker, a draining flag on the health server. Spec: `docs/superpowers/specs/2026-07-11-openshift-deployment-design.md`.

**Tech Stack:** Helm 3, OpenShift (CRC), KEDA (Custom Metrics Autoscaler), Red Hat build of OpenTelemetry, sclorg postgresql-16, redis:7, edoburu/pgbouncer, Python/FastAPI app (uv).

## Global Constraints

- Run all Python via `uv run …`; never pip/venv/poetry. Tests: `uv run pytest`. Lint: `uv run ruff check --fix` and `uv run ruff format`.
- No print statements; use `logging.getLogger("app.<component>")`.
- Config/infra (chart, scripts) is verified manually with `helm lint`/`helm template`/CRC — **no pytest for chart files** (project convention). App code changes DO get pytest coverage.
- **Deviation from spec, agreed rationale:** RSS is read via `psutil` (already a dependency, imported in `app/worker/runner.py`), not `/proc/self/status` — tests must run on Windows dev machines where `/proc` does not exist.
- All chart resources carry labels `app.kubernetes.io/name: jobprocessor`, `app.kubernetes.io/instance: {{ .Release.Name }}`, and `app.kubernetes.io/component: <component>`; NetworkPolicies select on these exact labels.
- Chart resource names are `{{ .Release.Name }}-<component>` via the `jobprocessor.fullname` helper.
- Helm commands in this plan run from repo root: `helm lint deploy/chart/jobprocessor` / `helm template rel deploy/chart/jobprocessor`.

---

### Task 1: Engine options — pool size and PgBouncer-safe prepared statements

PgBouncer transaction pooling breaks psycopg's server-side prepared statements; the chart also needs per-service pool sizes for the connection math. Add settings and thread them into `make_engine`.

**Files:**
- Modify: `app/core/config.py`
- Modify: `app/core/db.py`
- Modify: `app/worker/runner.py` (make_engine call), `app/main.py`, `app/ticker/` (its make_engine call — grep `make_engine(` to find the exact file), `app/users/sync.py`
- Test: `tests/unit/test_db.py` (create), `tests/unit/test_config.py` (extend)

**Interfaces:**
- Produces: `Settings.db_pool_size: int = 5`, `Settings.db_disable_prepared_statements: bool = False`, `Settings.worker_max_rss_mb: int | None = None`; `engine_kwargs(pool_size: int, disable_prepared_statements: bool) -> dict`; `make_engine(database_url: str, *, pool_size: int = 5, disable_prepared_statements: bool = False) -> AsyncEngine`.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_db.py
from app.core.db import engine_kwargs


def test_engine_kwargs_defaults():
    kwargs = engine_kwargs(pool_size=5, disable_prepared_statements=False)
    assert kwargs["pool_size"] == 5
    assert kwargs["pool_pre_ping"] is True
    assert kwargs["pool_timeout"] == 5
    assert "connect_args" not in kwargs


def test_engine_kwargs_disables_prepared_statements_for_pgbouncer():
    kwargs = engine_kwargs(pool_size=8, disable_prepared_statements=True)
    assert kwargs["pool_size"] == 8
    assert kwargs["connect_args"] == {"prepare_threshold": None}
```

Add to `tests/unit/test_config.py` (follow the file's existing style for constructing `Settings` — it must already pass `database_url`/`redis_url`):

```python
def test_new_deployment_settings_defaults():
    settings = Settings(database_url="postgresql+psycopg://u:p@h/db", redis_url="redis://h")
    assert settings.db_pool_size == 5
    assert settings.db_disable_prepared_statements is False
    assert settings.worker_max_rss_mb is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_db.py tests/unit/test_config.py -v`
Expected: FAIL — `ImportError: cannot import name 'engine_kwargs'` and `AttributeError`/assertion on new settings.

- [ ] **Step 3: Implement**

In `app/core/config.py`, add to `Settings` (near `worker_concurrency`):

```python
    db_pool_size: int = 5
    db_disable_prepared_statements: bool = False
    worker_max_rss_mb: int | None = None
```

Replace `app/core/db.py`'s `make_engine`:

```python
def engine_kwargs(pool_size: int, disable_prepared_statements: bool) -> dict:
    # pool_timeout=5: a saturated pool turns into a fast 503 on /ready
    # instead of a 30s hang (also bounds app-side checkout waits).
    kwargs: dict = {"pool_pre_ping": True, "pool_timeout": 5, "pool_size": pool_size}
    if disable_prepared_statements:
        # PgBouncer transaction pooling: a prepared statement lives on one
        # server connection but later executions may land on another.
        kwargs["connect_args"] = {"prepare_threshold": None}
    return kwargs


def make_engine(
    database_url: str,
    *,
    pool_size: int = 5,
    disable_prepared_statements: bool = False,
) -> AsyncEngine:
    return create_async_engine(
        database_url, **engine_kwargs(pool_size, disable_prepared_statements)
    )
```

Update every `make_engine(` caller (grep for it; expected: `app/worker/runner.py`, `app/main.py`, the ticker module, `app/users/sync.py`) to:

```python
engine = make_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    disable_prepared_statements=settings.db_disable_prepared_statements,
)
```

- [ ] **Step 4: Run the full test suite**

Run: `uv run pytest`
Expected: PASS (new tests plus no regressions).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/core/config.py app/core/db.py app/worker/runner.py app/main.py app/ticker app/users/sync.py tests/unit/test_db.py tests/unit/test_config.py
git commit -m "feat: engine pool size and PgBouncer-safe prepared-statement toggle"
```

---

### Task 2: MemoryRecycler + `worker.recycles` metric

**Files:**
- Create: `app/worker/recycler.py`
- Modify: `app/core/metrics.py`
- Test: `tests/unit/test_recycler.py` (create)

**Interfaces:**
- Produces: `MemoryRecycler(max_rss_mb: int | None, rss_bytes: Callable[[], int] | None = None)` with method `should_recycle() -> bool` and attribute `triggered: bool` (latches True permanently once breached); `app.core.metrics.worker_recycles` counter.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_recycler.py
from app.worker.recycler import MemoryRecycler


def test_disabled_when_threshold_is_none():
    recycler = MemoryRecycler(max_rss_mb=None, rss_bytes=lambda: 10**12)
    assert recycler.should_recycle() is False


def test_below_threshold_does_not_trigger():
    recycler = MemoryRecycler(max_rss_mb=100, rss_bytes=lambda: 50 * 1024 * 1024)
    assert recycler.should_recycle() is False
    assert recycler.triggered is False


def test_breach_triggers_and_latches():
    rss = {"value": 50 * 1024 * 1024}
    recycler = MemoryRecycler(max_rss_mb=100, rss_bytes=lambda: rss["value"])
    assert recycler.should_recycle() is False
    rss["value"] = 101 * 1024 * 1024
    assert recycler.should_recycle() is True
    rss["value"] = 10  # latched: dropping back below does not un-trigger
    assert recycler.should_recycle() is True
    assert recycler.triggered is True


def test_breach_logs_warning(caplog):
    import logging

    recycler = MemoryRecycler(max_rss_mb=1, rss_bytes=lambda: 2 * 1024 * 1024)
    with caplog.at_level(logging.WARNING, logger="app.worker"):
        assert recycler.should_recycle() is True
    assert any(r.message == "worker.recycling" for r in caplog.records)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_recycler.py -v`
Expected: FAIL — `ModuleNotFoundError: app.worker.recycler`.

- [ ] **Step 3: Implement**

Append to `app/core/metrics.py`:

```python
worker_recycles = _meter.create_counter(
    "worker.recycles", description="Workers that self-recycled on memory breach"
)
```

Create `app/worker/recycler.py`:

```python
"""Memory-threshold self-recycling: when RSS exceeds the configured cap the
worker stops claiming jobs, drains its TaskGroup, and exits 0 so the
orchestrator replaces the pod (long-term stability against slow leaks)."""

import logging
from collections.abc import Callable

import psutil

from app.core import metrics as app_metrics

log = logging.getLogger("app.worker")

_MB = 1024 * 1024


class MemoryRecycler:
    def __init__(
        self,
        max_rss_mb: int | None,
        rss_bytes: Callable[[], int] | None = None,
    ) -> None:
        self._max_bytes = max_rss_mb * _MB if max_rss_mb is not None else None
        self._rss_bytes = rss_bytes or psutil.Process().memory_info
        self._uses_psutil = rss_bytes is None
        self.triggered = False

    def _read_rss(self) -> int:
        raw = self._rss_bytes()
        return raw.rss if self._uses_psutil else raw

    def should_recycle(self) -> bool:
        if self._max_bytes is None or self.triggered:
            return self.triggered
        rss = self._read_rss()
        if rss > self._max_bytes:
            self.triggered = True
            app_metrics.worker_recycles.add(1)
            log.warning(
                "worker.recycling",
                extra={
                    "rss_mb": round(rss / _MB),
                    "max_rss_mb": round(self._max_bytes / _MB),
                },
            )
        return self.triggered
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_recycler.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/worker/recycler.py app/core/metrics.py tests/unit/test_recycler.py
git commit -m "feat: memory-threshold recycler with worker.recycles counter"
```

---

### Task 3: HealthServer draining flag (readiness flips during drain)

**Files:**
- Modify: `app/core/healthcheck.py` (`HealthServer.__init__` and the `/ready` handler)
- Test: `tests/integration/test_healthcheck.py` (extend — follow the file's existing fixtures for constructing/starting a `HealthServer`)

**Interfaces:**
- Consumes: existing `HealthServer(port, heartbeat, max_heartbeat_age_s, engine, redis_client)`.
- Produces: new keyword-only param `draining: Callable[[], bool] | None = None`; when it returns True, `GET /ready` responds 503 with body `{"status": "draining", "checks": {"draining": "true"}}` without probing Postgres/Redis. `/health` is unaffected (a draining worker is alive).

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_healthcheck.py`, reusing that file's existing pattern for building the engine/redis fixtures and issuing HTTP requests against `server.port` (copy the arrangement of the nearest existing `/ready` test):

```python
async def test_ready_returns_503_while_draining(engine, redis_client):
    heartbeat = Heartbeat()
    server = HealthServer(
        port=0,
        heartbeat=heartbeat,
        max_heartbeat_age_s=60.0,
        engine=engine,
        redis_client=redis_client,
        draining=lambda: True,
    )
    await server.start()
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"http://127.0.0.1:{server.port}/ready")
        assert resp.status_code == 503
        assert resp.json()["status"] == "draining"
        health = await httpx.AsyncClient().get(f"http://127.0.0.1:{server.port}/health")
        assert health.status_code == 200  # draining is not dead
    finally:
        await server.stop()
```

(Adjust fixture names/imports to match the existing tests in that file exactly.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_healthcheck.py -v -k draining`
Expected: FAIL — `TypeError: HealthServer.__init__() got an unexpected keyword argument 'draining'`.

- [ ] **Step 3: Implement**

In `app/core/healthcheck.py`: add `draining: Callable[[], bool] | None = None` as a keyword-only `__init__` param (import `Callable` from `collections.abc`), store `self._draining = draining`, and at the top of the `ready()` handler:

```python
        @app.get("/ready")
        async def ready() -> JSONResponse:
            if self._draining is not None and self._draining():
                return JSONResponse(
                    {"status": "draining", "checks": {"draining": "true"}},
                    status_code=503,
                )
            checks: dict[str, str] = {}
            # ... existing body unchanged ...
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/integration/test_healthcheck.py -v`
Expected: PASS (new test plus all existing).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/core/healthcheck.py tests/integration/test_healthcheck.py
git commit -m "feat: draining flag flips /ready to 503 without probing deps"
```

---

### Task 4: Wire the recycler into the worker run loop

**Files:**
- Modify: `app/worker/runner.py` (`run_forever`)
- Test: `tests/integration/test_worker.py` (extend)

**Interfaces:**
- Consumes: `MemoryRecycler` (Task 2), `Settings.worker_max_rss_mb` (Task 1), `HealthServer(draining=...)` (Task 3).
- Produces: `run_forever` returns 0 after a graceful drain when the recycler triggers; readiness reports draining during the drain.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_worker.py`, following that file's existing pattern for building `Settings` and invoking `run_forever` (there are existing tests using the `stop=` callback — mirror their setup/fixtures):

```python
async def test_worker_exits_zero_when_memory_threshold_breached(settings_factory):
    # threshold of 1 MB is always exceeded by a real process → the loop must
    # notice on its first iteration, drain, and return 0 without any stop().
    settings = settings_factory(worker_max_rss_mb=1)
    exit_code = await asyncio.wait_for(run_forever(settings), timeout=30)
    assert exit_code == 0
```

(If the file builds `Settings` directly rather than via a factory fixture, construct it the same way its neighbors do and set `worker_max_rss_mb=1`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_worker.py -v -k memory_threshold`
Expected: FAIL — `asyncio.TimeoutError` (the loop never exits: nothing checks memory yet).

- [ ] **Step 3: Implement in `run_forever`**

In `app/worker/runner.py`:

1. Import: `from app.worker.recycler import MemoryRecycler`.
2. Before the `HealthServer` construction: `recycler = MemoryRecycler(settings.worker_max_rss_mb)`.
3. Pass to the health server: `draining=lambda: recycler.triggered` (new kwarg in the existing `HealthServer(...)` call).
4. Change the loop condition from `while not _should_stop():` to:

```python
        while not _should_stop() and not recycler.should_recycle():
```

Exiting the `async with asyncio.TaskGroup()` block already drains in-flight jobs; the function already returns 0. No other changes.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/worker/runner.py tests/integration/test_worker.py
git commit -m "feat: worker self-recycles on RSS breach with draining readiness"
```

---

### Task 5: Chart scaffold — Chart.yaml, values.yaml, helpers with connection-math gate, credentials Secret, service-CA ConfigMap

**Files:**
- Create: `deploy/chart/jobprocessor/Chart.yaml`
- Create: `deploy/chart/jobprocessor/values.yaml`
- Create: `deploy/chart/jobprocessor/templates/_helpers.tpl`
- Create: `deploy/chart/jobprocessor/templates/credentials-secret.yaml`
- Create: `deploy/chart/jobprocessor/templates/service-ca-configmap.yaml`

**Interfaces:**
- Produces (used verbatim by every later chart task): helpers `jobprocessor.fullname`, `jobprocessor.labels`, `jobprocessor.validateConnections`, `jobprocessor.appDatabaseUrl`, `jobprocessor.directDatabaseUrl`, `jobprocessor.redisUrl`; Secret `{{ fullname }}-credentials` with keys `db-password`, `redis-password`; ConfigMap `{{ fullname }}-service-ca` whose injected key `service-ca.crt` is mounted by consumers at `/etc/pki/service-ca/ca.crt`; the values schema below.

- [ ] **Step 1: Create Chart.yaml**

```yaml
apiVersion: v2
name: jobprocessor
description: Distributed background job processing system for OpenShift
type: application
version: 0.1.0
appVersion: "0.1.0"
```

- [ ] **Step 2: Create values.yaml**

```yaml
image:
  repository: jobprocessor-app
  tag: dev
  pullPolicy: IfNotPresent

api:
  replicas: 2
  dbPoolSize: 5

worker:
  replicas: 2          # used when keda.enabled=false
  dbPoolSize: 5
  concurrency: 10
  maxRssMb: 512

ticker:
  dbPoolSize: 2

keda:
  enabled: false
  maxReplicas: 6
  lagCount: 10         # target backlog per replica, per stream
  unsafeSsl: true      # KEDA cannot mount the service-CA ConfigMap; server-auth skipped for the scaler only

otel:
  enabled: false
  exporterEndpoint: "" # non-empty => collector forwards OTLP there; empty => debug exporter only

postgres:
  image: quay.io/sclorg/postgresql-16-c9s:latest
  maxConnections: 100
  reservedConnections: 5
  database: jobs
  user: jobs
  storage: 1Gi

pgbouncer:
  image: edoburu/pgbouncer:v1.23.1-p2
  defaultPoolSize: 20
  maxClientConn: 60

redis:
  image: redis:7
  storage: 1Gi

tls:
  appToPgbouncer: true

secrets:
  # created out-of-band by deploy/openshift/init-secrets.sh; key: api_user_keys.json
  apiUserKeysSecret: jobprocessor-api-user-keys
```

- [ ] **Step 3: Create templates/_helpers.tpl**

```yaml
{{- define "jobprocessor.fullname" -}}
{{ .Release.Name }}
{{- end }}

{{- define "jobprocessor.labels" -}}
app.kubernetes.io/name: jobprocessor
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Connection math (spec: PgBouncer + KEDA). Fails template rendering when
     the fleet at max scale could exceed PgBouncer's client ceiling, or
     PgBouncer's server pool could exceed Postgres capacity. */}}
{{- define "jobprocessor.validateConnections" -}}
{{- $workerMax := ternary .Values.keda.maxReplicas .Values.worker.replicas .Values.keda.enabled | int -}}
{{- $demand := add (mul $workerMax (.Values.worker.dbPoolSize | int)) (mul (.Values.api.replicas | int) (.Values.api.dbPoolSize | int)) (.Values.ticker.dbPoolSize | int) -}}
{{- if gt $demand (.Values.pgbouncer.maxClientConn | int) -}}
{{- fail (printf "connection math: max client demand %d (workers %d x %d + api %d x %d + ticker %d) exceeds pgbouncer.maxClientConn %d" $demand $workerMax (.Values.worker.dbPoolSize | int) (.Values.api.replicas | int) (.Values.api.dbPoolSize | int) (.Values.ticker.dbPoolSize | int) (.Values.pgbouncer.maxClientConn | int)) -}}
{{- end -}}
{{- $serverBudget := sub (.Values.postgres.maxConnections | int) (.Values.postgres.reservedConnections | int) -}}
{{- if gt (.Values.pgbouncer.defaultPoolSize | int) $serverBudget -}}
{{- fail (printf "connection math: pgbouncer.defaultPoolSize %d exceeds postgres budget %d (maxConnections - reservedConnections)" (.Values.pgbouncer.defaultPoolSize | int) $serverBudget) -}}
{{- end -}}
{{- end }}

{{- define "jobprocessor.postgresHost" -}}
{{ include "jobprocessor.fullname" . }}-postgres.{{ .Release.Namespace }}.svc
{{- end }}

{{- define "jobprocessor.pgbouncerHost" -}}
{{ include "jobprocessor.fullname" . }}-pgbouncer.{{ .Release.Namespace }}.svc
{{- end }}

{{- define "jobprocessor.redisHost" -}}
{{ include "jobprocessor.fullname" . }}-redis.{{ .Release.Namespace }}.svc
{{- end }}

{{/* URLs embed $(DB_PASSWORD)/$(REDIS_PASSWORD): kubelet env-var expansion
     substitutes them from secretKeyRef env vars declared earlier in the
     container spec, keeping passwords out of rendered manifests. */}}
{{- define "jobprocessor.appDatabaseUrl" -}}
postgresql+psycopg://{{ .Values.postgres.user }}:$(DB_PASSWORD)@{{ include "jobprocessor.pgbouncerHost" . }}:6432/{{ .Values.postgres.database }}{{- if .Values.tls.appToPgbouncer -}}?sslmode=verify-full&sslrootcert=/etc/pki/service-ca/ca.crt{{- end -}}
{{- end }}

{{- define "jobprocessor.directDatabaseUrl" -}}
postgresql+psycopg://{{ .Values.postgres.user }}:$(DB_PASSWORD)@{{ include "jobprocessor.postgresHost" . }}:5432/{{ .Values.postgres.database }}?sslmode=verify-full&sslrootcert=/etc/pki/service-ca/ca.crt
{{- end }}

{{- define "jobprocessor.redisUrl" -}}
rediss://:$(REDIS_PASSWORD)@{{ include "jobprocessor.redisHost" . }}:6379/0?ssl_ca_certs=/etc/pki/service-ca/ca.crt
{{- end }}

{{/* Shared env block for app containers (api/worker/ticker). */}}
{{- define "jobprocessor.appEnv" -}}
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "jobprocessor.fullname" . }}-credentials
      key: db-password
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "jobprocessor.fullname" . }}-credentials
      key: redis-password
- name: DATABASE_URL
  value: {{ include "jobprocessor.appDatabaseUrl" . | quote }}
- name: REDIS_URL
  value: {{ include "jobprocessor.redisUrl" . | quote }}
- name: DB_DISABLE_PREPARED_STATEMENTS
  value: "true"
{{- if .Values.otel.enabled }}
- name: OTEL_ENABLED
  value: "true"
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: http://{{ include "jobprocessor.fullname" . }}-otel-collector.{{ .Release.Namespace }}.svc:4317
{{- end }}
{{- end }}

{{/* Service-CA bundle mount, paired volume in jobprocessor.caVolume. */}}
{{- define "jobprocessor.caVolumeMount" -}}
- name: service-ca
  mountPath: /etc/pki/service-ca
  readOnly: true
{{- end }}

{{- define "jobprocessor.caVolume" -}}
- name: service-ca
  configMap:
    name: {{ include "jobprocessor.fullname" . }}-service-ca
    items:
      - key: service-ca.crt
        path: ca.crt
{{- end }}
```

- [ ] **Step 4: Create templates/credentials-secret.yaml**

```yaml
{{- $name := printf "%s-credentials" (include "jobprocessor.fullname" .) }}
{{- $existing := lookup "v1" "Secret" .Release.Namespace $name }}
{{- $dbPass := randAlphaNum 32 }}
{{- $redisPass := randAlphaNum 32 }}
{{- if $existing }}
{{- $dbPass = index $existing.data "db-password" | b64dec }}
{{- $redisPass = index $existing.data "redis-password" | b64dec }}
{{- end }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ $name }}
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: credentials
type: Opaque
stringData:
  db-password: {{ $dbPass | quote }}
  redis-password: {{ $redisPass | quote }}
```

- [ ] **Step 5: Create templates/service-ca-configmap.yaml**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "jobprocessor.fullname" . }}-service-ca
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: service-ca
  annotations:
    service.beta.openshift.io/inject-cabundle: "true"
# OpenShift's service-CA operator injects data["service-ca.crt"]; ship empty.
data: {}
```

- [ ] **Step 6: Verify — lint, render, and prove the math gate fires**

```bash
helm lint deploy/chart/jobprocessor
helm template rel deploy/chart/jobprocessor > /dev/null && echo RENDER-OK
```
Expected: `1 chart(s) linted, 0 chart(s) failed` and `RENDER-OK`.

The math gate is `include`d by the pgbouncer template (Task 8); it can't fire yet. Note this and re-verify in Task 8 Step 4.

- [ ] **Step 7: Commit**

```bash
git add deploy/chart/jobprocessor
git commit -m "feat(chart): scaffold with connection-math gate, credentials, service-CA bundle"
```

---

### Task 6: Postgres StatefulSet + Service with serving-cert TLS

**Files:**
- Create: `deploy/chart/jobprocessor/templates/postgres-statefulset.yaml`
- Create: `deploy/chart/jobprocessor/templates/postgres-service.yaml`
- Create: `deploy/chart/jobprocessor/templates/postgres-tls-configmap.yaml`

**Interfaces:**
- Consumes: `{{ fullname }}-credentials` (db-password), helpers from Task 5.
- Produces: Service `{{ fullname }}-postgres:5432` with serving-cert secret `{{ fullname }}-postgres-tls`; pods labeled `app.kubernetes.io/component: postgres`.

- [ ] **Step 1: Create postgres-tls-configmap.yaml** (sclorg images load extra config from `/opt/app-root/src/postgresql-cfg/*.conf`)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "jobprocessor.fullname" . }}-postgres-tls-conf
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: postgres
data:
  ssl.conf: |
    ssl = on
    ssl_cert_file = '/opt/app-root/certs/tls.crt'
    ssl_key_file = '/opt/app-root/certs/tls.key'
```

- [ ] **Step 2: Create postgres-service.yaml**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "jobprocessor.fullname" . }}-postgres
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: postgres
  annotations:
    service.beta.openshift.io/serving-cert-secret-name: {{ include "jobprocessor.fullname" . }}-postgres-tls
spec:
  clusterIP: None
  selector:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
```

- [ ] **Step 3: Create postgres-statefulset.yaml**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ include "jobprocessor.fullname" . }}-postgres
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: postgres
spec:
  serviceName: {{ include "jobprocessor.fullname" . }}-postgres
  replicas: 1
  selector:
    matchLabels:
      {{- include "jobprocessor.labels" . | nindent 6 }}
      app.kubernetes.io/component: postgres
  template:
    metadata:
      labels:
        {{- include "jobprocessor.labels" . | nindent 8 }}
        app.kubernetes.io/component: postgres
    spec:
      containers:
        - name: postgres
          image: {{ .Values.postgres.image }}
          env:
            - name: POSTGRESQL_USER
              value: {{ .Values.postgres.user | quote }}
            - name: POSTGRESQL_DATABASE
              value: {{ .Values.postgres.database | quote }}
            - name: POSTGRESQL_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: db-password
            - name: POSTGRESQL_MAX_CONNECTIONS
              value: {{ .Values.postgres.maxConnections | quote }}
          ports:
            - containerPort: 5432
          readinessProbe:
            exec:
              command: ["/usr/bin/pg_isready", "-U", {{ .Values.postgres.user | quote }}]
            initialDelaySeconds: 10
            periodSeconds: 5
          livenessProbe:
            tcpSocket:
              port: 5432
            initialDelaySeconds: 30
            periodSeconds: 10
          volumeMounts:
            - name: data
              mountPath: /var/lib/pgsql/data
            - name: tls
              mountPath: /opt/app-root/certs
              readOnly: true
            - name: tls-conf
              mountPath: /opt/app-root/src/postgresql-cfg
              readOnly: true
      volumes:
        - name: tls
          secret:
            secretName: {{ include "jobprocessor.fullname" . }}-postgres-tls
            # 0640 root-group: passes postgres's key-permission check while
            # staying readable by the arbitrary-UID pod (GID 0 supplemental).
            defaultMode: 0640
        - name: tls-conf
          configMap:
            name: {{ include "jobprocessor.fullname" . }}-postgres-tls-conf
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: {{ .Values.postgres.storage }}
```

- [ ] **Step 4: Verify render**

```bash
helm template rel deploy/chart/jobprocessor | grep -A2 "serving-cert-secret-name"
helm lint deploy/chart/jobprocessor
```
Expected: annotation renders as `rel-postgres-tls`; lint passes.

- [ ] **Step 5: Commit**

```bash
git add deploy/chart/jobprocessor/templates/postgres-*.yaml
git commit -m "feat(chart): postgres StatefulSet with serving-cert TLS"
```

---

### Task 7: Redis StatefulSet + Service, TLS-only with password

**Files:**
- Create: `deploy/chart/jobprocessor/templates/redis-statefulset.yaml`
- Create: `deploy/chart/jobprocessor/templates/redis-service.yaml`

**Interfaces:**
- Consumes: `{{ fullname }}-credentials` (redis-password).
- Produces: Service `{{ fullname }}-redis:6379` (TLS-only) with serving-cert secret `{{ fullname }}-redis-tls`; pods labeled `app.kubernetes.io/component: redis`.

- [ ] **Step 1: Create redis-service.yaml**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "jobprocessor.fullname" . }}-redis
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: redis
  annotations:
    service.beta.openshift.io/serving-cert-secret-name: {{ include "jobprocessor.fullname" . }}-redis-tls
spec:
  clusterIP: None
  selector:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: redis
  ports:
    - name: redis
      port: 6379
      targetPort: 6379
```

- [ ] **Step 2: Create redis-statefulset.yaml**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ include "jobprocessor.fullname" . }}-redis
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: redis
spec:
  serviceName: {{ include "jobprocessor.fullname" . }}-redis
  replicas: 1
  selector:
    matchLabels:
      {{- include "jobprocessor.labels" . | nindent 6 }}
      app.kubernetes.io/component: redis
  template:
    metadata:
      labels:
        {{- include "jobprocessor.labels" . | nindent 8 }}
        app.kubernetes.io/component: redis
    spec:
      containers:
        - name: redis
          image: {{ .Values.redis.image }}
          command:
            - sh
            - -c
            # port 0 disables the plaintext listener: TLS-only, per spec.
            - >
              exec redis-server
              --requirepass "$REDIS_PASSWORD"
              --appendonly yes --appendfsync everysec
              --port 0 --tls-port 6379
              --tls-cert-file /certs/tls.crt
              --tls-key-file /certs/tls.key
              --tls-auth-clients no
          env:
            - name: REDIS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: redis-password
          ports:
            - containerPort: 6379
          readinessProbe:
            exec:
              command:
                - sh
                - -c
                - redis-cli --tls --cacert /certs/tls.crt -a "$REDIS_PASSWORD" ping | grep -q PONG
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            tcpSocket:
              port: 6379
            initialDelaySeconds: 15
            periodSeconds: 10
          volumeMounts:
            - name: data
              mountPath: /data
            - name: tls
              mountPath: /certs
              readOnly: true
      volumes:
        - name: tls
          secret:
            secretName: {{ include "jobprocessor.fullname" . }}-redis-tls
            defaultMode: 0640
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: {{ .Values.redis.storage }}
```

- [ ] **Step 3: Verify render**

```bash
helm template rel deploy/chart/jobprocessor | grep -B2 -A8 "tls-port"
helm lint deploy/chart/jobprocessor
```
Expected: redis command shows `--port 0 --tls-port 6379`; lint passes.

- [ ] **Step 4: Commit**

```bash
git add deploy/chart/jobprocessor/templates/redis-*.yaml
git commit -m "feat(chart): TLS-only redis StatefulSet with requirepass"
```

---

### Task 8: PgBouncer Deployment + Service (transaction pooling, TLS both sides)

**Files:**
- Create: `deploy/chart/jobprocessor/templates/pgbouncer-deployment.yaml`
- Create: `deploy/chart/jobprocessor/templates/pgbouncer-service.yaml`

**Interfaces:**
- Consumes: postgres Service/credentials, service-CA ConfigMap, `jobprocessor.validateConnections`.
- Produces: Service `{{ fullname }}-pgbouncer:6432` with serving-cert secret `{{ fullname }}-pgbouncer-tls`; pods labeled `app.kubernetes.io/component: pgbouncer`. **This template invokes the connection-math gate.**

- [ ] **Step 1: Create pgbouncer-service.yaml**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "jobprocessor.fullname" . }}-pgbouncer
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: pgbouncer
  annotations:
    service.beta.openshift.io/serving-cert-secret-name: {{ include "jobprocessor.fullname" . }}-pgbouncer-tls
spec:
  selector:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: pgbouncer
  ports:
    - name: pgbouncer
      port: 6432
      targetPort: 6432
```

- [ ] **Step 2: Create pgbouncer-deployment.yaml** (edoburu/pgbouncer generates pgbouncer.ini from env vars)

```yaml
{{- include "jobprocessor.validateConnections" . }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "jobprocessor.fullname" . }}-pgbouncer
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: pgbouncer
spec:
  replicas: 1
  selector:
    matchLabels:
      {{- include "jobprocessor.labels" . | nindent 6 }}
      app.kubernetes.io/component: pgbouncer
  template:
    metadata:
      labels:
        {{- include "jobprocessor.labels" . | nindent 8 }}
        app.kubernetes.io/component: pgbouncer
    spec:
      containers:
        - name: pgbouncer
          image: {{ .Values.pgbouncer.image }}
          env:
            - name: DB_HOST
              value: {{ include "jobprocessor.postgresHost" . | quote }}
            - name: DB_PORT
              value: "5432"
            - name: DB_USER
              value: {{ .Values.postgres.user | quote }}
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: db-password
            - name: DB_NAME
              value: {{ .Values.postgres.database | quote }}
            - name: AUTH_TYPE
              value: scram-sha-256
            - name: POOL_MODE
              value: transaction
            - name: DEFAULT_POOL_SIZE
              value: {{ .Values.pgbouncer.defaultPoolSize | quote }}
            - name: MAX_CLIENT_CONN
              value: {{ .Values.pgbouncer.maxClientConn | quote }}
            - name: LISTEN_PORT
              value: "6432"
            - name: SERVER_TLS_SSLMODE
              value: verify-full
            - name: SERVER_TLS_CA_FILE
              value: /etc/pki/service-ca/ca.crt
            {{- if .Values.tls.appToPgbouncer }}
            - name: CLIENT_TLS_SSLMODE
              value: require
            - name: CLIENT_TLS_CERT_FILE
              value: /certs/tls.crt
            - name: CLIENT_TLS_KEY_FILE
              value: /certs/tls.key
            {{- end }}
          ports:
            - containerPort: 6432
          readinessProbe:
            tcpSocket:
              port: 6432
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            tcpSocket:
              port: 6432
            initialDelaySeconds: 15
            periodSeconds: 10
          volumeMounts:
            {{- include "jobprocessor.caVolumeMount" . | nindent 12 }}
            - name: tls
              mountPath: /certs
              readOnly: true
      volumes:
        {{- include "jobprocessor.caVolume" . | nindent 8 }}
        - name: tls
          secret:
            secretName: {{ include "jobprocessor.fullname" . }}-pgbouncer-tls
            defaultMode: 0640
```

- [ ] **Step 3: Verify render**

```bash
helm template rel deploy/chart/jobprocessor > /dev/null && echo RENDER-OK
helm lint deploy/chart/jobprocessor
```
Expected: `RENDER-OK`; lint passes.

- [ ] **Step 4: Verify the connection-math gate fires on bad values**

```bash
helm template rel deploy/chart/jobprocessor --set pgbouncer.maxClientConn=5 2>&1 | grep "connection math"
helm template rel deploy/chart/jobprocessor --set pgbouncer.defaultPoolSize=200 2>&1 | grep "connection math"
```
Expected: both commands fail rendering and print the respective `connection math: …` message.

- [ ] **Step 5: Commit**

```bash
git add deploy/chart/jobprocessor/templates/pgbouncer-*.yaml
git commit -m "feat(chart): pgbouncer transaction pooling with TLS and enforced connection math"
```

---

### Task 9: API Deployment + Service + edge-TLS Route

**Files:**
- Create: `deploy/chart/jobprocessor/templates/api-deployment.yaml`
- Create: `deploy/chart/jobprocessor/templates/api-service.yaml`
- Create: `deploy/chart/jobprocessor/templates/api-route.yaml`

**Interfaces:**
- Consumes: `jobprocessor.appEnv`, `jobprocessor.caVolume(Mount)`.
- Produces: Service `{{ fullname }}-api:8000`; Route `{{ fullname }}-api` (edge TLS, insecure→redirect); pods labeled `app.kubernetes.io/component: api`.

- [ ] **Step 1: Create api-deployment.yaml**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "jobprocessor.fullname" . }}-api
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: api
spec:
  replicas: {{ .Values.api.replicas }}
  selector:
    matchLabels:
      {{- include "jobprocessor.labels" . | nindent 6 }}
      app.kubernetes.io/component: api
  template:
    metadata:
      labels:
        {{- include "jobprocessor.labels" . | nindent 8 }}
        app.kubernetes.io/component: api
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: api
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          # image CMD already runs uvicorn app.main:app on :8000
          env:
            {{- include "jobprocessor.appEnv" . | nindent 12 }}
            - name: DB_POOL_SIZE
              value: {{ .Values.api.dbPoolSize | quote }}
          ports:
            - containerPort: 8000
          readinessProbe:
            httpGet:
              path: /ready
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
            failureThreshold: 3
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 15
            periodSeconds: 15
          volumeMounts:
            {{- include "jobprocessor.caVolumeMount" . | nindent 12 }}
      volumes:
        {{- include "jobprocessor.caVolume" . | nindent 8 }}
```

- [ ] **Step 2: Create api-service.yaml**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "jobprocessor.fullname" . }}-api
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: api
spec:
  selector:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: api
  ports:
    - name: http
      port: 8000
      targetPort: 8000
```

- [ ] **Step 3: Create api-route.yaml**

```yaml
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: {{ include "jobprocessor.fullname" . }}-api
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: api
spec:
  to:
    kind: Service
    name: {{ include "jobprocessor.fullname" . }}-api
  port:
    targetPort: http
  tls:
    termination: edge
    insecureEdgeTerminationPolicy: Redirect
```

- [ ] **Step 4: Verify render and confirm `/health` exists on the API app**

```bash
helm template rel deploy/chart/jobprocessor | grep -A4 "termination: edge"
grep -rn "\"/health\"\|'/health'\|/health" app/main.py app/api | head
```
Expected: Route renders; grep confirms the API app serves `/health` and `/ready` (Compose healthcheck already hits `/ready`). **If `/health` is missing on the API app, change the livenessProbe path to `/ready`** and note it in the commit message.

- [ ] **Step 5: Commit**

```bash
git add deploy/chart/jobprocessor/templates/api-*.yaml
git commit -m "feat(chart): api Deployment, Service, and edge-TLS Route gateway"
```

---

### Task 10: Worker + Ticker Deployments (probes, recycler env, grace periods)

**Files:**
- Create: `deploy/chart/jobprocessor/templates/worker-deployment.yaml`
- Create: `deploy/chart/jobprocessor/templates/ticker-deployment.yaml`

**Interfaces:**
- Consumes: `jobprocessor.appEnv`, health endpoints from Tasks 3–4 (`HEALTH_PORT=8001`, `/health`, `/ready`), `WORKER_MAX_RSS_MB` (Task 1 setting).
- Produces: Deployments `{{ fullname }}-worker` (component label `worker` — KEDA targets it by name in Task 13) and `{{ fullname }}-ticker`.

- [ ] **Step 1: Create worker-deployment.yaml**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "jobprocessor.fullname" . }}-worker
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: worker
spec:
  {{- if not .Values.keda.enabled }}
  replicas: {{ .Values.worker.replicas }}
  {{- end }}
  selector:
    matchLabels:
      {{- include "jobprocessor.labels" . | nindent 6 }}
      app.kubernetes.io/component: worker
  template:
    metadata:
      labels:
        {{- include "jobprocessor.labels" . | nindent 8 }}
        app.kubernetes.io/component: worker
    spec:
      # bounds the recycler/SIGTERM drain (spec: stuck job can't wedge the pod)
      terminationGracePeriodSeconds: 50
      containers:
        - name: worker
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["python", "-m", "app.worker"]
          env:
            {{- include "jobprocessor.appEnv" . | nindent 12 }}
            - name: DB_POOL_SIZE
              value: {{ .Values.worker.dbPoolSize | quote }}
            - name: WORKER_CONCURRENCY
              value: {{ .Values.worker.concurrency | quote }}
            - name: WORKER_MAX_RSS_MB
              value: {{ .Values.worker.maxRssMb | quote }}
            - name: HEALTH_PORT
              value: "8001"
          ports:
            - containerPort: 8001
          readinessProbe:
            httpGet:
              path: /ready
              port: 8001
            initialDelaySeconds: 10
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8001
            initialDelaySeconds: 20
            periodSeconds: 15
          volumeMounts:
            {{- include "jobprocessor.caVolumeMount" . | nindent 12 }}
      volumes:
        {{- include "jobprocessor.caVolume" . | nindent 8 }}
```

- [ ] **Step 2: Create ticker-deployment.yaml**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "jobprocessor.fullname" . }}-ticker
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: ticker
spec:
  replicas: 1
  selector:
    matchLabels:
      {{- include "jobprocessor.labels" . | nindent 6 }}
      app.kubernetes.io/component: ticker
  template:
    metadata:
      labels:
        {{- include "jobprocessor.labels" . | nindent 8 }}
        app.kubernetes.io/component: ticker
    spec:
      terminationGracePeriodSeconds: 15
      containers:
        - name: ticker
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["python", "-m", "app.ticker"]
          env:
            {{- include "jobprocessor.appEnv" . | nindent 12 }}
            - name: DB_POOL_SIZE
              value: {{ .Values.ticker.dbPoolSize | quote }}
            - name: HEALTH_PORT
              value: "8001"
          ports:
            - containerPort: 8001
          readinessProbe:
            httpGet:
              path: /ready
              port: 8001
            initialDelaySeconds: 10
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8001
            initialDelaySeconds: 20
            periodSeconds: 15
          volumeMounts:
            {{- include "jobprocessor.caVolumeMount" . | nindent 12 }}
      volumes:
        {{- include "jobprocessor.caVolume" . | nindent 8 }}
```

- [ ] **Step 3: Verify render**

```bash
helm template rel deploy/chart/jobprocessor | grep -c "WORKER_MAX_RSS_MB"   # expect 1
helm template rel deploy/chart/jobprocessor --set keda.enabled=true | grep -A1 "kind: Deployment" | grep -c replicas || true
helm lint deploy/chart/jobprocessor
```
Expected: `WORKER_MAX_RSS_MB` present once; with `keda.enabled=true` the worker Deployment omits `replicas:` (KEDA owns scale); lint passes.

- [ ] **Step 4: Commit**

```bash
git add deploy/chart/jobprocessor/templates/worker-deployment.yaml deploy/chart/jobprocessor/templates/ticker-deployment.yaml
git commit -m "feat(chart): worker and ticker Deployments with probes and recycler env"
```

---

### Task 11: Migrate + users-sync hook Jobs

**Files:**
- Create: `deploy/chart/jobprocessor/templates/migrate-job.yaml`
- Create: `deploy/chart/jobprocessor/templates/users-sync-job.yaml`

**Interfaces:**
- Consumes: `jobprocessor.directDatabaseUrl` (hooks bypass PgBouncer), external Secret `.Values.secrets.apiUserKeysSecret` (key `api_user_keys.json`, created by Task 14's script). App reads keys from `/run/secrets/api_user_keys` (`Settings.api_user_keys_file` default).
- Produces: hook Jobs labeled `app.kubernetes.io/component: hook` (NetworkPolicies select this label).

- [ ] **Step 1: Create migrate-job.yaml**

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "jobprocessor.fullname" . }}-migrate
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: hook
  annotations:
    # post-install (NOT pre-install: pre-install hooks run before postgres
    # exists on first install and would wait forever); pre-upgrade gates rollouts.
    "helm.sh/hook": post-install,pre-upgrade
    "helm.sh/hook-weight": "-10"
    "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        {{- include "jobprocessor.labels" . | nindent 8 }}
        app.kubernetes.io/component: hook
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["alembic", "upgrade", "head"]
          env:
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: db-password
            - name: DATABASE_URL
              value: {{ include "jobprocessor.directDatabaseUrl" . | quote }}
            - name: REDIS_URL   # Settings requires it even though alembic doesn't use Redis
              value: rediss://unused:6379/0
          volumeMounts:
            {{- include "jobprocessor.caVolumeMount" . | nindent 12 }}
      volumes:
        {{- include "jobprocessor.caVolume" . | nindent 8 }}
```

- [ ] **Step 2: Create users-sync-job.yaml**

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "jobprocessor.fullname" . }}-users-sync
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: hook
  annotations:
    "helm.sh/hook": post-install,pre-upgrade
    "helm.sh/hook-weight": "-5"   # after migrate (weight -10)
    "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        {{- include "jobprocessor.labels" . | nindent 8 }}
        app.kubernetes.io/component: hook
    spec:
      restartPolicy: Never
      containers:
        - name: users-sync
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          command: ["python", "-m", "app.users.sync"]
          env:
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: {{ include "jobprocessor.fullname" . }}-credentials
                  key: db-password
            - name: DATABASE_URL
              value: {{ include "jobprocessor.directDatabaseUrl" . | quote }}
            - name: REDIS_URL
              value: rediss://unused:6379/0
          volumeMounts:
            {{- include "jobprocessor.caVolumeMount" . | nindent 12 }}
            - name: api-user-keys
              mountPath: /run/secrets
              readOnly: true
      volumes:
        {{- include "jobprocessor.caVolume" . | nindent 8 }}
        - name: api-user-keys
          secret:
            secretName: {{ .Values.secrets.apiUserKeysSecret }}
            items:
              - key: api_user_keys.json
                path: api_user_keys   # matches Settings.api_user_keys_file
```

- [ ] **Step 3: Verify — check alembic reads `DATABASE_URL` from env**

```bash
grep -n "DATABASE_URL\|get_settings\|database_url" alembic/env.py alembic.ini
helm template rel deploy/chart/jobprocessor --show-only templates/migrate-job.yaml
```
Expected: `alembic/env.py` resolves the URL from settings/env (Compose already passes `DATABASE_URL` to the migrate service, so it must). Hook Jobs render with `post-install,pre-upgrade`. **If alembic needs a sync driver URL** (no `+psycopg` async issue — the URL is already `postgresql+psycopg`, which alembic handles), no change; otherwise adapt the env in this template to what `alembic/env.py` actually reads.

- [ ] **Step 4: Commit**

```bash
git add deploy/chart/jobprocessor/templates/migrate-job.yaml deploy/chart/jobprocessor/templates/users-sync-job.yaml
git commit -m "feat(chart): migrate and users-sync hook Jobs (post-install,pre-upgrade)"
```

---

### Task 12: NetworkPolicies — default-deny ingress + egress with per-service allows

**Files:**
- Create: `deploy/chart/jobprocessor/templates/networkpolicies.yaml`

**Interfaces:**
- Consumes: component labels `api`, `worker`, `ticker`, `postgres`, `redis`, `pgbouncer`, `hook` set in Tasks 6–11.
- Produces: complete namespace lockdown per spec.

- [ ] **Step 1: Create networkpolicies.yaml**

```yaml
{{- $labels := include "jobprocessor.labels" . }}
# ---------- default deny everything, both directions ----------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-default-deny
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
---
# ---------- DNS egress for every pod ----------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-allow-dns
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector: {}
  policyTypes: [Egress]
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: openshift-dns
      ports:
        - { protocol: UDP, port: 5353 }
        - { protocol: TCP, port: 5353 }
        - { protocol: UDP, port: 53 }
        - { protocol: TCP, port: 53 }
---
# ---------- router -> api (the only external path) ----------
# kubelet probe traffic is not subject to NetworkPolicy on OVN-Kubernetes,
# so probe ports need no explicit allow.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-api-ingress
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: api
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              policy-group.network.openshift.io/ingress: ""
      ports:
        - { protocol: TCP, port: 8000 }
---
# ---------- apps -> pgbouncer / redis / otel ----------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-app-egress
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector:
    matchExpressions:
      - key: app.kubernetes.io/component
        operator: In
        values: [api, worker, ticker]
  policyTypes: [Egress]
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/component: pgbouncer
      ports:
        - { protocol: TCP, port: 6432 }
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/component: redis
      ports:
        - { protocol: TCP, port: 6379 }
    {{- if .Values.otel.enabled }}
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/component: otel-collector
      ports:
        - { protocol: TCP, port: 4317 }
    {{- end }}
---
# ---------- pgbouncer: in from apps, out to postgres ----------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-pgbouncer
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: pgbouncer
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - podSelector:
            matchExpressions:
              - key: app.kubernetes.io/component
                operator: In
                values: [api, worker, ticker]
      ports:
        - { protocol: TCP, port: 6432 }
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/component: postgres
      ports:
        - { protocol: TCP, port: 5432 }
---
# ---------- postgres: in from pgbouncer AND hook jobs (spec: hooks bypass pooling) ----------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-postgres-ingress
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: postgres
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchExpressions:
              - key: app.kubernetes.io/component
                operator: In
                values: [pgbouncer, hook]
      ports:
        - { protocol: TCP, port: 5432 }
---
# ---------- redis: in from apps ----------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-redis-ingress
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: redis
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchExpressions:
              - key: app.kubernetes.io/component
                operator: In
                values: [api, worker, ticker]
      ports:
        - { protocol: TCP, port: 6379 }
---
# ---------- hook jobs -> postgres ----------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-hook-egress
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: hook
  policyTypes: [Egress]
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/component: postgres
      ports:
        - { protocol: TCP, port: 5432 }
{{- if .Values.otel.enabled }}
---
# ---------- otel collector: in from apps ----------
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "jobprocessor.fullname" . }}-otel-ingress
  labels:
    {{- $labels | nindent 4 }}
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: otel-collector
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchExpressions:
              - key: app.kubernetes.io/component
                operator: In
                values: [api, worker, ticker]
      ports:
        - { protocol: TCP, port: 4317 }
{{- end }}
```

- [ ] **Step 2: Verify render**

```bash
helm template rel deploy/chart/jobprocessor | grep -c "kind: NetworkPolicy"    # expect 8 with otel off
helm template rel deploy/chart/jobprocessor --set otel.enabled=true | grep -c "kind: NetworkPolicy"  # expect 9
helm lint deploy/chart/jobprocessor
```

- [ ] **Step 3: Commit**

```bash
git add deploy/chart/jobprocessor/templates/networkpolicies.yaml
git commit -m "feat(chart): default-deny ingress+egress NetworkPolicies with per-service allows"
```

---

### Task 13: KEDA ScaledObject + OpenTelemetryCollector CR (behind flags)

**Files:**
- Create: `deploy/chart/jobprocessor/templates/worker-scaledobject.yaml`
- Create: `deploy/chart/jobprocessor/templates/otel-collector-cr.yaml`

**Interfaces:**
- Consumes: worker Deployment name `{{ fullname }}-worker` (Task 10), credentials Secret, stream names `jobs:stream:{high,normal,low}` / group `workers` (Settings defaults).
- Produces: `ScaledObject` when `keda.enabled`; `OpenTelemetryCollector` named `{{ fullname }}-otel` when `otel.enabled` — the operator creates Service `{{ fullname }}-otel-collector` (name + `-collector`), matching `jobprocessor.appEnv`'s endpoint. The operator labels collector pods `app.kubernetes.io/component: otel-collector`? **No** — add the label via the CR's `spec.podAnnotations`/labels field as shown below so Task 12's policies match.

- [ ] **Step 1: Create worker-scaledobject.yaml**

```yaml
{{- if .Values.keda.enabled }}
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: {{ include "jobprocessor.fullname" . }}-redis-auth
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
spec:
  secretTargetRef:
    - parameter: password
      name: {{ include "jobprocessor.fullname" . }}-credentials
      key: redis-password
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {{ include "jobprocessor.fullname" . }}-worker
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
spec:
  scaleTargetRef:
    name: {{ include "jobprocessor.fullname" . }}-worker
  minReplicaCount: 1
  maxReplicaCount: {{ .Values.keda.maxReplicas }}
  triggers:
    {{- range $stream := list "jobs:stream:high" "jobs:stream:normal" "jobs:stream:low" }}
    - type: redis-streams
      metadata:
        address: {{ include "jobprocessor.redisHost" $ }}:6379
        stream: {{ $stream }}
        consumerGroup: workers
        lagCount: {{ $.Values.keda.lagCount | quote }}
        activationLagCount: "1"
        enableTLS: "true"
        unsafeSsl: {{ $.Values.keda.unsafeSsl | quote }}
      authenticationRef:
        name: {{ include "jobprocessor.fullname" $ }}-redis-auth
    {{- end }}
{{- end }}
```

- [ ] **Step 2: Create otel-collector-cr.yaml** (pipeline shape mirrors `otel-collector-config.yaml` at repo root — read it and keep the same processors if it defines any, e.g. batch)

```yaml
{{- if .Values.otel.enabled }}
apiVersion: opentelemetry.io/v1beta1
kind: OpenTelemetryCollector
metadata:
  name: {{ include "jobprocessor.fullname" . }}-otel
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: otel-collector
spec:
  mode: deployment
  replicas: 1
  # propagate the component label to collector pods so NetworkPolicies match
  podAnnotations: {}
  additionalLabels:
    app.kubernetes.io/component: otel-collector
  config:
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317
    processors:
      batch: {}
    exporters:
      debug:
        verbosity: basic
      {{- if .Values.otel.exporterEndpoint }}
      otlp:
        endpoint: {{ .Values.otel.exporterEndpoint }}
        tls:
          insecure: true
      {{- end }}
    service:
      pipelines:
        {{- $exporters := ternary (list "debug" "otlp") (list "debug") (ne .Values.otel.exporterEndpoint "") }}
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: {{ $exporters | toJson }}
        metrics:
          receivers: [otlp]
          processors: [batch]
          exporters: {{ $exporters | toJson }}
        logs:
          receivers: [otlp]
          processors: [batch]
          exporters: {{ $exporters | toJson }}
{{- end }}
```

Note for implementer: verify the field for extra pod labels against the installed operator's CRD (`oc explain opentelemetrycollector.spec --recursive | grep -i label`); on current versions it is `spec.podLabels` or labels under `spec` metadata passthrough — adjust the key so collector pods carry `app.kubernetes.io/component: otel-collector`. If no such field exists, add a dedicated NetworkPolicy selecting the operator's own labels (`app.kubernetes.io/component: opentelemetry-collector`) instead and update Task 12's two otel policies accordingly.

- [ ] **Step 3: Verify render both flag states**

```bash
helm template rel deploy/chart/jobprocessor | grep -c "kind: ScaledObject" || echo none        # expect none
helm template rel deploy/chart/jobprocessor --set keda.enabled=true --set otel.enabled=true | grep -E "kind: (ScaledObject|TriggerAuthentication|OpenTelemetryCollector)" | sort | uniq -c
helm lint deploy/chart/jobprocessor
```
Expected: flags off → no CRs; flags on → 1 ScaledObject, 1 TriggerAuthentication, 1 OpenTelemetryCollector.

- [ ] **Step 4: Commit**

```bash
git add deploy/chart/jobprocessor/templates/worker-scaledobject.yaml deploy/chart/jobprocessor/templates/otel-collector-cr.yaml
git commit -m "feat(chart): KEDA ScaledObject and OTel collector CR behind values flags"
```

---

### Task 14: init-secrets.sh + chart README

**Files:**
- Create: `deploy/openshift/init-secrets.sh`
- Create: `deploy/chart/jobprocessor/README.md`

**Interfaces:**
- Consumes: key-file format `{"<user name>": "<raw key>", ...}` (see `app/users/sync.py::load_keys`); Secret name default `jobprocessor-api-user-keys` with key `api_user_keys.json` (Task 11 mount).
- Produces: idempotent script `init-secrets.sh <namespace> [secret-name] [user ...]`.

- [ ] **Step 1: Create deploy/openshift/init-secrets.sh**

```bash
#!/usr/bin/env bash
# Generates API user keys and creates the Kubernetes Secret the users-sync
# hook Job mounts. Idempotent: refuses to overwrite an existing secret so
# keys are never silently rotated. Keys are printed ONCE to stdout — hand
# them to the API consumers; they are not recoverable later (only hashes
# reach the database).
set -euo pipefail

NAMESPACE="${1:?usage: init-secrets.sh <namespace> [secret-name] [user ...]}"
SECRET_NAME="${2:-jobprocessor-api-user-keys}"
shift $(( $# > 2 ? 2 : $# ))
USERS=("${@:-alice bob}")
[ ${#USERS[@]} -eq 1 ] && read -ra USERS <<< "${USERS[0]}"

if oc get secret "$SECRET_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "secret $SECRET_NAME already exists in $NAMESPACE — leaving it untouched"
  exit 0
fi

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

{
  printf '{'
  sep=""
  for user in "${USERS[@]}"; do
    key="$(openssl rand -hex 32)"
    printf '%s\n  "%s": "%s"' "$sep" "$user" "$key"
    sep=","
    echo "USER $user KEY $key" >&2
  done
  printf '\n}\n'
} > "$TMP"

oc create secret generic "$SECRET_NAME" -n "$NAMESPACE" \
  --from-file=api_user_keys.json="$TMP"
echo "created secret $SECRET_NAME in $NAMESPACE (raw keys printed above on stderr)"
```

- [ ] **Step 2: Verify the script's JSON output shape locally** (no cluster needed)

Temporarily stub `oc` to inspect what would be created:

```bash
cd deploy/openshift
mkdir -p /tmp/ocstub && printf '#!/bin/sh\nif [ "$1" = get ]; then exit 1; fi\nexit 0\n' > /tmp/ocstub/oc && chmod +x /tmp/ocstub/oc
PATH=/tmp/ocstub:$PATH bash init-secrets.sh testns 2>/dev/null
```
Expected: exits 0. Then validate the JSON format matches `load_keys` expectations by capturing the temp-file content (add a temporary `cat "$TMP"` while testing, remove it after): a JSON object mapping names to 64-hex-char strings.

- [ ] **Step 3: Create deploy/chart/jobprocessor/README.md**

Content must include, concretely (no placeholders):

```markdown
# jobprocessor Helm chart (OpenShift)

## Prerequisites (cluster administrators)
Installed once per cluster via OperatorHub/OLM — this chart never manages operators:
- **Custom Metrics Autoscaler** (KEDA, `openshift-keda` namespace) — required only when `keda.enabled=true`. CRDs: `scaledobjects.keda.sh/v1alpha1`, `triggerauthentications.keda.sh/v1alpha1`.
- **Red Hat build of OpenTelemetry** — required only when `otel.enabled=true`. CRD: `opentelemetrycollectors.opentelemetry.io/v1beta1`.

## Install
    deploy/openshift/init-secrets.sh <namespace> jobprocessor-api-user-keys alice bob
    helm install jp deploy/chart/jobprocessor -n <namespace>

## Connection math (enforced at template time)
With shipped defaults:
- Client side: workers 6 (keda.maxReplicas) x 5 (worker.dbPoolSize) + api 2 x 5 + ticker 2 = **42 <= maxClientConn 60** OK
- Server side: defaultPoolSize 20 <= maxConnections 100 - reserved 5 = **95** OK
Raising `keda.maxReplicas` to 10 makes demand 62 > 60 and `helm template` fails with the exact violation. Set `maxReplicas` so `maxReplicas x worker.dbPoolSize` plus the api/ticker share never exceeds `pgbouncer.maxClientConn`.

## TLS
Postgres, Redis (TLS-only), and PgBouncer present OpenShift serving certs; clients verify against the injected service CA at /etc/pki/service-ca/ca.crt. External entry is the edge-TLS Route; nothing else is exposed. Note: Postgres has ssl=on but pg_hba is not overridden (sclorg image limitation) — plaintext to Postgres is prevented by NetworkPolicy (only PgBouncer and hook Jobs may connect, and both use verify-full).

## Worker self-recycling
`worker.maxRssMb` (default 512) — on RSS breach the worker stops claiming jobs, readiness flips to 503, in-flight jobs drain (bounded by terminationGracePeriodSeconds 50), the pod exits 0 and is replaced.
```

- [ ] **Step 4: Commit**

```bash
git add deploy/openshift/init-secrets.sh deploy/chart/jobprocessor/README.md
git commit -m "feat: api-key init script and chart README with operator prereqs and math example"
```

---

### Task 15: Live verification on CRC

No code — this is the spec's manual verification pass. Requires: CRC running (`crc start`), logged in as kubeadmin, operators installed (admin step: OperatorHub → install "Custom Metrics Autoscaler" and "Red Hat build of OpenTelemetry" with defaults).

- [ ] **Step 1: Build and push the image into CRC's registry**

```bash
oc new-project jobs
oc registry login
docker build -t default-route-openshift-image-registry.apps-crc.testing/jobs/jobprocessor-app:dev .
docker push default-route-openshift-image-registry.apps-crc.testing/jobs/jobprocessor-app:dev
```

- [ ] **Step 2: Init secrets and install**

```bash
bash deploy/openshift/init-secrets.sh jobs jobprocessor-api-user-keys alice bob   # capture printed keys
helm install jp deploy/chart/jobprocessor -n jobs \
  --set image.repository=image-registry.openshift-image-registry.svc:5000/jobs/jobprocessor-app \
  --set keda.enabled=true --set otel.enabled=true
oc get pods -n jobs -w    # until api/worker/ticker/postgres/redis/pgbouncer Ready, hooks Completed
```
Expected: hook Jobs complete, then all pods Ready (api pods flip Ready only after migrate finishes — expected on first install).

- [ ] **Step 3: TLS checks**

```bash
oc exec -n jobs statefulset/jp-redis -- redis-cli -p 6379 ping                  # plaintext → error/no PONG
oc exec -n jobs statefulset/jp-redis -- sh -c 'redis-cli --tls --cacert /certs/tls.crt -a "$REDIS_PASSWORD" ping'   # PONG
oc exec -n jobs statefulset/jp-postgres -- psql -U jobs -c "select ssl from pg_stat_ssl where pid <> pg_backend_pid();"  # all t (pgbouncer connections use TLS)
```

- [ ] **Step 4: NetworkPolicy checks**

```bash
oc run scratch -n jobs --rm -it --image=registry.access.redhat.com/ubi9/ubi -- bash -c \
  'timeout 3 bash -c "</dev/tcp/jp-postgres/5432" && echo REACHED || echo BLOCKED; \
   timeout 3 bash -c "</dev/tcp/jp-redis/6379" && echo REACHED || echo BLOCKED'
# expect BLOCKED twice (scratch pod has no allow rules)
oc exec -n jobs deploy/jp-api -- python -c "import urllib.request;urllib.request.urlopen('https://example.com', timeout=3)"
# expect timeout/URLError: default-deny egress blocks undeclared destinations
```

- [ ] **Step 5: End-to-end over the Route**

```bash
ROUTE=$(oc get route jp-api -n jobs -o jsonpath='{.spec.host}')
curl -sk https://$ROUTE/jobs -H "Authorization: Bearer <alice key from step 2>" \
  -H 'Content-Type: application/json' -d '{"type":"echo","payload":{"message":"hi"}}'
# poll the returned job id until status=completed
```
(Adjust path/auth header/payload to the API's actual contract — see `tests/integration/test_api.py` for the canonical request shape.)

- [ ] **Step 6: Memory recycling**

```bash
helm upgrade jp deploy/chart/jobprocessor -n jobs --reuse-values --set worker.maxRssMb=30
oc get pods -n jobs -l app.kubernetes.io/component=worker -w
# expect: worker pod logs "worker.recycling", readiness goes 0/1, pod exits 0
# (Completed) and a fresh pod is created by the Deployment/ReplicaSet
oc logs -n jobs -l app.kubernetes.io/component=worker --tail=50 | grep worker.recycling
helm upgrade jp deploy/chart/jobprocessor -n jobs --reuse-values --set worker.maxRssMb=512   # restore
```

- [ ] **Step 7: KEDA scaling**

Submit a burst of jobs (loop the Step 5 curl ~200 times or submit a large batch job), then:

```bash
oc get hpa,scaledobject -n jobs
oc get pods -n jobs -l app.kubernetes.io/component=worker -w
# expect scale-out toward (never past) keda.maxReplicas=6, then scale-in after drain
```

- [ ] **Step 8: OTel**

```bash
oc logs -n jobs deploy/jp-otel-collector | head -50
# expect debug exporter output showing spans/metrics from jobs-api / jobs-worker
```

- [ ] **Step 9: Record results and commit any fixes**

Document outcomes (and any image/CRD-field adjustments made) in `DECISIONS.md`, commit:

```bash
git add -A && git commit -m "chore: CRC verification fixes and decisions log"
```
