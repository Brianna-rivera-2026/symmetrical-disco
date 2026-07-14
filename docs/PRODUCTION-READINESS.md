# Production-Readiness Review

**Audited commit:** `dcbbafd3b313d2082c245eb4d9078a8899b25885` (branch `chart/ops-hardening`)
**Audit date:** 2026-07-14
**Scope:** the committed state of this repository, reviewed as an enterprise
on-prem OpenShift background job processing pipeline. Verdicts cover six
categories: Deployment, Performance, Security, Monitoring, Testing, SDD.
**Method:** static review of `app/`, `deploy/`, `docs/`, plus executable
checks run during this audit (full test suite, ruff, helm lint, helm template
render matrix) — outputs in the [Methodology appendix](#methodology-appendix).

## Verdict semantics

| Mark | Meaning |
|---|---|
| ✅ | Done in code/chart; evidence cited |
| 🟡 | Accepted risk — deliberately not done, rationale documented (this *is* the "at least in DOC" box-check) |
| ❌ | Open gap — must be closed in code, or explicitly promoted to 🟡 with a signed-off rationale, before production |

---

## Executive summary

**Not production-ready as committed.** The application layer is genuinely
strong — delivery guarantees, retry/recovery, auth, rate limiting, TLS
everywhere, default-deny network isolation, and a 292-test suite that passes
clean. What blocks production is the *platform* layer around it: the source
of truth (PostgreSQL) runs as a single instance with **no replication and no
backups of any kind**, and **nothing alerts** when the system degrades — the
rich telemetry that exists has no alert rules, dashboards, or SLOs on top of
it. An enterprise on-prem deployment cannot go live with unrecoverable data
loss one PV failure away and failures visible only to users.

**Conditional path to production:** close the two blockers (Postgres HA +
backups; a minimal alert set on the metrics that already exist), then work
down the high risks below. Everything else in this document is either done
or documented as an accepted risk.

### Ranked top risks

| # | Severity | Category | Gap |
|---|---|---|---|
| 1 | **Blocker** | Deployment | Postgres: single instance, no replication, no backups, no PITR — PV loss permanently destroys all job state |
| 2 | **Blocker** | Monitoring | No alert rules — queue stall, failure spikes, or a dead ticker page no one; failures surface via user reports |
| 3 | High | Deployment | Redis: single instance — node loss halts the queue; AOF `everysec` loses up to ~1s of acknowledged enqueues |
| 4 | High | Testing | No CI pipeline — tests, lint, and chart rendering are only run manually |
| 5 | High | Security | Supply chain: floating image tags (`python:3.11-slim`, `uv:latest`, `redis:7`), no image scanning, SBOM, or signing |
| 6 | Medium | Security | Secrets lifecycle is manual: raw API keys printed to stdout once, no Vault/External Secrets integration, no rotation automation |
| 7 | Medium | Performance | No load-test baseline, capacity numbers, or SLO targets |
| 8 | Medium | SDD | No Postgres-restore, upgrade/rollback, or incident-response runbooks |

---

## 1. Deployment

| Item | Verdict | Evidence | Remediation / rationale |
|---|---|---|---|
| Production target is a Helm chart on OpenShift, restricted-SCC compatible, live-verified on a real cluster | ✅ | `deploy/chart/jobprocessor/`; live CRC verification incl. TLS, netpols, KEDA scale 1→6, recycler (`DECISIONS.md` §6) | — |
| Postgres high availability | ❌ | `templates/postgres-statefulset.yaml:11` (`replicas: 1`), single RWO PVC | **Blocker.** Node failure = total outage for the source of truth until reschedule; sits in the hot path of auth, claims, and every status write. Needs replication + automated failover (an HA operator or external managed Postgres). |
| Postgres backups / PITR | ❌ | No backup Job, sidecar, or WAL archiving anywhere in the chart | **Blocker.** Losing the PV loses every job record permanently. Minimum: scheduled base backups + WAL archiving to off-cluster storage, plus a tested restore procedure. |
| Redis availability & durability | ❌ | `templates/redis-statefulset.yaml:10` (`replicas: 1`), `:33` (AOF `everysec`) | High. Queue halts on node loss (jobs in Postgres survive; the reconciler re-syncs after recovery — partial mitigation). Up to ~1s of acknowledged enqueues can be lost on crash. Sentinel/replication, or a documented acceptance of the outage window. |
| Ticker is a single replica with no leader election | 🟡 | `templates/ticker-deployment.yaml:9`; trade-off documented in `DECISIONS.md` §2 | Accepted: promote/reap/reconcile don't need horizontal scale; N>1 without leader election would double-process. Outage window bounded — recovery is rescheduling, and the reaper design tolerates late ticks. |
| Image pinning | ❌ | `values.yaml:1-4` (`tag: dev`, `pullPolicy: IfNotPresent`); `Dockerfile:1,7` (`python:3.11-slim`, `uv:latest`); `values.yaml:64,83` (`sclorg…:latest`, `redis:7`) | Deploy by immutable tag or digest; `IfNotPresent` + mutable tag can silently run stale code. Pin all base/infra images. |
| Air-gap / registry mirroring story | ❌ | No mirroring or `imagePullSecrets` guidance anywhere in `deploy/` | Enterprise on-prem clusters commonly require a private mirror; document required images + registry override values. |
| PodDisruptionBudgets for scaled components | ✅ | `templates/poddisruptionbudgets.yaml` — `maxUnavailable: 1`, with the KEDA-scale-to-1 rationale for not using `minAvailable`; singletons deliberately excluded (comment, lines 1–8) | — |
| Topology spread | ✅ | api/worker deployments: soft `topologySpreadConstraints` (`ScheduleAnyway`) with documented reasoning | — |
| Resource requests/limits on every workload incl. hooks | ✅ | `values.yaml` (all components + `hooks.resources`); memory-only limits with documented CPU-throttling rationale (`values.yaml:8-10`) | — |
| Liveness/readiness probes on every component | ✅ | API `/health` `/ready` over HTTPS; worker & ticker run a dedicated health server (`app/core/healthcheck.py`) probing loop heartbeat + dependencies | — |
| Graceful shutdown / drain | ✅ | `terminationGracePeriodSeconds` 30/50/15; worker drains in-flight jobs, memory recycler flips readiness and exits 0 (`app/worker/recycler.py`, verified live per `DECISIONS.md` §6) | — |
| Migration discipline | ✅ | Migrations as `post-install,pre-upgrade` hook Job (`templates/migrate-job.yaml`, weight −10, rationale comments); dedicated `jobs_migrator` role owns schema (`files/db-init/01-roles.sh`) | — |
| Credentials bootstrap survives upgrades | ✅ | `templates/credentials-secret.yaml` — pre-install/pre-upgrade hook, render-time `lookup()` preserves existing passwords, `resource-policy: keep` (each annotation's failure mode documented in comments) | — |
| Connection math enforced at template time | ✅ | Verified: `keda.maxReplicas=10` render fails with `connection math: max client demand 62 … exceeds pgbouncer.maxClientConn 60` | — |
| Autoscaling | 🟡 | KEDA `ScaledObject` per priority stream, opt-in (`keda.enabled=false` default); `unsafeSsl: true` for the scaler only — KEDA can't mount the service-CA ConfigMap (`values.yaml:53`) | Scaler-only TLS verification skip is documented; scaler traffic still requires the netpol allow + Redis auth. |
| Rollback / upgrade procedure | ❌ | No documented `helm rollback` procedure or migration-compatibility policy | Document: rollback steps, whether migrations are backward-compatible one release, and the order of operations. |
| Production values profile | 🟡 | `values.yaml:8-10` — defaults sized for CRC, explicitly commented "raise for real environments" | Documented, but a `values-prod.yaml` example would prevent the CRC sizes from reaching production by default. |

## 2. Performance

| Item | Verdict | Evidence | Remediation / rationale |
|---|---|---|---|
| Connection pooling with enforced capacity math | ✅ | PgBouncer transaction mode (`templates/pgbouncer-deployment.yaml`); `DB_DISABLE_PREPARED_STATEMENTS=true` (`_helpers.tpl:72-73`); template-time validator verified failing (§1) | — |
| Priority scheduling; starvation behavior | 🟡 | Strict high→normal→low drain (`DECISIONS.md` §3); starvation under sustained high-priority flood is explicit, with a sketched mitigation (dedicated low-stream workers) | Accepted by design absent a fairness requirement. |
| Burst absorption in the scheduler path | ✅ | Ticker drain-until-not-full loop + pipelined `XADD`/`ZREM`/bulk update (`DECISIONS.md` §2, `app/ticker/runner.py`) | — |
| Hung-handler / dead-worker recovery bounds | ✅ | `job_handler_timeout_s < visibility_timeout_s` enforced by validator (`app/core/config.py:52-59`); two-layer recovery documented (`DECISIONS.md` §2) | — |
| Worker long-run stability | ✅ | Memory-threshold self-recycling (`app/worker/recycler.py`), `maxRssMb` < memory limit invariant documented (`values.yaml:21-22`), verified live | — |
| API list scalability | ✅ | Cursor-based keyset pagination (`app/cursor.py`; `README.md` §List jobs); `status+created_at` index (migration `0006`) | — |
| Load-test baseline / capacity numbers | ❌ | No load-test harness or recorded numbers anywhere in repo | Without a measured jobs/sec ceiling and latency profile, `keda.lagCount`, pool sizes, and replica counts are educated guesses. Add a repeatable load run and record results. |
| SLO / latency targets | ❌ | No SLO definitions in docs | Define targets (e.g., p99 submit latency, queue-wait ceiling per priority, job success rate) — prerequisite for meaningful alerting (§4). |
| Queue-depth-driven scaling | ✅ | KEDA redis-streams lag triggers per stream, `lagCount` target per replica, ceiling tied into connection math (`templates/worker-scaledobject.yaml`) | — |

## 3. Security

| Item | Verdict | Evidence | Remediation / rationale |
|---|---|---|---|
| Authentication | ✅ | Per-user API keys, SHA-256 hash-only storage with documented fast-hash rationale for high-entropy keys (`app/users/keys.py:10-13`); raw keys never stored or logged (`app/users/sync.py`, `README.md` §Authentication) | — |
| Authorization / tenant isolation | ✅ | Jobs scoped to creating user; cross-user access returns 404 (`app/api/deps.py`, `tests/integration/test_auth_api.py`) | — |
| Revocation semantics | 🟡 | Delete user row → effective within `AUTH_CACHE_TTL_S` (60s); only successful lookups cached (`app/users/keys.py:22-26`); upsert-only sync means removal from the Secret does **not** revoke (documented, `README.md`) | 60s window + manual DB-row removal is documented. Enterprise fit: consider a `--prune` sync mode so Secret removal revokes. |
| Rate limiting | ✅ | Per-validated-user buckets — never raw-header keyed (anti-rotation rationale in `app/api/ratelimit.py:10-30`); IP fallback trust boundary documented both in code and chart (`api-deployment.yaml:47-51`); request body cap 256 KiB (`config.py:50`) | — |
| TLS on every hop | ✅ | Re-encrypt Route; API serves service-CA cert; PgBouncer client/server TLS `verify-full`; Redis TLS-only (plaintext port 0); pg_hba limitation documented with NetworkPolicy compensation (`deploy/chart/jobprocessor/README.md` §TLS); all verified live (`DECISIONS.md` §6) | — |
| Network isolation | ✅ | Default-deny both directions + explicit allows only (`templates/networkpolicies.yaml:2-11`); API ingress from router only; DNS-only baseline egress | — |
| Worker egress containment (SSRF defense-in-depth) | ✅ | 443-only to public IPs, RFC1918 + link-local/metadata CIDRs denied even when the app-level allowlist is misconfigured (`networkpolicies.yaml:92-119`) | — |
| Webhook/email allowlists | ✅ | Deny-by-default empty lists; suffix-with-label-boundary host matching; https-only webhooks; re-checked at worker before execution (`README.md` §Webhook host / email domain allowlists) | — |
| Secrets lifecycle | ❌ | `deploy/openshift/init-secrets.sh` prints raw keys to stdout once; in-cluster generation via Helm `lookup()` (`credentials-secret.yaml`); rotation is a manual oc-edit procedure (`README.md`) | Fine for a small deployment; enterprise baseline is Vault/External Secrets Operator sourcing, automated rotation, and no raw secrets in terminal scrollback/CI logs. At minimum document the scrollback risk. |
| Supply chain | ❌ | Floating base images (`Dockerfile:1,7`); community images for infra (`redis:7`, `edoburu/pgbouncer`, `sclorg…:latest`); no scanning, SBOM, or signing; `uv.lock` fully pinned (good) but no automated CVE audit | High. Pin digests, add image scanning (e.g., Quay/Clair or Trivy) and a dependency-audit step; enterprise registries usually require signed/attested images. |
| Container hardening | ✅ | Non-root `USER appuser` uid 1000 (`Dockerfile:15-16`); runs under restricted SCC with no privilege escalations requested (no `securityContext` overrides anywhere in templates; arbitrary-UID compatibility verified live, `DECISIONS.md` §6) | — |
| Least-privilege database roles | ✅ | `jobs_app` (DML-only) / `jobs_migrator` (DDL owner) split (`files/db-init/01-roles.sh`); PgBouncer admin rights dropped in audited commit (`dcbbafd`) | — |
| Input validation | ✅ | Typed Pydantic payload models per job type (`app/schemas/payloads.py`); strict enums; email/URL field validation | — |
| Audit trail | 🟡 | Structured JSON logs bind `user_id`/`user_name` to every authenticated action (`app/api/deps.py:60`); auth attempts counted by result (`app/core/metrics.py:32`) | Logs are an audit source but not tamper-evident and (see §4) the shipped log store is ephemeral. If compliance requires immutable audit, ship logs to durable off-cluster storage. |

## 4. Monitoring

| Item | Verdict | Evidence | Remediation / rationale |
|---|---|---|---|
| Instrumentation: traces, metrics, logs | ✅ | OTel SDK + FastAPI/SQLAlchemy/Redis/logging auto-instrumentation (`pyproject.toml`); 11 domain instruments incl. queue wait, processing duration, auth outcomes, reaper/reconciler counters, recycles (`app/core/metrics.py`); no-op safe when disabled | — |
| Structured logging | ✅ | JSON to stdout, context-bound job/user fields, trace-id correlation (`app/core/logging.py`); no print statements (project rule enforced) | — |
| Health surfaces | ✅ | `/health` (loop-heartbeat liveness), `/ready` (dependency checks, fail-fast 2s Redis probe) on every component incl. workers/ticker via dedicated health server (`app/core/healthcheck.py`) | — |
| Operational stats | ✅ | `/stats`: per-stream depth + in-flight, scheduled count, worker count, jobs by status, oldest-pending age (`README.md` §Queue and job stats) | — |
| Alerting | ❌ | No PrometheusRule/alert definitions anywhere in repo | **Blocker.** The signals already exist (`oldest_pending_age_seconds`, `jobs.failed`, ticker counters going flat, `/health` failures) — nothing consumes them. Minimum viable: alerts on queue stall, failure-rate spike, ticker silence, DB/Redis down. |
| Dashboards | ❌ | Grafana ships in the LGTM stack with no provisioned dashboards | Provision at least: queue depth/wait by priority, throughput/failure rate, worker count vs. KEDA ceiling, DB pool saturation. |
| Metrics backend fitness | 🟡 | `bootstrap-cluster.sh` deploys `grafana/otel-lgtm:latest` explicitly labeled dev/test-grade: single replica, `emptyDir` (telemetry lost on restart), collector→backend `tls.insecure: true` (script comments) | Documented as dev-grade in the script itself; production needs a durable, HA observability backend (or point `OTEL_EXPORTER_ENDPOINT` at the org's platform — supported). |
| Infra metrics (Postgres/Redis/PgBouncer) | ❌ | No exporters in the chart | App metrics can't explain infra saturation. Add postgres/redis/pgbouncer exporters or operator-provided metrics. |
| SLOs / error budgets | ❌ | None defined | See §2 — define targets first, then encode as alerts. |
| Trace continuity across the pipeline | ✅ | Trace context persisted with the job and restored in the worker (migration `0007_add_trace_context`; verified live end-to-end, `DECISIONS.md` §6) | — |

## 5. Testing

| Item | Verdict | Evidence | Remediation / rationale |
|---|---|---|---|
| Test suite passes | ✅ | **292 passed, 0 failed**, 4 deprecation warnings, 171.7s — full run this audit (see appendix) | — |
| Integration depth | ✅ | Testcontainers-backed integration suites cover API, auth e2e, queue semantics, worker, ticker, reaper, retry/backoff, cancellation, delayed jobs, idempotency, rate limiting, users-sync, healthcheck, metrics, and migrations (`tests/integration/` — 19 test files) | — |
| Migration testing | ✅ | `tests/integration/test_migration.py` exercises Alembic against a real Postgres | — |
| Lint/format clean | 🟡 | `ruff check`: passes. `ruff format --check`: **10 files would be reformatted** (test files) | Trivial: run `uv run ruff format`. Listed because "format drift with no CI" is how style rot starts. |
| CI pipeline | ❌ | No `.github/`, no `.gitlab-ci.yml`, no CI config of any kind | High. Every gate in this document is manual. Minimum: CI running pytest, ruff, `helm lint` + render matrix on every PR. |
| Chart rendering tests | 🟡 | `helm lint` passes (1 INFO: icon); render matrix passes; both fail-fast guards verified — but only manually, this audit | Fold the render matrix into CI (same commands, see appendix). |
| Load / performance tests | ❌ | None | See §2. |
| Chaos / failover drills | ❌ | Live CRC verification (`DECISIONS.md` §6) covered TLS, netpols, KEDA, recycler — as a one-time manual pass; no repeatable kill-the-worker / kill-postgres / kill-redis drill | Recovery paths (reaper, reconciler) are integration-tested at module level; a scripted cluster-level drill would verify them where it counts. |
| Coverage measurement | 🟡 | Not configured; suite breadth (292 tests / 20 integration areas) is the only quality signal | Acceptable; add `pytest-cov` if a number is wanted. |

## 6. SDD (Spec-Driven Development)

| Item | Verdict | Evidence | Remediation / rationale |
|---|---|---|---|
| Requirements → spec → plan traceability | ✅ | `docs/requirements/01…10` each map to a dated design spec and implementation plan under `docs/superpowers/` (13 specs, 13 plans) | — |
| Decision records | ✅ | `DECISIONS.md`: five substantive ADR-style entries with mechanics, rejected alternatives, and trade-offs, plus a self-critical "what I'd do differently" (worker heartbeats, §5) and live-verification findings (§6) | — |
| Operational README | ✅ | Root `README.md` (API, deployment split, auth ops, allowlist gotcha) + chart README (prerequisites, connection math, TLS caveats, recycler) | — |
| Runbooks | ❌ | Exactly one: `docs/runbooks/redis-total-loss-recovery.md` (good — symptoms, steps, rationale). Missing: Postgres backup/restore (moot until backups exist), upgrade/rollback, incident response for the alert set §4 calls for | Each blocker fix above must land with its runbook; alert definitions should link to response procedures. |
| Production configuration guidance | 🟡 | CRC-sized defaults flagged in comments (`values.yaml:8-10`); allowlist deny-all gotcha prominently documented (`README.md`) | A worked `values-prod.yaml` example would close the loop. |
| AI usage disclosure | ✅ | `AI_USAGE.md` | — |
| This document's own requirement ("all boxes checked in code or DOC") | 🟡 | Every ❌ above is now *documented*, which is necessary but not sufficient — ❌ items still need code or an explicit sign-off promoting them to 🟡 | This review is the box-checking mechanism; blockers cannot be signed off as accepted risks. |

---

## Methodology appendix

All commands run 2026-07-14 against commit `dcbbafd` on Windows 11 /
Docker 29.6.1 / Helm v4.2.3. Working tree was clean for all code and chart
paths (untracked files were documentation/PDF artifacts only).

| Check | Command | Result |
|---|---|---|
| Full test suite | `uv run pytest -q` | **292 passed, 4 warnings in 171.72s** (unit + testcontainers integration; Docker available, so the full suite ran) |
| Lint | `uv run ruff check` | `All checks passed!` |
| Format drift | `uv run ruff format --check` | `10 files would be reformatted, 93 files already formatted` (all under `tests/`) |
| Chart lint | `helm lint deploy/chart/jobprocessor` | `1 chart(s) linted, 0 chart(s) failed` (1 INFO: icon recommended) |
| Render: defaults | `helm template jp deploy/chart/jobprocessor` | OK |
| Render: KEDA | `… --set keda.enabled=true` | OK |
| Render: OTel | `… --set otel.enabled=true --set otel.exporterEndpoint=http://…:4317` | OK |
| Fail-fast: OTel w/o endpoint | `… --set otel.enabled=true` | Fails: `otel.exporterEndpoint must be set (to the cluster's existing OpenTelemetry collector) when otel.enabled=true` |
| Fail-fast: connection math | `… --set keda.enabled=true --set keda.maxReplicas=10` | Fails: `connection math: max client demand 62 (workers 10 x 5 + api 2 x 5 + ticker 2) exceeds pgbouncer.maxClientConn 60` |

**Reviewed statically:** every file under `app/` (config, auth deps, rate
limiting, logging, healthcheck, metrics, worker runner/recycler/timeout,
ticker), all 21 chart templates + `values.yaml` + `_helpers.tpl` +
`files/db-init/01-roles.sh`, both `deploy/openshift/` scripts, `Dockerfile`,
`.dockerignore`, `docker-compose.yml` (dev-only, per README), all
`docs/requirements/`, `docs/superpowers/` specs/plans, `docs/runbooks/`,
`DECISIONS.md`, `README.md`s.

**Not verified in this audit** (previously verified live per `DECISIONS.md`
§6, one-time manual pass on CRC): runtime TLS enforcement, NetworkPolicy
behavior, KEDA scaling, memory-recycler drain. Statements about those rely on
that documented verification, not on re-execution here.
