# Production-Readiness Review — Design

**Date:** 2026-07-14
**Status:** Approved (brainstorming complete)
**Deliverable type:** Documentation only — no code, chart, or config changes

## Problem

The system (FastAPI API + Redis Streams queue + worker/ticker processes +
PostgreSQL, deployed via the OpenShift Helm chart) has grown feature-by-feature
with per-feature specs, but there is no single document that answers: **is this
ready for enterprise on-prem production, and if not, exactly which boxes are
unchecked?** The user's requirement: every production concern must be checked
off *in code or at least in DOC* — i.e., every gap is either closed or written
down as a deliberately accepted risk with rationale.

## Decisions (settled during brainstorming)

| Decision | Choice |
|---|---|
| Deliverable | Doc-only audit checklist — no code fixes planned from this effort |
| Scope of audit | Committed state of the repo only; the uncommitted CNPG draft spec is **not referenced** |
| Audience | Assignment/interview reviewer — demonstrates enterprise operational judgment; org-process items (on-call rosters, change boards) out of scope |
| Verification depth | Static review + cheap executable checks (`uv run pytest`, `uv run ruff check`, `helm lint`, `helm template` render matrix). No cluster/live verification |
| Structure | Category scorecard (Approach A): six sections matching the requested categories, with a risk-ranked executive summary on top |
| Git | Nothing committed (neither this spec nor the deliverable) |

## The deliverable

One new file: **`docs/PRODUCTION-READINESS.md`**.

### Structure

1. **Executive summary**
   - One-paragraph overall verdict. The verdict follows from the blockers, not
     optimism; with an unreplicated, unbacked-up source of truth and no
     alerting, the expected verdict is *"not production-ready as committed,
     with a conditional path"* — the honesty is the point.
   - Ranked top-risks table: severity, category, one-line gap. Expected
     headliners (to be confirmed by the audit): Postgres single-instance with
     no backups/PITR; Redis single-instance queue (AOF `everysec` → up to ~1s
     acknowledged-write loss, node loss = queue outage until reschedule); no
     alert rules or dashboards on top of the OTel plumbing.

2. **Six category sections**, in this order: Deployment, Performance,
   Security, Monitoring, Testing, SDD. Each section is a table of concrete
   audit items with three columns of substance:
   - **Verdict** (see semantics below)
   - **Evidence** — `file:line` pointer into the repo, or the command whose
     output backs the claim
   - **Remediation / rationale** — for 🟡 the documented reason the risk is
     accepted; for ❌ a one-paragraph statement of what closing it would take

3. **Verdict semantics** (stated in the doc itself):
   - ✅ **Done** — implemented in code/chart, evidence cited
   - 🟡 **Accepted risk** — deliberately not done, rationale written down.
     This *is* the "at least in DOC" box-check
   - ❌ **Open gap** — must be closed in code or explicitly accepted (i.e.,
     promoted to 🟡 with a signed-off rationale) before production

4. **Methodology appendix** — what was reviewed (paths), which commands were
   run, and their actual outputs (summarized: pass/fail counts, lint results,
   render-matrix outcomes), so every executable claim is traceable.

### Evidence method

- **Static review** of `app/`, `deploy/chart/jobprocessor/`,
  `deploy/openshift/`, `docker-compose.yml`, `Dockerfile`, `docs/`
  (requirements, specs, runbooks), `DECISIONS.md`, `README.md`.
- **Executable checks** run during the audit and cited in the appendix:
  - `uv run pytest` (full suite; integration tests need Docker — if Docker is
    unavailable at audit time, run `tests/unit` and record the integration
    suite as *not executed in this audit*, never as "passing")
  - `uv run ruff check` and `uv run ruff format --check`
  - `helm lint deploy/chart/jobprocessor`
  - `helm template` render matrix: defaults; `keda.enabled=true`;
    `otel.enabled=true otel.exporterEndpoint=<dummy>`; expected-failure case
    (`otel.enabled=true` without endpoint must fail fast)
- **No unverified claims**: anything not directly observed is phrased as
  "documented as X" or "not verified in this audit."

### Audit inventory

~55 items across the six sections. This inventory is the checklist the audit
works through; items may be added during the audit if reading the code
surfaces concerns not listed here (additions are in-scope — the inventory is
a floor, not a ceiling).

**Deployment (~12):** image pinning (`tag: dev`, no digest) and pull policy;
registry/air-gap story; Postgres HA/backup/PITR posture; Redis HA/persistence
posture; ticker single-replica SPOF and leader-election absence; PDBs;
topology spread; resource requests/limits (incl. the no-CPU-limits rationale);
liveness/readiness probes on every component; graceful shutdown/drain paths;
migration hook discipline (pre-install/pre-upgrade, credentials hook);
rollback procedure; template-time connection-math enforcement; KEDA
autoscaling posture (opt-in) and the `unsafeSsl` scaler caveat; restricted-SCC
compliance; Route TLS mode.

**Performance (~8):** connection-pool math (PgBouncer transaction mode,
prepared statements disabled); strict-priority starvation (accepted by design,
DECISIONS §3); ticker drain-loop + pipelining; keyset pagination; worker
memory self-recycling; handler timeout vs visibility timeout ordering; absence
of load-test/capacity baseline; absence of SLO/latency targets; queue-depth
scaling behavior (KEDA lag target).

**Security (~14):** API-key auth (SHA-256 hash-only storage, per-user job
scoping, cache TTL revocation window); rate limiting; TLS on every hop incl.
the documented pg_hba caveat and its NetworkPolicy compensation; default-deny
NetworkPolicies; worker internet-egress CIDR denial (443-only, RFC1918 +
link-local blocked); webhook/email allowlists with deny-by-default; secrets
lifecycle (init script prints raw keys to stdout once; no Vault/External
Secrets/rotation automation; Secret update procedure is manual);
users-sync upsert-only semantics (removed users not auto-revoked); image
supply chain (no scanning, SBOM, or signing; base image currency; `redis:7`
and `edoburu/pgbouncer` are community images); dependency audit posture
(`uv.lock` pinned, no automated CVE scanning); non-root/arbitrary-UID
compliance; PgBouncer admin-rights removal (this branch); input validation
(Pydantic payload models); SSRF posture (https-only + host allowlist).

**Monitoring (~9):** OTel traces/metrics/logs wiring (opt-in) and what
`otel.enabled=false` means operationally; JSON structured logging with bound
job context; `/health`, `/ready`, `/stats` endpoints; absence of dashboards;
absence of alert rules (nothing pages when the queue stalls or failure rate
spikes); absence of infra exporters (Postgres/Redis/PgBouncer metrics);
absence of SLO definitions/error budgets; collector ownership (cluster-shared,
debug exporter by default); trace propagation across API → queue → worker.

**Testing (~7):** unit suite (run, counts recorded); integration suite via
testcontainers (run if Docker available, counts recorded); migration test;
auth e2e; rate-limit test; chart testing gap (no CI, `helm lint/template`
manual-only); load/chaos testing absence; live-cluster verification exists
but is manual and unrepeatable (DECISIONS §6); coverage measurement absence.

**SDD (~7):** requirements docs 01–10 traceability; per-feature spec+plan
trail; DECISIONS.md trade-off records; runbook coverage (Redis total-loss
exists; Postgres restore, generic incident response, and upgrade runbooks
missing); README deployment/auth/allowlist operational docs; production
values guidance (defaults are CRC-sized, documented as such); AI_USAGE.md
disclosure.

## Non-goals

- Fixing any gap found (no code/chart changes).
- Referencing or evaluating the uncommitted CNPG migration draft.
- Org-level process items: on-call rosters, alert routing/paging integration,
  change management, compliance frameworks (SOC2/ISO) — noted only where a
  missing artifact (e.g., incident runbook) is repo-appropriate.
- Live cluster or docker-compose runtime verification.
- Scoring against an external standard (12-factor, CIS).

## Verification plan

Doc-only change, so verification is editorial (per project convention, no
pytest for non-code deliverables):

1. Every ✅ has an evidence pointer that resolves (file exists, line matches,
   command output present in appendix).
2. Every ❌/🟡 has a non-empty remediation/rationale.
3. The executive-summary risk table contains only items that appear in a
   category section (no orphan risks).
4. All commands in the methodology appendix were actually run this audit;
   outputs pasted/summarized, dated.
5. The CNPG draft spec is not mentioned anywhere in the deliverable.

## Risks

- **Staleness**: the doc snapshots `chart/ops-hardening` at 2026-07-14; merges
  after that date silently invalidate verdicts. Mitigation: the doc header
  carries the audited commit hash.
- **Integration tests may not run** on the audit machine (Docker dependency).
  Mitigation: the "not executed" phrasing rule above — never claim untested
  things pass.
- **Inventory blind spots**: a checklist drafted before reading every file can
  miss items. Mitigation: inventory is explicitly a floor; audit adds items
  found while reading.
