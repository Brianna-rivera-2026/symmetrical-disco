# CloudNativePG Migration — Design

**Date:** 2026-07-14
**Status:** Draft (not yet implemented)
**Depends on:** the `chart/ops-hardening` branch (resources/PDBs/spread, pgbouncer admin rights removed)

## Problem

The Helm chart runs Postgres as a single-replica StatefulSet on one RWO PVC with
no replication, no failover, no backups, and no PITR. Postgres is the system's
source of truth and sits in the hot path of every operation (auth lookups, the
worker claim guard, all status writes). A node failure means minutes-to-tens-of-
minutes of total outage while the StatefulSet reschedules; losing the PV loses
everything. The chart also carries ~340 lines of bespoke database plumbing
(init bash, TLS configmap workarounds, a hand-configured PgBouncer with
restricted-SCC hacks) that only this repo maintains.

## Decisions (settled during brainstorming)

| Decision | Choice |
|---|---|
| Scope | Full migration: HA Postgres + pooler + backups in one spec |
| Operator | CloudNativePG (CNCF, community catalog; EDB available later if support contract required) |
| Cutover strategy | Clean cutover — old templates deleted, no dual-mode toggle, no subchart |
| Backups | Behind `postgres.backup.enabled` (default `false`); template fails fast if enabled without S3 values |
| Default topology | `postgres.instances: 1` (fits local CRC); `3` documented as the production value |
| Data migration | None — fresh bootstrap; existing dev PVCs are disposable |
| docker-compose | Untouched; `01-roles.sh` remains solely as the compose init script |

## Goals

- Automated failover: a node/pod loss costs seconds, not minutes, at `instances >= 2`.
- Declarative backups + PITR to any S3-compatible store when enabled.
- Delete the bespoke Postgres/PgBouncer plumbing the chart currently owns.
- Preserve every existing security property: `jobs_app`/`jobs_migrator` role
  split, TLS with verified CA on every DB connection, default-deny NetworkPolicies,
  hooks bypassing the pooler.
- Keep the local (CRC) footprint at roughly today's size with default values.

## Non-goals

- Migrating existing data (fresh bootstrap only).
- Changing docker-compose, the application code, or the Alembic migrations.
- Monitoring dashboards/alerts for Postgres (CNPG exposes Prometheus metrics;
  wiring them into the observability stack is a separate effort).
- Multi-namespace / shared database service.

## Architecture

The chart stops *running* Postgres and starts *declaring* it:

```
                      ┌───────────────────────────────┐
 api / worker ──────► │ Pooler "jp-pooler" :5432      │ ──► Cluster "jp-postgres"
 ticker               │ (CNPG-managed PgBouncer,      │     instances: N (1 local / 3 prod)
                      │  transaction mode, TLS)       │     streaming replication,
                      └───────────────────────────────┘     automated failover
 migrate / users-sync ────────────────────────────────────► jp-postgres-rw:5432
 (hook Jobs, direct)                                        (CNPG-created Service)
```

- **`Cluster` CR** `jp-postgres`: N instances, one PVC each, CNPG-created
  `-rw`/`-ro` Services, CNPG-issued CA and serving certs, native Prometheus
  metrics endpoint.
- **`Pooler` CR** `jp-pooler`: CNPG-managed PgBouncer in transaction mode,
  replacing the edoburu Deployment, its hand-written ini, and the SCC
  workarounds. Client-facing TLS uses the cluster's certificates; auth uses
  CNPG's managed `auth_query` (no `userlist.txt`). No `admin_users`.
- **App traffic** goes through the Pooler. **Hook Jobs** (migrate, users-sync)
  keep bypassing pooling — DDL and session state don't mix with transaction
  mode — now targeting `jp-postgres-rw`.

## Roles, credentials, bootstrap SQL

The `jobs_app`/`jobs_migrator` split survives unchanged in *semantics*; the
mechanism moves from `01-roles.sh` into the CR:

- **`jobs_migrator`** is the `bootstrap.initdb.owner` of database `jobs`.
  CNPG generates its credential Secret (the cluster's "app user" secret).
  The migrate Job's `DATABASE_URL` reads password from that Secret.
- **`jobs_app`** is a `managed.roles` entry with `login: true` and a
  `passwordSecret` of type `kubernetes.io/basic-auth`. That Secret is produced
  by the chart's existing credentials pre-install/pre-upgrade hook mechanism
  (which already solves idempotent generation), holding only this one password.
- **Grants** (`GRANT USAGE ON SCHEMA`, table/sequence DML grants,
  `ALTER DEFAULT PRIVILEGES` so future migration-created objects are readable)
  move verbatim from `01-roles.sh` into `bootstrap.initdb.postInitApplicationSQL`.
  Fresh-bootstrap-only, so the script's "adopt pre-existing tables" loop is
  dropped — there are no pre-existing tables.
- The `jp-credentials` Secret keeps `redis-password` and drops all three
  `db-*` keys.

Exact CRD field names are pinned at implementation time against the CNPG
version chosen; the ownership contract above is the design.

## TLS and DSN changes

CNPG issues its own CA per cluster (Secret `jp-postgres-ca`); the OpenShift
service CA no longer signs anything on the database path.

- App pods mount **two** CA bundles: the service CA (unchanged — Redis still
  uses it) and the CNPG cluster CA at a new path, e.g.
  `/etc/pki/cnpg-ca/ca.crt`.
- `_helpers.tpl` DSN changes:
  - `appDatabaseUrl`: host `jp-pooler.<ns>.svc:5432`,
    `sslmode=verify-full&sslrootcert=/etc/pki/cnpg-ca/ca.crt`.
  - `appDirectDatabaseUrl` / `migratorDatabaseUrl`: host `jp-postgres-rw.<ns>.svc:5432`,
    same CNPG CA. Password env vars re-point at the CNPG/hook-generated Secrets.
  - `redisUrl`: unchanged.
- `DB_DISABLE_PREPARED_STATEMENTS=true` stays — the Pooler is still
  transaction-mode PgBouncer.

## NetworkPolicies

CNPG pods carry `cnpg.io/cluster: jp-postgres` (and `cnpg.io/podRole` on the
pooler), not this chart's component labels. Changes to `networkpolicies.yaml`:

| Policy | Change |
|---|---|
| `pgbouncer` (in from apps, out to postgres) | **Deleted**, replaced by an equivalent policy selecting the Pooler's labels (ingress from api/worker/ticker on 5432; egress to cluster pods on 5432) |
| `postgres-ingress` | Rewritten to select `cnpg.io/cluster: jp-postgres`; admits: pooler pods (5432), hook Jobs (5432), **replica↔replica** (5432, self-selector — required for streaming replication), and the CNPG operator namespace (5432 + **8000**, the instance-manager status port the operator polls) |
| `app-egress` | pgbouncer destination swapped for the pooler's labels; port changes 6432 → 5432 (the Pooler listens on 5432) |
| `hook-egress` | destination swapped to `cnpg.io/cluster` labels |
| New: `postgres-egress` | replicas need egress to each other (5432) and to the operator; plus the existing DNS allowance already covers every pod |

Failure mode to design against: a missing replication or operator-status allow
is **invisible until the first failover**, then failover hangs. The
verification plan (below) includes an explicit failover drill for exactly this
reason. Kubelet probe traffic remains exempt on OVN-Kubernetes (existing note
in the file).

## Backups (`postgres.backup.enabled`, default false)

- When `false`: no backup resources rendered; `Cluster` CR has no
  `barmanObjectStore`. WAL stays local — RPO is "whatever the PVs survive."
  The chart README states this plainly so "we never turned it on" is at least
  a documented, visible risk rather than a silent one.
- When `true`: template **fails fast** (same pattern as `otel.exporterEndpoint`)
  unless all of `postgres.backup.endpointURL`, `.bucket`, and
  `.credentialsSecret` (existing Secret with S3 access/secret keys) are set.
  Renders:
  - `barmanObjectStore` on the Cluster: WAL archiving (continuous → RPO ≈
    minutes) + base backups, `retentionPolicy` from values (default `30d`).
  - A `ScheduledBackup` CR, default daily.
- Restore is `bootstrap.recovery` from the object store (full or PITR to a
  timestamp). A runbook — sibling to the existing Redis-total-loss runbook —
  documents the procedure, including the queue-consistency step after a
  point-in-time restore (flush Redis and let the reconciler rebuild, per the
  existing runbook's mechanism, since restored-Postgres × current-Redis states
  disagree).
- MinIO or any S3-compatible endpoint works; deploying MinIO is out of scope
  for the chart (bootstrap script may gain a dev-grade MinIO later; not part
  of this design).

## Operator installation (`bootstrap-cluster.sh`)

New section, same idempotent skip-if-present style as KEDA/OTel: install a
**version-pinned** CNPG release manifest into `cnpg-system`
(`kubectl apply --server-side -f cnpg-<version>.yaml`), then wait for the
deployment to become Ready. Plain manifests over OLM: CNPG's community-catalog
packaging lags releases and OLM adds nothing here; pinning the manifest URL
keeps the install reproducible and mirror-friendly (the URL/image are the only
things to mirror for air-gapped installs).

The chart itself gains a fail-fast guard: if `postgresql.cnpg.io/v1` is absent
from `.Capabilities.APIVersions`, `helm template`/`install` fails with a
message pointing at the bootstrap script. Because plain `helm template` (no
`--validate`) reports empty Capabilities, the guard is bypassable with
`--set global.skipCapabilitiesCheck=true` (default `false`); render-only
contexts (CI, docs) set it, real installs never do.

## values.yaml shape

```yaml
postgres:
  instances: 1            # production: 3 (documented in chart README)
  storage: 1Gi
  database: jobs
  maxConnections: 100     # becomes Cluster .spec.postgresql.parameters
  resources:              # unchanged from ops-hardening branch
    requests: { cpu: 100m, memory: 256Mi }
    limits: { memory: 1Gi }
  backup:
    enabled: false
    endpointURL: ""       # required when enabled
    bucket: ""            # required when enabled
    credentialsSecret: "" # required when enabled; existing Secret, keys ACCESS_KEY_ID / ACCESS_SECRET_KEY
    retention: 30d
    schedule: "0 0 2 * * *"   # CNPG 6-field cron: daily 02:00

pooler:                   # replaces the pgbouncer block
  instances: 1
  defaultPoolSize: 20
  maxClientConn: 60
  resources:
    requests: { cpu: 50m, memory: 64Mi }
    limits: { memory: 128Mi }
```

Removed: `postgres.image`, `postgres.user`, `postgres.reservedConnections`,
the whole `pgbouncer` block, `tls.appToPgbouncer` (the Pooler always serves
TLS from cluster certs).

**Connection-math validator:** keeps the client-side check
(`fleet demand <= pooler.maxClientConn`, now also `x pooler.instances`);
**drops the server-side check** — sizing `defaultPoolSize x pooler.instances`
against `max_connections` moves to a README formula, since the operator owns
superuser/replication connection reservations and a template-time inequality
would encode guesses about CNPG internals.

## Deleted vs added (chart)

**Deleted:** `postgres-statefulset.yaml` (92), `postgres-service.yaml` (18),
`postgres-tls-configmap.yaml` (12), `db-init-configmap.yaml` (10),
`pgbouncer-deployment.yaml` (98), `pgbouncer-ini-configmap.yaml` (34),
`pgbouncer-service.yaml` (17); `db-*` keys from the credentials hook; server-side
validator half. `01-roles.sh` moves out of the chart's `files/` (compose
references it directly; new home `deploy/compose/db-init/` with the compose
volume path updated — it must not ship inside the chart it no longer serves).

**Added:** `postgres-cluster.yaml` (~90 incl. backup conditional),
`postgres-pooler.yaml` (~35), `postgres-scheduledbackup.yaml` (~15, flag-gated),
NetworkPolicy rewrites (~net +30), bootstrap-cluster.sh section (~35),
values/README updates.

Net: ~120 fewer lines owned by the repo, and the deleted lines are the
highest-maintenance ones (bash, SCC workarounds, hand TLS).

## Verification plan

Config-layer change → manual verification, no pytest (per project convention):

1. **Render matrix:** `helm lint`; `helm template` with defaults, with
   `postgres.backup.enabled=true` + S3 values, with `instances=3`, with
   `keda.enabled=true otel.enabled=true`; assert: no old template names in
   output, DSNs point at pooler/`-rw` hosts with the CNPG CA path, fail-fast
   fires when backup enabled without S3 values.
2. **CRC install:** bootstrap script installs the operator; `helm install`;
   migrate + users-sync hooks succeed against `-rw`; app end-to-end (submit →
   process → status) through the Pooler.
3. **Failover drill (the netpol proof):** with `--set postgres.instances=2` on
   CRC (single node — this drill validates *policy and promotion*, not node
   loss): delete the primary pod; assert promotion completes, the app recovers
   within `pool_pre_ping` + reaper bounds, and no NetworkPolicy blocks
   replication or the operator's status port. A hung promotion here means the
   netpols are wrong — fix before merge.
4. **Backup smoke (optional, if an S3 endpoint is at hand):** enable backups
   against MinIO, take a `Backup`, verify object-store contents; full restore
   drill is deferred to the runbook's first scheduled rehearsal.

## Risks

- **NetworkPolicy gaps surface only at failover** — mitigated by drill #3 as a
  merge gate.
- **Operator lifecycle**: CNPG version upgrades and CRD changes become platform
  work; version-pinned manifest keeps it deliberate. Community support only
  (by choice); EDB is the paid path with the same CRDs if the org later
  requires a vendor.
- **CRD field drift**: this spec pins the *contract* (who owns which Secret,
  which SQL runs where); exact field names are validated against the pinned
  CNPG version during implementation.
- **Backups default off** — accepted trade-off from brainstorming; the README
  and values comments carry the warning, and enabling is a values-only change.
- **Rollback**: `git revert` + `helm upgrade` restores the old StatefulSet
  path, but CNPG PVCs are orphaned (data loss is acceptable pre-production —
  same fresh-bootstrap assumption in reverse). After production data exists,
  rollback becomes restore-from-backup; the runbook says so.

## Out of scope, explicitly

Compose parity beyond the `01-roles.sh` relocation; data import; Postgres
dashboards/alerts; MinIO deployment; `Pooler` read-only replicas (`-ro`
routing) — the app has no read/write split today.
