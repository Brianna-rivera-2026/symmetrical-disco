# jobprocessor Helm chart (OpenShift)

## Prerequisites (cluster administrators)
Run once per cluster, before any `helm install` of this chart — this chart never manages operators or cluster-wide infrastructure itself:

    deploy/openshift/bootstrap-cluster.sh [otel-namespace]   # default namespace: observability

Idempotent (safe to re-run). Installs, via OLM:
- **Custom Metrics Autoscaler** (KEDA, `openshift-keda` namespace) — required only when `keda.enabled=true`. CRDs: `scaledobjects.keda.sh/v1alpha1`, `triggerauthentications.keda.sh/v1alpha1`.
- **Red Hat build of OpenTelemetry** (operator, `openshift-operators` namespace) plus a shared `OpenTelemetryCollector` CR — required only when `otel.enabled=true`. This chart does not deploy its own collector; apps export OTLP directly to this cluster-wide one. The script prints the resulting endpoint (default: `http://otel-collector.observability.svc.cluster.local:4317`) — pass it as `otel.exporterEndpoint` when installing the chart. `helm template` fails fast if `otel.enabled=true` and `otel.exporterEndpoint` is empty. Because the collector isn't owned by this chart, the app-egress NetworkPolicy opens port 4317 to any destination rather than a specific pod selector. Set `OTEL_EXPORTER_ENDPOINT=<url>` before running the script to also forward the shared collector's output to an external backend, in addition to its debug exporter.

## Authentication setup
Before deploying this chart, the cluster operator must configure an identity provider (e.g., LDAP, OIDC, htpasswd) and create a group for API users:

    deploy/openshift/setup-idp.sh

This creates a group (default: `jobprocessor-users`) and associates users with it. After setup, users authenticate via:

    oc login https://api.example.com:6443   # prompts for user/password
    TOKEN=$(oc whoami -t)
    curl -H "Authorization: Bearer $TOKEN" https://api.jobprocessor.example.com/jobs

Configure the required group and RBAC creation via `auth.requiredGroup` and `auth.rbac.create` in `values.yaml`.

## Install
    helm install jp deploy/chart/jobprocessor -n <namespace>

## Connection math (enforced at template time)
With shipped defaults:
- Client side: workers 6 (keda.maxReplicas) x 5 (worker.dbPoolSize) + api 2 x 5 + ticker 2 = **42 <= maxClientConn 60** OK
- Server side: defaultPoolSize 20 <= maxConnections 100 - reserved 5 = **95** OK
Raising `keda.maxReplicas` to 10 makes demand 62 > 60 and `helm template` fails with the exact violation. Set `maxReplicas` so `maxReplicas x worker.dbPoolSize` plus the api/ticker share never exceeds `pgbouncer.maxClientConn`.

## TLS
Postgres, Redis (TLS-only), and PgBouncer present OpenShift serving certs; clients verify against the injected service CA at /etc/pki/service-ca/ca.crt. The API also presents a serving cert (uvicorn --ssl-*), and external entry is a re-encrypt Route — the router terminates client TLS and re-encrypts to the pod, verifying it against the service CA it trusts implicitly (no destinationCACertificate needed); nothing else is exposed. Note: Postgres has ssl=on but pg_hba is not overridden (sclorg image limitation) — plaintext to Postgres is prevented by NetworkPolicy (only PgBouncer and hook Jobs may connect, and both use verify-full).

## Worker self-recycling
`worker.maxRssMb` (default 512) — on RSS breach the worker stops claiming jobs, readiness flips to 503, in-flight jobs drain (bounded by terminationGracePeriodSeconds 50), the pod exits 0 and is replaced.
