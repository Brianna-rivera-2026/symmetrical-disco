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
