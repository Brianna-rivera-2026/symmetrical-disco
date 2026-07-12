#!/usr/bin/env bash
# One-time, cluster-admin bootstrap for shared infrastructure this chart
# assumes already exists: the KEDA (Custom Metrics Autoscaler) operator, a
# cluster-wide OpenTelemetry collector apps export directly to (this chart
# does not deploy its own -- see deploy/chart/jobprocessor/README.md), and a
# Grafana/LGTM stack (grafana/otel-lgtm: Grafana + Tempo + Loki + Prometheus)
# the collector forwards to, exposed via an edge-TLS Route.
# Idempotent: safe to re-run; operator installs are skip-if-present, but the
# collector CR and grafana-lgtm workload are fully owned by this script and
# always reconciled to the state below.
#
# Usage: bootstrap-cluster.sh [otel-namespace]
#   OTEL_EXPORTER_ENDPOINT=<url> bootstrap-cluster.sh   # forward the shared
#     collector's output to an external backend INSTEAD OF the in-cluster
#     grafana-lgtm stack (e.g. a company-wide observability platform).
set -euo pipefail

KEDA_NS="openshift-keda"
OTEL_NS="${1:-observability}"

echo "==> Custom Metrics Autoscaler (KEDA), namespace $KEDA_NS"
if oc get csv -n "$KEDA_NS" 2>/dev/null | grep -q "Custom Metrics Autoscaler"; then
  echo "already installed"
else
  oc apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $KEDA_NS
---
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: ${KEDA_NS}-og
  namespace: $KEDA_NS
spec:
  targetNamespaces:
    - $KEDA_NS
---
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: openshift-custom-metrics-autoscaler-operator
  namespace: $KEDA_NS
spec:
  channel: stable
  name: openshift-custom-metrics-autoscaler-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
  installPlanApproval: Automatic
EOF
fi

echo "==> Red Hat build of OpenTelemetry (operator), namespace openshift-operators"
if oc get csv -n openshift-operators 2>/dev/null | grep -q "Red Hat build of OpenTelemetry"; then
  echo "already installed"
else
  oc apply -f - <<EOF
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: opentelemetry-product
  namespace: openshift-operators
spec:
  channel: stable
  name: opentelemetry-product
  source: redhat-operators
  sourceNamespace: openshift-marketplace
  installPlanApproval: Automatic
EOF
fi

echo "==> waiting for both operators to reach Succeeded (up to ~5 min)..."
keda_phase=""
otel_phase=""
for _ in $(seq 1 30); do
  keda_phase=$(oc get csv -n "$KEDA_NS" -o jsonpath='{.items[?(@.spec.displayName=="Custom Metrics Autoscaler")].status.phase}' 2>/dev/null || true)
  otel_phase=$(oc get csv -n openshift-operators -o jsonpath='{.items[?(@.spec.displayName=="Red Hat build of OpenTelemetry")].status.phase}' 2>/dev/null || true)
  [ "$keda_phase" = "Succeeded" ] && [ "$otel_phase" = "Succeeded" ] && break
  sleep 10
done
[ "$keda_phase" = "Succeeded" ] || { echo "KEDA operator did not reach Succeeded (phase: $keda_phase)" >&2; exit 1; }
[ "$otel_phase" = "Succeeded" ] || { echo "OpenTelemetry operator did not reach Succeeded (phase: $otel_phase)" >&2; exit 1; }
echo "both operators Succeeded"

echo "==> namespace $OTEL_NS"
oc get namespace "$OTEL_NS" >/dev/null 2>&1 || oc create namespace "$OTEL_NS"

echo "==> Grafana/LGTM stack (Grafana + Tempo + Loki + Prometheus), namespace $OTEL_NS"
# Runs fine under the restricted SCC as-is (verified live on CRC: no extra
# securityContext needed) -- self-contained, in-memory/ephemeral storage, so
# this is a dev/test-grade single replica, not HA or durable across restarts.
oc apply -n "$OTEL_NS" -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana-lgtm
  labels:
    app: grafana-lgtm
spec:
  replicas: 1
  selector:
    matchLabels:
      app: grafana-lgtm
  template:
    metadata:
      labels:
        app: grafana-lgtm
    spec:
      containers:
        - name: grafana-lgtm
          image: docker.io/grafana/otel-lgtm:latest
          ports:
            - containerPort: 3000
            - containerPort: 4317
            - containerPort: 4318
          readinessProbe:
            httpGet:
              path: /api/health
              port: 3000
            initialDelaySeconds: 20
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /api/health
              port: 3000
            initialDelaySeconds: 30
            periodSeconds: 15
          volumeMounts:
            # the image's default /data (Tempo/Loki/Prometheus storage) isn't
            # group-writable, so it 403s under the restricted SCC's arbitrary
            # non-root UID -- verified live on CRC. emptyDir gets chowned to
            # the pod's auto-assigned fsGroup on mount, which fixes it.
            - name: data
              mountPath: /data
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: grafana-lgtm
  labels:
    app: grafana-lgtm
spec:
  selector:
    app: grafana-lgtm
  ports:
    - { name: http, port: 3000, targetPort: 3000 }
    - { name: otlp-grpc, port: 4317, targetPort: 4317 }
    - { name: otlp-http, port: 4318, targetPort: 4318 }
---
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: grafana
spec:
  to:
    kind: Service
    name: grafana-lgtm
  port:
    targetPort: http
  tls:
    termination: edge
    insecureEdgeTerminationPolicy: Redirect
EOF

echo "==> shared OpenTelemetryCollector 'otel', namespace $OTEL_NS"
# Hooked to grafana-lgtm by default (its bundled OTLP receiver on 4317);
# override with OTEL_EXPORTER_ENDPOINT to forward elsewhere instead.
forward_endpoint="${OTEL_EXPORTER_ENDPOINT:-http://grafana-lgtm.${OTEL_NS}.svc.cluster.local:4317}"
oc apply -n "$OTEL_NS" -f - <<EOF
apiVersion: opentelemetry.io/v1beta1
kind: OpenTelemetryCollector
metadata:
  name: otel
spec:
  mode: deployment
  replicas: 1
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
      otlp:
        endpoint: ${forward_endpoint}
        tls:
          insecure: true
    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: ["debug", "otlp"]
        metrics:
          receivers: [otlp]
          processors: [batch]
          exporters: ["debug", "otlp"]
        logs:
          receivers: [otlp]
          processors: [batch]
          exporters: ["debug", "otlp"]
EOF

echo "==> waiting for grafana-lgtm to become Ready..."
oc rollout status deployment/grafana-lgtm -n "$OTEL_NS" --timeout=180s

grafana_host=$(oc get route grafana -n "$OTEL_NS" -o jsonpath='{.spec.host}' 2>/dev/null || true)

echo
echo "Shared collector endpoint -- pass as otel.exporterEndpoint when installing"
echo "the jobprocessor chart with otel.enabled=true:"
echo "  http://otel-collector.${OTEL_NS}.svc.cluster.local:4317"
echo
echo "Grafana (traces/logs/metrics from the shared collector), default login admin/admin:"
echo "  https://${grafana_host}"
