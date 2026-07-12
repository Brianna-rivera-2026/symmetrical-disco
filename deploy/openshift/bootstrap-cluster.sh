#!/usr/bin/env bash
# One-time, cluster-admin bootstrap for shared infrastructure this chart
# assumes already exists: the KEDA (Custom Metrics Autoscaler) operator, and
# a cluster-wide OpenTelemetry collector apps export directly to (this chart
# does not deploy its own -- see deploy/chart/jobprocessor/README.md).
# Idempotent: safe to re-run; skips anything already present.
#
# Usage: bootstrap-cluster.sh [otel-namespace]
#   OTEL_EXPORTER_ENDPOINT=<url> bootstrap-cluster.sh   # also forward the
#     shared collector's output to an external backend, in addition to the
#     debug exporter (e.g. a company-wide observability platform).
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

echo "==> shared OpenTelemetryCollector 'otel', namespace $OTEL_NS"
oc get namespace "$OTEL_NS" >/dev/null 2>&1 || oc create namespace "$OTEL_NS"

if oc get opentelemetrycollector otel -n "$OTEL_NS" >/dev/null 2>&1; then
  echo "shared collector already exists -- leaving it untouched (edit in place to change its config)"
else
  exporters='["debug"]'
  extra_exporter=""
  if [ -n "${OTEL_EXPORTER_ENDPOINT:-}" ]; then
    exporters='["debug","otlp"]'
    extra_exporter=$(cat <<EOF
      otlp:
        endpoint: ${OTEL_EXPORTER_ENDPOINT}
        tls:
          insecure: true
EOF
)
  fi
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
$extra_exporter
    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: $exporters
        metrics:
          receivers: [otlp]
          processors: [batch]
          exporters: $exporters
        logs:
          receivers: [otlp]
          processors: [batch]
          exporters: $exporters
EOF
fi

echo
echo "Shared collector endpoint -- pass as otel.exporterEndpoint when installing"
echo "the jobprocessor chart with otel.enabled=true:"
echo "  http://otel-collector.${OTEL_NS}.svc.cluster.local:4317"
