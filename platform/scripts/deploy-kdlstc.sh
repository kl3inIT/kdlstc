#!/usr/bin/env bash
set -euo pipefail

APP_NS="${APP_NS:-stc-hy}"
APP_HOST="${KDLSTC_HOST:-kdlstc-stc.10.123.123.194.nip.io}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_MANIFEST="${KDLSTC_MANIFEST:-$ROOT/k8s/kdlstc.yaml}"
PUBLIC_ORIGIN="http://${APP_HOST}"

temporary_manifest="$(mktemp)"
trap 'rm -f "$temporary_manifest"' EXIT

for secret in kdlstc-keycloak jmix-airflow-api; do
  kubectl -n "$APP_NS" get secret "$secret" -o name >/dev/null
 done

APP_NS="$APP_NS" "$ROOT/scripts/create-kdlstc-database.sh"

APP_NS="$APP_NS" \
KDLSTC_BASE_URL="$PUBLIC_ORIGIN" \
KDLSTC_FRONTEND_ORIGIN="$PUBLIC_ORIGIN" \
  "$ROOT/scripts/create-kdlstc-keycloak-client.sh"

sed "s/kdlstc\.example\.com/${APP_HOST}/g" "$BASE_MANIFEST" > "$temporary_manifest"
kubectl apply -f "$temporary_manifest"

kubectl -n "$APP_NS" rollout status deployment/kdlstc-backend --timeout=5m
kubectl -n "$APP_NS" rollout status deployment/kdlstc-frontend --timeout=5m
kubectl -n "$APP_NS" get deployment,service,ingress \
  -l app.kubernetes.io/part-of=kdlstc -o wide

echo "KDLSTC is ready at $PUBLIC_ORIGIN"
