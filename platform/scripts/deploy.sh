#!/usr/bin/env bash
# Install or upgrade the platform on Rancher. Idempotent — run it again after
# editing anything under helm/.
#
# Run create-secrets.sh first; every chart here expects its secret to exist.
#
#   ./deploy.sh              everything
#   ./deploy.sh airflow      one component
#   ./deploy.sh database-ui  lightweight PostgreSQL inspector
set -euo pipefail

APP_NS="${APP_NS:-stc-hy}"
AIRFLOW_NS="${AIRFLOW_NS:-stc-hy-airflow}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WANT="${1:-all}"

# Public values contain only safe placeholders. Deployment-specific overrides
# live in ignored *-values.local.yaml files and are passed through these vars.
KEYCLOAK_VALUES_LOCAL="${KEYCLOAK_VALUES_LOCAL:-}"
SEAWEEDFS_VALUES_LOCAL="${SEAWEEDFS_VALUES_LOCAL:-}"
OAUTH2_PROXY_VALUES_LOCAL="${OAUTH2_PROXY_VALUES_LOCAL:-}"
AIRFLOW_VALUES_LOCAL="${AIRFLOW_VALUES_LOCAL:-}"
SEAWEEDFS_ADMIN_MANIFEST="${SEAWEEDFS_ADMIN_MANIFEST:-$ROOT/k8s/seaweedfs-admin-ingress.yaml}"
PGWEB_MANIFEST="${PGWEB_MANIFEST:-$ROOT/k8s/pgweb.yaml}"

# Chart versions are pinned. An unpinned upgrade is how a working cluster
# quietly becomes a different working cluster.
KEYCLOAK_CHART_VERSION=7.2.2
SEAWEEDFS_CHART_VERSION=4.41.0
OAUTH2_PROXY_CHART_VERSION=10.7.0
AIRFLOW_CHART_VERSION=1.22.0

want() { [ "$WANT" = all ] || [ "$WANT" = "$1" ]; }

helm repo add codecentric  https://codecentric.github.io/helm-charts        >/dev/null 2>&1 || true
helm repo add seaweedfs    https://seaweedfs.github.io/seaweedfs/helm       >/dev/null 2>&1 || true
helm repo add oauth2-proxy https://oauth2-proxy.github.io/manifests         >/dev/null 2>&1 || true
helm repo add apache-airflow https://airflow.apache.org                     >/dev/null 2>&1 || true
helm repo update >/dev/null

if want keycloak; then
  echo "==> keycloak"
  values=(-f "$ROOT/helm/keycloak-values.yaml")
  [ -z "$KEYCLOAK_VALUES_LOCAL" ] || values+=(-f "$KEYCLOAK_VALUES_LOCAL")
  helm upgrade --install kc codecentric/keycloakx \
    --version "$KEYCLOAK_CHART_VERSION" -n "$APP_NS" \
    "${values[@]}" --timeout 10m
fi

if want seaweedfs; then
  echo "==> seaweedfs"
  values=(-f "$ROOT/helm/seaweedfs-values.yaml")
  [ -z "$SEAWEEDFS_VALUES_LOCAL" ] || values+=(-f "$SEAWEEDFS_VALUES_LOCAL")
  helm upgrade --install sw seaweedfs/seaweedfs \
    --version "$SEAWEEDFS_CHART_VERSION" -n "$APP_NS" \
    "${values[@]}" --timeout 10m
  kubectl apply -f "$SEAWEEDFS_ADMIN_MANIFEST"
fi

if want oauth2-proxy; then
  echo "==> oauth2-proxy"
  values=(-f "$ROOT/helm/oauth2-proxy-values.yaml")
  [ -z "$OAUTH2_PROXY_VALUES_LOCAL" ] || values+=(-f "$OAUTH2_PROXY_VALUES_LOCAL")
  helm upgrade --install o2p oauth2-proxy/oauth2-proxy \
    --version "$OAUTH2_PROXY_CHART_VERSION" -n "$APP_NS" \
    "${values[@]}" --timeout 5m
fi

if want database-ui; then
  echo "==> database UI (pgweb)"
  kubectl apply -f "$PGWEB_MANIFEST"
  kubectl -n "$APP_NS" rollout status deployment/pgweb --timeout=5m
fi

if want mock; then
  echo "==> mock sources"
  "$ROOT/scripts/sync-source.sh"
  kubectl apply -f "$ROOT/k8s/mock-qlgia.yaml"
fi

if want airflow; then
  echo "==> airflow"
  # The DAG ConfigMap has to exist before the pods mount it, or they hang in
  # ContainerCreating waiting for a volume that is never coming.
  "$ROOT/scripts/sync-dags.sh"
  bash "$ROOT/scripts/sync-dbt.sh"
  kubectl apply -f "$ROOT/k8s/airflow-kpo-rbac.yaml"

  # The apache-airflow repo index points at archive.apache.org, which is
  # frequently unreachable from here. The chart is vendored in this repo so a
  # deploy never depends on that host being up; the mirror is only a fallback
  # for bumping the pinned version.
  CHART="$ROOT/vendor/airflow-$AIRFLOW_CHART_VERSION.tgz"
  if [ ! -f "$CHART" ]; then
    echo "    chart not vendored, fetching from mirror"
    mkdir -p "$ROOT/vendor"
    curl -fsS --max-time 120 -o "$CHART" \
      "https://dlcdn.apache.org/airflow/helm-chart/$AIRFLOW_CHART_VERSION/airflow-$AIRFLOW_CHART_VERSION.tgz"
  fi

  values=(-f "$ROOT/helm/airflow-values.yaml")
  [ -z "$AIRFLOW_VALUES_LOCAL" ] || values+=(-f "$AIRFLOW_VALUES_LOCAL")
  helm upgrade --install stc-airflow "$CHART" -n "$AIRFLOW_NS" \
    "${values[@]}" --timeout 15m
fi

echo "Done."
