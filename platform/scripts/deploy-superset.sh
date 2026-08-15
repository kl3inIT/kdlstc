#!/usr/bin/env bash
# Deploy Apache Superset into its own namespace (stc-hy-bi).
#
# Secrets are generated here and live only in the cluster: the Secret
# superset-secrets holds them for reference, helm receives them via --set.
# Re-running reuses existing passwords, so upgrades never rotate anything
# by accident.
#
# Also prepares the READ-ONLY role imate_reader on stc_imate — the identity
# Superset uses to query the warehouse. BI never holds write access.
set -euo pipefail

NS="${NS:-stc-hy-bi}"
CHART_VERSION="0.22.4"
RELEASE="superset"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ── secrets: generate once, reuse forever ────────────────────────────────
get_or_new() {
  local key="$1"
  local val
  if val=$(kubectl -n "$NS" get secret superset-secrets \
             -o jsonpath="{.data.$key}" 2>/dev/null) && [ -n "$val" ]; then
    echo "$val" | base64 -d
  else
    openssl rand -hex 16
  fi
}
SECRET_KEY=$(get_or_new secret-key)
DB_PW=$(get_or_new db-password)
# Pinned as well: the subchart otherwise generates a fresh admin password on
# every install, while the PVC keeps the FIRST one — reinstall over a kept
# volume then fails auth forever. Learned the hard way.
PG_ADMIN=$(get_or_new pg-admin-password)
ADMIN_PW=$(get_or_new admin-password)
READER_PW=$(get_or_new imate-reader-password)

kubectl -n "$NS" create secret generic superset-secrets \
  --from-literal=secret-key="$SECRET_KEY" \
  --from-literal=db-password="$DB_PW" \
  --from-literal=pg-admin-password="$PG_ADMIN" \
  --from-literal=admin-password="$ADMIN_PW" \
  --from-literal=imate-reader-password="$READER_PW" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "secret superset-secrets: ok"

# ── imate_reader: read-only identity on the warehouse ────────────────────
PG_ADMIN_PW=$(kubectl -n stc-hy-airflow get secret stc-airflow-postgresql \
  -o jsonpath='{.data.postgres-password}' | base64 -d)
kubectl -n stc-hy-airflow exec -i stc-airflow-postgresql-0 -- sh -c "
  export PGPASSWORD='$PG_ADMIN_PW'
  set -e
  psql -U postgres -v ON_ERROR_STOP=1 -v pw='$READER_PW' <<'EOSQL'
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'imate_reader') THEN
    CREATE ROLE imate_reader LOGIN;
  END IF;
END \$\$;
ALTER ROLE imate_reader LOGIN PASSWORD :'pw';
EOSQL
  psql -U postgres -d stc_imate -v ON_ERROR_STOP=1 <<'EOSQL'
GRANT CONNECT ON DATABASE stc_imate TO imate_reader;
GRANT USAGE ON SCHEMA curated, refdata, ingestion, metadata TO imate_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA curated, refdata, ingestion, metadata TO imate_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE imate_etl IN SCHEMA curated
  GRANT SELECT ON TABLES TO imate_reader;
EOSQL
  echo 'imate_reader: ok'
"

# ── helm ─────────────────────────────────────────────────────────────────
# Hai --set cho CÙNG một mật khẩu DB là chủ đích, không thừa:
#   postgresql.auth.password  → subchart, quyết định postgres init ra gì
#   database.password         → phía APP đọc; bỏ trống là nó dùng default
#                               "superset" và lệch auth vĩnh viễn
helm repo add superset https://apache.github.io/superset >/dev/null 2>&1 || true
helm repo update superset >/dev/null 2>&1 || true

helm upgrade --install "$RELEASE" superset/superset \
  --version "$CHART_VERSION" \
  --namespace "$NS" \
  -f "$ROOT/helm/superset-values.yaml" \
  --set postgresql.auth.password="$DB_PW" \
  --set postgresql.auth.postgresPassword="$PG_ADMIN" \
  --set database.password="$DB_PW" \
  --set init.adminUser.password="$ADMIN_PW" \
  --set extraSecretEnv.SUPERSET_SECRET_KEY="$SECRET_KEY" \
  --timeout 15m \
  --wait

echo
echo "Superset: http://bi-stc.10.123.123.194.nip.io"
echo "Dang nhap: admin / (kubectl -n $NS get secret superset-secrets -o jsonpath='{.data.admin-password}' | base64 -d)"
