#!/usr/bin/env bash
# Give Airflow the READ-ONLY identity that step 6 (serving) uses.
#
# Step 6 has to check the door from outside: connecting as imate_etl would
# prove nothing, since the writer can read everything by definition. It needs
# imate_reader — the same role Superset connects with — so a missing grant
# fails the pipeline instead of quietly emptying a dashboard.
#
# The password is NOT generated here. It already exists in the BI namespace
# because Superset uses it; a second password for the same role would mean two
# secrets to rotate and one of them silently wrong. This copies that one.
#
# Idempotent: safe to re-run, reads whatever password is current.
set -euo pipefail

AIRFLOW_NS="${AIRFLOW_NS:-stc-hy-airflow}"
BI_NS="${BI_NS:-stc-hy-bi}"
DB_HOST="${DB_HOST:-stc-airflow-postgresql.stc-hy-airflow.svc.cluster.local}"
DB_NAME="${DB_NAME:-stc_imate}"
DB_USER="${DB_USER:-imate_reader}"
CONN_ID="${CONN_ID:-imate_reader}"

echo "==> doc mat khau imate_reader tu secret superset-secrets (ns ${BI_NS})"
READER_PW="$(kubectl -n "${BI_NS}" get secret superset-secrets \
  -o jsonpath='{.data.imate-reader-password}' | base64 -d)"
[ -n "${READER_PW}" ] || { echo "khong doc duoc mat khau" >&2; exit 1; }

echo "==> kiem tra vai tro dang nhap duoc va CHI doc duoc"
kubectl -n "${AIRFLOW_NS}" exec -i stc-airflow-postgresql-0 -- \
  env PGPASSWORD="${READER_PW}" psql -U "${DB_USER}" -d "${DB_NAME}" -q -t <<'SQL'
SELECT 'doc duoc fact_document: ' || count(*) FROM curated.fact_document;
SQL

echo "==> tao/ cap nhat secret imate-reader-db (ns ${AIRFLOW_NS})"
kubectl -n "${AIRFLOW_NS}" create secret generic imate-reader-db \
  --from-literal=host="${DB_HOST}" \
  --from-literal=dbname="${DB_NAME}" \
  --from-literal=user="${DB_USER}" \
  --from-literal=password="${READER_PW}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> tao/ cap nhat Airflow Connection '${CONN_ID}'"
# delete-then-add: `connections add` refuses an existing id, and there is no
# upsert. The delete is allowed to fail on the first ever run.
kubectl -n "${AIRFLOW_NS}" exec deploy/stc-airflow-api-server -- \
  airflow connections delete "${CONN_ID}" >/dev/null 2>&1 || true
kubectl -n "${AIRFLOW_NS}" exec deploy/stc-airflow-api-server -- \
  airflow connections add "${CONN_ID}" \
    --conn-type postgres \
    --conn-host "${DB_HOST}" \
    --conn-schema "${DB_NAME}" \
    --conn-login "${DB_USER}" \
    --conn-password "${READER_PW}" \
    --conn-port 5432

echo "==> xong. Connection hien co:"
kubectl -n "${AIRFLOW_NS}" exec deploy/stc-airflow-api-server -- \
  airflow connections list 2>/dev/null | grep -E "imate|conn_id"
