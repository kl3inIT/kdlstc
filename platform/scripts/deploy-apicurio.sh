#!/usr/bin/env bash
# Dựng Apicurio Registry cho lớp 3 của kiến trúc.
#
# Idempotent: chạy lại an toàn. Mật khẩu sinh trong cụm ở lần đầu và được đọc
# lại ở các lần sau — kho mã giữ cách dựng lại, không giữ giá trị.
set -euo pipefail

NS="${NS:-stc-hy}"
PG_NS="${PG_NS:-stc-hy-airflow}"
PG_POD="${PG_POD:-stc-airflow-postgresql-0}"
PG_HOST="${PG_HOST:-stc-airflow-postgresql.stc-hy-airflow.svc.cluster.local}"
DB="${DB:-apicurio}"
DB_USER="${DB_USER:-apicurio}"

echo "==> lấy mật khẩu quản trị PostgreSQL"
PG_ADMIN_PW="$(kubectl -n "${PG_NS}" get secret stc-airflow-postgresql \
  -o jsonpath='{.data.postgres-password}' | base64 -d)"

echo "==> mật khẩu cho vai trò ${DB_USER}: đọc lại nếu đã có, sinh mới nếu chưa"
if kubectl -n "${NS}" get secret apicurio-db >/dev/null 2>&1; then
  DB_PW="$(kubectl -n "${NS}" get secret apicurio-db -o jsonpath='{.data.password}' | base64 -d)"
  echo "    dùng lại mật khẩu sẵn có"
else
  DB_PW="$(head -c 32 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 28)"
  echo "    đã sinh mật khẩu mới"
fi

echo "==> tạo vai trò và database (bỏ qua nếu đã có)"
kubectl -n "${PG_NS}" exec -i "${PG_POD}" -- env PGPASSWORD="${PG_ADMIN_PW}" \
  psql -U postgres -d postgres -v ON_ERROR_STOP=0 -q <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DB_USER}') THEN
    CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PW}';
  ELSE
    ALTER ROLE ${DB_USER} PASSWORD '${DB_PW}';
  END IF;
END \$\$;
SELECT 'CREATE DATABASE ${DB} OWNER ${DB_USER}'
 WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '${DB}')\gexec
SQL

echo "==> secret apicurio-db"
kubectl -n "${NS}" create secret generic apicurio-db \
  --from-literal=jdbc-url="jdbc:postgresql://${PG_HOST}:5432/${DB}" \
  --from-literal=user="${DB_USER}" \
  --from-literal=password="${DB_PW}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> áp manifest"
kubectl apply -f "$(dirname "$0")/../k8s/apicurio.yaml"

echo "==> chờ sẵn sàng"
kubectl -n "${NS}" rollout status deploy/apicurio --timeout=6m

echo "==> kiểm tra API v3"
kubectl -n "${NS}" run apicurio-probe --rm -i --restart=Never \
  --image=curlimages/curl:8.11.1 --quiet -- \
  curl -sS "http://apicurio.${NS}.svc.cluster.local/apis/registry/v3/system/info"
echo ""
