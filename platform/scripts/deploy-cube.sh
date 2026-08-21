#!/usr/bin/env bash
# Dựng Cube — lớp ngữ nghĩa cho lớp 4.
#
# Idempotent. Mật khẩu SQL API sinh trong cụm lần đầu, đọc lại các lần sau.
# Mật khẩu vào kho KHÔNG sinh mới: lấy đúng mật khẩu imate_reader mà Superset
# đang dùng — một vai trò chỉ nên có một mật khẩu.
set -euo pipefail

NS="${NS:-stc-hy}"
BI_NS="${BI_NS:-stc-hy-bi}"
DB_HOST="${DB_HOST:-stc-airflow-postgresql.stc-hy-airflow.svc.cluster.local}"
DB_NAME="${DB_NAME:-stc_imate}"
DB_USER="${DB_USER:-imate_reader}"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "==> lấy mật khẩu vai trò chỉ đọc từ secret của Superset"
READER_PW="$(kubectl -n "${BI_NS}" get secret superset-secrets \
  -o jsonpath='{.data.imate-reader-password}' | base64 -d)"
[ -n "${READER_PW}" ] || { echo "khong doc duoc mat khau imate_reader" >&2; exit 1; }

get_or_new() {  # $1 = key trong secret cube-db
  local existing
  existing="$(kubectl -n "${NS}" get secret cube-db -o jsonpath="{.data.$1}" 2>/dev/null || true)"
  if [ -n "${existing}" ]; then echo "${existing}" | base64 -d
  else head -c 32 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 28; fi
}

API_SECRET="$(get_or_new api-secret)"
SQL_PW="$(get_or_new sql-password)"

echo "==> secret cube-db"
kubectl -n "${NS}" create secret generic cube-db \
  --from-literal=host="${DB_HOST}" \
  --from-literal=dbname="${DB_NAME}" \
  --from-literal=user="${DB_USER}" \
  --from-literal=password="${READER_PW}" \
  --from-literal=api-secret="${API_SECRET}" \
  --from-literal=sql-user="cube" \
  --from-literal=sql-password="${SQL_PW}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> configmap cube-model"
kubectl -n "${NS}" create configmap cube-model \
  --from-file="${HERE}/../cube/model/cubes" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> áp manifest"
kubectl apply -f "${HERE}/../k8s/cube.yaml"
# Model nằm trong ConfigMap nên đổi model không tự cuộn pod — ép cuộn.
kubectl -n "${NS}" rollout restart deploy/cube >/dev/null

echo "==> chờ sẵn sàng"
kubectl -n "${NS}" rollout status deploy/cube --timeout=6m
