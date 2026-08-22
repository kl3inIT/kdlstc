#!/usr/bin/env bash
set -euo pipefail

APP_NS="${APP_NS:-stc-hy}"
POSTGRES_NS="${POSTGRES_NS:-stc-hy-airflow}"
POSTGRES_POD="${POSTGRES_POD:-stc-airflow-postgresql-0}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-stc-airflow-postgresql}"
SECRET_NAME="${KDLSTC_DB_SECRET:-kdlstc-db}"
DATABASE="kdlstc_control"
ROLE="kdlstc_app"
HOST="${POSTGRES_SERVICE}.${POSTGRES_NS}.svc.cluster.local"

if encoded_password="$(kubectl -n "$APP_NS" get secret "$SECRET_NAME" \
  -o jsonpath='{.data.password}' 2>/dev/null)" && [ -n "$encoded_password" ]; then
  password="$(printf '%s' "$encoded_password" | base64 -d)"
  echo "Secret $APP_NS/$SECRET_NAME exists; reusing its password."
else
  password="$(openssl rand -hex 24)"
fi

cat <<'SQL' | kubectl -n "$POSTGRES_NS" exec -i "$POSTGRES_POD" -- \
  env APP_PASSWORD="$password" sh -c '
    export PGPASSWORD="$POSTGRES_PASSWORD"
    exec psql -U postgres -v ON_ERROR_STOP=1 -v app_password="$APP_PASSWORD"
  '
SELECT format('CREATE ROLE kdlstc_app LOGIN PASSWORD %L', :'app_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'kdlstc_app') \gexec
SELECT format('ALTER ROLE kdlstc_app LOGIN PASSWORD %L', :'app_password') \gexec
SELECT 'CREATE DATABASE kdlstc_control OWNER kdlstc_app'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'kdlstc_control') \gexec
ALTER DATABASE kdlstc_control OWNER TO kdlstc_app;
REVOKE ALL ON DATABASE kdlstc_control FROM PUBLIC;
GRANT CONNECT, CREATE, TEMPORARY ON DATABASE kdlstc_control TO kdlstc_app;
SQL

kubectl -n "$APP_NS" create secret generic "$SECRET_NAME" \
  --from-literal=url="jdbc:postgresql://${HOST}:5432/${DATABASE}" \
  --from-literal=username="$ROLE" \
  --from-literal=password="$password" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl -n "$APP_NS" label secret "$SECRET_NAME" \
  app.kubernetes.io/name=kdlstc-db \
  app.kubernetes.io/part-of=kdlstc --overwrite >/dev/null

echo "Database $DATABASE and Secret $APP_NS/$SECRET_NAME are ready (values not printed)."
