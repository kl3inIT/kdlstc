#!/usr/bin/env bash
# Give the iMate slice its own PostgreSQL, end to end.
#
# Target is the IN-NAMESPACE PostgreSQL (stc-airflow-postgresql) — a separate
# instance from jmix-ha where stc_dwh lives, so the POC is isolated by
# construction: its own server, its own database (stc_imate), its own role
# (imate_etl) that owns that database and nothing else.
#
#   1. role imate_etl + database stc_imate   on stc-airflow-postgresql
#   2. K8s Secret  imate-db                  read by the fetch pod
#   3. Airflow Conn imate_dwh                read by the worker-side tasks
#
# Idempotent: an existing secret's password is reused; role and database are
# created only if absent. The admin password never leaves the cluster — the
# psql runs INSIDE the postgres pod where the secret is already mounted.
set -euo pipefail

NS="${NS:-stc-hy-airflow}"
PG_POD="stc-airflow-postgresql-0"
PG_HOST="stc-airflow-postgresql.${NS}.svc.cluster.local"
DB="stc_imate"
ROLE="imate_etl"

# ── password: reuse if the secret already exists ─────────────────────────
if PW=$(kubectl -n "$NS" get secret imate-db -o jsonpath='{.data.password}' 2>/dev/null) \
   && [ -n "$PW" ]; then
  PW=$(echo "$PW" | base64 -d)
  echo "secret imate-db da ton tai — dung lai mat khau"
else
  PW=$(openssl rand -hex 24)
  echo "sinh mat khau moi cho $ROLE"
fi

# ── role + database, executed inside the postgres pod ────────────────────
# The admin password is read from the chart's own secret and handed to psql
# through the exec env — it never lands in a file or in the repo.
PG_ADMIN_PW=$(kubectl -n "$NS" get secret stc-airflow-postgresql \
  -o jsonpath='{.data.postgres-password}' | base64 -d)

kubectl -n "$NS" exec -i "$PG_POD" -- sh -c "
  export PGPASSWORD='$PG_ADMIN_PW'
  set -e
  psql -U postgres -v ON_ERROR_STOP=1 -v pw='$PW' <<'EOSQL'
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'imate_etl') THEN
    CREATE ROLE imate_etl LOGIN;
  END IF;
END \$\$;
ALTER ROLE imate_etl LOGIN PASSWORD :'pw';
EOSQL
  psql -U postgres -tc \"SELECT 1 FROM pg_database WHERE datname = '$DB'\" | grep -q 1 \
    || psql -U postgres -v ON_ERROR_STOP=1 -c \"CREATE DATABASE $DB OWNER $ROLE\"
  echo 'role + database: ok'
"

# ── secret for the fetch pod ─────────────────────────────────────────────
kubectl -n "$NS" create secret generic imate-db \
  --from-literal=host="$PG_HOST" \
  --from-literal=dbname="$DB" \
  --from-literal=user="$ROLE" \
  --from-literal=password="$PW" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "secret imate-db: ok"

# ── Airflow connection for worker-side tasks ─────────────────────────────
kubectl -n "$NS" exec deploy/stc-airflow-api-server -c api-server -- sh -c "
  airflow connections delete imate_dwh >/dev/null 2>&1 || true
  airflow connections add imate_dwh \
    --conn-type postgres --conn-host '$PG_HOST' --conn-port 5432 \
    --conn-schema '$DB' --conn-login '$ROLE' --conn-password '$PW' \
    >/dev/null 2>&1 && echo 'airflow connection imate_dwh: ok'"
