#!/usr/bin/env bash
# Create every Secret the platform expects, from environment variables.
#
# No password is ever committed to this repo. Set the variables below in your
# shell (or source a file that is gitignored) and run this once per cluster.
# Re-running is safe: each secret is replaced, not appended.
#
# Required:
#   DWH_HOST DWH_DBNAME DWH_USER DWH_PASSWORD    warehouse on jmix-ha
#   KC_DB_PASSWORD                               Keycloak's own database
#   KC_ADMIN_USER KC_ADMIN_PASSWORD              Keycloak bootstrap admin
#   KC_AIRFLOW_CLIENT_SECRET                     realm client "airflow"
#   KC_SW_CLIENT_SECRET                          realm client "seaweedfs"
#   O2P_COOKIE_SECRET                            32 bytes, base64url
#
# Generate a cookie secret with:
#   openssl rand -base64 32 | tr -- '+/' '-_' | tr -d '='
set -euo pipefail

APP_NS="${APP_NS:-stc-hy}"
AIRFLOW_NS="${AIRFLOW_NS:-stc-hy-airflow}"

require() {
  for name in "$@"; do
    if [ -z "${!name:-}" ]; then
      echo "missing environment variable: $name" >&2
      exit 1
    fi
  done
}

require DWH_HOST DWH_DBNAME DWH_USER DWH_PASSWORD \
        KC_DB_PASSWORD KC_ADMIN_USER KC_ADMIN_PASSWORD \
        KC_AIRFLOW_CLIENT_SECRET KC_SW_CLIENT_SECRET O2P_COOKIE_SECRET

recreate() {  # recreate <namespace> <name> <literal>...
  local ns="$1" name="$2"; shift 2
  kubectl -n "$ns" create secret generic "$name" "$@" \
    --dry-run=client -o yaml | kubectl apply -f -
}

# ── stc-hy ───────────────────────────────────────────────────────────────
recreate "$APP_NS" keycloak-db \
  --from-literal=password="$KC_DB_PASSWORD"

recreate "$APP_NS" keycloak-admin \
  --from-literal=username="$KC_ADMIN_USER" \
  --from-literal=password="$KC_ADMIN_PASSWORD"

# oauth2-proxy reads all three from one secret, with these exact key names.
recreate "$APP_NS" oauth2-proxy-sw \
  --from-literal=client-id="seaweedfs" \
  --from-literal=client-secret="$KC_SW_CLIENT_SECRET" \
  --from-literal=cookie-secret="$O2P_COOKIE_SECRET"

# ── stc-hy-airflow ───────────────────────────────────────────────────────
recreate "$AIRFLOW_NS" keycloak-airflow \
  --from-literal=client-secret="$KC_AIRFLOW_CLIENT_SECRET"

recreate "$AIRFLOW_NS" dwh-db \
  --from-literal=host="$DWH_HOST" \
  --from-literal=dbname="$DWH_DBNAME" \
  --from-literal=user="$DWH_USER" \
  --from-literal=password="$DWH_PASSWORD"

echo "Secrets applied to $APP_NS and $AIRFLOW_NS."
