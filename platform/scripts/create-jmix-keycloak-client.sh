#!/usr/bin/env bash
set -euo pipefail

APP_NS="${APP_NS:-stc-hy}"
REALM="${KEYCLOAK_REALM:-khodl}"
CLIENT_ID="${JMIX_KEYCLOAK_CLIENT_ID:-kdlstc}"
SECRET_NAME="${JMIX_KEYCLOAK_SECRET:-kdlstc-keycloak}"
JMIX_BASE_URL="${JMIX_BASE_URL:-http://localhost:8080}"
FRONTEND_ORIGIN="${JMIX_FRONTEND_ORIGIN:-http://localhost:5174}"

KC_USER="${KC_ADMIN_USER:-$(kubectl -n "$APP_NS" get secret keycloak-admin -o jsonpath='{.data.username}' | base64 -d)}"
KC_PASSWORD="${KC_ADMIN_PASSWORD:-$(kubectl -n "$APP_NS" get secret keycloak-admin -o jsonpath='{.data.password}' | base64 -d)}"
if [ -n "${JMIX_KEYCLOAK_CLIENT_SECRET:-}" ]; then
  CLIENT_SECRET="$JMIX_KEYCLOAK_CLIENT_SECRET"
elif existing_secret="$(kubectl -n "$APP_NS" get secret "$SECRET_NAME" -o json 2>/dev/null)"; then
  CLIENT_SECRET="$(printf '%s' "$existing_secret" | python -c 'import base64,json,sys; print(base64.b64decode(json.load(sys.stdin)["data"]["client-secret"]).decode())')"
else
  CLIENT_SECRET="$(python -c 'import secrets; print(secrets.token_urlsafe(36))')"
fi

kubectl -n "$APP_NS" exec -i kc-keycloakx-0 -c keycloak -- bash -s <<EOF
set -euo pipefail
K=/opt/keycloak/bin/kcadm.sh
\$K config credentials --server http://localhost:8080 --realm master \
  --user '$KC_USER' --password '$KC_PASSWORD' >/dev/null
CID=\$(\$K get clients -r '$REALM' -q clientId='$CLIENT_ID' --fields id --format csv --noquotes | head -1)
if [ -z "\$CID" ]; then
  \$K create clients -r '$REALM' -b '{
    "clientId": "$CLIENT_ID",
    "enabled": true,
    "protocol": "openid-connect",
    "publicClient": false,
    "serviceAccountsEnabled": false,
    "standardFlowEnabled": true,
    "directAccessGrantsEnabled": false,
    "rootUrl": "$JMIX_BASE_URL",
    "redirectUris": [
      "$JMIX_BASE_URL/login/oauth2/code/keycloak",
      "$FRONTEND_ORIGIN/login/oauth2/code/keycloak"
    ],
    "webOrigins": ["$FRONTEND_ORIGIN"],
    "attributes": {"pkce.code.challenge.method": "S256"}
  }' >/dev/null
  CID=\$(\$K get clients -r '$REALM' -q clientId='$CLIENT_ID' --fields id --format csv --noquotes | head -1)
fi
\$K update clients/\$CID -r '$REALM' \
  -s enabled=true \
  -s publicClient=false \
  -s serviceAccountsEnabled=false \
  -s standardFlowEnabled=true \
  -s directAccessGrantsEnabled=false \
  -s rootUrl='$JMIX_BASE_URL' \
  -s 'redirectUris=["$JMIX_BASE_URL/login/oauth2/code/keycloak","$FRONTEND_ORIGIN/login/oauth2/code/keycloak"]' \
  -s 'webOrigins=["$FRONTEND_ORIGIN"]' \
  -s secret='$CLIENT_SECRET'
\$K update clients/\$CID -r '$REALM' \
  -s 'attributes={"pkce.code.challenge.method":"S256"}'
HAS_MAPPER=\$(\$K get clients/\$CID/protocol-mappers/models -r '$REALM' --fields name --format csv --noquotes 2>/dev/null | grep -c '^realm-roles$' || true)
if [ "\$HAS_MAPPER" = "0" ]; then
  \$K create clients/\$CID/protocol-mappers/models -r '$REALM' -b '{
    "name": "realm-roles",
    "protocol": "openid-connect",
    "protocolMapper": "oidc-usermodel-realm-role-mapper",
    "config": {
      "claim.name": "roles",
      "jsonType.label": "String",
      "multivalued": "true",
      "userinfo.token.claim": "true",
      "access.token.claim": "true",
      "id.token.claim": "true"
    }
  }' >/dev/null
fi
echo "Keycloak client $CLIENT_ID: ready"
EOF

kubectl -n "$APP_NS" create secret generic "$SECRET_NAME" \
  --from-literal=client-id="$CLIENT_ID" \
  --from-literal=client-secret="$CLIENT_SECRET" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "Secret $APP_NS/$SECRET_NAME: ready (values not printed)"
