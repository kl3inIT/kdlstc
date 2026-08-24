#!/usr/bin/env bash
set -euo pipefail

APP_NS="${APP_NS:-stc-hy}"
AIRFLOW_NS="${AIRFLOW_NS:-stc-hy-airflow}"
AIRFLOW_DEPLOYMENT="${AIRFLOW_DEPLOYMENT:-stc-airflow-api-server}"
# The live external identity keeps its historical value until credentials rotate.
USERNAME="${KDLSTC_AIRFLOW_USERNAME:-jmix-api}"
SECRET_NAME="${KDLSTC_AIRFLOW_SECRET:-jmix-airflow-api}"
BASE_URL="http://stc-airflow-api-server.${AIRFLOW_NS}.svc.cluster.local:8080"

existing_secret="$(kubectl -n "$APP_NS" get secret "$SECRET_NAME" -o json 2>/dev/null || true)"
if [ -n "$existing_secret" ]; then
  password="$(printf '%s' "$existing_secret" | python -c 'import base64,json,sys; print(base64.b64decode(json.load(sys.stdin)["data"]["password"]).decode())')"
else
  password="$(python -c 'import secrets; print(secrets.token_urlsafe(36))')"
fi

user_exists="$(kubectl -n "$AIRFLOW_NS" exec "deploy/$AIRFLOW_DEPLOYMENT" -c api-server -- \
  python -c 'import os,sys,sqlalchemy as sa; e=sa.create_engine(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]); c=e.connect(); print(c.execute(sa.text("SELECT count(*) FROM ab_user WHERE username=:u"),{"u":sys.argv[1]}).scalar()); c.close()' \
  "$USERNAME" | tail -1)"

if [ "$user_exists" = "0" ]; then
  kubectl -n "$AIRFLOW_NS" exec "deploy/$AIRFLOW_DEPLOYMENT" -c api-server -- \
    airflow users create --username "$USERNAME" --password "$password" \
      --firstname KDLSTC --lastname API --role Op --email kdlstc-api@internal.invalid >/dev/null
elif [ -z "$existing_secret" ]; then
  echo "Airflow user $USERNAME exists but Secret $APP_NS/$SECRET_NAME is missing; refusing to rotate it implicitly." >&2
  exit 1
fi

kubectl -n "$APP_NS" create secret generic "$SECRET_NAME" \
  --from-literal=username="$USERNAME" \
  --from-literal=password="$password" \
  --from-literal=base-url="$BASE_URL" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

payload="$(KDLSTC_USER="$USERNAME" KDLSTC_PASSWORD="$password" python -c 'import json,os; print(json.dumps({"username":os.environ["KDLSTC_USER"],"password":os.environ["KDLSTC_PASSWORD"]}))')"
printf '%s' "$payload" | kubectl -n "$AIRFLOW_NS" exec -i "deploy/$AIRFLOW_DEPLOYMENT" -c api-server -- \
  python -c 'import json,requests,sys; p=json.load(sys.stdin); r=requests.post("http://localhost:8080/auth/token",json=p,timeout=10); r.raise_for_status(); t=r.json()["access_token"]; q=requests.get("http://localhost:8080/api/v2/dags/imate_01_discover/dagRuns?limit=1",headers={"Authorization":"Bearer "+t},timeout=10); q.raise_for_status(); print("Airflow service identity: token OK, DAG API OK")'

echo "Secret $APP_NS/$SECRET_NAME: ready (values not printed)"
