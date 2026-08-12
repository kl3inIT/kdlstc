#!/usr/bin/env bash
# Generate TABMIS workbooks and drop them in the intake bucket.
#
# Runs inside the cluster because that is where both the warehouse (for the
# reference codes) and the bucket are reachable. A throwaway pod installs the
# three libraries it needs; nothing is added to the Airflow image for what is
# only ever a seeding step.
#
#   ./seed-tabmis.sh              every period
#   ./seed-tabmis.sh 2026-03      one period
set -euo pipefail

NS="${NS:-stc-hy-airflow}"
SECRET="${SECRET:-dwh-db}"
POD="tabmis-seeder"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARGS="$*"

kubectl -n "$NS" create configmap tabmis-gen \
  --from-file=gen_tabmis.py="$ROOT/sources/gen_tabmis.py" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=true >/dev/null

kubectl -n "$NS" run "$POD" --restart=Never --image=python:3.12-slim \
  --overrides="$(cat <<JSON
{
  "spec": {
    "restartPolicy": "Never",
    "containers": [{
      "name": "seeder",
      "image": "python:3.12-slim",
      "command": ["sh","-c","pip install --no-cache-dir -q openpyxl boto3 psycopg2-binary && python /gen/gen_tabmis.py $ARGS"],
      "env": [
        {"name":"DWH_HOST",   "valueFrom":{"secretKeyRef":{"name":"$SECRET","key":"host"}}},
        {"name":"DWH_DBNAME", "valueFrom":{"secretKeyRef":{"name":"$SECRET","key":"dbname"}}},
        {"name":"DWH_USER",   "valueFrom":{"secretKeyRef":{"name":"$SECRET","key":"user"}}},
        {"name":"DWH_PASSWORD","valueFrom":{"secretKeyRef":{"name":"$SECRET","key":"password"}}},
        {"name":"S3_ENDPOINT","value":"http://sw-seaweedfs-s3.stc-hy.svc.cluster.local:8333"},
        {"name":"PYTHONUNBUFFERED","value":"1"}
      ],
      "volumeMounts": [{"name":"gen","mountPath":"/gen","readOnly":true}],
      "resources": {"requests":{"cpu":"200m","memory":"256Mi"},
                    "limits":{"cpu":"1","memory":"1Gi"}}
    }],
    "volumes": [{"name":"gen","configMap":{"name":"tabmis-gen"}}]
  }
}
JSON
)" >/dev/null

echo "dang sinh tep..."
kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/"$POD" --timeout=600s \
  && STATUS=ok || STATUS=failed

kubectl -n "$NS" logs "$POD" || true
kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null

[ "$STATUS" = ok ] || { echo "SEED THAT BAI"; exit 1; }
