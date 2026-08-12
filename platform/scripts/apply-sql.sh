#!/usr/bin/env bash
# Apply warehouse SQL against stc_dwh on the jmix-ha cluster.
#
# The database sits outside Kubernetes and is not reachable from a laptop, so
# the SQL is shipped in as a ConfigMap and run by a throwaway pod that already
# has psql and network line-of-sight. Credentials come from the dwh-db secret
# and never touch this repo.
#
#   ./apply-sql.sh                 run every file in sql/ in name order
#   ./apply-sql.sh 03_slice_qlgia.sql   run just one
set -euo pipefail

NS="${NS:-stc-hy-airflow}"
SECRET="${SECRET:-dwh-db}"
POD="dwh-sql-runner"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ $# -gt 0 ]; then
  FILES=("$@")
else
  FILES=()
  for f in "$ROOT"/sql/*.sql; do FILES+=("$(basename "$f")"); done
fi

echo "namespace : $NS"
echo "files     : ${FILES[*]}"

kubectl -n "$NS" create configmap dwh-sql \
  --from-file="$ROOT/sql" --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=true >/dev/null

# psql -v ON_ERROR_STOP=1 so a broken statement fails the pod instead of
# leaving the schema half-applied and the exit code green.
SCRIPT="set -e"
for f in "${FILES[@]}"; do
  SCRIPT="$SCRIPT; echo '--> $f'; psql -v ON_ERROR_STOP=1 -q -f /sql/$f"
done

kubectl -n "$NS" run "$POD" --restart=Never --image=postgres:18-alpine \
  --overrides="$(cat <<JSON
{
  "spec": {
    "restartPolicy": "Never",
    "containers": [{
      "name": "psql",
      "image": "postgres:18-alpine",
      "command": ["sh","-c","$SCRIPT"],
      "env": [
        {"name":"PGHOST",    "valueFrom":{"secretKeyRef":{"name":"$SECRET","key":"host"}}},
        {"name":"PGDATABASE","valueFrom":{"secretKeyRef":{"name":"$SECRET","key":"dbname"}}},
        {"name":"PGUSER",    "valueFrom":{"secretKeyRef":{"name":"$SECRET","key":"user"}}},
        {"name":"PGPASSWORD","valueFrom":{"secretKeyRef":{"name":"$SECRET","key":"password"}}}
      ],
      "volumeMounts": [{"name":"sql","mountPath":"/sql","readOnly":true}],
      "resources": {"requests":{"cpu":"50m","memory":"64Mi"},
                    "limits":{"cpu":"500m","memory":"256Mi"}}
    }],
    "volumes": [{"name":"sql","configMap":{"name":"dwh-sql"}}]
  }
}
JSON
)" >/dev/null

kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/"$POD" --timeout=180s \
  && STATUS=ok || STATUS=failed

kubectl -n "$NS" logs "$POD" || true
kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null

[ "$STATUS" = ok ] || { echo "SQL apply FAILED"; exit 1; }
echo "SQL applied."
