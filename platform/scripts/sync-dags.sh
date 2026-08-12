#!/usr/bin/env bash
# Push platform/dags/ into the ConfigMap that Airflow mounts as its DAG folder.
#
# Why a ConfigMap: the chart mounts nothing for DAGs when persistence and
# gitSync are both off, and this namespace has one PVC left in its quota. A
# ConfigMap costs no quota and needs no git server.
#
# Limits worth remembering: 1MiB total, and kubelet takes up to ~60s to
# propagate a change into running pods. Airflow then re-parses on its own
# schedule (dag_processor.min_file_process_interval, currently 120s), so allow
# up to about three minutes before a change is live. No pod restart needed.
set -euo pipefail

NS="${NS:-stc-hy-airflow}"
CM="${CM:-airflow-dags}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SIZE=$(cat "$ROOT"/dags/*.py | wc -c)
if [ "$SIZE" -gt 900000 ]; then
  echo "DAG folder is ${SIZE} bytes — too close to the 1MiB ConfigMap ceiling."
  echo "Move to gitSync or a baked image before adding more."
  exit 1
fi

# Each file is listed explicitly rather than pointing at the directory,
# because .airflowignore is a dotfile and would otherwise be left out — and
# without it Airflow refuses to walk a ConfigMap-backed DAG folder at all.
ARGS=()
for f in "$ROOT"/dags/*.py; do
  ARGS+=(--from-file="$(basename "$f")=$f")
done
ARGS+=(--from-file=".airflowignore=$ROOT/dags/.airflowignore")

kubectl -n "$NS" create configmap "$CM" \
  "${ARGS[@]}" --dry-run=client -o yaml | kubectl apply -f -

echo "Synced $(ls "$ROOT"/dags/*.py | wc -l) files (${SIZE} bytes) to $NS/$CM."
echo "Allow ~3 minutes for the scheduler to pick it up, or force it with:"
echo "  kubectl -n $NS rollout restart deploy/stc-airflow-dag-processor"
