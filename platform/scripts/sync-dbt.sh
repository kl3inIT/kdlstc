#!/usr/bin/env bash
# Publish the dbt project as two ConfigMaps for KubernetesPodOperator jobs.
# ConfigMaps are sufficient while the project is small; move to a baked dbt
# image or git checkout before the combined payload approaches Kubernetes' 1MiB
# object limit.
set -euo pipefail

NS="${NS:-stc-hy-airflow}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBT_ROOT="$ROOT/dbt"
PROJECT_CM="${DBT_PROJECT_CM:-airflow-dbt-project}"
MODELS_CM="${DBT_MODELS_CM:-airflow-dbt-models}"

MODEL_ARGS=()
declare -A SEEN=()
SIZE=0

while IFS= read -r -d '' file; do
  name="$(basename "$file")"
  if [[ -n "${SEEN[$name]:-}" ]]; then
    echo "Duplicate dbt model filename '$name'; ConfigMap keys must be unique."
    exit 1
  fi
  SEEN[$name]=1
  MODEL_ARGS+=(--from-file="$name=$file")
  SIZE=$((SIZE + $(wc -c < "$file")))
done < <(find "$DBT_ROOT/models" -type f \( -name '*.sql' -o -name '*.yml' -o -name '*.yaml' \) -print0 | sort -z)

SIZE=$((SIZE + $(wc -c < "$DBT_ROOT/dbt_project.yml") + $(wc -c < "$DBT_ROOT/profiles.yml")))
if [ "$SIZE" -gt 900000 ]; then
  echo "dbt project is ${SIZE} bytes — too close to the 1MiB ConfigMap ceiling."
  exit 1
fi

kubectl -n "$NS" create configmap "$PROJECT_CM" \
  --from-file="dbt_project.yml=$DBT_ROOT/dbt_project.yml" \
  --from-file="profiles.yml=$DBT_ROOT/profiles.yml" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" create configmap "$MODELS_CM" \
  "${MODEL_ARGS[@]}" --dry-run=client -o yaml | kubectl apply -f -

echo "Synced dbt project (${SIZE} bytes) to $NS/$PROJECT_CM and $NS/$MODELS_CM."
