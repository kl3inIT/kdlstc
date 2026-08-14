#!/usr/bin/env bash
# Build and push the project Airflow image using a registry secret that
# already exists in the Airflow namespace.  The credential is copied only to a
# temporary Docker config and removed on exit.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/.." && pwd)"
NS="${NS:-stc-hy-airflow}"
REGISTRY_SECRET="${REGISTRY_SECRET:-registry-credentials}"
IMAGE="${AIRFLOW_IMAGE:-ghcr.io/kl3init/kdlstc-airflow:3.2.2-dlt1.21.0-r1}"

DOCKER_CONFIG_DIR="$(mktemp -d)"
trap 'rm -rf "$DOCKER_CONFIG_DIR"' EXIT

kubectl -n "$NS" get secret "$REGISTRY_SECRET" \
  -o jsonpath='{.data.\.dockerconfigjson}' \
  | base64 -d > "$DOCKER_CONFIG_DIR/config.json"

docker --config "$DOCKER_CONFIG_DIR" build --pull \
  --file "$ROOT/Dockerfile.airflow" \
  --tag "$IMAGE" \
  "$REPO_ROOT"
docker --config "$DOCKER_CONFIG_DIR" push "$IMAGE"

echo "Pushed $IMAGE"
