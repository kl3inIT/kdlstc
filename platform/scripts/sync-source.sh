#!/usr/bin/env bash
# Push platform/sources/ into the mock source's ConfigMap and restart it.
#
# The mock runs on a stock python:3.12-slim with the script mounted in, so
# changing a simulated source is an edit plus this script — no image build, no
# registry round-trip.
set -euo pipefail

NS="${NS:-stc-hy}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

kubectl -n "$NS" create configmap mock-qlgia-src \
  --from-file=mock_qlgia.py="$ROOT/sources/mock_qlgia.py" \
  --dry-run=client -o yaml | kubectl apply -f -

# The process reads the file once at startup, so it has to be restarted —
# unlike Airflow, which re-parses DAGs on a timer.
kubectl -n "$NS" rollout restart deploy/mock-qlgia
kubectl -n "$NS" rollout status  deploy/mock-qlgia --timeout=120s
