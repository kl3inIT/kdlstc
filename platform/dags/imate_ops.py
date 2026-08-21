"""
Worker-side helpers shared by the seven iMate DAGs.

Split from imate_common on purpose: this module imports Airflow and the
Kubernetes client, which the fetch pod never needs. imate_common stays
importable with the standard library plus psycopg2.
"""

import json
import os
import re
import socket
from datetime import datetime, timezone
from urllib.parse import urlparse

from imate_common import API_ADDR, API_HOST, SOURCE_CODE, TENANT_ID, imate_cursor
from warehouse import S3_ENDPOINT

# The kubernetes client is imported INSIDE fetch_pod, not here: it costs
# several seconds, and five of the seven DAGs only need ticket() and the
# worklist counters. Importing it at module level made imate_06 blow the
# 30-second DagBag import deadline — measured, not theoretical.

# The Airflow image itself: psycopg2, boto3 and the mounted /src modules are
# all it needs, and it is already cached on every node.
FETCH_IMAGE = os.environ.get(
    "IMATE_FETCH_IMAGE", "ghcr.io/kl3init/kdlstc-airflow:3.2.2-dlt1.21.0-gx1.21.0-r1")
NAMESPACE = "stc-hy-airflow"


def ticket(stage, raw_run_id):
    """
    Open a run ticket and hand back what every task downstream needs.

    Also resolves the S3 endpoint's hostname to an IP here, on the worker:
    a hostNetwork pod uses the NODE's resolver, which knows nothing about
    cluster-internal service names. Resolving before the pod exists means the
    pod never needs cluster DNS at all.
    """
    run_id = f"r_im{stage}" + re.sub(r"[^0-9A-Za-z]", "", str(raw_run_id))[-24:]
    period = datetime.now(timezone.utc).date().isoformat()

    s3_url = urlparse(S3_ENDPOINT)
    s3_endpoint = f"http://{socket.gethostbyname(s3_url.hostname)}:{s3_url.port or 8333}"

    with imate_cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.runs (run_id, source_code, period, status, raw_path)
            VALUES (%s, %s, %s, 'received', %s)
            ON CONFLICT (run_id) DO UPDATE
               SET status = 'received', started_at = now(),
                   finished_at = NULL, message = NULL, row_count = NULL
            """,
            (run_id, SOURCE_CODE, period, f"bronze/imate/"),
        )
    return {"run_id": run_id, "period": period, "s3_endpoint": s3_endpoint}


def worklist_count(status):
    with imate_cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM ingestion.doc_worklist "
            "WHERE tenant_id = %s AND status = %s", (TENANT_ID, status)
        )
        return cur.fetchone()[0]


def run_message(run_id):
    """The summary the fetch pod wrote into the ledger, parsed."""
    with imate_cursor() as cur:
        cur.execute("SELECT message FROM ingestion.runs WHERE run_id = %s",
                    (run_id,))
        row = cur.fetchone()
    try:
        return json.loads(row[0]) if row and row[0] else {}
    except (TypeError, ValueError):
        return {}


def _secret_env(k8s, name, key):
    # The slice's own secret — the pod never sees the shared dwh-db identity.
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name="imate-db", key=key)),
    )


def fetch_pod(task_id, stage, timeout_minutes):
    """
    The pod that talks to the source. hostNetwork is the whole point: worker
    pods sit on 10.42.0.0/16 and that source range is filtered on port 80,
    while a pod borrowing the node's network namespace originates from
    10.123.123.x and passes. The privilege is confined to this pod — never
    granted to the workers, where it would bypass NetworkPolicy for every DAG.

    Code arrives the same way the DAGs do: the airflow-dags ConfigMap mounted
    read-only at /src. One delivery pipeline, no second copy to drift.
    """
    from datetime import timedelta

    from airflow.providers.cncf.kubernetes.operators.pod import (
        KubernetesPodOperator,
    )
    from kubernetes.client import models as k8s

    return KubernetesPodOperator(
        task_id=task_id,
        name=f"imate-{stage}",
        namespace=NAMESPACE,
        kubernetes_conn_id=None,
        in_cluster=True,
        image=FETCH_IMAGE,
        image_pull_policy="IfNotPresent",
        hostnetwork=True,       # KPO spells it as one word
        # A hostNetwork pod inherits the NODE's resolver, which knows nothing
        # about cluster services; this policy gives it cluster DNS back.
        dnspolicy="ClusterFirstWithHostNet",
        cmds=["python", f"/src/imate_fetch.py", stage,
              "--run-id", "{{ ti.xcom_pull(task_ids='open_run')['run_id'] }}"],
        env_vars=[
            k8s.V1EnvVar(name="PYTHONPATH", value="/src"),
            k8s.V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
            k8s.V1EnvVar(name="IMATE_API_ADDR", value=API_ADDR),
            k8s.V1EnvVar(name="IMATE_API_HOST", value=API_HOST),
            k8s.V1EnvVar(name="IMATE_TENANT_ID", value=TENANT_ID),
            k8s.V1EnvVar(
                name="S3_ENDPOINT",
                value="{{ ti.xcom_pull(task_ids='open_run')['s3_endpoint'] }}"),
            _secret_env(k8s, "IMATE_DWH_HOST", "host"),
            _secret_env(k8s, "IMATE_DWH_DBNAME", "dbname"),
            _secret_env(k8s, "IMATE_DWH_USER", "user"),
            _secret_env(k8s, "IMATE_DWH_PASSWORD", "password"),
        ],
        volumes=[k8s.V1Volume(
            name="src",
            config_map=k8s.V1ConfigMapVolumeSource(name="airflow-dags"))],
        volume_mounts=[k8s.V1VolumeMount(
            name="src", mount_path="/src", read_only=True)],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        container_security_context=k8s.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=k8s.V1Capabilities(drop=["ALL"]),
            seccomp_profile=k8s.V1SeccompProfile(type="RuntimeDefault"),
        ),
        automount_service_account_token=False,
        get_logs=True,
        log_events_on_failure=False,
        startup_timeout_seconds=180,
        execution_timeout=timedelta(minutes=timeout_minutes),
        reattach_on_restart=True,
        on_finish_action="delete_pod",
        do_xcom_push=False,
        labels={"app.kubernetes.io/name": "imate-fetch", "stc/source": SOURCE_CODE},
    )
