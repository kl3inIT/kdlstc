"""
imate_01_discover — step 1 of 7: the claim.

Asks the source one question — "what is new or changed?" — and records the
answer in ingestion.doc_worklist. Nothing else: no detail calls, no bronze.
The walk itself runs in a hostNetwork pod (see imate_ops.fetch_pod) because
the workers' IP range is filtered on port 80.

Emits imate://worklist when there is work for DAG 02 — including work left
over from a previous partially-failed landing, so retries need no separate
scheduler.
"""

from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from imate_assets import WORKLIST, chain_context, publish_chain_event
from imate_ops import fetch_pod, run_message, ticket, worklist_count
from imate_common import set_status as set_run_status


@dag(
    dag_id="imate_01_discover",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_01_discover():

    @task
    def open_run(**context):
        return {
            **ticket("01", context["dag_run"].run_id),
            **chain_context(context, "01", 1),
        }

    discover = fetch_pod("discover", "discover", timeout_minutes=10)

    @task(outlets=[WORKLIST])
    def close_run(info, *, outlet_events=None):
        """
        Read what the pod wrote into the ledger and decide whether to ring
        the bell. A skipped task emits no asset event, so "nothing to do"
        keeps the six DAGs downstream asleep instead of running empty.
        """
        summary = run_message(info["run_id"])
        pending = worklist_count("discovered")
        set_run_status(info["run_id"], "parsed")
        if not pending:
            raise AirflowSkipException("Khong co gi moi va khong con viec cho.")
        result = {"pending": pending, **summary}
        publish_chain_event(outlet_events, WORKLIST, info, result)
        return result

    info = open_run()
    info >> discover >> close_run(info)


imate_01_discover()
