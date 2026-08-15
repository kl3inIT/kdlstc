"""
imate_02_land_bronze — step 2 of 7: evidence, byte for byte.

Wakes on imate://worklist, fetches the detail payload for every 'discovered'
document and stores it in bronze, content-addressed. Failures are
per-document: what could not be fetched stays 'discovered' with the error on
record, and the next worklist event retries it.
"""

from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from imate_assets import BRONZE, WORKLIST
from imate_ops import fetch_pod, run_message, ticket, worklist_count
from imate_common import set_status as set_run_status


@dag(
    dag_id="imate_02_land_bronze",
    schedule=[WORKLIST],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_02_land_bronze():

    @task
    def open_run(**context):
        # Check BEFORE spawning a pod: a hostNetwork pod is a privilege, and
        # starting one to discover there is nothing to do is waste with a
        # security surface attached.
        if not worklist_count("discovered"):
            raise AirflowSkipException("Khong co van ban cho tai.")
        return ticket("02", context["dag_run"].run_id)

    land = fetch_pod("land", "land", timeout_minutes=30)

    @task(outlets=[BRONZE])
    def close_run(info):
        summary = run_message(info["run_id"])
        set_run_status(info["run_id"], "parsed")
        if not summary.get("landed"):
            raise AirflowSkipException("Khong tep nao dap dat.")
        return summary

    info = open_run()
    info >> land >> close_run(info)


imate_02_land_bronze()
