"""
imate_04b_quality_gate — second half of step 4: judge the batch, block or bless.

Numbered 04b, not 05, because the architecture diagram puts the quality gate
INSIDE Silver-2. It runs as its own DAG anyway for one reason: the gate scores
the whole typed snapshot, not the batch that just arrived, so it needs to start
after Silver-2 has finished writing rather than inside that transaction. The
file number therefore tracks the ARCHITECTURE step, and the letter records the
split — one DAG per stage was never the promise; one stage per number is.

Rules come from metadata.quality_rules, not from constants in this file. A
threshold is a business decision — somebody owns it and approves it — so it has
to be readable and changeable without a deploy.

great_expectations evaluates each rule and produces the evidence; the decision
about consequence stays here, because GX has no notion of "this failure stops
the batch and that one only costs points". See imate_quality for that split.

Scored over the WHOLE current typed snapshot, not just the batch that arrived:
the report is drawn from the whole table, so the gate must judge what the reader
will actually see. A clean batch landing on a broken table is not a pass.
"""

import json
from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from airflow.exceptions import AirflowException

from imate_assets import SILVER_TWO, VERDICT
from imate_common import TENANT_ID, imate_cursor, set_status as set_run_status
from imate_quality import evaluate, load_rules, load_snapshot, record
from imate_ops import ticket, worklist_count




@dag(
    dag_id="imate_04b_quality_gate",
    schedule=[SILVER_TWO],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_04b_quality_gate():

    @task
    def open_run(**context):
        if not worklist_count("mapped"):
            raise AirflowSkipException("Khong co dot nao cho cham diem.")
        return ticket("05", context["dag_run"].run_id)

    @task
    def judge(info):
        run_id = info["run_id"]

        rules = load_rules("imate")
        if not rules:
            raise AirflowException(
                "khong co luat nao trong metadata.quality_rules cho 'imate' — "
                "cong chat luong khong duoc phep di qua khi khong co luat")

        verdict = evaluate(load_snapshot(), rules)
        record(run_id, verdict)
        print("ket qua: " + json.dumps(verdict, ensure_ascii=False, indent=2))

        if verdict["blocked_by"]:
            set_run_status(run_id, "quality_failed",
                           quality_score=verdict["score"],
                           message=json.dumps(verdict, ensure_ascii=False))
            raise AirflowException(
                f"luat chan lo khong dat: {verdict['blocked_by']} — dung tai day")

        if verdict["scoring_failed"]:
            set_run_status(run_id, "quality_failed",
                           quality_score=verdict["score"],
                           message=json.dumps(verdict, ensure_ascii=False))
            raise AirflowException(
                f"luat cham diem khong dat: {verdict['scoring_failed']} — "
                f"diem tong {verdict['score']} khong cuu duoc mot luat rot")

        set_run_status(run_id, "quality_passed",
                       quality_score=verdict["score"],
                       message=json.dumps(verdict, ensure_ascii=False))
        return verdict

    @task(outlets=[VERDICT])
    def close_run(verdict):
        return verdict

    info = open_run()
    close_run(judge(info))


imate_04b_quality_gate()
