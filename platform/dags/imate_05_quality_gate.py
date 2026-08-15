"""
imate_05_quality_gate — step 5 of 7: judge the batch, block or bless.

Two verdicts, deliberately never averaged into one number:

    mapping_coverage   share of documents whose kind AND issuing body resolved.
                       Low coverage is somebody's homework (a new numbering
                       convention, a missing rule) — the pipeline still runs.
    quality_score      share of documents free of structural defects (no
                       usable date). Low quality is broken data — an incident,
                       and the batch stops HERE, before anything reaches the
                       tables the report reads.

Scores are computed over the WHOLE current typed snapshot, not just this
batch: the report is drawn from the whole table, so the gate must judge what
the reader will actually see.
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
from imate_ops import ticket, worklist_count


MIN_MAPPING_COVERAGE = 0.50
MIN_QUALITY_SCORE = 0.95


@dag(
    dag_id="imate_05_quality_gate",
    schedule=[SILVER_TWO],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_05_quality_gate():

    @task
    def open_run(**context):
        if not worklist_count("mapped"):
            raise AirflowSkipException("Khong co dot nao cho cham diem.")
        return ticket("05", context["dag_run"].run_id)

    @task
    def judge(info):
        run_id = info["run_id"]
        with imate_cursor() as cur:
            cur.execute("""
                SELECT count(*),
                       count(*) FILTER (WHERE kind_mapped AND body_mapped),
                       count(*) FILTER (WHERE defect_reason IS NULL)
                  FROM staging.stg_imate__document_typed
                 WHERE tenant_id = %s
            """, (TENANT_ID,))
            total, resolved, clean = cur.fetchone()

            coverage = resolved / total if total else 0.0
            quality = clean / total if total else 0.0

            cur.execute("""
                INSERT INTO metadata.quality_exceptions
                    (run_id, table_name, rule_name, severity,
                     failed_rows, pass_rate, details)
                VALUES
                    (%s, 'stg_imate__document_typed', 'document_no_resolves',
                     'scoring', %s, %s, %s),
                    (%s, 'stg_imate__document_typed', 'uploaded_date_present',
                     'blocker', %s, %s, %s)
            """, (run_id, total - resolved, round(coverage, 4),
                  json.dumps({"threshold": MIN_MAPPING_COVERAGE}),
                  run_id, total - clean, round(quality, 4),
                  json.dumps({"threshold": MIN_QUALITY_SCORE})))

        verdict = {"total": total, "mapping_coverage": round(coverage, 4),
                   "quality_score": round(quality, 4)}

        if quality < MIN_QUALITY_SCORE:
            set_run_status(run_id, "quality_failed",
                           quality_score=round(quality, 4),
                           message=json.dumps(verdict))
            raise AirflowException(
                f"quality_score {quality:.4f} < {MIN_QUALITY_SCORE} — chan dot")
        if coverage < MIN_MAPPING_COVERAGE:
            set_run_status(run_id, "quality_failed",
                           quality_score=round(quality, 4),
                           message=json.dumps(verdict))
            raise AirflowException(
                f"mapping_coverage {coverage:.4f} < {MIN_MAPPING_COVERAGE} — chan dot")

        set_run_status(run_id, "quality_passed",
                       quality_score=round(quality, 4),
                       message=json.dumps(verdict))
        return verdict

    @task(outlets=[VERDICT])
    def close_run(verdict):
        return verdict

    info = open_run()
    close_run(judge(info))


imate_05_quality_gate()
