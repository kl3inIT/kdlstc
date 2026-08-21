"""
imate_05_publish — step 5 of 7: the star appears.

The SQL that builds the star lives in platform/dbt-imate, not in this file. dbt
buys three things the hand-written version could not have without writing them:
tests that run as part of the build, a dependency graph that decides its own
order, and lineage anyone can read.

Cutting over was verified rather than assumed — dbt built into a separate schema
first and every table matched the hand-written output exactly: 6.141 documents,
99.214 routing rows, 504 days, and the same 96.652 / 2.133 / 429 split of
resolved recipients. Zero rows differed.

Builds the three dimensions and merge-upserts the two facts. Only documents
that came through the gate AND carry no structural defect are published;
a defective document is parked as 'failed' with its reason, never silently
dropped from a total.

Load mode is merge-upsert by natural key (globalId) — the third load mode on
this platform, because an iMate document is a LIVING record: qlgia appends by
cursor, tabmis replaces by period, imate replaces by document.

fact_document and fact_routing are deliberately separate tables at separate
grains. COUNT(*) on fact_document IS the report; ~16 routing rows per
document joined in without an aggregate would multiply it by 16.
"""

import json
from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from imate_assets import CURATED, VERDICT
from imate_common import TENANT_ID, imate_cursor, set_status as set_run_status
from imate_ops import dbt_pod, ticket





@dag(
    dag_id="imate_05_publish",
    schedule=[VERDICT],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_05_publish():

    @task
    def open_run(**context):
        return ticket("05", context["dag_run"].run_id)

    @task
    def pick_batch(info):
        """Decide what is publishable before spending a pod on it."""
        with imate_cursor() as cur:
            cur.execute("""
                SELECT t.global_id, t.defect_reason
                  FROM staging.stg_imate__document_typed t
                  JOIN ingestion.doc_worklist w ON w.global_id = t.global_id
                 WHERE w.tenant_id = %s AND w.status = 'mapped'
            """, (TENANT_ID,))
            rows = cur.fetchall()

        publishable = [g for g, d in rows if d is None]
        defective = [[g, d] for g, d in rows if d is not None]
        if not publishable:
            raise AirflowSkipException("Khong co van ban du dieu kien cong bo.")
        return {**info, "publishable": publishable, "defective": defective}

    build_star = dbt_pod("build_star")

    @task
    def finish(batch):
        """
        Record the batch and move the work list forward.

        Runs after dbt, not inside it: dbt owns the shape of the warehouse, while
        the run ledger and the work list are the pipeline's own bookkeeping. A
        transformation tool has no business deciding a document is now published.
        """
        run_id = batch["run_id"]
        batch_id = "b_" + run_id[2:]
        publishable = batch["publishable"]
        defective = [tuple(x) for x in batch["defective"]]

        with imate_cursor() as cur:
            cur.execute("""
                INSERT INTO curated.batch_summary
                    (batch_id, run_id, source_code, period, target_table,
                     row_count, publish_status, data_freshness)
                SELECT %s, %s, 'imate', %s, 'curated.fact_document', %s,
                       'approved', max(t.source_updated_at)
                  FROM staging.stg_imate__document_typed t
                 WHERE t.global_id = ANY(%s)
                ON CONFLICT (batch_id) DO NOTHING
            """, (batch_id, run_id, batch["period"], len(publishable),
                  publishable))

            cur.execute("""
                UPDATE ingestion.doc_worklist
                   SET status = 'published', updated_at = now()
                 WHERE global_id = ANY(%s)
            """, (publishable,))

            # A defective document is parked with its reason, never dropped from
            # a total — the count has to stay explainable.
            if defective:
                from psycopg2.extras import execute_values
                execute_values(cur, """
                    UPDATE ingestion.doc_worklist AS w
                       SET status = 'failed', last_error = v.reason,
                           updated_at = now()
                      FROM (VALUES %s) AS v (gid, reason)
                     WHERE w.global_id = v.gid
                """, defective)

            cur.execute("SELECT count(*) FROM curated.fact_document")
            fact_docs = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM curated.fact_routing")
            fact_routings = cur.fetchone()[0]

        summary = {"published": len(publishable), "defective": len(defective),
                   "fact_document": fact_docs, "fact_routing": fact_routings,
                   "batch_id": batch_id, "engine": "dbt"}
        set_run_status(run_id, "published", row_count=len(publishable),
                       message=json.dumps(summary, ensure_ascii=False))
        return summary

    @task(outlets=[CURATED])
    def close_run(result):
        return result

    batch = pick_batch(open_run())
    batch >> build_star >> close_run(finish(batch))


imate_05_publish()
