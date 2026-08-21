"""
imate_05_publish — step 5 of 7: the star appears.

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
from imate_ops import ticket


DIMS_SQL = """
-- Calendar: every day in range EXISTS, so an empty day shows up as a zero
-- instead of silently missing from the report.
INSERT INTO curated.dim_date
    (date_key, full_date, year, quarter, month, month_key, month_label,
     day_of_month, day_of_week, day_label, is_weekend)
SELECT to_char(d, 'YYYYMMDD')::int, d,
       extract(year from d)::int, extract(quarter from d)::int,
       extract(month from d)::int, to_char(d, 'YYYYMM')::int,
       'Tháng ' || to_char(d, 'MM/YYYY'),
       extract(day from d)::int, extract(isodow from d)::int,
       to_char(d, 'DD/MM/YYYY'), extract(isodow from d) IN (6, 7)
FROM generate_series(
        (SELECT min(uploaded_date) FROM staging.stg_imate__document_typed
          WHERE uploaded_date IS NOT NULL),
        (SELECT max(uploaded_date) FROM staging.stg_imate__document_typed
          WHERE uploaded_date IS NOT NULL),
        interval '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- Unknown members: a fact row must never be dropped for want of a dimension.
INSERT INTO curated.dim_document_kind (kind_key, kind_code, kind_name, kind_group, sort_order)
VALUES (-1, '(chua xac dinh)', 'Chưa xác định', NULL, 999)
ON CONFLICT (kind_code) DO NOTHING;
INSERT INTO curated.dim_issuing_body (body_key, body_code, body_name, body_level, is_confirmed)
VALUES (-1, '(chua xac dinh)', 'Chưa xác định', NULL, false)
ON CONFLICT (body_code) DO NOTHING;

-- New reference rows get keys above the current maximum; existing keys are
-- never renumbered — a renumbered key silently re-points every fact row.
INSERT INTO curated.dim_document_kind (kind_key, kind_code, kind_name, kind_group, sort_order)
SELECT (SELECT coalesce(max(kind_key), 0) FROM curated.dim_document_kind)
         + row_number() OVER (ORDER BY k.kind_code),
       k.kind_code, k.kind_name, k.kind_group, k.sort_order
  FROM refdata.document_kind k
 WHERE NOT EXISTS (SELECT 1 FROM curated.dim_document_kind d
                    WHERE d.kind_code = k.kind_code);

INSERT INTO curated.dim_issuing_body (body_key, body_code, body_name, body_level, is_confirmed)
SELECT (SELECT coalesce(max(body_key), 0) FROM curated.dim_issuing_body)
         + row_number() OVER (ORDER BY b.body_code),
       b.body_code, b.body_name, b.body_level, b.is_confirmed
  FROM refdata.issuing_body b
 WHERE NOT EXISTS (SELECT 1 FROM curated.dim_issuing_body d
                    WHERE d.body_code = b.body_code);

-- Names may have been confirmed since last publish — dims follow refdata.
UPDATE curated.dim_document_kind d
   SET kind_name = k.kind_name
  FROM refdata.document_kind k
 WHERE k.kind_code = d.kind_code
   AND d.kind_name IS DISTINCT FROM k.kind_name;

UPDATE curated.dim_issuing_body d
   SET body_name = b.body_name, body_level = b.body_level,
       is_confirmed = b.is_confirmed
  FROM refdata.issuing_body b
 WHERE b.body_code = d.body_code
   AND (d.body_name, coalesce(d.body_level, ''), d.is_confirmed)
       IS DISTINCT FROM (b.body_name, coalesce(b.body_level, ''), b.is_confirmed);
"""

FACT_DOC_SQL = """
INSERT INTO curated.fact_document
    (global_id, document_id, document_no, subject, date_key, kind_key,
     body_key, uploaded_at, process_status, receipt_type, routing_count,
     attachment_count, run_id, batch_id)
SELECT t.global_id, t.document_id, t.document_no, t.subject,
       to_char(t.uploaded_date, 'YYYYMMDD')::int,
       coalesce((SELECT k.kind_key FROM curated.dim_document_kind k
                  WHERE k.kind_code = t.kind_code), -1),
       coalesce((SELECT b.body_key FROM curated.dim_issuing_body b
                  WHERE b.body_code = t.body_code), -1),
       t.uploaded_at, t.process_status, t.receipt_type,
       t.routing_count, t.attachment_count, %(run_id)s, %(batch_id)s
  FROM staging.stg_imate__document_typed t
 WHERE t.global_id = ANY(%(gids)s)
"""

FACT_ROUTING_SQL = """
INSERT INTO curated.fact_routing
    (global_id, seq, date_key, sender_handle, sender_contact_id,
     receiver_handle, receiver_kind, receiver_contact_id, receiver_unit_name,
     action, role, seen_at, acted_at, run_id)
SELECT r.global_id, r.seq,
       to_char(t.uploaded_date, 'YYYYMMDD')::int,
       r.sender,
       CASE WHEN r.sender LIKE '#%%' THEN substr(r.sender, 2) END,
       r.receiver,
       CASE WHEN r.receiver LIKE '#%%' THEN 'contact'
            WHEN EXISTS (SELECT 1 FROM refdata.imate_unit u
                          WHERE u.unit_name = r.receiver) THEN 'unit'
            ELSE 'unknown' END,
       CASE WHEN r.receiver LIKE '#%%' THEN substr(r.receiver, 2) END,
       CASE WHEN r.receiver NOT LIKE '#%%' THEN r.receiver END,
       r.action, r.role,
       CASE WHEN r.seen_at  ~ '^\\d{4}-\\d{2}-\\d{2}T' THEN r.seen_at::timestamptz END,
       CASE WHEN r.acted_at ~ '^\\d{4}-\\d{2}-\\d{2}T' THEN r.acted_at::timestamptz END,
       %(run_id)s
  FROM staging.stg_imate__routing r
  JOIN staging.stg_imate__document_typed t ON t.global_id = r.global_id
 WHERE r.global_id = ANY(%(gids)s)
"""


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
    def publish(info):
        run_id = info["run_id"]
        batch_id = "b_" + run_id[2:]

        with imate_cursor() as cur:
            cur.execute("""
                SELECT t.global_id, t.defect_reason
                  FROM staging.stg_imate__document_typed t
                  JOIN ingestion.doc_worklist w ON w.global_id = t.global_id
                 WHERE w.tenant_id = %s AND w.status = 'mapped'
            """, (TENANT_ID,))
            rows = cur.fetchall()
            publishable = [g for g, d in rows if d is None]
            defective = [(g, d) for g, d in rows if d is not None]
            if not publishable:
                raise AirflowSkipException("Khong co van ban du dieu kien cong bo.")

            cur.execute(DIMS_SQL)

            # Merge-upsert by document: replace exactly what changed.
            cur.execute("DELETE FROM curated.fact_routing WHERE global_id = ANY(%s)",
                        (publishable,))
            cur.execute("DELETE FROM curated.fact_document WHERE global_id = ANY(%s)",
                        (publishable,))
            cur.execute(FACT_DOC_SQL, {"gids": publishable, "run_id": run_id,
                                       "batch_id": batch_id})
            cur.execute(FACT_ROUTING_SQL, {"gids": publishable, "run_id": run_id})

            cur.execute("""
                INSERT INTO curated.batch_summary
                    (batch_id, run_id, source_code, period, target_table,
                     row_count, publish_status, data_freshness)
                SELECT %s, %s, 'imate', %s, 'curated.fact_document', %s,
                       'approved', max(t.source_updated_at)
                  FROM staging.stg_imate__document_typed t
                 WHERE t.global_id = ANY(%s)
                ON CONFLICT (batch_id) DO NOTHING
            """, (batch_id, run_id, info["period"], len(publishable),
                  publishable))

            cur.execute("""
                UPDATE ingestion.doc_worklist
                   SET status = 'published', updated_at = now()
                 WHERE global_id = ANY(%s)
            """, (publishable,))
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
                   "batch_id": batch_id}
        set_run_status(run_id, "published", row_count=len(publishable),
                       message=json.dumps(summary))
        return summary

    @task(outlets=[CURATED])
    def close_run(result):
        return result

    info = open_run()
    close_run(publish(info))


imate_05_publish()
