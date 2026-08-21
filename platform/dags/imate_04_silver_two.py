"""
imate_04_silver_two — step 4 of 7: type, split, resolve, judge.

The only stage that READS Vietnamese document numbers. Three numbering
conventions live side by side in this tenant:

    A   so/[nam/]KYHIEU-COQUAN      01/TB-SCT     28/2025/QD-UBND
    B   so-KYHIEU/COQUAN[#ma]       444-CV/DU#H45.05
    C   so/COQUAN[-PHONG]           5748/UBND-NC  (cong van carries no kind)

Convention C is why "no kind token" is NOT a parse failure: per ND 30/2020 a
cong van deliberately has no kind abbreviation, so the token after the slash
is the ISSUING BODY and the kind is CV. Getting this wrong once put UBND in
the kind column and produced 186 garbage "kinds" — measured, not hypothetical.

Kinds resolve against the CLOSED statutory list; issuing bodies are an OPEN
set and auto-register with is_confirmed = false. What parses nowhere lands in
mapping_rejections with a row count — a decision somebody owes, not a row
that vanished.
"""

import json
from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from imate_assets import SILVER_ONE, SILVER_TWO
from imate_common import TENANT_ID, imate_cursor, set_status as set_run_status
from imate_ops import ticket


# Token = letters (incl. Vietnamese) and dots. Kept identical across the three
# patterns so a token means the same thing wherever it is captured.
_TOK = r"[A-Za-zĐđÂâÊêÔôƠơƯư\.]"

PAT_A = rf"^\s*(\d+)\s*/\s*(?:(\d{{4}})\s*/\s*)?({_TOK}{{1,8}})\s*-\s*(.+?)\s*$"
PAT_B = rf"^\s*(\d+)\s*-\s*({_TOK}{{1,8}})\s*/\s*([^#]+?)\s*(?:#(.*))?$"
PAT_C = rf"^\s*(\d+)\s*/\s*({_TOK}{{2,12}})(?:\s*-\s*(.+?))?\s*$"

TYPED_SQL = """
WITH src AS (
    SELECT d.*, btrim(coalesce(d.document_no, '')) AS no
      FROM staging.stg_imate__document d
     WHERE d.global_id = ANY(%(gids)s)
), parsed AS (
    SELECT s.*,
           regexp_match(s.no, %(pat_a)s) AS ma,
           regexp_match(s.no, %(pat_b)s) AS mb,
           regexp_match(s.no, %(pat_c)s) AS mc,
           CASE WHEN s.uploaded_at ~ '^\\d{4}-\\d{2}-\\d{2}T'
                THEN s.uploaded_at::timestamptz END AS up_ts,
           CASE WHEN s.updated_at ~ '^\\d{4}-\\d{2}-\\d{2}T'
                THEN s.updated_at::timestamptz END AS upd_ts
      FROM src s
), judged AS (
    SELECT p.*,
        CASE
          WHEN p.no = '' OR p.no IN ('00','0')                       THEN 'empty'
          WHEN p.ma IS NOT NULL AND EXISTS (SELECT 1 FROM refdata.document_kind k
                                             WHERE k.kind_code = upper(p.ma[3])) THEN 'A'
          WHEN p.ma IS NOT NULL                                      THEN 'A_cv'
          WHEN p.mb IS NOT NULL AND EXISTS (SELECT 1 FROM refdata.document_kind k
                                             WHERE k.kind_code = upper(p.mb[2])) THEN 'B'
          WHEN p.mb IS NOT NULL                                      THEN 'B_cv'
          WHEN p.mc IS NOT NULL                                      THEN 'C_cv'
          ELSE 'none'
        END AS pattern
      FROM parsed p
), resolved AS (
    -- Resolve kind and body ONCE, then judge them separately below. The first
    -- build computed both flags from the same `pattern IN (...)` test, which
    -- measured on real data selected the same 5.863 rows for both — one fact
    -- wearing two names. They are different questions and must be asked apart:
    -- the kind list is CLOSED by decree, the body list is OPEN and grows.
    SELECT j.*,
        CASE j.pattern WHEN 'A' THEN upper(j.ma[3]) WHEN 'B' THEN upper(j.mb[2])
                       WHEN 'A_cv' THEN 'CV' WHEN 'B_cv' THEN 'CV'
                       WHEN 'C_cv' THEN 'CV' END AS kind,
        CASE j.pattern WHEN 'A'    THEN upper(j.ma[4])
                       WHEN 'A_cv' THEN upper(j.ma[3])   -- token IS the body
                       WHEN 'B'    THEN upper(j.mb[3])
                       WHEN 'B_cv' THEN upper(j.mb[3])
                       WHEN 'C_cv' THEN upper(j.mc[2]) END AS body
      FROM judged j
)
INSERT INTO staging.stg_imate__document_typed
    (run_id, global_id, tenant_id, document_id, document_no, subject,
     uploaded_at, uploaded_date, source_updated_at, process_status,
     serial_no, issued_year, kind_code, body_code, parse_pattern,
     kind_mapped, body_mapped, routing_count, attachment_count,
     receipt_type, defect_reason)
SELECT
    %(run_id)s, j.global_id, j.tenant_id, j.document_id, j.document_no,
    j.subject, j.up_ts, j.up_ts::date, j.upd_ts, j.process_status,
    CASE j.pattern WHEN 'A' THEN j.ma[1] WHEN 'A_cv' THEN j.ma[1]
                   WHEN 'B' THEN j.mb[1] WHEN 'B_cv' THEN j.mb[1]
                   WHEN 'C_cv' THEN j.mc[1] END,
    CASE WHEN j.pattern IN ('A','A_cv') THEN j.ma[2]::int END,
    j.kind,
    j.body,
    j.pattern,
    -- kind_mapped: the resolved kind exists in the CLOSED list of Nghi dinh
    -- 30/2020. Failing means a numbering convention we have no rule for.
    EXISTS (SELECT 1 FROM refdata.document_kind k WHERE k.kind_code = j.kind),
    -- body_mapped: a body code was actually extracted. Membership is NOT
    -- required here — the body list is open and auto-registers a few statements
    -- below, so requiring it would test the order of this transaction rather
    -- than the data. Whether the name is CONFIRMED is a third question, asked
    -- at the gate against refdata.issuing_body.
    j.body IS NOT NULL AND j.body <> '',
    (SELECT count(*) FROM staging.stg_imate__routing r
      WHERE r.global_id = j.global_id),
    (SELECT count(*) FROM staging.stg_imate__attachment a
      WHERE a.global_id = j.global_id),
    (SELECT rc.document_type FROM staging.stg_imate__receipt rc
      WHERE rc.global_id = j.global_id ORDER BY rc.seq LIMIT 1),
    CASE WHEN j.up_ts IS NULL THEN 'missing_uploaded_at' END
FROM resolved j
"""


@dag(
    dag_id="imate_04_silver_two",
    schedule=[SILVER_ONE],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_04_silver_two():

    @task
    def open_run(**context):
        return ticket("04", context["dag_run"].run_id)

    @task
    def transform(info):
        run_id = info["run_id"]
        with imate_cursor() as cur:
            cur.execute(
                "SELECT global_id FROM ingestion.doc_worklist "
                "WHERE tenant_id = %s AND status = 'staged'", (TENANT_ID,))
            gids = [r[0] for r in cur.fetchall()]
            if not gids:
                raise AirflowSkipException("Khong co gi cho ep kieu.")

            # Replace, don't append: one current typed row per document.
            cur.execute("DELETE FROM staging.stg_imate__document_typed "
                        "WHERE global_id = ANY(%s)", (gids,))
            cur.execute(TYPED_SQL, {"gids": gids, "run_id": run_id,
                                    "pat_a": PAT_A, "pat_b": PAT_B,
                                    "pat_c": PAT_C})

            # Bodies are an open set: register what the data shows, named by
            # its own code until somebody confirms a real name.
            cur.execute("""
                INSERT INTO refdata.issuing_body
                       (body_code, body_name, is_confirmed, created_by)
                SELECT DISTINCT t.body_code, t.body_code, false, 'pipeline'
                  FROM staging.stg_imate__document_typed t
                 WHERE t.global_id = ANY(%s) AND t.body_code IS NOT NULL
                ON CONFLICT (body_code) DO NOTHING
            """, (gids,))

            # What parsed nowhere becomes a ticket someone owns.
            cur.execute("""
                INSERT INTO metadata.mapping_rejections
                       (run_id, source_code, code_type, source_value,
                        row_count, reason)
                SELECT %s, 'imate', 'document_no',
                       coalesce(t.document_no, '(null)'), count(*),
                       'khong tach duoc so/loai/co quan (pattern=' || t.parse_pattern || ')'
                  FROM staging.stg_imate__document_typed t
                 WHERE t.global_id = ANY(%s)
                   AND t.parse_pattern IN ('none', 'empty')
                 GROUP BY t.document_no, t.parse_pattern
            """, (run_id, gids))

            cur.execute("""
                UPDATE ingestion.doc_worklist
                   SET status = 'mapped', updated_at = now()
                 WHERE global_id = ANY(%s)
            """, (gids,))

            cur.execute("""
                SELECT count(*),
                       count(*) FILTER (WHERE kind_mapped AND body_mapped),
                       count(*) FILTER (WHERE defect_reason IS NOT NULL)
                  FROM staging.stg_imate__document_typed
                 WHERE global_id = ANY(%s)
            """, (gids,))
            total, mapped, defects = cur.fetchone()

        summary = {"typed": total, "mapped": mapped, "defects": defects}
        set_run_status(run_id, "mapped", row_count=total,
                       message=json.dumps(summary))
        return summary

    @task(outlets=[SILVER_TWO])
    def close_run(result):
        return result

    info = open_run()
    close_run(transform(info))


imate_04_silver_two()
