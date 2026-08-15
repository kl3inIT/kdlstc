"""
imate_03_silver_one — step 3 of 7: flatten, verbatim.

Wakes on imate://bronze, opens each landed JSON and lays it out as four text
tables. Nothing is cast, trimmed or judged here — the payload only changes
SHAPE. A value that fails three stages later can therefore be quoted back
exactly as the source sent it, with the row it came from.

Runs on the worker: from here on the pipeline touches only S3 and the
warehouse, so no pod and no special network privileges.
"""

import json
from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from psycopg2.extras import execute_values

from imate_assets import BRONZE, SILVER_ONE
from imate_common import S3_BUCKET, TENANT_ID, imate_cursor, set_status as set_run_status
from imate_ops import ticket
from warehouse import object_store


CHUNK = 200          # documents per transaction — a crash loses one chunk


def _s(value):
    """Verbatim text or NULL — silver-1's entire type system."""
    return None if value is None else str(value)


@dag(
    dag_id="imate_03_silver_one",
    schedule=[BRONZE],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_03_silver_one():

    @task
    def open_run(**context):
        return ticket("03", context["dag_run"].run_id)

    @task
    def load(info):
        run_id = info["run_id"]
        with imate_cursor() as cur:
            cur.execute(
                "SELECT global_id, bronze_key FROM ingestion.doc_worklist "
                "WHERE tenant_id = %s AND status = 'landed' "
                "ORDER BY source_updated_at", (TENANT_ID,)
            )
            todo = cur.fetchall()
        if not todo:
            raise AirflowSkipException("Khong co van ban cho trai phang.")

        s3 = object_store()
        staged = failed = 0

        for start in range(0, len(todo), CHUNK):
            chunk = todo[start:start + CHUNK]
            docs, routings, receipts, attachments = [], [], [], []
            ok_ids, bad = [], []

            for gid, key in chunk:
                try:
                    doc = json.loads(
                        s3.get_object(Bucket=S3_BUCKET, Key=key)
                        ["Body"].read().decode("utf-8"))
                    docs.append((
                        run_id, gid, _s(doc.get("tenantId")),
                        _s(doc.get("documentId")), _s(doc.get("version")),
                        _s(doc.get("documentNo")), _s(doc.get("subject")),
                        _s(doc.get("uploadedAt")), _s(doc.get("createdAt")),
                        _s(doc.get("updatedAt")), _s(doc.get("processStatus")),
                        key,
                    ))
                    for seq, r in enumerate(doc.get("routings") or []):
                        routings.append((
                            run_id, gid, seq, _s(r.get("sender")),
                            _s(r.get("receiver")), _s(r.get("action")),
                            _s(r.get("role")), _s(r.get("receivedAt")),
                            _s(r.get("seenAt")), _s(r.get("actedAt")),
                        ))
                    for seq, r in enumerate(doc.get("receipts") or []):
                        receipts.append((
                            run_id, gid, seq, _s(r.get("role")),
                            _s(r.get("documentType")),
                            _s(r.get("documentStatus")), _s(r.get("reachedAt")),
                        ))
                    for seq, a in enumerate(doc.get("attachments") or []):
                        attachments.append((
                            run_id, gid, seq, _s(a.get("name")),
                            _s(a.get("raw")), _s(a.get("contentHash")),
                            _s(a.get("isMain")), _s(a.get("isUserGenerated")),
                            _s(len(a.get("renders") or [])),
                            _s(a.get("scanned") is not None),
                        ))
                    ok_ids.append(gid)
                except Exception as exc:              # noqa: BLE001
                    bad.append((gid, str(exc)[:300]))

            with imate_cursor() as cur:
                if ok_ids:
                    # One current version per document: replace, don't append.
                    for table in ("stg_imate__document", "stg_imate__routing",
                                  "stg_imate__receipt", "stg_imate__attachment"):
                        cur.execute(
                            f"DELETE FROM staging.{table} WHERE global_id = ANY(%s)",
                            (ok_ids,))
                    execute_values(cur, """
                        INSERT INTO staging.stg_imate__document
                            (run_id, global_id, tenant_id, document_id, version,
                             document_no, subject, uploaded_at, created_at,
                             updated_at, process_status, bronze_key)
                        VALUES %s""", docs)
                    if routings:
                        execute_values(cur, """
                            INSERT INTO staging.stg_imate__routing
                                (run_id, global_id, seq, sender, receiver, action,
                                 role, received_at, seen_at, acted_at)
                            VALUES %s""", routings)
                    if receipts:
                        execute_values(cur, """
                            INSERT INTO staging.stg_imate__receipt
                                (run_id, global_id, seq, role, document_type,
                                 document_status, reached_at)
                            VALUES %s""", receipts)
                    if attachments:
                        execute_values(cur, """
                            INSERT INTO staging.stg_imate__attachment
                                (run_id, global_id, seq, name, raw_url,
                                 content_hash, is_main, is_user_generated,
                                 render_count, has_scanned)
                            VALUES %s""", attachments)
                    execute_values(cur, """
                        UPDATE ingestion.doc_worklist AS w
                           SET status = 'staged', updated_at = now()
                          FROM (VALUES %s) AS v (gid)
                         WHERE w.global_id = v.gid
                    """, [(g,) for g in ok_ids])
                if bad:
                    execute_values(cur, """
                        UPDATE ingestion.doc_worklist AS w
                           SET status = 'failed', last_error = v.err,
                               updated_at = now()
                          FROM (VALUES %s) AS v (gid, err)
                         WHERE w.global_id = v.gid
                    """, bad)
            staged += len(ok_ids)
            failed += len(bad)
            print(f"trai phang: {staged + failed}/{len(todo)}", flush=True)

        set_run_status(info["run_id"], "parsed", row_count=staged,
                       message=json.dumps({"staged": staged, "failed": failed}))
        return {"staged": staged, "failed": failed}

    @task(outlets=[SILVER_ONE])
    def close_run(result):
        if not result["staged"]:
            raise AirflowSkipException("Khong dong nao sang silver-1.")
        return result

    info = open_run()
    close_run(load(info))


imate_03_silver_one()
