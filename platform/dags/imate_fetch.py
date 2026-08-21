"""
The part of the iMate pipeline that touches the network.

Runs inside a hostNetwork pod launched by the DAG, not in the Airflow worker —
see imate_common for why. Invoked as:

    python /src/imate_fetch.py discover --run-id r_imate01_...

It writes its results to the warehouse rather than returning them, because a
work list of 6,141 rows is not an XCom payload. That is also what makes the
seven-DAG split cheap here: the hand-off had to be a table regardless.
"""

import argparse
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from psycopg2.extras import execute_values

from imate_common import (
    DLT_VERSION,
    LIST_CONTRACT,
    S3_BUCKET,
    PAGE_SIZE,
    imate_cursor,
    MAX_PAGES,
    SOURCE_CODE,
    SourceContractError,
    DRIFT_ALERT,
    STOP_AFTER_CLEAN_PAGES,
    TENANT_CODE,
    TENANT_ID,
    api_get,
    set_status,
    check_list_contract,
    document_detail,
    list_page,
)
from imate_schema import SchemaBreakingChange, check as check_schema
from warehouse import object_store


LAND_WORKERS = 8


def log(message):
    print(message, flush=True)


def archive_list_page(s3, run_id, page, body):
    """Ghi một trang danh sách vào Bronze, khoá theo nội dung."""
    blob = json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(blob).hexdigest()
    key = f"bronze/list/{digest[:16]}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=blob,
                  ContentType="application/json",
                  Metadata={"run-id": run_id, "page": str(page)})
    return key


def discover(run_id):
    """
    Walk the document list newest-first and record what is new or changed.

    No range filter exists on this API — filter[updatedAt] matches exactly and
    a 'gte:' prefix is rejected with HTTP 400 — so there is no way to ask for
    "everything since X". What is available is ordering, and that is enough:
    sorted newest-first, every new or updated document is at the front, so the
    walk can stop once it has seen consecutive pages of nothing but documents
    it already had at the same updatedAt.

    The direction matters more than it looks. If a document is updated while
    the walk is in progress it jumps to page 1, which pushes unread rows to
    HIGHER offsets — the walk re-reads one row it already saw. Sorted oldest
    first the same event pushes unread rows to LOWER offsets and the walk steps
    straight over one. A duplicate is deduplicated by primary key; a skip is
    silent data loss.
    """
    # Compared as DATETIMES, not strings. The first build compared strings and
    # every run classified all 6,141 documents as "changed": Postgres renders
    # microseconds ('...49.473000+00:00') where the API sends milliseconds
    # ('...49.473Z'), so no string ever matched and every scan became a full
    # re-land. Parsing both sides makes the representation irrelevant.
    with imate_cursor() as cur:
        cur.execute(
            "SELECT global_id, source_updated_at FROM ingestion.doc_worklist "
            "WHERE tenant_id = %s", (TENANT_ID,)
        )
        known = dict(cur.fetchall())
    log(f"da biet: {len(known)} van ban")

    s3 = object_store()
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except Exception:                                 # noqa: BLE001
        pass                                          # already exists

    page, clean_streak = 1, 0
    drift = {"drift": "SKIPPED"}
    stop_reason = "chua-chay"
    seen, fresh, changed = 0, 0, 0
    source_total = None

    while page <= MAX_PAGES:
        body = list_page(page, sort="DESC")
        documents = body["documents"]
        source_total = body["meta"]["total"]
        check_list_contract(documents)

        # Registry check runs on the FIRST page only. One page is a fair sample
        # of the shape — every row in a response comes from the same serialiser
        # — and checking all 62 would register 62 identical versions.
        if page == 1:
            drift = check_schema("documents-list", documents, LIST_CONTRACT)
            log(f"schema: {json.dumps(drift, ensure_ascii=False)}")

        # Cất chính câu trả lời của nguồn, không chỉ cất kết luận rút ra từ nó.
        #
        # Bước này QUYẾT ĐỊNH văn bản nào vào sổ công việc, nhưng trước đây bằng
        # chứng cho quyết định ấy không được lưu ở đâu — Bronze chỉ có payload chi
        # tiết do bước 2 tải. Sáu tháng sau có người hỏi "vì sao hôm ấy hệ thống
        # cho rằng văn bản này đã thay đổi", sổ cái chỉ trả lời được "3 trang, 2
        # mới" chứ không đưa ra được dữ liệu thô đã dẫn tới con số đó.
        #
        # Khoá đặt theo mã băm nội dung như mọi thứ khác trong Bronze: ba trang y
        # hệt nhau ở chín mươi phần trăm số lượt sẽ ghi đè lên chính chúng, nên
        # gần như không tốn thêm chỗ.
        archive_list_page(s3, run_id, page, body)

        batch = []
        page_fresh = page_changed = 0
        for row in documents:
            seen += 1
            previous = known.get(row["globalId"])
            current = datetime.fromisoformat(
                row["updatedAt"].replace("Z", "+00:00"))
            if previous is None:
                page_fresh += 1
            elif previous != current:
                page_changed += 1
            else:
                continue
            batch.append((
                row["globalId"], row["tenantId"], row["documentId"],
                row["documentNo"], row["subject"], row["uploadedAt"],
                row["updatedAt"], row["processStatus"], run_id,
            ))

        if batch:
            with imate_cursor() as cur:
                execute_values(cur, """
                    INSERT INTO ingestion.doc_worklist
                        (global_id, tenant_id, document_id, document_no,
                         subject, uploaded_at, source_updated_at,
                         process_status, discover_run)
                    VALUES %s
                    ON CONFLICT (global_id) DO UPDATE SET
                        document_no       = EXCLUDED.document_no,
                        subject           = EXCLUDED.subject,
                        uploaded_at       = EXCLUDED.uploaded_at,
                        source_updated_at = EXCLUDED.source_updated_at,
                        process_status    = EXCLUDED.process_status,
                        discover_run      = EXCLUDED.discover_run,
                        -- a changed document must be fetched again, so it goes
                        -- back to the start of the pipeline rather than keeping
                        -- whatever state its previous version reached
                        status            = 'discovered',
                        last_error        = NULL,
                        updated_at        = now()
                """, batch)

        fresh += page_fresh
        changed += page_changed
        clean_streak = 0 if (page_fresh or page_changed) else clean_streak + 1
        log(f"trang {page:>3}: {len(documents):>3} dong, "
            f"moi={page_fresh:>3} doi={page_changed:>3} "
            f"(chuoi sach={clean_streak})")

        if clean_streak >= STOP_AFTER_CLEAN_PAGES:
            log(f"dung som sau {STOP_AFTER_CLEAN_PAGES} trang khong co gi moi")
            stop_reason = "chuoi-sach"
            break
        if not body["meta"]["hasNextPage"]:
            stop_reason = "het-trang"
            break
        page += 1
    else:
        # Vòng lặp chạy hết mà không break: đã chạm trần.
        #
        # Một cầu chì cháy trong im lặng còn tệ hơn không có cầu chì. Ở đây nghĩa
        # là còn trang chưa đọc — hoặc tenant lớn hơn dự tính, hoặc phép so mốc
        # thời gian hỏng khiến mọi bản ghi trông như mới. Cả hai đều là chuyện
        # phải biết ngay, không phải chuyện phát hiện qua một con số lệch ở báo
        # cáo vài tuần sau.
        stop_reason = "cham-tran"
        log(f"CANH BAO: cham tran {MAX_PAGES} trang — lot quet CHUA DAY DU")

    # The source's own total against ours. They diverge when a document is
    # deleted upstream — which this walk can never see, because a deletion
    # leaves no trace in a list you stop reading early.
    with imate_cursor() as cur:
        cur.execute("SELECT count(*) FROM ingestion.doc_worklist "
                    "WHERE tenant_id = %s", (TENANT_ID,))
        held = cur.fetchone()[0]

    drift_count = (held - source_total) if source_total is not None else None

    # Nguồn xoá bản ghi thì cách quét dừng-sớm không bao giờ thấy. Điểm mù này
    # được đo, nhưng đo mà không có ngưỡng thì con số nằm im trong sổ và không ai
    # đọc. Vượt ngưỡng là phải nói ra.
    warnings = []
    if stop_reason == "cham-tran":
        warnings.append(f"cham tran {MAX_PAGES} trang — quet chua day du")
    if drift_count is not None and abs(drift_count) > DRIFT_ALERT:
        warnings.append(
            f"lech {drift_count} ban ghi so voi nguon (nguong {DRIFT_ALERT}) — "
            "co the nguon da xoa van ban ma quet dung som khong thay")

    summary = {
        "run_id": run_id,
        "extractor": f"dlt-rest-client/{DLT_VERSION}",
        "schema_drift": drift,
        "stop_reason": stop_reason,
        "warnings": warnings,
        "pages_read": page,
        "rows_seen": seen,
        "new": fresh,
        "changed": changed,
        "worklist_total": held,
        "source_total": source_total,
        "drift": drift_count,
    }
    for warning in warnings:
        log("CANH BAO: " + warning)
    log("ket qua: " + json.dumps(summary, ensure_ascii=False))

    # Reported through the run ledger rather than XCom. The pod could push an
    # XCom, but that needs a sidecar and a shared volume, and the numbers have
    # to be in the ledger anyway — routing them through Airflow as well would
    # give two places to disagree about what this run did.
    with imate_cursor(autocommit=True) as cur:
        cur.execute(
            "UPDATE ingestion.runs SET row_count = %s, message = %s "
            "WHERE run_id = %s",
            (fresh + changed, json.dumps(summary, ensure_ascii=False), run_id),
        )
    return summary


def refresh_reference():
    """
    Pull units and contacts from the open statistics endpoints.

    These lists are FILTERED upstream — only units/people with at least one
    routing appear — so this refresh only ever adds, never deletes: a contact
    absent from today's answer may still be referenced by last year's rows.
    The full lists live behind a JWT (/api/contacts, /api/publishers); until
    one is granted this is the best obtainable coverage, measured at 79%.
    """
    units, users, page = [], [], 1
    while True:
        body = api_get(f"/api/statistics/tenants/{TENANT_ID}/units"
                       f"?page={page}&pageSize={PAGE_SIZE}")
        units += body["units"]
        if not body["meta"]["hasNextPage"]:
            break
        page += 1
    page = 1
    while True:
        body = api_get(f"/api/statistics/tenants/{TENANT_ID}/users"
                       f"?page={page}&pageSize={PAGE_SIZE}")
        users += body["users"]
        if not body["meta"]["hasNextPage"]:
            break
        page += 1

    with imate_cursor() as cur:
        execute_values(cur, """
            INSERT INTO refdata.imate_unit
                (unit_global_id, unit_id, unit_name, tenant_code)
            VALUES %s
            ON CONFLICT (unit_global_id) DO UPDATE
               SET unit_name = EXCLUDED.unit_name, loaded_at = now()
        """, [(u["globalId"], u["unitId"], u["name"], TENANT_CODE)
              for u in units])
        execute_values(cur, """
            INSERT INTO refdata.imate_contact
                (contact_global_id, contact_id, display_name, unit_name, tenant_code)
            VALUES %s
            ON CONFLICT (contact_global_id) DO UPDATE
               SET display_name = EXCLUDED.display_name,
                   unit_name    = EXCLUDED.unit_name, loaded_at = now()
        """, [(u["globalId"], u["contactId"], u["displayName"],
               u.get("unitName"), TENANT_CODE) for u in users])
    log(f"danh ba: {len(units)} unit, {len(users)} contact")


def land(run_id):
    """
    Fetch the detail payload for every 'discovered' document and store it in
    bronze byte for byte.

    The bronze key is content-addressed — imate/doc/<globalId>/<sha[:16]>.json
    — so a re-fetched identical payload lands on the same key (a harmless
    overwrite) while a genuinely changed document gets a NEW object next to
    the old one. Bronze thereby keeps every version ever seen, which is what
    makes replay-from-bronze possible later.

    Failures are per-document, not per-run: one unfetchable document keeps its
    'discovered' status with the error recorded, and the rest of the batch
    proceeds. The whole run only fails when nothing could be fetched at all —
    that means the source itself is down, and retrying the batch is the fix.
    """
    refresh_reference()

    with imate_cursor() as cur:
        cur.execute(
            "SELECT global_id FROM ingestion.doc_worklist "
            "WHERE tenant_id = %s AND status = 'discovered' "
            "ORDER BY source_updated_at", (TENANT_ID,)
        )
        pending = [r[0] for r in cur.fetchall()]
    log(f"cho tai: {len(pending)} van ban")
    if not pending:
        return

    s3 = object_store()
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except Exception:                                 # noqa: BLE001
        pass                                          # already exists

    def fetch_one(gid):
        try:
            detail = document_detail(gid)
            if detail.get("globalId") != gid:
                raise ValueError("payload globalId khong khop")
            blob = json.dumps(detail, ensure_ascii=False,
                              sort_keys=True).encode("utf-8")
            digest = hashlib.sha256(blob).hexdigest()
            key = f"bronze/doc/{gid}/{digest[:16]}.json"
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=blob,
                          ContentType="application/json")
            return (gid, key, digest, None)
        except Exception as exc:                     # noqa: BLE001
            return (gid, None, None, str(exc)[:300])

    landed, failed = [], []
    with ThreadPoolExecutor(max_workers=LAND_WORKERS) as pool:
        for n, result in enumerate(pool.map(fetch_one, pending), 1):
            (landed if result[3] is None else failed).append(result)
            if n % 500 == 0:
                log(f"tai: {n}/{len(pending)}")

    # DB writes stay on the main thread: psycopg2 connections are not shared
    # across threads, and one batched statement beats 6,000 tiny ones anyway.
    with imate_cursor() as cur:
        if landed:
            execute_values(cur, """
                UPDATE ingestion.doc_worklist AS w SET
                    status = 'landed', bronze_key = v.key,
                    content_hash = v.hash, land_run = v.run,
                    attempts = 0, last_error = NULL, updated_at = now()
                FROM (VALUES %s) AS v (gid, key, hash, run)
                WHERE w.global_id = v.gid
            """, [(g, k, h, run_id) for g, k, h, _ in landed])
        if failed:
            execute_values(cur, """
                UPDATE ingestion.doc_worklist AS w SET
                    attempts = w.attempts + 1, last_error = v.err,
                    updated_at = now()
                FROM (VALUES %s) AS v (gid, err)
                WHERE w.global_id = v.gid
            """, [(g, e) for g, _, _, e in failed])

    summary = {"run_id": run_id,
               "extractor": f"dlt-rest-client/{DLT_VERSION}",
               "pending": len(pending),
               "landed": len(landed), "failed": len(failed)}
    log("ket qua: " + json.dumps(summary, ensure_ascii=False))
    with imate_cursor(autocommit=True) as cur:
        cur.execute(
            "UPDATE ingestion.runs SET row_count = %s, message = %s "
            "WHERE run_id = %s",
            (len(landed), json.dumps(summary, ensure_ascii=False), run_id),
        )
    if pending and not landed:
        raise SystemExit("khong tai duoc van ban nao — nguon co van de")


def main():
    parser = argparse.ArgumentParser(prog="imate_fetch")
    parser.add_argument("stage", choices=["discover", "land"])
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    # A source that changed shape has to say so IN THE LEDGER, not just die.
    #
    # Before this, a broken contract raised inside the pod, the pod exited, the
    # Airflow task went red — and the run ticket stayed at 'received' forever
    # because the closing task never ran. Whoever was on duty saw one red box
    # and no reason. The status existed in the code and in the documentation
    # and was never once reachable.
    #
    # The pod writes it itself because the pod is the only thing that knows.
    try:
        if args.stage == "discover":
            discover(args.run_id)
        elif args.stage == "land":
            land(args.run_id)
    except (SourceContractError, SchemaBreakingChange) as exc:
        reason = f"{type(exc).__name__}: {exc}"
        log("NGUON DOI CAU TRUC — chan lo: " + reason)
        set_status(args.run_id, "schema_blocked",
                   message=json.dumps({"run_id": args.run_id,
                                       "stage": args.stage,
                                       "reason": reason[:2000]},
                                      ensure_ascii=False))
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
