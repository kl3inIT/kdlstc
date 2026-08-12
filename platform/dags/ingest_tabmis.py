"""
ingest_tabmis — second vertical slice: budget execution submitted as Excel.

Same layers as ingest_qlgia, three structural differences that come from the
source being a person with a file rather than a service with an endpoint:

  There is no cursor. A submission is the whole truth for its month, so a
  period is REPLACED wholesale. Upserting would leave behind rows the corrected
  file no longer contains — the way a restated month keeps a ghost.

  Bronze stores the workbook byte for byte, not a parsed rendering of it. When
  somebody disputes a figure six months from now, the artefact to open is the
  file they actually sent.

  The batch owes the submitter an answer. A report naming the failing rows is
  written back beside the uploaded file, and it is written whether the batch
  passed or failed — a rejected file with no explanation is worse than no
  pipeline at all.

One DAG run handles one file. That keeps the run ledger honest (one ticket,
one batch, one period) and lets a bad submission fail without stopping the
queue behind it.
"""

import hashlib
import io
import json
import re
from datetime import datetime, timedelta, timezone

try:                                    # Airflow 3
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:                     # Airflow 2 fallback
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from airflow.exceptions import AirflowException

from psycopg2.extras import Json, execute_values

from warehouse import object_store, set_run_status, warehouse_cursor, write_audit

SOURCE_CODE = "tabmis"
INTAKE_BUCKET = "intake"
BRONZE_BUCKET = "bronze"
FISCAL_YEAR = 2026

MIN_QUALITY_SCORE = 0.95
MIN_MAPPING_COVERAGE = 0.50

# Where the header row actually is cannot be assumed: the export puts a merged
# title block above it. The header is found by looking for this column.
HEADER_ANCHOR = "ma dvqhns"

# Worksheet column heading -> staging column.
COLUMN_MAP = {
    "ky": "period",
    "ma dvqhns": "unit_code",
    "ten don vi": "unit_name",
    "ma dia ban": "locality_code",
    "chuong": "chapter_code",
    "loai": "category_code",
    "khoan": "subcategory_code",
    "muc": "item_code",
    "tieu muc": "line_code",
    "ma nguon kp": "funding_code",
    "ma linh vuc chi": "sector_code",
    "du toan giao": "allocated_amount",
    "dieu chinh": "adjusted_amount",
    "thuc chi": "executed_amount",
    "tam ung": "advance_amount",
    "luy ke": "executed_ytd",
}

STRUCTURAL_REASONS = ("subtotal_row",)
MAPPING_REASONS = ("unit_unknown", "line_unknown", "funding_unknown")


def _norm(text):
    """Fold a heading down to something matchable: lowercase, single spaces."""
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


# =========================================================================
# Silver-2. Amount parsing is the interesting part: a cell that reads
# "1.234.567" to a human is a string to a database, and casting it naively
# gives 1.234 — wrong by a factor of a million, silently.
# =========================================================================
BUILD_TYPED_SQL = r"""
INSERT INTO staging.stg_tabmis__budget_typed (
    run_id, file_id, row_number, period, fiscal_year,
    unit_code, locality_code, line_code, funding_code, sector_code,
    allocated_amount, adjusted_amount, executed_amount, advance_amount,
    executed_ytd, raw_path, is_valid, reject_reason, reject_detail
)
WITH money AS (
    SELECT
        s.*,
        -- one CASE per amount column, kept explicit rather than clever: the
        -- Vietnamese convention is dot-as-thousand-separator, so 1.234.567 is
        -- one million two hundred thousand, NOT one point two three four.
        CASE WHEN s.allocated_amount ~ '^-?[0-9]+$'
               THEN s.allocated_amount::numeric
             WHEN s.allocated_amount ~ '^-?[0-9]{1,3}(\.[0-9]{3})+$'
               THEN replace(s.allocated_amount, '.', '')::numeric
             WHEN s.allocated_amount IS NULL OR btrim(s.allocated_amount) = ''
               THEN 0 END                                     AS m_allocated,
        CASE WHEN s.adjusted_amount ~ '^-?[0-9]+$'
               THEN s.adjusted_amount::numeric
             WHEN s.adjusted_amount ~ '^-?[0-9]{1,3}(\.[0-9]{3})+$'
               THEN replace(s.adjusted_amount, '.', '')::numeric
             WHEN s.adjusted_amount IS NULL OR btrim(s.adjusted_amount) = ''
               THEN 0 END                                     AS m_adjusted,
        CASE WHEN s.executed_amount ~ '^-?[0-9]+$'
               THEN s.executed_amount::numeric
             WHEN s.executed_amount ~ '^-?[0-9]{1,3}(\.[0-9]{3})+$'
               THEN replace(s.executed_amount, '.', '')::numeric
             WHEN s.executed_amount IS NULL OR btrim(s.executed_amount) = ''
               THEN 0 END                                     AS m_executed,
        CASE WHEN s.advance_amount ~ '^-?[0-9]+$'
               THEN s.advance_amount::numeric
             WHEN s.advance_amount ~ '^-?[0-9]{1,3}(\.[0-9]{3})+$'
               THEN replace(s.advance_amount, '.', '')::numeric
             WHEN s.advance_amount IS NULL OR btrim(s.advance_amount) = ''
               THEN 0 END                                     AS m_advance,
        CASE WHEN s.executed_ytd ~ '^-?[0-9]+$'
               THEN s.executed_ytd::numeric
             WHEN s.executed_ytd ~ '^-?[0-9]{1,3}(\.[0-9]{3})+$'
               THEN replace(s.executed_ytd, '.', '')::numeric
             WHEN s.executed_ytd IS NULL OR btrim(s.executed_ytd) = ''
               THEN 0 END                                     AS m_ytd
    FROM staging.stg_tabmis__budget s
    WHERE s.run_id = %(run_id)s
),
resolved AS (
    SELECT
        m.*,
        u.unit_code IS NOT NULL AS unit_ok,
        b.line_code IS NOT NULL AS line_ok,
        f.funding_code IS NOT NULL AS funding_ok,
        prev.executed_ytd AS prev_ytd
    FROM money m
    LEFT JOIN refdata.budget_unit u    ON u.unit_code = m.unit_code
    LEFT JOIN refdata.budget_line b    ON b.line_code = m.line_code
                                      AND b.fiscal_year = %(fiscal_year)s
    LEFT JOIN refdata.funding_source f ON f.funding_code = m.funding_code
    LEFT JOIN LATERAL (
        SELECT fb.executed_ytd
        FROM curated.fact_budget fb
        WHERE fb.unit_code   = m.unit_code
          AND fb.line_code   = m.line_code
          AND fb.funding_code = m.funding_code
          AND fb.period < m.period
        ORDER BY fb.period DESC
        LIMIT 1
    ) prev ON TRUE
),
judged AS (
    SELECT
        r.*,
        CASE
            -- Structural first. A subtotal is not bad data, it is data at the
            -- wrong grain; charging it to the quality score would bake in a
            -- permanent false failure, since every file has them.
            WHEN r.unit_name ILIKE 'Cong:%%'
              OR r.unit_name ILIKE 'TONG CONG%%'
              OR btrim(coalesce(r.line_code, '')) = ''  THEN 'subtotal_row'
            WHEN r.period IS DISTINCT FROM %(period)s    THEN 'period_mismatch'
            WHEN r.m_allocated IS NULL
              OR r.m_adjusted  IS NULL
              OR r.m_executed  IS NULL
              OR r.m_advance   IS NULL
              OR r.m_ytd       IS NULL                   THEN 'amount_unparseable'
            WHEN r.m_allocated < 0                       THEN 'amount_negative'
            WHEN NOT r.unit_ok                           THEN 'unit_unknown'
            WHEN NOT r.line_ok                           THEN 'line_unknown'
            WHEN NOT r.funding_ok                        THEN 'funding_unknown'
            WHEN r.prev_ytd IS NOT NULL
             AND r.m_ytd < r.prev_ytd                    THEN 'ytd_regression'
        END AS reason
    FROM resolved r
)
SELECT
    run_id, file_id, row_number, period, %(fiscal_year)s,
    unit_code, locality_code, line_code, funding_code, sector_code,
    m_allocated, m_adjusted, m_executed, m_advance, m_ytd,
    raw_path,
    reason IS NULL,
    reason,
    -- the sentence quoted back to the submitter, with the offending value
    CASE reason
        WHEN 'amount_unparseable' THEN
            'so tien khong doc duoc: du toan="' || coalesce(allocated_amount,'') ||
            '", thuc chi="' || coalesce(executed_amount,'') ||
            '", luy ke="' || coalesce(executed_ytd,'') || '"'
        WHEN 'unit_unknown'    THEN 'ma DVQHNS "' || coalesce(unit_code,'') || '" khong co trong danh muc don vi'
        WHEN 'line_unknown'    THEN 'ma tieu muc "' || coalesce(line_code,'') || '" khong co trong muc luc NSNN nam ' || %(fiscal_year)s
        WHEN 'funding_unknown' THEN 'ma nguon kinh phi "' || coalesce(funding_code,'') || '" khong co trong danh muc'
        WHEN 'amount_negative' THEN 'du toan giao am: ' || m_allocated::text
        WHEN 'period_mismatch' THEN 'ky trong dong ("' || coalesce(period,'') || '") khac ky cua tep (' || %(period)s || ')'
        WHEN 'ytd_regression'  THEN 'luy ke ' || m_ytd::text || ' nho hon ky truoc ' || prev_ytd::text
        ELSE NULL
    END
FROM judged;
"""


@dag(
    dag_id="ingest_tabmis",
    description="TABMIS Excel submissions -> Bronze -> Silver -> quality gate -> Gold",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["warehouse", "bronze", "tabmis", "file-intake"],
    doc_md=__doc__,
)
def ingest_tabmis():

    # ---------------------------------------------------------------------
    @task
    def claim_file(**context) -> dict:
        """
        Take the oldest unprocessed workbook and open a ticket for it.

        Files are identified by content checksum, not by name: a submitter who
        renames and re-uploads the same bytes has not submitted anything new,
        and should be told so rather than silently reprocessed.
        """
        dag_run = context["dag_run"]
        run_id = "r_" + re.sub(r"[^0-9A-Za-z]", "", str(dag_run.run_id))[:40]
        s3 = object_store()

        pages = s3.get_paginator("list_objects_v2")
        candidates = []
        for page in pages.paginate(Bucket=INTAKE_BUCKET, Prefix="tabmis/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.lower().endswith(".xlsx"):
                    continue
                if "/da-xu-ly/" in key:
                    continue
                candidates.append((obj["LastModified"], key, obj["Size"]))

        if not candidates:
            raise AirflowSkipException("Khong co tep moi trong intake/tabmis/")

        candidates.sort()
        with warehouse_cursor() as cur:
            # Only a file that REACHED A VERDICT is finished with.
            #
            #   accepted / superseded — its numbers are in the warehouse
            #   rejected              — the submitter has been told to fix and resend,
            #                           so re-reading the same bytes would just loop
            #
            # A row still sitting at 'received' means the batch died mid-flight.
            # Those must stay claimable, or a transient failure quietly retires
            # the file forever — the pipeline would look idle while a month of
            # data silently never lands.
            cur.execute(
                "SELECT checksum_sha256 FROM ingestion.intake_files "
                "WHERE source_code = %s AND status <> 'received'", (SOURCE_CODE,)
            )
            seen = {r[0] for r in cur.fetchall()}

        chosen = None
        for _, key, size in candidates:
            body = s3.get_object(Bucket=INTAKE_BUCKET, Key=key)["Body"].read()
            checksum = hashlib.sha256(body).hexdigest()
            if checksum not in seen:
                chosen = (key, size, checksum)
                break

        if chosen is None:
            raise AirflowSkipException("Moi tep trong intake da duoc xu ly.")

        key, size, checksum = chosen
        period = key.split("/")[1] if len(key.split("/")) > 2 else None
        file_id = "f_" + checksum[:16]
        raw_prefix = f"{BRONZE_BUCKET}/{SOURCE_CODE}/{period}/{run_id}/"

        with warehouse_cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion.runs
                    (run_id, source_code, period, status, raw_path)
                VALUES (%s, %s, %s, 'received', %s)
                ON CONFLICT (run_id) DO UPDATE
                   SET status = 'received', started_at = now(),
                       finished_at = NULL, message = NULL,
                       row_count = NULL, quality_score = NULL
                """,
                (run_id, SOURCE_CODE, period, raw_prefix),
            )
            cur.execute(
                """
                INSERT INTO ingestion.intake_files
                    (file_id, source_code, object_key, original_name, period,
                     checksum_sha256, size_bytes, run_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'received')
                ON CONFLICT (checksum_sha256) DO UPDATE
                   SET run_id = EXCLUDED.run_id, status = 'received'
                """,
                (file_id, SOURCE_CODE, key, key.split("/")[-1], period,
                 checksum, size, run_id),
            )
            for stmt in (
                "DELETE FROM staging.stg_tabmis__budget       WHERE run_id = %s",
                "DELETE FROM staging.stg_tabmis__budget_typed WHERE run_id = %s",
                "DELETE FROM metadata.quarantine_rows         WHERE run_id = %s",
                "DELETE FROM metadata.quality_exceptions      WHERE run_id = %s",
                "DELETE FROM metadata.mapping_rejections      WHERE run_id = %s",
            ):
                cur.execute(stmt, (run_id,))

        write_audit("airflow", "file_claimed", run_id,
                    {"key": key, "checksum": checksum, "period": period})
        return {"run_id": run_id, "file_id": file_id, "key": key,
                "period": period, "checksum": checksum, "size": size,
                "raw_prefix": raw_prefix}

    # ---------------------------------------------------------------------
    @task
    def land_in_bronze(ticket: dict) -> dict:
        """
        Copy the workbook into Bronze untouched, with a manifest beside it.

        Not a parsed rendering — the bytes. Six months from now the question
        will be "what exactly did they send us", and only the original answers.
        """
        s3 = object_store()
        body = s3.get_object(Bucket=INTAKE_BUCKET, Key=ticket["key"])["Body"].read()

        prefix = f"{SOURCE_CODE}/{ticket['period']}/{ticket['run_id']}/"
        name = ticket["key"].split("/")[-1]
        s3.put_object(
            Bucket=BRONZE_BUCKET, Key=prefix + name, Body=body,
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        manifest = {
            "run_id": ticket["run_id"],
            "source_code": SOURCE_CODE,
            "period": ticket["period"],
            "original_key": ticket["key"],
            "original_name": name,
            "checksum_sha256": ticket["checksum"],
            "size_bytes": ticket["size"],
            "landed_at": datetime.now(timezone.utc).isoformat(),
        }
        s3.put_object(
            Bucket=BRONZE_BUCKET, Key=prefix + "_manifest.json",
            Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        set_run_status(ticket["run_id"], "received",
                       checksum_sha256=ticket["checksum"])
        return {"bronze_key": f"{BRONZE_BUCKET}/{prefix}{name}"}

    # ---------------------------------------------------------------------
    @task
    def load_silver_one(ticket: dict, bronze: dict) -> dict:
        """
        Parse the workbook out of Bronze into Silver-1, verbatim.

        openpyxl is imported here rather than at module level so the DAG file
        still parses on the dag-processor, which does not carry the library.

        Two things this step must not assume: that the header is row 1 (a
        merged title block sits above it), and that a cell holding a date is a
        date — Excel stores 2026-03-28 as the number 46109.
        """
        from openpyxl import load_workbook   # noqa: PLC0415 — see docstring

        run_id = ticket["run_id"]
        s3 = object_store()
        bucket, key = bronze["bronze_key"].split("/", 1)
        blob = s3.get_object(Bucket=bucket, Key=key)["Body"].read()

        wb = load_workbook(io.BytesIO(blob), data_only=True, read_only=True)
        ws = wb[wb.sheetnames[0]]

        header_row, headers = None, None
        for idx, row in enumerate(ws.iter_rows(min_row=1, max_row=30,
                                               values_only=True), start=1):
            normalised = [_norm(c) for c in row]
            if HEADER_ANCHOR in normalised:
                header_row, headers = idx, normalised
                break
        if header_row is None:
            set_run_status(run_id, "schema_blocked",
                           message=f"khong tim thay dong tieu de chua '{HEADER_ANCHOR}'")
            raise AirflowException(
                "Khong nhan ra bang: thieu cot 'Ma DVQHNS' trong 30 dong dau."
            )

        position = {}
        for col_idx, name in enumerate(headers):
            target = COLUMN_MAP.get(name)
            if target and target not in position:
                position[target] = col_idx

        missing = sorted(set(COLUMN_MAP.values()) - set(position))
        if missing:
            set_run_status(run_id, "schema_blocked",
                           message="thieu cot: " + ", ".join(missing))
            raise AirflowException("Bieu thieu cot bat buoc: " + ", ".join(missing))

        columns = ["run_id", "file_id", "row_number"] + list(COLUMN_MAP.values()) + ["raw_path"]
        rows = []
        for offset, values in enumerate(
                ws.iter_rows(min_row=header_row + 1, values_only=True), start=1):
            if values is None or all(v is None or str(v).strip() == "" for v in values):
                continue                      # spacer rows between groups
            record = [run_id, ticket["file_id"], header_row + offset]
            for target in COLUMN_MAP.values():
                idx = position[target]
                cell = values[idx] if idx < len(values) else None
                if isinstance(cell, datetime):
                    cell = cell.date().isoformat()
                record.append(None if cell is None else str(cell).strip())
            record.append(bronze["bronze_key"])
            rows.append(tuple(record))

        wb.close()

        with warehouse_cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO staging.stg_tabmis__budget ({', '.join(columns)}) VALUES %s",
                rows, page_size=1000,
            )
            cur.execute(
                "UPDATE ingestion.intake_files SET row_count = %s WHERE file_id = %s",
                (len(rows), ticket["file_id"]),
            )

        set_run_status(run_id, "parsed", row_count=len(rows))
        return {"header_row": header_row, "staged_rows": len(rows)}

    # ---------------------------------------------------------------------
    @task
    def build_silver_two(ticket: dict, staged: dict) -> dict:
        """Type, resolve against reference data, and judge every worksheet row."""
        run_id = ticket["run_id"]

        with warehouse_cursor() as cur:
            cur.execute(BUILD_TYPED_SQL, {
                "run_id": run_id,
                "fiscal_year": FISCAL_YEAR,
                "period": ticket["period"],
            })
            cur.execute(
                """
                SELECT COALESCE(reject_reason, '__valid__'), COUNT(*)
                FROM staging.stg_tabmis__budget_typed
                WHERE run_id = %s GROUP BY 1
                """, (run_id,),
            )
            tally = dict(cur.fetchall())

            cur.execute(
                """
                INSERT INTO metadata.mapping_rejections
                    (run_id, source_code, code_type, source_value,
                     row_count, reason, assignee, due_date)
                SELECT %(run_id)s, %(source_code)s,
                       CASE reject_reason WHEN 'unit_unknown' THEN 'budget_unit'
                                          WHEN 'line_unknown' THEN 'budget_line'
                                          ELSE 'funding_source' END,
                       CASE reject_reason WHEN 'unit_unknown' THEN unit_code
                                          WHEN 'line_unknown' THEN line_code
                                          ELSE funding_code END,
                       COUNT(*),
                       'ma khong co trong danh muc',
                       'Phong QLNS',
                       CURRENT_DATE + 7
                FROM staging.stg_tabmis__budget_typed
                WHERE run_id = %(run_id)s
                  AND reject_reason IN ('unit_unknown','line_unknown','funding_unknown')
                GROUP BY 2, 3, 4
                """,
                {"run_id": run_id, "source_code": SOURCE_CODE},
            )
            queued = cur.rowcount

        set_run_status(run_id, "mapped")
        return {"tally": tally, "mapping_questions_queued": queued}

    # ---------------------------------------------------------------------
    @task
    def quality_gate(ticket: dict, silver_two: dict) -> dict:
        """Same two measures as the other slice, for the same reasons."""
        run_id = ticket["run_id"]
        tally = silver_two["tally"]

        total = sum(tally.values())
        valid = tally.get("__valid__", 0)
        structural = sum(tally.get(r, 0) for r in STRUCTURAL_REASONS)
        unmapped = sum(tally.get(r, 0) for r in MAPPING_REASONS)
        business = total - structural
        eligible = business - unmapped
        defective = eligible - valid

        mapping_coverage = (eligible / business) if business else 0.0
        quality_score = (valid / eligible) if eligible else 0.0

        with warehouse_cursor() as cur:
            for reason, count in sorted(tally.items()):
                if reason == "__valid__":
                    continue
                denominator = business if reason in MAPPING_REASONS else eligible
                cur.execute(
                    """
                    INSERT INTO metadata.quality_exceptions
                        (run_id, table_name, rule_name, severity,
                         failed_rows, pass_rate, details)
                    VALUES (%s, %s, %s, 'scoring', %s, %s, %s)
                    """,
                    (run_id, "staging.stg_tabmis__budget_typed", reason, count,
                     round(1 - count / denominator, 4) if denominator else None,
                     Json({"structural": reason in STRUCTURAL_REASONS,
                           "mapping": reason in MAPPING_REASONS})),
                )
            cur.execute(
                """
                INSERT INTO metadata.quarantine_rows
                    (run_id, source_table, rule_name, payload, raw_path)
                SELECT run_id, 'staging.stg_tabmis__budget_typed', reject_reason,
                       to_jsonb(t) - 'run_id', raw_path
                FROM staging.stg_tabmis__budget_typed t
                WHERE run_id = %s AND NOT is_valid
                  AND reject_reason <> ALL(%s)
                """,
                (run_id, list(STRUCTURAL_REASONS)),
            )
            quarantined = cur.rowcount

        verdict = {
            "worksheet_rows": total,
            "structural_excluded": structural,
            "unmapped_rows": unmapped,
            "eligible_rows": eligible,
            "valid_rows": valid,
            "defective_rows": defective,
            "quarantined_rows": quarantined,
            "mapping_coverage": round(mapping_coverage, 4),
            "quality_score": round(quality_score, 4),
        }

        def refuse(message):
            set_run_status(run_id, "quality_failed",
                           quality_score=round(quality_score, 4), message=message)
            with warehouse_cursor(autocommit=True) as cur2:
                cur2.execute(
                    "UPDATE ingestion.intake_files SET status='rejected', "
                    "error_count=%s, processed_at=now() WHERE file_id=%s",
                    (defective + unmapped, ticket["file_id"]),
                )
            raise AirflowException(f"{message}: {verdict}")

        if eligible == 0:
            refuse("khong con dong nao dung de cong bo")
        if mapping_coverage < MIN_MAPPING_COVERAGE:
            refuse(f"do phu ma chi {mapping_coverage:.1%}, duoi nguong "
                   f"{MIN_MAPPING_COVERAGE:.0%}")
        if quality_score < MIN_QUALITY_SCORE:
            refuse(f"chat luong {quality_score:.1%}, duoi nguong "
                   f"{MIN_QUALITY_SCORE:.0%}")

        set_run_status(run_id, "quality_passed",
                       quality_score=round(quality_score, 4))
        write_audit("airflow", "quality_passed", run_id, verdict)
        return verdict

    # ---------------------------------------------------------------------
    @task
    def publish(ticket: dict, verdict: dict) -> dict:
        """
        Replace the period. Not upsert — replace.

        A resubmission is the complete truth for its month. Deleting the period
        first is what makes a restatement that REMOVES rows behave correctly;
        an upsert would leave those rows behind forever, and nobody would
        notice until the year-end totals refused to reconcile.
        """
        run_id = ticket["run_id"]
        period = ticket["period"]
        batch_id = "b_" + run_id[2:]

        with warehouse_cursor() as cur:
            cur.execute("DELETE FROM curated.fact_budget WHERE period = %s", (period,))
            replaced = cur.rowcount

            cur.execute(
                """
                INSERT INTO curated.fact_budget (
                    unit_code, line_code, fiscal_year, funding_code, period,
                    chapter_code, locality_code, sector_code,
                    allocated_amount, adjusted_amount, executed_amount,
                    advance_amount, executed_ytd,
                    run_id, batch_id, file_id
                )
                SELECT t.unit_code, t.line_code, t.fiscal_year, t.funding_code,
                       t.period,
                       max(s.chapter_code), max(t.locality_code), max(t.sector_code),
                       sum(t.allocated_amount), sum(t.adjusted_amount),
                       sum(t.executed_amount), sum(t.advance_amount),
                       sum(t.executed_ytd),
                       %(run_id)s, %(batch_id)s, %(file_id)s
                FROM staging.stg_tabmis__budget_typed t
                JOIN staging.stg_tabmis__budget s
                  ON s.run_id = t.run_id AND s.row_number = t.row_number
                WHERE t.run_id = %(run_id)s AND t.is_valid
                GROUP BY t.unit_code, t.line_code, t.fiscal_year,
                         t.funding_code, t.period
                """,
                {"run_id": run_id, "batch_id": batch_id, "file_id": ticket["file_id"]},
            )
            published = cur.rowcount

            cur.execute(
                """
                INSERT INTO curated.batch_summary
                    (batch_id, run_id, source_code, period, target_table,
                     row_count, quality_score, publish_status, data_freshness)
                VALUES (%s, %s, %s, %s, 'curated.fact_budget', %s, %s, 'approved', now())
                ON CONFLICT (batch_id) DO UPDATE SET
                    row_count     = EXCLUDED.row_count,
                    quality_score = EXCLUDED.quality_score,
                    published_at  = now()
                """,
                (batch_id, run_id, SOURCE_CODE, period, published,
                 verdict["quality_score"]),
            )
            cur.execute(
                "UPDATE ingestion.intake_files "
                "SET status='accepted', processed_at=now(), error_count=%s "
                "WHERE file_id=%s",
                (verdict["defective_rows"] + verdict["unmapped_rows"],
                 ticket["file_id"]),
            )
            # An earlier file for the same period is now history, not truth.
            cur.execute(
                "UPDATE ingestion.intake_files SET status='superseded' "
                "WHERE source_code=%s AND period=%s AND file_id<>%s "
                "AND status='accepted'",
                (SOURCE_CODE, period, ticket["file_id"]),
            )

        set_run_status(run_id, "published", row_count=published)
        write_audit("airflow", "published", run_id,
                    {"period": period, "rows_replaced": replaced,
                     "rows_published": published, "batch_id": batch_id})
        return {"batch_id": batch_id, "rows_replaced": replaced,
                "rows_published": published}

    # ---------------------------------------------------------------------
    @task(trigger_rule="none_skipped")
    def write_report(ticket: dict) -> dict:
        """
        Tell the submitter what happened, in the folder they uploaded to.

        The trigger rule has to thread a needle. This step must run when the
        batch was REFUSED — a rejected file nobody is told about is the failure
        mode the documentation names for this source. But it must NOT run when
        there was no file at all, and that is most of the time: the DAG polls
        every ten minutes and usually finds an empty inbox.

        'all_done' got the first half right and the second half wrong — it
        fired on every empty poll and failed for want of a file to report on,
        painting the DAG red around the clock for something that was working
        exactly as intended.

        'none_skipped' is the rule that distinguishes them. A refusal leaves
        upstream tasks in failed/upstream_failed, none of them skipped, so this
        still runs. An empty inbox skips claim_file, which skips everything
        downstream including this — and a run where every task skipped is a
        quiet success, not a failure.
        """
        run_id = ticket["run_id"]
        s3 = object_store()

        with warehouse_cursor() as cur:
            cur.execute(
                "SELECT status, row_count, quality_score, message "
                "FROM ingestion.runs WHERE run_id = %s", (run_id,)
            )
            row = cur.fetchone()
            status, row_count, score, message = row if row else (None, None, None, None)

            cur.execute(
                """
                SELECT row_number, reject_reason, reject_detail
                FROM staging.stg_tabmis__budget_typed
                WHERE run_id = %s AND NOT is_valid AND reject_reason <> ALL(%s)
                ORDER BY row_number LIMIT 200
                """,
                (run_id, list(STRUCTURAL_REASONS)),
            )
            problems = cur.fetchall()

            cur.execute(
                "SELECT COUNT(*) FROM staging.stg_tabmis__budget_typed "
                "WHERE run_id = %s AND NOT is_valid AND reject_reason <> ALL(%s)",
                (run_id, list(STRUCTURAL_REASONS)),
            )
            problem_total = cur.fetchone()[0]

            # Split the failures the way the submitter experiences them, not the
            # way the gate scores them: "a code we do not recognise" and "a
            # figure that is wrong" call for different people to do different
            # things. Reporting only quality_score produced a line that read
            # "100% quality" directly above eight rows to fix.
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE reject_reason = ANY(%s)),
                    COUNT(*) FILTER (WHERE NOT is_valid
                                     AND reject_reason <> ALL(%s)
                                     AND reject_reason <> ALL(%s))
                FROM staging.stg_tabmis__budget_typed
                WHERE run_id = %s
                """,
                (list(MAPPING_REASONS), list(MAPPING_REASONS),
                 list(STRUCTURAL_REASONS), run_id),
            )
            unmapped_rows, defective_rows = cur.fetchone()

        accepted = status == "published"
        lines = [
            f"KET QUA TIEP NHAN TEP  —  {ticket['key'].split('/')[-1]}",
            f"Ky bao cao : {ticket['period']}",
            f"Ma phien   : {run_id}",
            f"Thoi diem  : {datetime.now(timezone.utc).astimezone().strftime('%d/%m/%Y %H:%M')}",
            "",
            ("KET QUA    : DA TIEP NHAN" if accepted else "KET QUA    : TU CHOI"),
        ]
        if row_count is not None and accepted:
            lines.append(f"Da nap     : {row_count} dong vao kho "
                         f"(ky {ticket['period']} da duoc thay the)")
        if message:
            lines.append(f"Ly do      : {message}")

        if unmapped_rows or defective_rows:
            lines += ["", "TOM TAT VAN DE:"]
            if unmapped_rows:
                lines.append(f"  {unmapped_rows:>5} dong  ma khong co trong danh muc "
                             f"-> can bo sung danh muc, KHONG phai sua tep")
            if defective_rows:
                lines.append(f"  {defective_rows:>5} dong  so lieu sai "
                             f"-> can sua trong tep roi nop lai")
            lines.append("")
            lines.append("  Hai loai nay khac nhau: loai tren la thieu danh muc o phia kho,")
            lines.append("  loai duoi la so trong bieu chua dung.")

        if problems:
            lines += ["", f"CHI TIET CAC DONG ({problem_total} dong):", ""]
            for number, reason, detail in problems:
                lines.append(f"  dong {number:>6}  [{reason}]  {detail or ''}".rstrip())
            if problem_total > len(problems):
                lines.append(f"  ... con {problem_total - len(problems)} dong nua")
        elif accepted:
            lines += ["", "Khong co dong nao loi."]

        lines += ["", "Dong 'Cong:' va 'TONG CONG' trong bieu duoc bo qua co chu dich "
                      "— chung la dong tong, khong phai du lieu chi tiet."]

        report = "\n".join(lines) + "\n"
        report_key = ticket["key"] + ".ketqua.txt"
        s3.put_object(Bucket=INTAKE_BUCKET, Key=report_key,
                      Body=report.encode("utf-8"),
                      ContentType="text/plain; charset=utf-8")

        # Accepted files move out of the inbox so the folder shows what is
        # still outstanding. Rejected ones stay put, to be corrected and resent.
        archived = None
        if accepted:
            parts = ticket["key"].split("/")
            archived = "/".join(parts[:-1] + ["da-xu-ly", parts[-1]])
            s3.copy_object(Bucket=INTAKE_BUCKET, Key=archived,
                           CopySource={"Bucket": INTAKE_BUCKET, "Key": ticket["key"]})
            s3.delete_object(Bucket=INTAKE_BUCKET, Key=ticket["key"])

        with warehouse_cursor() as cur:
            cur.execute(
                "UPDATE ingestion.intake_files SET report_key = %s WHERE file_id = %s",
                (report_key, ticket["file_id"]),
            )

        return {"report_key": report_key, "archived_to": archived,
                "problem_rows": problem_total}

    ticket = claim_file()
    bronze = land_in_bronze(ticket)
    staged = load_silver_one(ticket, bronze)
    silver_two = build_silver_two(ticket, staged)
    verdict = quality_gate(ticket, silver_two)
    published = publish(ticket, verdict)
    write_report(ticket) << published


ingest_tabmis()
