"""
ingest_qlgia — first vertical slice of the finance warehouse.

Source -> Bronze -> Silver-1 -> Silver-2 -> quality gate -> Silver-3 -> Gold,
with a run ticket threaded through every step so any published number can be
walked back to the bytes it came from.

Three decisions worth knowing before reading the code:

  The source publishes SURVEY POINTS, the report needs one number. A surveyor
  visits three to five outlets for the same commodity in the same district, so
  the grain has to change somewhere. It changes in exactly one place —
  build_grain — and lands in its own table rather than hiding inside the
  publish statement, so the collapse can be inspected before Gold sees it.

  Silver-1 is loaded FROM BRONZE, not from the HTTP response held in memory.
  It costs an extra read and buys replay: if a mapping rule changes next
  month, the same batch can be rebuilt from stored bytes without asking the
  source for numbers it may no longer be able to reproduce.

  Rows that fail validation are not dropped. They are written to Silver-2 with
  a reject_reason and copied to quarantine. Judging happens per observation,
  so one mistyped price does not discard the four sound ones beside it, and
  the count of what was discarded travels with the average.

The source publishes by SURVEY period, which is not the calendar period — May
2026 has two, June has one. So the DAG pulls incrementally on a lastModified
cursor and a single batch may legitimately carry rows from several periods.
"""

import hashlib
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

try:                                    # Airflow 3
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:                     # Airflow 2 fallback
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from airflow.exceptions import AirflowException

from psycopg2.extras import Json, execute_values

from warehouse import (
    BRONZE_BUCKET,
    object_store,
    set_run_status,
    warehouse_cursor,
    write_audit,
)

SOURCE_CODE = "qlgia"
SOURCE_BASE_URL = "http://mock-qlgia.stc-hy.svc.cluster.local"
PAGE_SIZE = 500
HTTP_TIMEOUT = 30

# Publish thresholds. Two separate numbers because they mean different things:
# a low mapping coverage is somebody's homework, a low quality score is broken
# data. Only the second one should feel like an incident.
MIN_QUALITY_SCORE = 0.95
MIN_MAPPING_COVERAGE = 0.50

# An average resting on this few observations is reported but flagged, so a
# reader can tell a well-surveyed district from a thinly-surveyed one.
THIN_SURVEY_POINTS = 2

# What a source row must look like. Checked against a sample BEFORE anything
# is written, so a source that silently changed shape is refused while Bronze
# is still clean.
FIELD_CONTRACT = {
    "id":           (int,),
    "itemCode":     (str,),
    "itemName":     (str,),
    "areaCode":     (str, type(None)),
    "outletCode":   (str, type(None)),
    "outletName":   (str,),
    "periodCode":   (str,),
    "surveyDate":   (str,),
    "uom":          (str,),
    "unitPrice":    (int, float),
    "lastModified": (str,),
}

# Reject reasons grouped by what they mean for the publish decision.
STRUCTURAL_REASONS = ("subtotal_row",)
MAPPING_REASONS = ("commodity_unmapped", "locality_unmapped")


def _fetch(path, **params):
    url = f"{SOURCE_BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(
            {k: v for k, v in params.items() if v is not None}
        )
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8"))


# =========================================================================
# Silver-2 build. One statement so the whole derivation is visible in one
# place and runs in one transaction. Grain here is still one survey point.
# =========================================================================
BUILD_TYPED_SQL = """
INSERT INTO staging.stg_qlgia__price_typed (
    run_id, commodity_code, locality_code, survey_period, survey_date,
    outlet_code, unit_of_measure, price, source_updated_at,
    src_item_code, src_area_code, source_row_id, raw_path,
    is_valid, reject_reason
)
WITH parsed AS (
    -- Safe casts only: anything unconvertible becomes NULL and is judged
    -- below, rather than aborting the whole batch with a cast error.
    SELECT
        s.run_id,
        s.commodity_code AS src_item_code,
        s.locality_code  AS src_area_code,
        s.survey_period,
        s.outlet_code,
        s.source_row_id,
        s.raw_path,
        s.unit_of_measure,
        CASE WHEN s.survey_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
             THEN s.survey_date::date END                  AS survey_date,
        CASE WHEN s.price ~ '^-?\\d+(\\.\\d+)?$'
             THEN s.price::numeric END                     AS price,
        CASE WHEN s.source_updated_at ~ '^\\d{4}-\\d{2}-\\d{2}T'
             THEN s.source_updated_at::timestamptz END     AS source_updated_at
    FROM staging.stg_qlgia__price s
    WHERE s.run_id = %(run_id)s
),
mapped AS (
    SELECT
        p.*,
        mc.standard_value AS commodity_code,
        ml.standard_value AS locality_code
    FROM parsed p
    LEFT JOIN metadata.mapping_rules mc
           ON mc.source_code  = %(source_code)s
          AND mc.code_type    = 'commodity'
          AND mc.source_value = p.src_item_code
          AND mc.valid_to     >= CURRENT_DATE
    LEFT JOIN metadata.mapping_rules ml
           ON ml.source_code  = %(source_code)s
          AND ml.code_type    = 'locality'
          AND ml.source_value = p.src_area_code
          AND ml.valid_to     >= CURRENT_DATE
),
deduped AS (
    -- The source re-sends corrected readings for the same outlet. Newest
    -- lastModified wins; source id breaks exact ties. The key includes the
    -- outlet, because several outlets in one district are not duplicates.
    SELECT DISTINCT ON (
        src_item_code,
        COALESCE(src_area_code, '~null~'),
        survey_period,
        COALESCE(outlet_code, '~null~')
    ) *
    FROM mapped
    ORDER BY src_item_code,
             COALESCE(src_area_code, '~null~'),
             survey_period,
             COALESCE(outlet_code, '~null~'),
             source_updated_at DESC NULLS LAST,
             source_row_id     DESC
),
reference_price AS (
    -- Median per commodity across every point in the batch, used to catch
    -- unit-of-measure slips. Subtotal and non-positive rows would drag it.
    SELECT src_item_code,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price
    FROM mapped
    WHERE price > 0
      AND src_area_code IS DISTINCT FROM 'QLG-TONG'
    GROUP BY src_item_code
),
judged AS (
    SELECT
        d.*,
        CASE
            -- Structural first: a subtotal row is not a defect, it simply
            -- does not belong at this grain. Judging it as bad data would
            -- put a permanent false failure in the quality score.
            WHEN d.src_area_code = 'QLG-TONG'      THEN 'subtotal_row'
            WHEN d.src_area_code IS NULL           THEN 'locality_missing'
            WHEN d.outlet_code IS NULL             THEN 'outlet_missing'
            WHEN d.survey_date IS NULL             THEN 'survey_date_unparseable'
            WHEN d.price IS NULL                   THEN 'price_unparseable'
            WHEN d.price <= 0                      THEN 'price_not_positive'
            WHEN d.commodity_code IS NULL          THEN 'commodity_unmapped'
            WHEN d.locality_code  IS NULL          THEN 'locality_unmapped'
            WHEN r.median_price IS NOT NULL
             AND d.price > 20 * r.median_price     THEN 'price_out_of_range'
        END AS reject_reason
    FROM deduped d
    LEFT JOIN reference_price r ON r.src_item_code = d.src_item_code
)
SELECT
    run_id, commodity_code, locality_code, survey_period, survey_date,
    outlet_code, unit_of_measure, price, source_updated_at,
    src_item_code, src_area_code, source_row_id, raw_path,
    reject_reason IS NULL AS is_valid,
    reject_reason
FROM judged;
"""


# =========================================================================
# Silver-3 build — the grain change.
#
# Rows with no warehouse codes are excluded rather than counted: an unmapped
# or district-less observation belongs to no group, so it cannot be one
# group's rejected point.
# =========================================================================
BUILD_GRAIN_SQL = """
INSERT INTO staging.stg_qlgia__price_grain (
    run_id, commodity_code, locality_code, survey_period, survey_date,
    unit_of_measure, avg_price, min_price, max_price,
    survey_points, rejected_points, raw_paths
)
SELECT
    run_id,
    commodity_code,
    locality_code,
    survey_period,
    MIN(survey_date)     FILTER (WHERE is_valid),
    MIN(unit_of_measure) FILTER (WHERE is_valid),
    ROUND(AVG(price)     FILTER (WHERE is_valid), 2),
    MIN(price)           FILTER (WHERE is_valid),
    MAX(price)           FILTER (WHERE is_valid),
    COUNT(*)             FILTER (WHERE is_valid),
    COUNT(*)             FILTER (WHERE NOT is_valid),
    ARRAY_AGG(DISTINCT raw_path)
FROM staging.stg_qlgia__price_typed
WHERE run_id = %(run_id)s
  AND commodity_code IS NOT NULL
  AND locality_code  IS NOT NULL
GROUP BY run_id, commodity_code, locality_code, survey_period;
"""


@dag(
    dag_id="ingest_qlgia",
    description="Price Management survey points -> Bronze -> Silver -> quality gate -> Gold",
    schedule="0 3 * * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["warehouse", "bronze", "qlgia"],
    doc_md=__doc__,
)
def ingest_qlgia():

    # ---------------------------------------------------------------------
    @task
    def open_run(**context) -> dict:
        """
        Open the ticket and read the incremental cursor.

        run_id is derived from the Airflow run id rather than generated fresh,
        so a retry reuses the same ticket instead of littering the ledger with
        half-finished rows.
        """
        dag_run = context["dag_run"]
        run_id = "r_" + re.sub(r"[^0-9A-Za-z]", "", str(dag_run.run_id))[:40]

        stamp = (context.get("logical_date")
                 or dag_run.start_date
                 or datetime.now(timezone.utc))
        period = stamp.strftime("%Y-%m-%d")
        raw_prefix = f"{BRONZE_BUCKET}/{SOURCE_CODE}/{period}/{run_id}/"

        with warehouse_cursor() as cur:
            cur.execute(
                "SELECT cursor_value FROM ingestion.cursors WHERE source_code = %s",
                (SOURCE_CODE,),
            )
            row = cur.fetchone()
            cursor_value = row[0] if row else None

            # Retries land here again; reset the ticket rather than duplicate it.
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

            # A retry must not see rows from its own previous attempt.
            for statement in (
                "DELETE FROM staging.stg_qlgia__price        WHERE run_id = %s",
                "DELETE FROM staging.stg_qlgia__price_typed  WHERE run_id = %s",
                "DELETE FROM staging.stg_qlgia__price_grain  WHERE run_id = %s",
                "DELETE FROM metadata.quarantine_rows        WHERE run_id = %s",
                "DELETE FROM metadata.quality_exceptions     WHERE run_id = %s",
                "DELETE FROM metadata.mapping_rejections     WHERE run_id = %s",
            ):
                cur.execute(statement, (run_id,))

        write_audit("airflow", "run_opened", run_id,
                    {"source": SOURCE_CODE, "cursor": cursor_value})
        return {
            "run_id": run_id,
            "period": period,
            "cursor": cursor_value,
            "raw_prefix": raw_prefix,
        }

    # ---------------------------------------------------------------------
    @task
    def check_schema_contract(ticket: dict) -> dict:
        """
        Refuse a source that changed shape, before writing anything.

        A missing or retyped field stops the batch. An unknown extra field is
        recorded and allowed through — the warehouse should not break because
        somebody added a column, but it should not surface it unasked either.
        """
        run_id = ticket["run_id"]
        sample = _fetch("/api/prices", page=1, pageSize=25)
        rows = sample.get("items", [])

        if not rows:
            return {"unknown_fields": [], "sampled": 0}

        violations = []
        unknown_fields = set()

        for row in rows:
            for field, allowed_types in FIELD_CONTRACT.items():
                if field not in row:
                    violations.append(f"{field}: missing")
                elif not isinstance(row[field], allowed_types):
                    violations.append(
                        f"{field}: expected {'/'.join(t.__name__ for t in allowed_types)}, "
                        f"got {type(row[field]).__name__}"
                    )
            unknown_fields |= set(row) - set(FIELD_CONTRACT)

        if violations:
            detail = sorted(set(violations))
            set_run_status(run_id, "schema_blocked",
                           message="; ".join(detail)[:900])
            with warehouse_cursor(autocommit=True) as cur:
                cur.execute(
                    "INSERT INTO metadata.quality_exceptions "
                    "(run_id, table_name, rule_name, severity, failed_rows, details) "
                    "VALUES (%s, %s, %s, 'blocker', %s, %s)",
                    (run_id, "source.api/prices", "schema_contract",
                     len(rows), Json({"violations": detail})),
                )
            write_audit("airflow", "schema_blocked", run_id, {"violations": detail})
            raise AirflowException(
                "Source schema contract violated, nothing was ingested: "
                + "; ".join(detail)
            )

        return {"unknown_fields": sorted(unknown_fields), "sampled": len(rows)}

    # ---------------------------------------------------------------------
    @task
    def land_in_bronze(ticket: dict, contract: dict) -> dict:
        """
        Page through the source and store every response verbatim.

        Bronze is immutable and keeps the source's own field names. No
        renaming, no casting, no filtering — this is the evidence layer.
        """
        run_id = ticket["run_id"]
        prefix = f"{SOURCE_CODE}/{ticket['period']}/{run_id}/"
        s3 = object_store()

        digest = hashlib.sha256()
        keys, row_count, page = [], 0, 1
        newest_cursor = ticket["cursor"]

        while True:
            payload = _fetch("/api/prices",
                             updatedSince=ticket["cursor"],
                             page=page,
                             pageSize=PAGE_SIZE)
            items = payload.get("items", [])
            if not items:
                break

            body = json.dumps(payload, ensure_ascii=False,
                              sort_keys=True).encode("utf-8")
            digest.update(body)

            key = f"{prefix}page-{page:05d}.json"
            s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=body,
                          ContentType="application/json")
            keys.append(key)
            row_count += len(items)

            for item in items:
                stamp = item.get("lastModified")
                if stamp and (newest_cursor is None or stamp > newest_cursor):
                    newest_cursor = stamp

            next_page = payload.get("nextPage")
            if not next_page:
                break
            page = next_page

        checksum = digest.hexdigest()

        if row_count == 0:
            set_run_status(run_id, "published", row_count=0,
                           message="no rows newer than cursor")
            raise AirflowSkipException("Nothing new since the last cursor.")

        # Manifest makes the batch self-describing on disk: someone holding
        # only the bucket can tell what this folder is without the database.
        manifest = {
            "run_id": run_id,
            "source_code": SOURCE_CODE,
            "period": ticket["period"],
            "cursor_from": ticket["cursor"],
            "cursor_to": newest_cursor,
            "row_count": row_count,
            "pages": keys,
            "checksum_sha256": checksum,
            "unknown_fields": contract.get("unknown_fields", []),
            "landed_at": datetime.now(timezone.utc).isoformat(),
        }
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=f"{prefix}_manifest.json",
            Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        set_run_status(run_id, "received", row_count=row_count,
                       checksum_sha256=checksum)
        write_audit("airflow", "bronze_landed", run_id,
                    {"rows": row_count, "pages": len(keys), "checksum": checksum})

        return {"keys": keys, "row_count": row_count,
                "checksum": checksum, "cursor_to": newest_cursor}

    # ---------------------------------------------------------------------
    @task
    def load_silver_one(ticket: dict, bronze: dict) -> dict:
        """
        Read Bronze back and land it in Silver-1.

        Reading from storage rather than reusing the in-memory response is
        what makes this step replayable, and it proves the stored bytes are
        actually usable rather than merely present.

        Renaming happens here and nowhere else: camelCase source vocabulary
        becomes warehouse column names, values stay text.
        """
        run_id = ticket["run_id"]
        s3 = object_store()
        rows = []

        for key in bronze["keys"]:
            body = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)["Body"].read()
            raw_path = f"{BRONZE_BUCKET}/{key}"
            for item in json.loads(body).get("items", []):
                price = item.get("unitPrice")
                rows.append((
                    run_id,
                    str(item.get("id")) if item.get("id") is not None else None,
                    item.get("itemCode"),
                    item.get("itemName"),
                    item.get("areaCode"),
                    item.get("outletCode"),
                    item.get("outletName"),
                    item.get("periodCode"),
                    item.get("surveyDate"),
                    item.get("uom"),
                    None if price is None else str(price),
                    item.get("lastModified"),
                    raw_path,
                ))

        with warehouse_cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO staging.stg_qlgia__price (
                    run_id, source_row_id, commodity_code, commodity_name,
                    locality_code, outlet_code, outlet_name, survey_period,
                    survey_date, unit_of_measure, price, source_updated_at,
                    raw_path
                ) VALUES %s
                """,
                rows,
                page_size=1000,
            )

        set_run_status(run_id, "parsed")
        return {"staged_rows": len(rows)}

    # ---------------------------------------------------------------------
    @task
    def build_silver_two(ticket: dict, staged: dict) -> dict:
        """
        Type, deduplicate, map to warehouse codes, and judge every point.

        Source codes with no mapping rule become a queued business question
        with an owner and a due date, not a silently dropped row.
        """
        run_id = ticket["run_id"]

        with warehouse_cursor() as cur:
            cur.execute(BUILD_TYPED_SQL,
                        {"run_id": run_id, "source_code": SOURCE_CODE})

            cur.execute(
                """
                SELECT COALESCE(reject_reason, '__valid__'), COUNT(*)
                FROM staging.stg_qlgia__price_typed
                WHERE run_id = %s
                GROUP BY 1
                """,
                (run_id,),
            )
            tally = dict(cur.fetchall())

            # Unmapped source codes -> a queue somebody owns.
            cur.execute(
                """
                INSERT INTO metadata.mapping_rejections
                    (run_id, source_code, code_type, source_value,
                     row_count, reason, assignee, due_date)
                SELECT %(run_id)s, %(source_code)s,
                       CASE WHEN reject_reason = 'commodity_unmapped'
                            THEN 'commodity' ELSE 'locality' END,
                       CASE WHEN reject_reason = 'commodity_unmapped'
                            THEN src_item_code ELSE src_area_code END,
                       COUNT(*),
                       'no mapping rule for this source code',
                       'Phong Gia',
                       CURRENT_DATE + 7
                FROM staging.stg_qlgia__price_typed
                WHERE run_id = %(run_id)s
                  AND reject_reason IN ('commodity_unmapped', 'locality_unmapped')
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
        """
        Decide whether this batch may be published. Judged per survey point.

        Two independent measures, because they call for different responses:

          mapping_coverage  how much of the batch the warehouse understands.
                            Low means somebody owes a mapping rule.
          quality_score     of the points it does understand, how many are
                            sound. Low means the data itself is wrong.

        Structural exclusions (subtotal rows) count against neither — they are
        expected, and charging them to the score would bake in a permanent
        false failure.
        """
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
                    (run_id, "staging.stg_qlgia__price_typed", reason,
                     count,
                     round(1 - count / denominator, 4) if denominator else None,
                     Json({"structural": reason in STRUCTURAL_REASONS,
                           "mapping": reason in MAPPING_REASONS})),
                )

            # Keep the failing rows themselves, not just the counts.
            cur.execute(
                """
                INSERT INTO metadata.quarantine_rows
                    (run_id, source_table, rule_name, payload, raw_path)
                SELECT run_id, 'staging.stg_qlgia__price_typed', reject_reason,
                       to_jsonb(t) - 'run_id', raw_path
                FROM staging.stg_qlgia__price_typed t
                WHERE run_id = %s
                  AND NOT is_valid
                  AND reject_reason <> ALL(%s)
                """,
                (run_id, list(STRUCTURAL_REASONS)),
            )
            quarantined = cur.rowcount

        verdict = {
            "survey_points": total,
            "structural_excluded": structural,
            "unmapped_points": unmapped,
            "eligible_points": eligible,
            "valid_points": valid,
            "defective_points": defective,
            "quarantined_points": quarantined,
            "mapping_coverage": round(mapping_coverage, 4),
            "quality_score": round(quality_score, 4),
        }

        if eligible == 0:
            set_run_status(run_id, "quality_failed", quality_score=0,
                           message="no points survived mapping")
            raise AirflowException(f"Nothing publishable in this batch: {verdict}")

        if mapping_coverage < MIN_MAPPING_COVERAGE:
            set_run_status(run_id, "quality_failed",
                           quality_score=round(quality_score, 4),
                           message=f"mapping coverage {mapping_coverage:.2%} "
                                   f"below {MIN_MAPPING_COVERAGE:.0%}")
            raise AirflowException(
                f"Too much of the batch is unmapped to publish safely: {verdict}"
            )

        if quality_score < MIN_QUALITY_SCORE:
            set_run_status(run_id, "quality_failed",
                           quality_score=round(quality_score, 4),
                           message=f"quality {quality_score:.2%} "
                                   f"below {MIN_QUALITY_SCORE:.0%}")
            raise AirflowException(f"Quality gate refused this batch: {verdict}")

        set_run_status(run_id, "quality_passed",
                       quality_score=round(quality_score, 4))
        write_audit("airflow", "quality_passed", run_id, verdict)
        return verdict

    # ---------------------------------------------------------------------
    @task
    def build_grain(ticket: dict, verdict: dict) -> dict:
        """
        Collapse survey points into the reporting grain.

        This is the only place the grain changes: several observations become
        one commodity x district x period row carrying the average, the spread,
        and — crucially — how many observations it rests on.

        Two situations are surfaced rather than smoothed over:

          a group whose points were ALL rejected produces no Gold row at all.
          Publishing a zero there would be inventing a number.

          a group resting on one or two observations is published but flagged,
          because an average of one is a single shop's price wearing a
          statistic's clothes.
        """
        run_id = ticket["run_id"]

        with warehouse_cursor() as cur:
            cur.execute(BUILD_GRAIN_SQL, {"run_id": run_id})

            cur.execute(
                """
                SELECT
                    COUNT(*)                                        AS groups_total,
                    COUNT(*) FILTER (WHERE survey_points = 0)       AS groups_dropped,
                    COUNT(*) FILTER (WHERE survey_points BETWEEN 1 AND %(thin)s)
                                                                    AS groups_thin,
                    COALESCE(SUM(survey_points), 0)                 AS points_kept,
                    COALESCE(SUM(rejected_points), 0)               AS points_rejected,
                    ROUND(AVG(survey_points) FILTER (WHERE survey_points > 0), 2)
                                                                    AS points_per_group
                FROM staging.stg_qlgia__price_grain
                WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id, "thin": THIN_SURVEY_POINTS},
            )
            (groups_total, groups_dropped, groups_thin,
             points_kept, points_rejected, points_per_group) = cur.fetchone()

            # A dropped group is a hole in the report. Name it, with coordinates.
            if groups_dropped:
                cur.execute(
                    """
                    INSERT INTO metadata.quality_exceptions
                        (run_id, table_name, rule_name, severity,
                         failed_rows, pass_rate, details)
                    SELECT %(run_id)s, 'staging.stg_qlgia__price_grain',
                           'grain_dropped_all_points_rejected', 'scoring',
                           COUNT(*),
                           ROUND(1 - COUNT(*)::numeric / %(total)s, 4),
                           jsonb_build_object('groups', jsonb_agg(
                               jsonb_build_object(
                                   'commodity', commodity_code,
                                   'locality',  locality_code,
                                   'period',    survey_period,
                                   'rejected',  rejected_points)))
                    FROM staging.stg_qlgia__price_grain
                    WHERE run_id = %(run_id)s AND survey_points = 0
                    """,
                    {"run_id": run_id, "total": groups_total},
                )

            # A thin group is publishable but should not read as authoritative.
            if groups_thin:
                cur.execute(
                    """
                    INSERT INTO metadata.quality_exceptions
                        (run_id, table_name, rule_name, severity,
                         failed_rows, pass_rate, details)
                    SELECT %(run_id)s, 'staging.stg_qlgia__price_grain',
                           'grain_thin_survey_coverage', 'scoring',
                           COUNT(*),
                           ROUND(1 - COUNT(*)::numeric / %(total)s, 4),
                           jsonb_build_object(
                               'threshold', %(thin)s,
                               'groups', jsonb_agg(
                                   jsonb_build_object(
                                       'commodity', commodity_code,
                                       'locality',  locality_code,
                                       'period',    survey_period,
                                       'points',    survey_points)))
                    FROM staging.stg_qlgia__price_grain
                    WHERE run_id = %(run_id)s
                      AND survey_points BETWEEN 1 AND %(thin)s
                    """,
                    {"run_id": run_id, "total": groups_total,
                     "thin": THIN_SURVEY_POINTS},
                )

        summary = {
            "groups_total": groups_total,
            "groups_publishable": groups_total - groups_dropped,
            "groups_dropped": groups_dropped,
            "groups_thin": groups_thin,
            "points_kept": points_kept,
            "points_rejected": points_rejected,
            "points_per_group": float(points_per_group or 0),
        }
        write_audit("airflow", "grain_built", run_id, summary)
        return summary

    # ---------------------------------------------------------------------
    @task
    def publish(ticket: dict, bronze: dict, verdict: dict, grain: dict) -> dict:
        """
        Merge the grain rows into Gold and declare the batch visible.

        Upsert on the business key, so re-running a batch converges instead of
        accumulating. The cursor only advances after a successful publish — a
        failed batch is retried, never skipped.
        """
        run_id = ticket["run_id"]
        batch_id = "b_" + run_id[2:]

        with warehouse_cursor() as cur:
            cur.execute(
                """
                INSERT INTO curated.fact_price (
                    commodity_code, locality_code, survey_period, survey_date,
                    unit_of_measure, avg_price, min_price, max_price,
                    survey_points, rejected_points, run_id, batch_id, raw_paths
                )
                SELECT commodity_code, locality_code, survey_period, survey_date,
                       unit_of_measure, avg_price, min_price, max_price,
                       survey_points, rejected_points,
                       run_id, %(batch_id)s, raw_paths
                FROM staging.stg_qlgia__price_grain
                WHERE run_id = %(run_id)s AND survey_points > 0
                ON CONFLICT (commodity_code, locality_code, survey_period)
                DO UPDATE SET
                    survey_date     = EXCLUDED.survey_date,
                    unit_of_measure = EXCLUDED.unit_of_measure,
                    avg_price       = EXCLUDED.avg_price,
                    min_price       = EXCLUDED.min_price,
                    max_price       = EXCLUDED.max_price,
                    survey_points   = EXCLUDED.survey_points,
                    rejected_points = EXCLUDED.rejected_points,
                    run_id          = EXCLUDED.run_id,
                    batch_id        = EXCLUDED.batch_id,
                    raw_paths       = EXCLUDED.raw_paths,
                    updated_at      = now()
                """,
                {"run_id": run_id, "batch_id": batch_id},
            )
            published_rows = cur.rowcount

            # One summary row per survey period actually present, because a
            # single incremental batch can span several of them.
            cur.execute(
                """
                INSERT INTO curated.batch_summary
                    (batch_id, run_id, source_code, period, target_table,
                     row_count, quality_score, publish_status, data_freshness)
                SELECT %(batch_id)s || '_' || g.survey_period,
                       %(run_id)s, %(source_code)s, g.survey_period,
                       'curated.fact_price',
                       COUNT(*), %(score)s, 'approved',
                       (SELECT MAX(t.source_updated_at)
                          FROM staging.stg_qlgia__price_typed t
                         WHERE t.run_id = g.run_id
                           AND t.survey_period = g.survey_period)
                FROM staging.stg_qlgia__price_grain g
                WHERE g.run_id = %(run_id)s AND g.survey_points > 0
                GROUP BY g.run_id, g.survey_period
                ON CONFLICT (batch_id) DO UPDATE SET
                    row_count      = EXCLUDED.row_count,
                    quality_score  = EXCLUDED.quality_score,
                    data_freshness = EXCLUDED.data_freshness,
                    published_at   = now()
                """,
                {"batch_id": batch_id, "run_id": run_id,
                 "source_code": SOURCE_CODE, "score": verdict["quality_score"]},
            )

            cur.execute(
                """
                INSERT INTO ingestion.cursors (source_code, cursor_value, run_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_code) DO UPDATE
                   SET cursor_value = EXCLUDED.cursor_value,
                       run_id       = EXCLUDED.run_id,
                       updated_at   = now()
                """,
                (SOURCE_CODE, bronze["cursor_to"], run_id),
            )

        set_run_status(run_id, "published", row_count=published_rows)
        write_audit("airflow", "published", run_id,
                    {"rows": published_rows, "batch_id": batch_id,
                     "cursor_to": bronze["cursor_to"],
                     "groups_dropped": grain["groups_dropped"]})

        return {"batch_id": batch_id, "published_rows": published_rows,
                "cursor_to": bronze["cursor_to"]}

    ticket = open_run()
    contract = check_schema_contract(ticket)
    bronze = land_in_bronze(ticket, contract)
    staged = load_silver_one(ticket, bronze)
    silver_two = build_silver_two(ticket, staged)
    verdict = quality_gate(ticket, silver_two)
    grain = build_grain(ticket, verdict)
    publish(ticket, bronze, verdict, grain)


ingest_qlgia()
