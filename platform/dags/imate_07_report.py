"""
imate_07_report — step 7 of 7: the answer.

Reads through the serving layer's contract rather than straight off Gold: it
starts when step 6 has confirmed the curated tables are readable by the BI
identity, so the report and the dashboard can never be looking at a table the
other one cannot reach.

Produces the deliverable that was asked for: document counts per day,
filterable by issuing body, by month and by kind. One CSV at
day x kind x body grain (a pivot table filters the rest), plus a rendered
summary in the logs so the result is visible without downloading anything.

Days with zero documents are present in the CSV. The 2025 hole in this
tenant is real source data; a report that smoothed it over would be the
pipeline lying about its input.
"""

import csv
import io
import json
from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from imate_assets import SERVING
from imate_common import S3_BUCKET, imate_cursor, set_status as set_run_status
from imate_ops import ticket
from warehouse import object_store




@dag(
    dag_id="imate_07_report",
    schedule=[SERVING],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_07_report():

    @task
    def open_run(**context):
        return ticket("07", context["dag_run"].run_id)

    @task
    def render(info):
        run_id = info["run_id"]
        with imate_cursor() as cur:
            # Day x kind x body — the finest grain the report filters need.
            # dim_date LEFT JOIN keeps zero days alive.
            cur.execute("""
                SELECT dd.full_date, dd.month_label,
                       coalesce(k.kind_code, ''), coalesce(k.kind_name, ''),
                       coalesce(b.body_code, ''), coalesce(b.body_name, ''),
                       count(f.global_id)
                  FROM curated.dim_date dd
                  LEFT JOIN curated.fact_document f ON f.date_key = dd.date_key
                  LEFT JOIN curated.dim_document_kind k ON k.kind_key = f.kind_key
                  LEFT JOIN curated.dim_issuing_body b ON b.body_key = f.body_key
                 GROUP BY 1, 2, 3, 4, 5, 6
                 ORDER BY 1, 3, 5
            """)
            detail = cur.fetchall()

            cur.execute("""
                SELECT dd.month_label, count(f.global_id)
                  FROM curated.dim_date dd
                  LEFT JOIN curated.fact_document f ON f.date_key = dd.date_key
                 GROUP BY dd.month_key, dd.month_label
                 ORDER BY dd.month_key
            """)
            by_month = cur.fetchall()

            cur.execute("""
                SELECT k.kind_name, count(*) FROM curated.fact_document f
                  JOIN curated.dim_document_kind k ON k.kind_key = f.kind_key
                 GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """)
            by_kind = cur.fetchall()

            cur.execute("""
                SELECT b.body_name, count(*) FROM curated.fact_document f
                  JOIN curated.dim_issuing_body b ON b.body_key = f.body_key
                 GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """)
            by_body = cur.fetchall()

            cur.execute("SELECT count(*) FROM curated.fact_document")
            total = cur.fetchone()[0]

        if not total:
            raise AirflowSkipException("fact_document rong — chua co gi de bao cao.")

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["ngay", "thang", "ma_loai", "ten_loai",
                         "ma_don_vi_gui", "ten_don_vi_gui", "so_van_ban"])
        for row in detail:
            writer.writerow(row)

        s3 = object_store()
        try:
            s3.create_bucket(Bucket=S3_BUCKET)
        except Exception:                             # noqa: BLE001
            pass                                      # already exists
        key = f"reports/{run_id}/bao-cao-van-ban-theo-ngay.csv"
        s3.put_object(Bucket=S3_BUCKET, Key=key,
                      Body=buffer.getvalue().encode("utf-8-sig"),
                      ContentType="text/csv")

        lines = [f"TONG SO VAN BAN: {total:,}", "", "THEO THANG:"]
        peak = max((c for _, c in by_month), default=1) or 1
        for label, count in by_month:
            bar = "#" * round(count / peak * 40)
            lines.append(f"  {label:<16} {count:>6,}  {bar}")
        lines += ["", "TOP LOAI VAN BAN:"]
        lines += [f"  {name:<28} {count:>6,}" for name, count in by_kind]
        lines += ["", "TOP DON VI GUI:"]
        lines += [f"  {name:<28} {count:>6,}" for name, count in by_body]
        print("\n".join(lines), flush=True)

        summary = {"total": total, "csv": f"{S3_BUCKET}/{key}",
                   "months": len(by_month), "detail_rows": len(detail)}
        set_run_status(run_id, "published", row_count=total,
                       message=json.dumps(summary, ensure_ascii=False))
        return summary

    render(open_run())


imate_07_report()
