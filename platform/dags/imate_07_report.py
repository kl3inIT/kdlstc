"""
imate_07_report — step 7 of 7: the answer.

Every number here comes from the semantic layer, not from SQL written in this
file. That is the whole point of step 6 existing: the report and the Superset
dashboard now ask the SAME definition of "số văn bản", so the two cannot drift
apart. Before this, each wrote its own query and agreement was luck.

Days with zero documents survive the trip — dim_date is a joined cube, so a day
nobody sent anything still appears with a count of zero rather than vanishing.

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
from imate_semantic import query as ask_cube, rows as cube_rows
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

        # Bốn câu hỏi, không câu nào là SQL. Tên chỉ tiêu và tên chiều là hợp
        # đồng với lớp ngữ nghĩa; bảng nào, join ra sao là việc của Cube.
        detail = cube_rows(
            {"measures": ["van_ban.so_van_ban"],
             "dimensions": ["ngay.ngay", "ngay.thang", "loai.ma_loai",
                            "loai.ten_loai", "don_vi.ma_don_vi",
                            "don_vi.ten_don_vi"],
             "order": {"ngay.ngay": "asc"},
             "limit": 50000},
            "ngay.ngay", "ngay.thang", "loai.ma_loai", "loai.ten_loai",
            "don_vi.ma_don_vi", "don_vi.ten_don_vi", "van_ban.so_van_ban")

        by_month = cube_rows(
            {"measures": ["van_ban.so_van_ban"],
             "dimensions": ["ngay.thang", "ngay.thang_key"],
             "order": {"ngay.thang_key": "asc"}},
            "ngay.thang", "van_ban.so_van_ban")

        by_kind = cube_rows(
            {"measures": ["van_ban.so_van_ban"], "dimensions": ["loai.ten_loai"],
             "order": {"van_ban.so_van_ban": "desc"}, "limit": 10},
            "loai.ten_loai", "van_ban.so_van_ban")

        by_body = cube_rows(
            {"measures": ["van_ban.so_van_ban"], "dimensions": ["don_vi.ten_don_vi"],
             "order": {"van_ban.so_van_ban": "desc"}, "limit": 10},
            "don_vi.ten_don_vi", "van_ban.so_van_ban")

        total = int((ask_cube({"measures": ["van_ban.so_van_ban"]}) or
                     [{}])[0].get("van_ban.so_van_ban") or 0)

        # Một chỉ tiêu đo trên bảng dữ kiện chỉ trả về ngày CÓ văn bản — hỏi thêm
        # chính chiều thời gian để lấy đủ dải, rồi bù những ngày trống thành dòng
        # số 0. Lỗ hổng dữ liệu là chuyện thật của nguồn; báo cáo làm phẳng nó đi
        # là báo cáo nói dối về đầu vào của chính mình.
        all_days = cube_rows(
            {"measures": ["ngay.so_ngay"],
             "dimensions": ["ngay.ngay", "ngay.thang"],
             "order": {"ngay.ngay": "asc"}, "limit": 50000},
            "ngay.ngay", "ngay.thang")
        seen_days = {row[0] for row in detail}
        empty_days = [(d, m, "", "", "", "", 0)
                      for d, m in all_days if d not in seen_days]
        detail = sorted(detail + empty_days, key=lambda r: (r[0], r[2], r[4]))

        seen_months = {label for label, _ in by_month}
        by_month += [(m, 0) for m in
                     dict.fromkeys(m for _, m in all_days if m not in seen_months)]

        # Cube trả số dưới dạng chuỗi; ép về số ngay để phần dựng biểu đồ và
        # phần ghi CSV không phải đoán kiểu.
        by_month = [(label, int(count or 0)) for label, count in by_month]
        by_kind = [(name, int(count or 0)) for name, count in by_kind]
        by_body = [(name, int(count or 0)) for name, count in by_body]
        detail = [(*row[:-1], int(row[-1] or 0)) for row in detail]

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
                   "months": len(by_month), "detail_rows": len(detail),
                   "empty_days": len(empty_days),
                   "engine": "cube"}
        set_run_status(run_id, "published", row_count=total,
                       message=json.dumps(summary, ensure_ascii=False))
        return summary

    render(open_run())


imate_07_report()
