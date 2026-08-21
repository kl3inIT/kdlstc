"""
imate_08_replay — dựng lại kho từ Bronze, không gọi lại nguồn.

Đây không phải bước thứ tám của kiến trúc. Bảy bước là đường dữ liệu chảy xuôi;
DAG này là đường đi ngược, chạy khi có người cần dựng lại quá khứ.

Vì sao cần. Bronze giữ MỌI phiên bản từng thấy của mỗi văn bản, khoá đặt theo mã
băm nội dung. Nghĩa là kho đang nắm đủ nguyên liệu để trả lời ba câu hỏi rất
thật, mà trước DAG này thì không có đường nào dùng tới:

    - Phát hiện quy tắc bóc tách sai từ tháng trước: sửa quy tắc rồi dựng lại,
      KHÔNG phải đi xin nguồn gửi lại dữ liệu.
    - Sếp hỏi vì sao con số tháng 6 hôm nay khác bản in tháng trước.
    - Nguồn hỏng hoặc mất kết nối dài ngày: vẫn dựng lại được kho.

Hai chế độ, đều chạy bằng tay và đều phải nêu rõ phạm vi:

    as_of=<ngày>        dùng phiên bản Bronze mới nhất TÍNH ĐẾN ngày đó. Đây là
                        chế độ trả lời câu "báo cáo hôm ấy dựa trên dữ liệu nào".
    as_of=latest        dùng phiên bản mới nhất. Dành cho trường hợp sửa quy tắc
                        rồi muốn áp lại cho toàn bộ dữ liệu đang có.

DAG này KHÔNG tự chạy theo lịch và KHÔNG tự công bố. Nó chỉ đưa văn bản về trạng
thái 'landed' rồi dừng — chuỗi 03 đến 07 sẽ tự chạy tiếp qua Asset như thường lệ,
đi qua đúng cổng chất lượng và đúng các phép kiểm của dbt. Phát lại mà bỏ qua các
cổng ấy thì chỉ là ghi đè, không phải dựng lại.
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

from imate_assets import BRONZE
from imate_common import S3_BUCKET, TENANT_ID, imate_cursor, set_status as set_run_status
from imate_ops import ticket
from warehouse import object_store


@dag(
    dag_id="imate_08_replay",
    schedule=None,                      # chỉ chạy khi có người bấm
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc", "replay"],
    params={
        "as_of": "latest",
        "limit": 0,                     # 0 = không giới hạn
    },
)
def imate_08_replay():

    @task
    def open_run(**context):
        return ticket("08", context["dag_run"].run_id)

    @task(outlets=[BRONZE])
    def rewind(info, **context):
        """
        Chọn phiên bản Bronze theo mốc thời gian rồi đưa văn bản về 'landed'.

        Chọn theo LastModified của đối tượng trong kho lưu trữ, không theo
        updatedAt của nguồn: câu hỏi ở đây là "kho biết gì tính đến ngày đó",
        không phải "nguồn sửa lần cuối lúc nào". Hai trục thời gian khác nhau, và
        lẫn chúng chính là cách một bản phát lại cho ra kết quả không ai giải
        thích nổi.
        """
        params = context["params"]
        as_of = str(params.get("as_of", "latest")).strip()
        limit = int(params.get("limit") or 0)
        run_id = info["run_id"]

        cutoff = None
        if as_of and as_of != "latest":
            try:
                cutoff = datetime.fromisoformat(as_of)
            except ValueError as exc:
                raise AirflowException(
                    f"as_of khong hop le: {as_of!r} — dung dinh dang "
                    "YYYY-MM-DD hoac 'latest'") from exc
            if cutoff.tzinfo is None:
                from datetime import timezone
                cutoff = cutoff.replace(tzinfo=timezone.utc)

        with imate_cursor() as cur:
            cur.execute(
                "SELECT global_id FROM ingestion.doc_worklist "
                "WHERE tenant_id = %s AND bronze_key IS NOT NULL "
                "ORDER BY global_id", (TENANT_ID,))
            gids = [r[0] for r in cur.fetchall()]
        if limit:
            gids = gids[:limit]
        if not gids:
            raise AirflowSkipException("Khong co van ban nao co du lieu Bronze.")

        s3 = object_store()
        chosen, missing = [], []

        for gid in gids:
            # Mọi phiên bản của một văn bản nằm chung một tiền tố, nên chỉ cần
            # liệt kê tiền tố ấy rồi chọn theo mốc thời gian.
            objects = s3.list_objects_v2(
                Bucket=S3_BUCKET, Prefix=f"bronze/doc/{gid}/").get("Contents", [])
            if cutoff is not None:
                objects = [o for o in objects if o["LastModified"] <= cutoff]
            if not objects:
                missing.append(gid)
                continue
            newest = max(objects, key=lambda o: o["LastModified"])
            chosen.append((gid, newest["Key"]))

        if not chosen:
            raise AirflowException(
                f"khong co phien ban Bronze nao tinh den {as_of} — "
                "kiem tra lai moc thoi gian")

        # Đưa về 'landed' và trỏ vào ĐÚNG phiên bản đã chọn. Từ đây chuỗi 03..07
        # chạy tiếp như một lô bình thường, qua đủ mọi cổng kiểm.
        from psycopg2.extras import execute_values
        with imate_cursor() as cur:
            execute_values(cur, """
                UPDATE ingestion.doc_worklist AS w
                   SET status = 'landed', bronze_key = v.key,
                       last_error = NULL, updated_at = now()
                  FROM (VALUES %s) AS v (gid, key)
                 WHERE w.global_id = v.gid
            """, chosen)

        summary = {
            "run_id": run_id,
            "as_of": as_of,
            "van_ban_phat_lai": len(chosen),
            "khong_co_phien_ban": len(missing),
            "engine": "replay-from-bronze",
        }
        print("ket qua: " + json.dumps(summary, ensure_ascii=False), flush=True)

        # Văn bản chưa tồn tại tại thời điểm ấy là chuyện bình thường, không phải
        # lỗi — nhưng phải đếm được, nếu không một bản phát lại thiếu dữ liệu sẽ
        # trông y hệt một bản phát lại đầy đủ.
        if missing:
            print(f"CANH BAO: {len(missing)} van ban khong co phien ban Bronze "
                  f"nao tinh den {as_of} — chung khong nam trong ban phat lai",
                  flush=True)

        set_run_status(run_id, "parsed", row_count=len(chosen),
                       message=json.dumps(summary, ensure_ascii=False))
        return summary

    rewind(open_run())


imate_08_replay()
