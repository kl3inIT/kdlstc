"""
DWH-02 — Transform: Raw → Dim/Fact + Data Quality + Đối soát

Luồng: Raw (bẩn, y hệt nguồn)
         → chuẩn hóa MST + dedup tên   → dim_nnt
         → data-quality gate            → quarantine_thu (dòng lỗi)
         → fact_thu_ngan_sach           (dòng đạt)
         → đối soát TMS vs KBNN         → doi_soat_thu

Khái niệm áp dụng:
- Chuẩn hóa (normalization): MST 2 nguồn viết khác nhau -> 1 dạng chuẩn.
- Dedup + xử lý xung đột: cùng MST, 2 nguồn ghi tên khác -> chọn nguồn tin cậy,
  đánh dấu _co_xung_dot để rà soát sau (không im lặng bỏ qua).
- DQ gate: dòng lỗi vào quarantine, KHÔNG cho làm bẩn kho, và không chặn cả lô.
- Idempotency: xóa theo partition (_ngay_nap) rồi nạp lại -> chạy N lần kết quả như 1.
- Reconciliation: so tổng 2 nguồn, lệch thì ghi nhận trạng thái LECH.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task

DWH_CONN = "postgres_learn_dwh"


@dag(
    dag_id="dwh02_transform_reconcile",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "transform", "dq", "reconcile"],
)
def transform_reconcile():

    # ============================================================
    # BƯỚC 1 — Chuẩn hóa MST + dedup tên NNT  →  dim_nnt
    # ============================================================
    @task
    def build_dim_nnt() -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=DWH_CONN)
        with pg.get_conn() as conn:
            cur = conn.cursor()

            # Lấy bản MỚI NHẤT mỗi MST từ Raw TMS (Raw là append-only nên có nhiều version)
            # Nguồn TMS được coi là nguồn tin cậy cho TÊN người nộp thuế.
            cur.execute("""
                WITH tms_moi_nhat AS (
                    SELECT DISTINCT ON (mst)
                           mst, ten_nnt, dia_chi, co_quan_thue
                    FROM raw_tms_nnt
                    ORDER BY mst, _source_updated_at DESC, _load_id DESC
                ),
                -- Tên mà phía Kho bạc ghi (có thể khác), MST đã bỏ gạch
                kbnn_ten AS (
                    SELECT DISTINCT ON (REPLACE(mst, '-', ''))
                           REPLACE(mst, '-', '') AS mst,
                           ten_nnt_kb
                    FROM raw_kbnn_giao_dich_thu
                    WHERE mst IS NOT NULL
                    ORDER BY REPLACE(mst, '-', ''), _load_id DESC
                )
                INSERT INTO dim_nnt (mst, ten_nnt, dia_chi, co_quan_thue, _nguon_ten, _co_xung_dot, updated_at)
                SELECT t.mst,
                       t.ten_nnt,                       -- ưu tiên tên từ TMS
                       t.dia_chi,
                       t.co_quan_thue,
                       'TMS_ORACLE',
                       -- Đánh dấu nếu Kho bạc ghi tên KHÁC (so sánh nguyên văn).
                       -- Cố ý KHÔNG dùng UPPER() cả 2 vế: khác hoa/thường CŨNG là
                       -- một dạng bất nhất giữa 2 nguồn, cần rà soát chứ không im lặng bỏ qua.
                       (k.ten_nnt_kb IS NOT NULL AND TRIM(k.ten_nnt_kb) <> TRIM(t.ten_nnt)),
                       NOW()
                FROM tms_moi_nhat t
                LEFT JOIN kbnn_ten k ON k.mst = t.mst
                ON CONFLICT (mst) DO UPDATE SET      -- idempotent: chạy lại chỉ cập nhật
                    ten_nnt      = EXCLUDED.ten_nnt,
                    dia_chi      = EXCLUDED.dia_chi,
                    co_quan_thue = EXCLUDED.co_quan_thue,
                    _nguon_ten   = EXCLUDED._nguon_ten,
                    _co_xung_dot = EXCLUDED._co_xung_dot,
                    updated_at   = NOW();
            """)
            conn.commit()

            cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE _co_xung_dot) FROM dim_nnt")
            tong, xung_dot = cur.fetchone()

        print(f"dim_nnt: {tong} NNT | {xung_dot} bản ghi có TÊN KHÁC NHAU giữa 2 nguồn")
        return {"tong": tong, "xung_dot": xung_dot}

    # ============================================================
    # BƯỚC 2 — Đơn vị sử dụng ngân sách → dim_don_vi
    # ============================================================
    @task
    def build_dim_don_vi() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="pg_kbnn")     # đọc thẳng từ nguồn (bảng danh mục nhỏ)
        dwh = PostgresHook(postgres_conn_id=DWH_CONN)

        rows = pg.get_records("SELECT ma_dvsdns, ten_dvsdns, cap_ns FROM don_vi")
        with dwh.get_conn() as conn:
            cur = conn.cursor()
            cur.executemany("""
                INSERT INTO dim_don_vi (ma_dvsdns, ten_dvsdns, cap_ns, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (ma_dvsdns) DO UPDATE SET
                    ten_dvsdns = EXCLUDED.ten_dvsdns,
                    cap_ns     = EXCLUDED.cap_ns,
                    updated_at = NOW()
            """, rows)
            conn.commit()
        print(f"dim_don_vi: {len(rows)} đơn vị")
        return len(rows)

    # ============================================================
    # BƯỚC 3 — DQ gate: tách dòng lỗi vào quarantine
    # ============================================================
    @task
    def dq_gate_quarantine() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=DWH_CONN)
        with pg.get_conn() as conn:
            cur = conn.cursor()
            # Chạy lại được: xóa quarantine cũ của lô này
            cur.execute("DELETE FROM quarantine_thu WHERE _source_system = 'KBNN_PG'")

            # Quy tắc chất lượng: MST rỗng, số tiền <= 0, tiểu mục không có trong danh mục
            cur.execute("""
                INSERT INTO quarantine_thu (_source_system, _source_id, ly_do_loi, du_lieu_goc)
                SELECT r._source_system,
                       r.id,
                       CASE
                           WHEN r.mst IS NULL OR TRIM(r.mst) = ''      THEN 'MST rong'
                           WHEN r.so_tien IS NULL OR r.so_tien <= 0    THEN 'So tien khong hop le'
                           WHEN d.ma_tieu_muc IS NULL                  THEN 'Tieu muc khong co trong danh muc NSNN'
                           ELSE 'Khong xac dinh'
                       END,
                       to_jsonb(r)
                FROM raw_kbnn_giao_dich_thu r
                LEFT JOIN dim_muc_luc_nsnn d ON d.ma_tieu_muc = r.ma_tieu_muc
                WHERE r.mst IS NULL OR TRIM(r.mst) = ''
                   OR r.so_tien IS NULL OR r.so_tien <= 0
                   OR d.ma_tieu_muc IS NULL;
            """)
            n = cur.rowcount
            conn.commit()
        print(f"quarantine_thu: {n} dòng bị chặn (không cho vào kho)")
        return n

    # ============================================================
    # BƯỚC 4 — Nạp fact_thu_ngan_sach (chỉ dòng ĐẠT chất lượng)
    # ============================================================
    @task
    def load_fact_thu() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=DWH_CONN)
        with pg.get_conn() as conn:
            cur = conn.cursor()

            # IDEMPOTENT: xóa theo partition rồi nạp lại -> chạy N lần = chạy 1 lần
            cur.execute("DELETE FROM fact_thu_ngan_sach WHERE _source_system = 'KBNN_PG'")

            cur.execute("""
                INSERT INTO fact_thu_ngan_sach
                    (ngay, mst, ma_tieu_muc, ma_kich_ban, so_tien, _source_system, _source_id, _ngay_nap)
                SELECT r.ngay_thu,
                       REPLACE(r.mst, '-', ''),          -- CHUẨN HÓA MST: bỏ gạch
                       r.ma_tieu_muc,
                       'TH',                              -- kịch bản: Thực hiện
                       r.so_tien,
                       r._source_system,
                       r.id,
                       CURRENT_DATE
                FROM raw_kbnn_giao_dich_thu r
                JOIN dim_muc_luc_nsnn d ON d.ma_tieu_muc = r.ma_tieu_muc      -- tiểu mục hợp lệ
                JOIN dim_nnt n          ON n.mst = REPLACE(r.mst, '-', '')     -- NNT tồn tại
                JOIN dim_thoi_gian t    ON t.ngay = r.ngay_thu                 -- ngày hợp lệ
                WHERE r.so_tien > 0;
            """)
            n = cur.rowcount
            conn.commit()
        print(f"fact_thu_ngan_sach: {n} dòng đã nạp")
        return n

    # ============================================================
    # BƯỚC 5 — ĐỐI SOÁT: so tổng TMS vs KBNN
    # ============================================================
    @task
    def doi_soat(fact_rows: int) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=DWH_CONN)
        with pg.get_conn() as conn:
            cur = conn.cursor()

            # Tổng bên THUẾ (nguồn TMS, lấy bản mới nhất mỗi khoản nộp)
            cur.execute("""
                SELECT COALESCE(SUM(so_tien_nop), 0), COUNT(*)
                FROM (
                    SELECT DISTINCT ON (id) id, so_tien_nop
                    FROM raw_tms_khoan_nop
                    ORDER BY id, _source_updated_at DESC, _load_id DESC
                ) x;
            """)
            tong_tms, sl_tms = cur.fetchone()

            # Tổng bên KHO BẠC (đã vào kho)
            cur.execute("""
                SELECT COALESCE(SUM(so_tien), 0), COUNT(*)
                FROM fact_thu_ngan_sach WHERE _source_system = 'KBNN_PG';
            """)
            tong_kbnn, sl_kbnn = cur.fetchone()

            chenh_lech = tong_tms - tong_kbnn
            trang_thai = "KHOP" if chenh_lech == 0 else "LECH"

            # Ghi nhận kết quả đối soát (idempotent theo kỳ)
            cur.execute("DELETE FROM doi_soat_thu WHERE ky = 'TONG'")
            cur.execute("""
                INSERT INTO doi_soat_thu
                    (ky, tong_tien_tms, tong_tien_kbnn, chenh_lech,
                     so_ban_ghi_tms, so_ban_ghi_kbnn, trang_thai)
                VALUES ('TONG', %s, %s, %s, %s, %s, %s)
            """, (tong_tms, tong_kbnn, chenh_lech, sl_tms, sl_kbnn, trang_thai))
            conn.commit()

        print("=" * 60)
        print(f"  ĐỐI SOÁT THU NGÂN SÁCH")
        print(f"  Nguồn THUẾ  (TMS) : {tong_tms:>18,} đ  ({sl_tms} bản ghi)")
        print(f"  Nguồn KHO BẠC     : {tong_kbnn:>18,} đ  ({sl_kbnn} bản ghi)")
        print(f"  CHÊNH LỆCH        : {chenh_lech:>18,} đ")
        print(f"  TRẠNG THÁI        : {trang_thai}")
        print("=" * 60)
        if trang_thai == "LECH":
            print(f">> CANH BAO: lech {chenh_lech:,} d — thieu {sl_tms - sl_kbnn} ban ghi ben Kho bac.")
            print(">> Can ra soat: khoan nop nao co o TMS ma chua vao Kho bac?")

        return {"tong_tms": int(tong_tms), "tong_kbnn": int(tong_kbnn),
                "chenh_lech": int(chenh_lech), "trang_thai": trang_thai}

    # ============================================================
    # Luồng phụ thuộc
    # ============================================================
    dim_nnt = build_dim_nnt()
    dim_dv = build_dim_don_vi()
    dq = dq_gate_quarantine()

    fact = load_fact_thu()
    # fact cần dim_nnt xong trước (vì JOIN dim_nnt) và DQ gate đã chạy
    dim_nnt >> fact
    dq >> fact
    dim_dv >> fact

    doi_soat(fact)


transform_reconcile()
