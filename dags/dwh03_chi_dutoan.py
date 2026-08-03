"""
DWH-03 — Chi ngân sách + Dự toán vs Thực hiện

Hoàn thiện bức tranh tài chính: DWH-01/02 lo phần THU, DWH-03 lo phần CHI
và câu hỏi nghiệp vụ quan trọng nhất của Sở Tài chính:
    "Đơn vị nào tiêu vượt dự toán?"

Luồng:
  giao_dich_chi (KBNN)  → raw_kbnn_giao_dich_chi   [ingest, watermark]
  du_toan_ngan_sach     → fact_chi (kịch bản DT)   [dự toán]
  raw chi               → fact_chi (kịch bản TH)   [thực hiện]
  DT vs TH              → doi_soat_chi             [phát hiện vượt dự toán]

Khái niệm mới so với DWH-01/02:
- dim_kich_ban: cùng 1 bảng fact chứa CẢ dự toán (DT) và thực hiện (TH)
  -> mượn ý tưởng DimScenario của AdventureWorksDW.
- So sánh 2 kịch bản để ra tỷ lệ giải ngân và cảnh báo vượt chi.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task

DWH_CONN = "postgres_learn_dwh"
NAM = 2026


@dag(
    dag_id="dwh03_chi_dutoan",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "chi", "dutoan", "reconcile"],
)
def chi_dutoan():

    # ============================================================
    # BƯỚC 1 — Ingest giao dịch CHI vào Raw (incremental)
    # ============================================================
    @task
    def ingest_chi() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        src = PostgresHook(postgres_conn_id="pg_kbnn")
        dwh = PostgresHook(postgres_conn_id=DWH_CONN)

        with dwh.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT last_value FROM etl_watermark_ms "
                "WHERE source_system='KBNN_PG' AND source_table='giao_dich_chi'"
            )
            wm = cur.fetchone()[0]
            print(f"[chi] watermark hien tai: {wm}")

            rows = src.get_records(
                "SELECT id, ma_dvsdns, so_tien, ngay_chi, ma_tieu_muc, updated_at "
                "FROM giao_dich_chi WHERE updated_at > %s ORDER BY updated_at",
                parameters=(wm,),
            )
            print(f"[chi] so dong lay duoc: {len(rows)}")

            if rows:
                cur.executemany(
                    "INSERT INTO raw_kbnn_giao_dich_chi "
                    "(_source_system, _source_updated_at, id, ma_dvsdns, so_tien, ngay_chi, ma_tieu_muc) "
                    "VALUES ('KBNN_PG', %s, %s, %s, %s, %s, %s)",
                    [(r[5], r[0], r[1], r[2], r[3], r[4]) for r in rows],
                )
                max_wm = max(r[5] for r in rows)
                cur.execute(
                    "UPDATE etl_watermark_ms SET last_value=%s, updated_at=NOW() "
                    "WHERE source_system='KBNN_PG' AND source_table='giao_dich_chi'",
                    (max_wm,),
                )
                print(f"[chi] watermark moi: {max_wm}")
            conn.commit()      # nạp data + đẩy mốc cùng 1 transaction
        return len(rows)

    # ============================================================
    # BƯỚC 2 — Nạp DỰ TOÁN vào fact (kịch bản 'DT')
    # ============================================================
    @task
    def load_fact_du_toan() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=DWH_CONN)
        with pg.get_conn() as conn:
            cur = conn.cursor()
            # Idempotent: xóa kịch bản DT của năm rồi nạp lại
            cur.execute(
                "DELETE FROM fact_chi_ngan_sach WHERE ma_kich_ban='DT' "
                "AND EXTRACT(YEAR FROM ngay)=%s", (NAM,)
            )
            # Dự toán quy ước ghi vào ngày 01/01 của năm
            cur.execute("""
                INSERT INTO fact_chi_ngan_sach
                    (ngay, ma_dvsdns, ma_tieu_muc, ma_kich_ban, so_tien,
                     _source_system, _source_id, _ngay_nap)
                SELECT MAKE_DATE(dt.nam, 1, 1),
                       dt.ma_dvsdns,
                       dt.ma_tieu_muc,
                       'DT',
                       dt.so_tien_dt,
                       'DUTOAN_PG',
                       dt.id,
                       CURRENT_DATE
                FROM du_toan_ngan_sach dt
                JOIN dim_don_vi dv        ON dv.ma_dvsdns  = dt.ma_dvsdns
                JOIN dim_muc_luc_nsnn m   ON m.ma_tieu_muc = dt.ma_tieu_muc
                JOIN dim_thoi_gian t      ON t.ngay = MAKE_DATE(dt.nam, 1, 1)
                WHERE dt.nam = %s;
            """, (NAM,))
            n = cur.rowcount
            conn.commit()
        print(f"fact_chi (DU TOAN): {n} dong")
        return n

    # ============================================================
    # BƯỚC 3 — Nạp THỰC HIỆN vào fact (kịch bản 'TH')
    # ============================================================
    @task
    def load_fact_thuc_hien() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=DWH_CONN)
        with pg.get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM fact_chi_ngan_sach WHERE ma_kich_ban='TH'")
            cur.execute("""
                INSERT INTO fact_chi_ngan_sach
                    (ngay, ma_dvsdns, ma_tieu_muc, ma_kich_ban, so_tien,
                     _source_system, _source_id, _ngay_nap)
                SELECT r.ngay_chi,
                       r.ma_dvsdns,
                       r.ma_tieu_muc,
                       'TH',
                       r.so_tien,
                       r._source_system,
                       r.id,
                       CURRENT_DATE
                FROM (
                    -- Raw append-only: lấy bản mới nhất mỗi id gốc
                    SELECT DISTINCT ON (id) *
                    FROM raw_kbnn_giao_dich_chi
                    ORDER BY id, _source_updated_at DESC, _load_id DESC
                ) r
                JOIN dim_don_vi dv      ON dv.ma_dvsdns  = r.ma_dvsdns
                JOIN dim_muc_luc_nsnn m ON m.ma_tieu_muc = r.ma_tieu_muc
                JOIN dim_thoi_gian t    ON t.ngay = r.ngay_chi
                WHERE r.so_tien > 0;
            """)
            n = cur.rowcount
            conn.commit()
        print(f"fact_chi (THUC HIEN): {n} dong")
        return n

    # ============================================================
    # BƯỚC 4 — ĐỐI SOÁT: Dự toán vs Thực hiện theo từng đơn vị
    # ============================================================
    @task
    def doi_soat_du_toan(n_dt: int, n_th: int) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id=DWH_CONN)
        with pg.get_conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM doi_soat_chi WHERE nam=%s", (NAM,))
            cur.execute("""
                INSERT INTO doi_soat_chi
                    (nam, ma_dvsdns, ten_dvsdns, tong_du_toan, tong_thuc_hien,
                     con_lai, ty_le_giai_ngan, trang_thai)
                SELECT %s AS nam,
                       dv.ma_dvsdns,
                       dv.ten_dvsdns,
                       COALESCE(dt.tong, 0),
                       COALESCE(th.tong, 0),
                       COALESCE(dt.tong, 0) - COALESCE(th.tong, 0),
                       CASE WHEN COALESCE(dt.tong,0) = 0 THEN NULL
                            ELSE ROUND(COALESCE(th.tong,0) * 100.0 / dt.tong, 2) END,
                       CASE WHEN COALESCE(dt.tong,0) = 0            THEN 'CHUA_CO_DT'
                            WHEN COALESCE(th.tong,0) > dt.tong      THEN 'VUOT_DU_TOAN'
                            ELSE 'TRONG_DU_TOAN' END
                FROM dim_don_vi dv
                LEFT JOIN (
                    SELECT ma_dvsdns, SUM(so_tien) AS tong
                    FROM fact_chi_ngan_sach
                    WHERE ma_kich_ban='DT' AND EXTRACT(YEAR FROM ngay)=%s
                    GROUP BY ma_dvsdns
                ) dt ON dt.ma_dvsdns = dv.ma_dvsdns
                LEFT JOIN (
                    SELECT ma_dvsdns, SUM(so_tien) AS tong
                    FROM fact_chi_ngan_sach
                    WHERE ma_kich_ban='TH' AND EXTRACT(YEAR FROM ngay)=%s
                    GROUP BY ma_dvsdns
                ) th ON th.ma_dvsdns = dv.ma_dvsdns;
            """, (NAM, NAM, NAM))
            conn.commit()

            cur.execute("""
                SELECT ten_dvsdns, tong_du_toan, tong_thuc_hien, con_lai,
                       ty_le_giai_ngan, trang_thai
                FROM doi_soat_chi WHERE nam=%s ORDER BY ty_le_giai_ngan DESC NULLS LAST
            """, (NAM,))
            rows = cur.fetchall()

        print("=" * 88)
        print(f"  ĐỐI SOÁT CHI NGÂN SÁCH {NAM} — DỰ TOÁN vs THỰC HIỆN")
        print("=" * 88)
        print(f"  {'Đơn vị':<28} {'Dự toán':>15} {'Thực hiện':>15} {'Còn lại':>15} {'%':>7}  Trạng thái")
        print("-" * 88)
        so_vuot = 0
        for r in rows:
            ten, dt, th, cl, tl, tt = r
            tl_s = f"{tl:.1f}" if tl is not None else "-"
            print(f"  {ten:<28} {int(dt):>15,} {int(th):>15,} {int(cl):>15,} {tl_s:>7}  {tt}")
            if tt == "VUOT_DU_TOAN":
                so_vuot += 1
        print("=" * 88)
        if so_vuot:
            print(f">> CANH BAO: {so_vuot} don vi VUOT DU TOAN — can ra soat ngay!")
        else:
            print(">> Tat ca don vi trong du toan.")

        return {"so_don_vi": len(rows), "so_vuot_du_toan": so_vuot}

    # ---------- Luồng ----------
    raw_chi = ingest_chi()
    dt = load_fact_du_toan()
    th = load_fact_thuc_hien()

    raw_chi >> th          # phải ingest xong mới nạp thực hiện
    doi_soat_du_toan(dt, th)


chi_dutoan()
