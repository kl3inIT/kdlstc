"""
DWH-01 — Ingest 2 nguồn khác engine vào Raw (bất biến)

Bài toán thực tế: Kho dữ liệu tài chính tỉnh
  - Nguồn 1: Hệ thống Thuế (Oracle)      -> raw_tms_nnt, raw_tms_khoan_nop
  - Nguồn 2: Kho bạc      (PostgreSQL)   -> raw_kbnn_giao_dich_thu

Khái niệm áp dụng:
- Multi-source: mỗi nguồn 1 Connection + 1 Hook khác nhau (Oracle vs Postgres).
- Watermark theo từng (source_system, source_table) — không dùng mốc chung.
- Raw bất biến: chỉ INSERT, không UPDATE/DELETE; giữ _source_updated_at để truy vết.
- Idempotency: nạp data + đẩy mốc trong CÙNG 1 transaction ở kho đích.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


DWH_CONN = "postgres_learn_dwh"   # kho đích


def _lay_watermark(cur, source_system: str, source_table: str):
    cur.execute(
        "SELECT last_value FROM etl_watermark_ms WHERE source_system=%s AND source_table=%s",
        (source_system, source_table),
    )
    row = cur.fetchone()
    return row[0] if row else None


# BẪY MULTI-ENGINE: driver oracledb bind Python datetime thành kiểu DATE của
# Oracle — mà DATE KHÔNG có phần thập phân giây. Watermark 15:13:58.517696 bị
# cắt còn 15:13:58 => mọi dòng đều "lớn hơn" => nạp lại toàn bộ (nhân đôi).
# Cách chữa: truyền watermark dạng CHUỖI và ép Oracle parse bằng TO_TIMESTAMP.
_ORA_TS_FMT = "YYYY-MM-DD HH24:MI:SS.FF6"


def _wm_str(wm) -> str:
    """Đổi watermark sang chuỗi giữ đủ 6 chữ số micro-giây cho Oracle."""
    return wm.strftime("%Y-%m-%d %H:%M:%S.%f")


@dag(
    dag_id="dwh01_ingest_multisource",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "multisource", "ingest"],
)
def ingest_multisource():

    # ============================================================
    # NGUỒN 1 — Oracle (Hệ thống Thuế)
    # ============================================================
    @task
    def ingest_oracle_nnt() -> int:
        from airflow.providers.oracle.hooks.oracle import OracleHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        src = OracleHook(oracle_conn_id="oracle_tms")
        dwh = PostgresHook(postgres_conn_id=DWH_CONN)

        with dwh.get_conn() as dwh_conn:
            cur = dwh_conn.cursor()
            wm = _lay_watermark(cur, "TMS_ORACLE", "nnt")
            print(f"[nnt] watermark hien tai: {wm}")

            # Chỉ lấy bản ghi MỚI HƠN mốc (incremental)
            rows = src.get_records(
                "SELECT mst, ten_nnt, dia_chi, co_quan_thue, updated_at "
                f"FROM tms.nnt WHERE updated_at > TO_TIMESTAMP(:1, '{_ORA_TS_FMT}') "
                "ORDER BY updated_at",
                parameters=[_wm_str(wm)],
            )
            print(f"[nnt] so dong lay duoc: {len(rows)}")

            if rows:
                cur.executemany(
                    "INSERT INTO raw_tms_nnt "
                    "(_source_system, _source_updated_at, mst, ten_nnt, dia_chi, co_quan_thue) "
                    "VALUES ('TMS_ORACLE', %s, %s, %s, %s, %s)",
                    [(r[4], r[0], r[1], r[2], r[3]) for r in rows],
                )
                # Đẩy mốc = giá trị lớn nhất vừa nạp — CÙNG transaction
                max_wm = max(r[4] for r in rows)
                cur.execute(
                    "UPDATE etl_watermark_ms SET last_value=%s, updated_at=NOW() "
                    "WHERE source_system='TMS_ORACLE' AND source_table='nnt'",
                    (max_wm,),
                )
                print(f"[nnt] watermark moi: {max_wm}")
            dwh_conn.commit()   # cả INSERT + UPDATE mốc cùng commit
        return len(rows)

    @task
    def ingest_oracle_khoan_nop() -> int:
        from airflow.providers.oracle.hooks.oracle import OracleHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        src = OracleHook(oracle_conn_id="oracle_tms")
        dwh = PostgresHook(postgres_conn_id=DWH_CONN)

        with dwh.get_conn() as dwh_conn:
            cur = dwh_conn.cursor()
            wm = _lay_watermark(cur, "TMS_ORACLE", "khoan_nop")
            print(f"[khoan_nop] watermark hien tai: {wm}")

            rows = src.get_records(
                "SELECT id, to_khai_id, so_tien_nop, ngay_nop, ma_tieu_muc, updated_at "
                f"FROM tms.khoan_nop WHERE updated_at > TO_TIMESTAMP(:1, '{_ORA_TS_FMT}') "
                "ORDER BY updated_at",
                parameters=[_wm_str(wm)],
            )
            print(f"[khoan_nop] so dong lay duoc: {len(rows)}")

            if rows:
                cur.executemany(
                    "INSERT INTO raw_tms_khoan_nop "
                    "(_source_system, _source_updated_at, id, to_khai_id, so_tien_nop, ngay_nop, ma_tieu_muc) "
                    "VALUES ('TMS_ORACLE', %s, %s, %s, %s, %s, %s)",
                    [(r[5], r[0], r[1], r[2], r[3], r[4]) for r in rows],
                )
                max_wm = max(r[5] for r in rows)
                cur.execute(
                    "UPDATE etl_watermark_ms SET last_value=%s, updated_at=NOW() "
                    "WHERE source_system='TMS_ORACLE' AND source_table='khoan_nop'",
                    (max_wm,),
                )
                print(f"[khoan_nop] watermark moi: {max_wm}")
            dwh_conn.commit()
        return len(rows)

    # ============================================================
    # NGUỒN 2 — PostgreSQL (Kho bạc)
    # ============================================================
    @task
    def ingest_kbnn_giao_dich_thu() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        src = PostgresHook(postgres_conn_id="pg_kbnn")
        dwh = PostgresHook(postgres_conn_id=DWH_CONN)

        with dwh.get_conn() as dwh_conn:
            cur = dwh_conn.cursor()
            wm = _lay_watermark(cur, "KBNN_PG", "giao_dich_thu")
            print(f"[gd_thu] watermark hien tai: {wm}")

            rows = src.get_records(
                "SELECT id, mst, ten_nnt_kb, so_tien, ngay_thu, ma_tieu_muc, ma_sac_thue, updated_at "
                "FROM giao_dich_thu WHERE updated_at > %s ORDER BY updated_at",
                parameters=(wm,),
            )
            print(f"[gd_thu] so dong lay duoc: {len(rows)}")

            if rows:
                cur.executemany(
                    "INSERT INTO raw_kbnn_giao_dich_thu "
                    "(_source_system, _source_updated_at, id, mst, ten_nnt_kb, so_tien, ngay_thu, ma_tieu_muc, ma_sac_thue) "
                    "VALUES ('KBNN_PG', %s, %s, %s, %s, %s, %s, %s, %s)",
                    [(r[7], r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows],
                )
                max_wm = max(r[7] for r in rows)
                cur.execute(
                    "UPDATE etl_watermark_ms SET last_value=%s, updated_at=NOW() "
                    "WHERE source_system='KBNN_PG' AND source_table='giao_dich_thu'",
                    (max_wm,),
                )
                print(f"[gd_thu] watermark moi: {max_wm}")
            dwh_conn.commit()
        return len(rows)

    # ============================================================
    # Tổng kết: in ra số dòng đã nạp từ mỗi nguồn
    # ============================================================
    @task
    def tong_ket(n_nnt: int, n_kn: int, n_thu: int) -> None:
        print("=" * 50)
        print(f"Oracle TMS  - nnt          : {n_nnt} dong")
        print(f"Oracle TMS  - khoan_nop    : {n_kn} dong")
        print(f"KBNN Postgres - giao_dich_thu: {n_thu} dong")
        print("=" * 50)
        if n_nnt == 0 and n_kn == 0 and n_thu == 0:
            print(">> Khong co du lieu moi (watermark da o cuoi) - incremental hoat dong dung!")

    # 3 nguồn chạy SONG SONG (độc lập nhau), rồi gộp tổng kết
    tong_ket(ingest_oracle_nnt(), ingest_oracle_khoan_nop(), ingest_kbnn_giao_dich_thu())


ingest_multisource()
