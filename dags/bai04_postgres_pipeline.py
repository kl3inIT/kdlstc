"""
Bài 4 — Pipeline thật với PostgreSQL: Raw → Clean → Warehouse

Mục tiêu học:
- Connection: Airflow lưu thông tin DB (không hardcode password).
- SQLExecuteQueryOperator: Airflow chạy SQL (không ôm dữ liệu).
- Phân tầng thật: raw → staging → warehouse.
- Khử trùng + idempotency: MERGE theo ngay_den, chạy lại không nhân đôi.
- Data-quality check: fail DAG nếu dữ liệu bẩn, không cho lan xuống warehouse.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.sql import SqlSensor


@dag(
    dag_id="bai04_postgres_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # chỉ chạy tay
    catchup=False,
    tags=["hoc-airflow", "postgres", "dwh"],
)
def postgres_pipeline():
    # ========================================
    # LAYER 1: RAW → STAGING (Làm sạch)
    # ========================================
    clean_and_dedup = SQLExecuteQueryOperator(
        task_id="clean_and_dedup",
        conn_id="postgres_learn_dwh",  # Connection phải tạo trước trên UI
        sql="""
            -- Khử rác: loại dòng NULL so_tien
            DELETE FROM raw_thu_chi WHERE so_tien IS NULL;

            -- Khử trùng: giữ bản mới nhất (theo loaded_at), partition theo ngay_den
            DELETE FROM stg_thu_chi_clean
            WHERE (ngay_den, id) IN (
                SELECT ngay_den, id FROM (
                    SELECT ngay_den, id,
                           ROW_NUMBER() OVER (PARTITION BY ngay_den, id ORDER BY loaded_at DESC) AS rn
                    FROM raw_thu_chi WHERE so_tien IS NOT NULL
                ) t WHERE rn > 1
            );

            -- Nạp clean staging
            INSERT INTO stg_thu_chi_clean (id, ma_don_vi, loai, so_tien, ngay, ghi_chu, processed_at)
            SELECT id, ma_don_vi, loai, so_tien, ngay, ghi_chu, NOW()
            FROM raw_thu_chi WHERE so_tien IS NOT NULL
            ON CONFLICT (id) DO UPDATE SET
                ma_don_vi = EXCLUDED.ma_don_vi,
                loai = EXCLUDED.loai,
                so_tien = EXCLUDED.so_tien,
                ngay = EXCLUDED.ngay,
                ghi_chu = EXCLUDED.ghi_chu,
                processed_at = NOW();
        """,
    )

    # ========================================
    # LAYER 2: DATA-QUALITY CHECK (Fail nếu rác)
    # ========================================
    @task
    def validate_data() -> bool:
        """
        Demo data-quality check: tính tổng tiền, nếu <= 0 thì fail.
        Trong thực tế: check số dòng, NULL, âm, khóa ngoại, v.v.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_learn_dwh")
        result = pg.get_first("SELECT COALESCE(SUM(so_tien), 0) AS total FROM stg_thu_chi_clean;")
        total = result[0] if result else 0

        print(f"Tong tien sau khi lam sach: {total}")
        if total <= 0:
            raise ValueError(f"Tong tien phai > 0, nhan duoc {total}")
        return True

    # ========================================
    # LAYER 3: STAGING → WAREHOUSE (Idempotent MERGE)
    # ========================================
    load_to_warehouse = SQLExecuteQueryOperator(
        task_id="load_to_warehouse",
        conn_id="postgres_learn_dwh",
        sql="""
            -- MERGE idempotent theo ngay_den: XÓA ngày đó rồi INSERT
            -- Chạy lại bao nhiêu lần cũng không nhân bản.
            DELETE FROM dwh_thu_chi WHERE ngay_den = '{{ ds }}';

            INSERT INTO dwh_thu_chi (id, ngay_den, ma_don_vi, loai, so_tien, ngay, ghi_chu, updated_at)
            SELECT id, ngay_den, ma_don_vi, loai, so_tien, ngay, ghi_chu, NOW()
            FROM stg_thu_chi_clean;
        """,
    )

    # ========================================
    # Nối chuỗi: clean → validate → load
    # ========================================
    clean_and_dedup >> validate_data() >> load_to_warehouse


postgres_pipeline()
