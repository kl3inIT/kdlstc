"""Buổi 2 — nạp incremental từ nguồn giả vào raw_thu_chi.

Điểm chính: insert dữ liệu và đẩy watermark dùng cùng PostgreSQL transaction.
Nếu bất kỳ bước nào lỗi, transaction rollback; lần chạy lại đọc đúng watermark cũ.
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


CONN_ID = "postgres_learn_dwh"
SOURCE_NAME = "nguon_thu_chi"


@dag(
    dag_id="buoi02_incremental_load",
    start_date=datetime(2026, 7, 13),
    schedule=None,
    catchup=False,
    tags=["hoc-airflow", "incremental", "watermark"],
)
def incremental_load():
    @task(retries=1)
    def load_new_source_rows() -> dict[str, str | int]:
        """Load rows newer than the watermark, then advance it atomically."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        connection = hook.get_conn()

        try:
            with connection.cursor() as cursor:
                # Lock this source's one watermark row so two overlapping DAG
                # runs cannot both read the same value and load the same slice.
                cursor.execute(
                    """
                    SELECT last_value
                    FROM etl_watermark
                    WHERE bang = %s
                    FOR UPDATE
                    """,
                    (SOURCE_NAME,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(f"Missing watermark for source: {SOURCE_NAME}")

                previous_watermark = row[0]
                cursor.execute(
                    """
                    SELECT MAX(updated_at)
                    FROM nguon_thu_chi
                    WHERE updated_at > %s
                    """,
                    (previous_watermark,),
                )
                next_watermark = cursor.fetchone()[0]

                if next_watermark is None:
                    connection.commit()
                    return {
                        "loaded_rows": 0,
                        "watermark": previous_watermark.isoformat(),
                    }

                cursor.execute(
                    """
                    INSERT INTO raw_thu_chi (
                        source_id, ma_don_vi, loai, so_tien, ngay, ghi_chu, source_updated_at
                    )
                    SELECT id, ma_don_vi, loai, so_tien, ngay, ghi_chu, updated_at
                    FROM nguon_thu_chi
                    WHERE updated_at > %s
                    ORDER BY updated_at, id
                    ON CONFLICT (source_id, source_updated_at) DO NOTHING
                    """,
                    (previous_watermark,),
                )
                loaded_rows = cursor.rowcount

                cursor.execute(
                    """
                    UPDATE etl_watermark
                    SET last_value = %s, updated_at = NOW()
                    WHERE bang = %s
                    """,
                    (next_watermark, SOURCE_NAME),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {
            "loaded_rows": loaded_rows,
            "watermark": next_watermark.isoformat(),
        }

    load_new_source_rows()


incremental_load()
