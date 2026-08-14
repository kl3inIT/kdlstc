"""Buổi 4 — làm sạch Raw, chọn version mới nhất và dựng lại Staging.

Raw giữ toàn bộ lịch sử. Staging chỉ giữ trạng thái sạch mới nhất của
mỗi source_id. TRUNCATE và INSERT nằm trong cùng PostgreSQL transaction,
nên nếu transform lỗi thì bản Staging cũ được khôi phục bởi rollback.
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


CONN_ID = "postgres_learn_dwh"


@dag(
    dag_id="buoi04_transform_staging",
    start_date=datetime(2026, 7, 14),
    schedule=None,
    catchup=False,
    tags=["hoc-airflow", "staging", "clean", "dedup"],
)
def transform_staging():
    @task(retries=1)
    def rebuild_staging() -> dict[str, int]:
        """Atomically replace Staging with the latest clean Raw versions."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        connection = hook.get_conn()

        try:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE stg.thu_chi")

                cursor.execute(
                    """
                    WITH ranked_raw AS (
                        SELECT
                            BTRIM(source_id) AS source_id,
                            UPPER(BTRIM(ma_don_vi)) AS ma_don_vi,
                            LOWER(BTRIM(loai)) AS loai,
                            so_tien,
                            ngay,
                            NULLIF(BTRIM(ghi_chu), '') AS ghi_chu,
                            source_updated_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY BTRIM(source_id)
                                ORDER BY source_updated_at DESC, loaded_at DESC
                            ) AS version_rank
                        FROM raw_thu_chi
                    )
                    INSERT INTO stg.thu_chi (
                        source_id,
                        ma_don_vi,
                        loai,
                        so_tien,
                        ngay,
                        ghi_chu,
                        source_updated_at,
                        staged_at
                    )
                    SELECT
                        source_id,
                        ma_don_vi,
                        loai,
                        so_tien,
                        ngay,
                        ghi_chu,
                        source_updated_at,
                        CURRENT_TIMESTAMP
                    FROM ranked_raw
                    WHERE version_rank = 1
                    ORDER BY source_id
                    """
                )
                staged_rows = cursor.rowcount

            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return {"staged_rows": staged_rows}

    rebuild_staging()


transform_staging()
