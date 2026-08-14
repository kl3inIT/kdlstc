-- BUỔI 4: Tạo lớp Staging trong schema riêng.
-- Chạy một lần trong database learn_dwh bằng role quản trị schema.

CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.thu_chi (
    source_id          TEXT PRIMARY KEY,
    ma_don_vi          TEXT NOT NULL,
    loai               TEXT NOT NULL CHECK (loai IN ('thu', 'chi')),
    so_tien            NUMERIC(15,2) NOT NULL,
    ngay               DATE NOT NULL,
    ghi_chu            TEXT,
    source_updated_at  TIMESTAMP NOT NULL,
    staged_at          TIMESTAMPTZ NOT NULL
);

-- Airflow chỉ cần dùng schema và đọc/nạp lại bảng Staging.
GRANT USAGE
ON SCHEMA stg
TO airflow_learn;

GRANT SELECT, INSERT, TRUNCATE
ON TABLE stg.thu_chi
TO airflow_learn;

