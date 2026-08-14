-- BUỔI 3: Raw append-only / bất biến
-- Chạy trong database learn_dwh bằng role quản trị schema.

-- Airflow chỉ được đọc và thêm version mới vào Raw.
-- Không cho sửa, xóa hoặc truncate lịch sử đã ingest.
REVOKE UPDATE, DELETE, TRUNCATE
ON TABLE raw_thu_chi
FROM airflow_learn;

GRANT SELECT, INSERT
ON TABLE raw_thu_chi
TO airflow_learn;

-- Demo: hệ thống nguồn sửa Q2-001 từ 2 triệu thành 2,2 triệu.
-- Đây là UPDATE hợp lệ ở bảng nguồn. Raw không sửa dòng cũ; DAG sẽ
-- append một version mới nhờ source_updated_at mới.
UPDATE nguon_thu_chi
SET so_tien = 2200000,
    ghi_chu = 'Dieu chinh Thu Q2',
    updated_at = TIMESTAMP '2026-07-01 08:00:00'
WHERE id = 'Q2-001'
  AND updated_at < TIMESTAMP '2026-07-01 08:00:00';

-- Sau khi chạy DAG buoi02_incremental_load, phải thấy hai version:
SELECT source_id, so_tien, ghi_chu, source_updated_at, loaded_at
FROM raw_thu_chi
WHERE source_id = 'Q2-001'
ORDER BY source_updated_at;
