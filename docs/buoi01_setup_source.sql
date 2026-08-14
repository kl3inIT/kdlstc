-- ============================================================
-- BUỔI 1: Dựng nguồn giả + bảng watermark
-- Chạy trong database: learn_dwh
-- ============================================================

-- 1. Bảng NGUỒN (giả lập hệ thống nghiệp vụ của Sở Tài chính)
--    Có cột updated_at -> dùng làm watermark
DROP TABLE IF EXISTS nguon_thu_chi;
CREATE TABLE nguon_thu_chi (
    id          TEXT PRIMARY KEY,
    ma_don_vi   TEXT NOT NULL,
    loai        TEXT NOT NULL,          -- thu / chi
    so_tien     NUMERIC(15,2),
    ngay        DATE NOT NULL,
    ghi_chu     TEXT,
    updated_at  TIMESTAMP NOT NULL      -- <== WATERMARK COLUMN
);

-- Dữ liệu QUÝ 1 (coi như đã nạp vào kho từ trước)
INSERT INTO nguon_thu_chi (id, ma_don_vi, loai, so_tien, ngay, ghi_chu, updated_at) VALUES
  ('Q1-001', 'DV001', 'thu', 1000000, '2026-01-15', 'Thu Q1', '2026-01-15 08:00:00'),
  ('Q1-002', 'DV002', 'chi',  -300000, '2026-02-10', 'Chi Q1', '2026-02-10 09:00:00'),
  ('Q1-003', 'DV001', 'thu', 1500000, '2026-03-20', 'Thu Q1', '2026-03-20 10:00:00');

-- Dữ liệu QUÝ 2 (dữ liệu MỚI cần nạp incremental)
INSERT INTO nguon_thu_chi (id, ma_don_vi, loai, so_tien, ngay, ghi_chu, updated_at) VALUES
  ('Q2-001', 'DV003', 'thu', 2000000, '2026-04-05', 'Thu Q2', '2026-04-05 08:00:00'),
  ('Q2-002', 'DV001', 'chi',  -500000, '2026-05-12', 'Chi Q2', '2026-05-12 09:00:00'),
  ('Q2-003', 'DV004', 'thu', 3000000, '2026-06-25', 'Thu Q2', '2026-06-25 10:00:00');

-- 2. Bảng WATERMARK (nằm TRONG kho dữ liệu, không phải metadata DB của Airflow)
DROP TABLE IF EXISTS etl_watermark;
CREATE TABLE etl_watermark (
    bang        TEXT PRIMARY KEY,       -- tên bảng nguồn đang theo dõi
    last_value  TIMESTAMP NOT NULL,     -- mốc: đã nạp tới thời điểm này
    updated_at  TIMESTAMP DEFAULT NOW()
);

-- Khởi tạo mốc = cuối Q1. Nghĩa là: "đã nạp hết tới 2026-03-31".
-- => Lần chạy incremental tới sẽ CHỈ lấy dữ liệu updated_at > mốc này (tức Q2).
INSERT INTO etl_watermark (bang, last_value) VALUES
  ('nguon_thu_chi', '2026-03-31 23:59:59');

-- 3. Xác nhận
SELECT '=== NGUON (6 dong: 3 Q1 + 3 Q2) ===' AS info;
SELECT id, ma_don_vi, so_tien, ngay, updated_at FROM nguon_thu_chi ORDER BY updated_at;

SELECT '=== WATERMARK hien tai ===' AS info;
SELECT * FROM etl_watermark;

SELECT '=== Thu nghiem: dung watermark loc ra Q2 ===' AS info;
SELECT id, so_tien, updated_at
FROM nguon_thu_chi
WHERE updated_at > (SELECT last_value FROM etl_watermark WHERE bang = 'nguon_thu_chi')
ORDER BY updated_at;
