-- ============================================================
-- DWH-03 setup: Raw cho chi + nguồn Dự toán
-- ============================================================

-- 1. Raw cho giao dịch CHI (bất biến, append-only)
CREATE TABLE IF NOT EXISTS raw_kbnn_giao_dich_chi (
    _load_id           BIGSERIAL PRIMARY KEY,
    _source_system     TEXT NOT NULL DEFAULT 'KBNN_PG',
    _loaded_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_updated_at TIMESTAMP,
    id                 BIGINT,
    ma_dvsdns          TEXT,
    so_tien            NUMERIC(18,0),
    ngay_chi           DATE,
    ma_tieu_muc        TEXT
);

GRANT SELECT, INSERT ON raw_kbnn_giao_dich_chi TO airflow_learn;

-- 2. Bảng DỰ TOÁN (kế hoạch ngân sách giao đầu năm cho từng đơn vị)
--    Đây là "kịch bản DT" để so với "thực hiện TH"
CREATE TABLE IF NOT EXISTS du_toan_ngan_sach (
    id           BIGSERIAL PRIMARY KEY,
    nam          INT NOT NULL,
    ma_dvsdns    TEXT NOT NULL,
    ma_tieu_muc  TEXT NOT NULL,
    so_tien_dt   NUMERIC(18,0) NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (nam, ma_dvsdns, ma_tieu_muc)
);

GRANT SELECT, INSERT, UPDATE ON du_toan_ngan_sach TO airflow_learn;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO airflow_learn;

-- 3. Bảng đối soát CHI (dự toán vs thực hiện)
CREATE TABLE IF NOT EXISTS doi_soat_chi (
    id            BIGSERIAL PRIMARY KEY,
    nam           INT NOT NULL,
    ma_dvsdns     TEXT NOT NULL,
    ten_dvsdns    TEXT,
    tong_du_toan  NUMERIC(18,0),
    tong_thuc_hien NUMERIC(18,0),
    con_lai       NUMERIC(18,0),
    ty_le_giai_ngan NUMERIC(5,2),   -- %
    trang_thai    TEXT,             -- TRONG_DU_TOAN / VUOT_DU_TOAN / CHUA_CO_DT
    kiem_tra_luc  TIMESTAMP DEFAULT NOW()
);

GRANT SELECT, INSERT, UPDATE, DELETE ON doi_soat_chi TO airflow_learn;

-- 4. Watermark cho nguồn dự toán
INSERT INTO etl_watermark_ms (source_system, source_table, last_value) VALUES
    ('DUTOAN_PG', 'du_toan_ngan_sach', '1900-01-01')
ON CONFLICT (source_system, source_table) DO NOTHING;

-- 5. Nạp dữ liệu DỰ TOÁN (giao đầu năm 2026 cho 5 đơn vị)
--    Cố ý để 1 đơn vị bị VƯỢT dự toán -> bài học phát hiện vượt chi
INSERT INTO du_toan_ngan_sach (nam, ma_dvsdns, ma_tieu_muc, so_tien_dt) VALUES
    -- Văn phòng UBND tỉnh: dự toán rộng rãi
    (2026, '1067001', '6001', 800000000),
    (2026, '1067001', '6101', 200000000),
    (2026, '1067001', '7001', 400000000),
    -- Sở Tài chính
    (2026, '1067002', '6001', 500000000),
    (2026, '1067002', '6051', 150000000),
    (2026, '1067002', '7051', 200000000),
    -- Sở Giáo dục: dự toán HẸP -> dễ vượt
    (2026, '1067003', '6001', 200000000),
    (2026, '1067003', '6101', 100000000),
    -- Sở Y tế
    (2026, '1067004', '6001', 600000000),
    (2026, '1067004', '7001', 300000000),
    -- UBND TP Hưng Yên
    (2026, '1067005', '6001', 400000000),
    (2026, '1067005', '6051', 100000000)
ON CONFLICT (nam, ma_dvsdns, ma_tieu_muc) DO UPDATE SET
    so_tien_dt = EXCLUDED.so_tien_dt,
    updated_at = NOW();

SELECT '=== Setup DWH-03 xong ===' AS info;
SELECT 'du_toan_ngan_sach' AS bang, COUNT(*) AS n, SUM(so_tien_dt) AS tong FROM du_toan_ngan_sach;
