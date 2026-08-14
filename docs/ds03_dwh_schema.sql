-- ============================================================
-- KHO ĐÍCH (learn_dwh) — Mô hình kho dữ liệu tài chính tỉnh
-- Khuôn mượn từ AdventureWorksDW (FactFinance/DimAccount/DimScenario)
-- đổi sang Mục lục Ngân sách Nhà nước Việt Nam
-- ============================================================

-- ---------- LỚP RAW (bất biến, append-only) ----------
-- Giữ nguyên dữ liệu như nguồn, thêm cột kỹ thuật để truy vết
CREATE TABLE IF NOT EXISTS raw_tms_nnt (
    _load_id       BIGSERIAL PRIMARY KEY,
    _source_system TEXT NOT NULL DEFAULT 'TMS_ORACLE',
    _loaded_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_updated_at TIMESTAMP,           -- watermark từ nguồn
    mst            TEXT,
    ten_nnt        TEXT,
    dia_chi        TEXT,
    co_quan_thue   TEXT
);

CREATE TABLE IF NOT EXISTS raw_tms_khoan_nop (
    _load_id       BIGSERIAL PRIMARY KEY,
    _source_system TEXT NOT NULL DEFAULT 'TMS_ORACLE',
    _loaded_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_updated_at TIMESTAMP,
    id             BIGINT,
    to_khai_id     BIGINT,
    so_tien_nop    NUMERIC(18,0),
    ngay_nop       DATE,
    ma_tieu_muc    TEXT
);

CREATE TABLE IF NOT EXISTS raw_kbnn_giao_dich_thu (
    _load_id       BIGSERIAL PRIMARY KEY,
    _source_system TEXT NOT NULL DEFAULT 'KBNN_PG',
    _loaded_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    _source_updated_at TIMESTAMP,
    id             BIGINT,
    mst            TEXT,                    -- có thể lệch định dạng (có gạch)
    ten_nnt_kb     TEXT,                    -- có thể khác tên bên TMS
    so_tien        NUMERIC(18,0),
    ngay_thu       DATE,
    ma_tieu_muc    TEXT,
    ma_sac_thue    TEXT
);

-- ---------- WATERMARK (theo từng nguồn + bảng) ----------
CREATE TABLE IF NOT EXISTS etl_watermark_ms (
    source_system  TEXT NOT NULL,
    source_table   TEXT NOT NULL,
    last_value     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_system, source_table)
);

-- ---------- LỚP DIM (chiều) ----------

-- Mục lục NSNN: phân cấp Chương → Loại → Khoản → Mục → Tiểu mục
CREATE TABLE IF NOT EXISTS dim_muc_luc_nsnn (
    ma_tieu_muc    TEXT PRIMARY KEY,        -- mã tiểu mục (cấp thấp nhất)
    ten_tieu_muc   TEXT NOT NULL,
    ma_muc         TEXT,
    ten_muc        TEXT,
    ma_khoan       TEXT,
    ten_khoan      TEXT,
    ma_loai        TEXT,
    ten_loai       TEXT,
    ma_chuong      TEXT,
    ten_chuong     TEXT,
    tinh_chat      TEXT                     -- 'THU' hoặc 'CHI'
);

-- Người nộp thuế (đã dedup + chuẩn hóa MST từ 2 nguồn)
CREATE TABLE IF NOT EXISTS dim_nnt (
    mst            TEXT PRIMARY KEY,        -- MST đã chuẩn hóa (bỏ gạch)
    ten_nnt        TEXT NOT NULL,           -- tên chuẩn (ưu tiên nguồn TMS)
    dia_chi        TEXT,
    co_quan_thue   TEXT,
    _nguon_ten     TEXT,                    -- nguồn nào cung cấp tên chuẩn
    _co_xung_dot   BOOLEAN DEFAULT FALSE,   -- 2 nguồn ghi tên khác nhau?
    updated_at     TIMESTAMP DEFAULT NOW()
);

-- Đơn vị sử dụng ngân sách
CREATE TABLE IF NOT EXISTS dim_don_vi (
    ma_dvsdns      TEXT PRIMARY KEY,
    ten_dvsdns     TEXT NOT NULL,
    cap_ns         INT,
    updated_at     TIMESTAMP DEFAULT NOW()
);

-- Kịch bản: Dự toán / Thực hiện  (<= DimScenario của AdventureWorksDW)
CREATE TABLE IF NOT EXISTS dim_kich_ban (
    ma_kich_ban    TEXT PRIMARY KEY,
    ten_kich_ban   TEXT NOT NULL
);

-- Thời gian
CREATE TABLE IF NOT EXISTS dim_thoi_gian (
    ngay           DATE PRIMARY KEY,
    nam            INT NOT NULL,
    quy            INT NOT NULL,
    thang          INT NOT NULL,
    ky            TEXT NOT NULL             -- YYYY-MM
);

-- ---------- LỚP FACT (sự kiện) ----------

-- Thu ngân sách (hợp nhất từ TMS + KBNN)
CREATE TABLE IF NOT EXISTS fact_thu_ngan_sach (
    id             BIGSERIAL PRIMARY KEY,
    ngay           DATE NOT NULL REFERENCES dim_thoi_gian(ngay),
    mst            TEXT REFERENCES dim_nnt(mst),
    ma_tieu_muc    TEXT REFERENCES dim_muc_luc_nsnn(ma_tieu_muc),
    ma_kich_ban    TEXT REFERENCES dim_kich_ban(ma_kich_ban),
    so_tien        NUMERIC(18,0) NOT NULL,
    _source_system TEXT NOT NULL,           -- TMS hay KBNN
    _source_id     BIGINT,                  -- id gốc ở nguồn (truy vết)
    _ngay_nap      DATE NOT NULL,           -- partition key cho idempotency
    updated_at     TIMESTAMP DEFAULT NOW()
);

-- Chi ngân sách
CREATE TABLE IF NOT EXISTS fact_chi_ngan_sach (
    id             BIGSERIAL PRIMARY KEY,
    ngay           DATE NOT NULL REFERENCES dim_thoi_gian(ngay),
    ma_dvsdns      TEXT REFERENCES dim_don_vi(ma_dvsdns),
    ma_tieu_muc    TEXT REFERENCES dim_muc_luc_nsnn(ma_tieu_muc),
    ma_kich_ban    TEXT REFERENCES dim_kich_ban(ma_kich_ban),
    so_tien        NUMERIC(18,0) NOT NULL,
    _source_system TEXT NOT NULL,
    _source_id     BIGINT,
    _ngay_nap      DATE NOT NULL,
    updated_at     TIMESTAMP DEFAULT NOW()
);

-- ---------- QUARANTINE (dòng lỗi, không cho vào kho) ----------
CREATE TABLE IF NOT EXISTS quarantine_thu (
    id             BIGSERIAL PRIMARY KEY,
    _source_system TEXT,
    _source_id     BIGINT,
    ly_do_loi      TEXT NOT NULL,           -- vì sao bị chặn
    du_lieu_goc    JSONB,                   -- giữ nguyên dòng gốc để rà soát
    phat_hien_luc  TIMESTAMP DEFAULT NOW()
);

-- ---------- BẢNG ĐỐI SOÁT (reconciliation) ----------
CREATE TABLE IF NOT EXISTS doi_soat_thu (
    id             BIGSERIAL PRIMARY KEY,
    ky             TEXT NOT NULL,           -- kỳ đối soát YYYY-MM
    tong_tien_tms  NUMERIC(18,0),
    tong_tien_kbnn NUMERIC(18,0),
    chenh_lech     NUMERIC(18,0),
    so_ban_ghi_tms  INT,
    so_ban_ghi_kbnn INT,
    trang_thai     TEXT,                    -- KHOP / LECH
    kiem_tra_luc   TIMESTAMP DEFAULT NOW()
);

-- ---------- DỮ LIỆU DANH MỤC ----------

INSERT INTO dim_kich_ban (ma_kich_ban, ten_kich_ban) VALUES
    ('DT', 'Du toan'),
    ('TH', 'Thuc hien')
ON CONFLICT (ma_kich_ban) DO NOTHING;

-- Mục lục NSNN (mã thật theo TT324/2016/TT-BTC, rút gọn cho demo)
INSERT INTO dim_muc_luc_nsnn (ma_tieu_muc, ten_tieu_muc, ma_muc, ten_muc, ma_khoan, ten_khoan, ma_loai, ten_loai, ma_chuong, ten_chuong, tinh_chat) VALUES
    ('1001','Thue thu nhap ca nhan tu tien luong','1000','Thue thu nhap ca nhan','054','Hoat dong kinh doanh','050','Cong nghiep che bien','160','Cac don vi kinh te','THU'),
    ('1052','Thue TNDN cua don vi trong nuoc','1050','Thue thu nhap doanh nghiep','054','Hoat dong kinh doanh','050','Cong nghiep che bien','160','Cac don vi kinh te','THU'),
    ('1701','Thue GTGT hang san xuat kinh doanh trong nuoc','1700','Thue gia tri gia tang','054','Hoat dong kinh doanh','050','Cong nghiep che bien','160','Cac don vi kinh te','THU'),
    ('2863','Le phi mon bai bac 1','2850','Le phi quan ly nha nuoc','054','Hoat dong kinh doanh','050','Cong nghiep che bien','160','Cac don vi kinh te','THU'),
    ('6001','Tien luong theo ngach bac','6000','Tien luong','132','Quan ly nha nuoc','130','Hoat dong cua co quan QLNN','012','Van phong UBND','CHI'),
    ('6051','Phu cap chuc vu','6050','Phu cap luong','132','Quan ly nha nuoc','130','Hoat dong cua co quan QLNN','012','Van phong UBND','CHI'),
    ('6101','Tien dien','6100','Thanh toan dich vu cong cong','132','Quan ly nha nuoc','130','Hoat dong cua co quan QLNN','012','Van phong UBND','CHI'),
    ('7001','Chi mua sam tai san','7000','Chi mua sam','132','Quan ly nha nuoc','130','Hoat dong cua co quan QLNN','012','Van phong UBND','CHI'),
    ('7051','Chi sua chua tai san','7050','Chi sua chua','132','Quan ly nha nuoc','130','Hoat dong cua co quan QLNN','012','Van phong UBND','CHI')
ON CONFLICT (ma_tieu_muc) DO NOTHING;

-- Sinh dim_thoi_gian cho 2026
INSERT INTO dim_thoi_gian (ngay, nam, quy, thang, ky)
SELECT d::date,
       EXTRACT(YEAR FROM d)::int,
       EXTRACT(QUARTER FROM d)::int,
       EXTRACT(MONTH FROM d)::int,
       TO_CHAR(d, 'YYYY-MM')
FROM generate_series('2026-01-01'::date, '2026-12-31'::date, '1 day') AS d
ON CONFLICT (ngay) DO NOTHING;

-- Khởi tạo watermark cho 2 nguồn (mốc 0 = chưa nạp gì)
INSERT INTO etl_watermark_ms (source_system, source_table, last_value) VALUES
    ('TMS_ORACLE', 'nnt',            '1900-01-01'),
    ('TMS_ORACLE', 'khoan_nop',      '1900-01-01'),
    ('KBNN_PG',    'giao_dich_thu',  '1900-01-01'),
    ('KBNN_PG',    'giao_dich_chi',  '1900-01-01')
ON CONFLICT (source_system, source_table) DO NOTHING;

-- ---------- XÁC NHẬN ----------
SELECT '=== Bang da tao ===' AS info;
SELECT table_name FROM information_schema.tables
WHERE table_schema='public' AND table_name LIKE ANY(ARRAY['raw_%','dim_%','fact_%','etl_%','quarantine_%','doi_soat_%'])
ORDER BY table_name;

SELECT '=== Danh muc ===' AS info;
SELECT 'dim_muc_luc_nsnn' AS bang, COUNT(*) AS n FROM dim_muc_luc_nsnn
UNION ALL SELECT 'dim_kich_ban', COUNT(*) FROM dim_kich_ban
UNION ALL SELECT 'dim_thoi_gian', COUNT(*) FROM dim_thoi_gian
UNION ALL SELECT 'etl_watermark_ms', COUNT(*) FROM etl_watermark_ms;
