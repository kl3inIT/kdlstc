-- ============================================================
-- NGUỒN 2: Kho bạc Nhà nước (KBNN) — PostgreSQL (db: kbnn_src)
-- Mô phỏng hệ thống thu/chi ngân sách qua kho bạc
-- ============================================================

DROP TABLE IF EXISTS giao_dich_chi CASCADE;
DROP TABLE IF EXISTS giao_dich_thu CASCADE;
DROP TABLE IF EXISTS don_vi CASCADE;

-- Đơn vị sử dụng ngân sách
CREATE TABLE don_vi (
    ma_dvsdns    TEXT PRIMARY KEY,       -- mã đơn vị SDNS
    ten_dvsdns   TEXT NOT NULL,
    cap_ns       INT NOT NULL,           -- cấp ngân sách (1=TW,2=tỉnh,3=huyện)
    updated_at   TIMESTAMP DEFAULT NOW() NOT NULL
);

-- Giao dịch THU ngân sách (đối ứng khoản nộp bên thuế)
CREATE TABLE giao_dich_thu (
    id           BIGSERIAL PRIMARY KEY,
    mst          TEXT,                   -- MST người nộp (có thể lệch định dạng)
    ten_nnt_kb   TEXT,                   -- tên NNT ghi ở kho bạc (có thể khác TMS)
    so_tien      NUMERIC(18,0) NOT NULL,
    ngay_thu     DATE NOT NULL,
    ma_tieu_muc  TEXT,
    ma_sac_thue  TEXT,
    updated_at   TIMESTAMP DEFAULT NOW() NOT NULL
);

-- Giao dịch CHI ngân sách
CREATE TABLE giao_dich_chi (
    id           BIGSERIAL PRIMARY KEY,
    ma_dvsdns    TEXT NOT NULL REFERENCES don_vi(ma_dvsdns),
    so_tien      NUMERIC(18,0) NOT NULL,
    ngay_chi     DATE NOT NULL,
    ma_tieu_muc  TEXT,
    updated_at   TIMESTAMP DEFAULT NOW() NOT NULL
);

SELECT 'schema kbnn_src created' AS info;
