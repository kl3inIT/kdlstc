-- ============================================================
-- NGUỒN 1: Hệ thống Thuế (TMS) — Oracle
-- Mô phỏng hệ thống quản lý thuế của cơ quan thuế tỉnh
-- ============================================================

-- Dọn bảng cũ (idempotent)
BEGIN EXECUTE IMMEDIATE 'DROP TABLE tms.khoan_nop CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE tms.to_khai CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE tms.nnt CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE tms.sac_thue CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

-- Danh mục sắc thuế (loại thuế)
CREATE TABLE tms.sac_thue (
    ma_sac_thue   VARCHAR2(10) PRIMARY KEY,
    ten_sac_thue  VARCHAR2(100) NOT NULL
);

-- Người nộp thuế
CREATE TABLE tms.nnt (
    mst            VARCHAR2(20) PRIMARY KEY,   -- mã số thuế
    ten_nnt        VARCHAR2(200) NOT NULL,
    dia_chi        VARCHAR2(300),
    co_quan_thue   VARCHAR2(100),
    updated_at     TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL  -- watermark
);

-- Tờ khai thuế
CREATE TABLE tms.to_khai (
    id             NUMBER PRIMARY KEY,
    mst            VARCHAR2(20) NOT NULL,
    ma_sac_thue    VARCHAR2(10) NOT NULL,
    ky_khai        VARCHAR2(7) NOT NULL,        -- YYYY-MM
    so_tien_ke_khai NUMBER(18,0),
    ngay_khai      DATE NOT NULL,
    updated_at     TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_tk_nnt FOREIGN KEY (mst) REFERENCES tms.nnt(mst),
    CONSTRAINT fk_tk_sac FOREIGN KEY (ma_sac_thue) REFERENCES tms.sac_thue(ma_sac_thue)
);

-- Khoản nộp (thực nộp vào kho bạc)
CREATE TABLE tms.khoan_nop (
    id             NUMBER PRIMARY KEY,
    to_khai_id     NUMBER NOT NULL,
    so_tien_nop    NUMBER(18,0) NOT NULL,
    ngay_nop       DATE NOT NULL,
    ma_tieu_muc    VARCHAR2(10),                -- tiểu mục Mục lục NSNN
    updated_at     TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_kn_tk FOREIGN KEY (to_khai_id) REFERENCES tms.to_khai(id)
);

-- Dữ liệu danh mục sắc thuế
INSERT INTO tms.sac_thue VALUES ('GTGT', 'Thue gia tri gia tang');
INSERT INTO tms.sac_thue VALUES ('TNDN', 'Thue thu nhap doanh nghiep');
INSERT INTO tms.sac_thue VALUES ('TNCN', 'Thue thu nhap ca nhan');
INSERT INTO tms.sac_thue VALUES ('MON',  'Le phi mon bai');

COMMIT;

SELECT 'sac_thue' AS bang, COUNT(*) AS so_dong FROM tms.sac_thue;
