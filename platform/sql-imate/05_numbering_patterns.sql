-- Quy ước đánh số văn bản — dữ liệu, không phải hằng số trong mã.
--
-- Ba mẫu này đến từ Nghị định 30/2020, nhưng thực tế các đơn vị vẫn phát sinh
-- biến thể: một tỉnh sáp nhập, một cơ quan đổi cách ghi, và tự nhiên có 219 văn
-- bản không mẫu nào khớp. Khi ấy việc phải làm là thêm một mẫu — và nếu mẫu nằm
-- trong mã Python thì thêm một mẫu là một lần review code và một lần triển khai.
--
-- Cùng lý do đã đưa ngưỡng chất lượng vào metadata.quality_rules: đây là quyết
-- định nghiệp vụ, có người sở hữu, và người sở hữu ấy không phải kỹ sư.

CREATE TABLE IF NOT EXISTS refdata.numbering_pattern (
    pattern_code text PRIMARY KEY,
    description  text NOT NULL,

    -- Biểu thức chính quy, cú pháp POSIX của PostgreSQL.
    regex        text NOT NULL,

    -- Nhóm bắt nào ứng với cái gì. NULL nghĩa là mẫu này không mang thông tin đó.
    -- Ví dụ công văn không mang ký hiệu loại, nên kind_group là NULL còn phần
    -- đứng ở vị trí ký hiệu chính là CƠ QUAN — nhận nhầm chỗ này từng sinh ra
    -- 186 loại văn bản rác, trong đó có "UBND".
    serial_group integer NOT NULL,
    year_group   integer,
    kind_group   integer,
    body_group   integer,

    -- Mẫu có ký hiệu loại thì phải tra danh mục ĐÓNG; không có thì mặc định là
    -- công văn.
    default_kind text,

    try_order    integer NOT NULL,
    is_active    boolean NOT NULL DEFAULT true,
    approved_by  text,
    approved_at  timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN refdata.numbering_pattern.try_order IS
  'Thu tu thu mau; mau hep thu truoc mau rong';
COMMENT ON COLUMN refdata.numbering_pattern.default_kind IS
  'Loai mac dinh khi mau khong mang ky hieu loai — cong van';

INSERT INTO refdata.numbering_pattern
    (pattern_code, description, regex, serial_group, year_group,
     kind_group, body_group, default_kind, try_order, approved_by, approved_at)
VALUES
  ('A', 'so/[nam/]KYHIEU-COQUAN — dang pho bien nhat',
   '^\s*(\d+)\s*/\s*(?:(\d{4})\s*/\s*)?([A-Za-zĐđÂâÊêÔôƠơƯư\.]{1,8})\s*-\s*(.+?)\s*$',
   1, 2, 3, 4, NULL, 10, 'nhom-ky-thuat', now()),

  ('B', 'so-KYHIEU/COQUAN[#ma] — thuong gap o khoi Dang',
   '^\s*(\d+)\s*-\s*([A-Za-zĐđÂâÊêÔôƠơƯư\.]{1,8})\s*/\s*([^#]+?)\s*(?:#(.*))?$',
   1, NULL, 2, 3, NULL, 20, 'nhom-ky-thuat', now()),

  ('C_cv', 'so/COQUAN — cong van, KHONG mang ky hieu loai',
   '^\s*(\d+)\s*/\s*([A-Za-zĐđÂâÊêÔôƠơƯư\.]{2,12})(?:\s*-\s*(.+?))?\s*$',
   1, NULL, NULL, 2, 'CV', 30, 'nhom-ky-thuat', now())

ON CONFLICT (pattern_code) DO UPDATE SET
    description  = EXCLUDED.description,
    regex        = EXCLUDED.regex,
    serial_group = EXCLUDED.serial_group,
    year_group   = EXCLUDED.year_group,
    kind_group   = EXCLUDED.kind_group,
    body_group   = EXCLUDED.body_group,
    default_kind = EXCLUDED.default_kind,
    try_order    = EXCLUDED.try_order;
