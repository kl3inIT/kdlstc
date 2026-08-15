-- =========================================================================
-- Seed for the iMate slice: document kinds and the well-known issuing bodies.
--
-- The kind list is CLOSED: these abbreviations are fixed by Nghi dinh
-- 30/2020/ND-CP (Phu luc I) plus the legislative kinds that arrive from
-- above (luat, nghi dinh, thong tu). An abbreviation not in this table is a
-- mapping gap somebody owes a decision on — never silently invented.
--
-- The issuing-body list is OPEN: 339 distinct codes were measured in one
-- tenant and new ones appear with every senders' reorganisation. Bodies are
-- therefore auto-registered by the pipeline with is_confirmed = false; this
-- seed only gives readable names to the most frequent ones. Every name here
-- is still inferred from the code, so none of them is confirmed either.
--
-- Idempotent: safe to re-run.
-- =========================================================================

INSERT INTO refdata.document_kind (kind_code, kind_name, kind_group, sort_order) VALUES
  -- van ban quy pham phap luat (den tu cap tren)
  ('L',    'Luật',                  'quy_pham', 1),
  ('PL',   'Pháp lệnh',             'quy_pham', 2),
  ('NĐ',   'Nghị định',             'quy_pham', 3),
  ('TT',   'Thông tư',              'quy_pham', 4),
  ('TTLT', 'Thông tư liên tịch',    'quy_pham', 5),
  -- van ban hanh chinh (ND 30/2020, Phu luc I)
  ('NQ',   'Nghị quyết',            'hanh_chinh', 10),
  ('QĐ',   'Quyết định',            'hanh_chinh', 11),
  ('QD',   'Quyết định',            'hanh_chinh', 11),  -- viet khong dau van gap
  ('CT',   'Chỉ thị',               'hanh_chinh', 12),
  ('QC',   'Quy chế',               'hanh_chinh', 13),
  ('TB',   'Thông báo',             'hanh_chinh', 14),
  ('HD',   'Hướng dẫn',             'hanh_chinh', 15),
  ('CTR',  'Chương trình',          'hanh_chinh', 16),
  ('KH',   'Kế hoạch',              'hanh_chinh', 17),
  ('PA',   'Phương án',             'hanh_chinh', 18),
  ('ĐA',   'Đề án',                 'hanh_chinh', 19),
  ('DA',   'Dự án',                 'hanh_chinh', 20),
  ('BC',   'Báo cáo',               'hanh_chinh', 21),
  ('BB',   'Biên bản',              'hanh_chinh', 22),
  ('TTR',  'Tờ trình',              'hanh_chinh', 23),
  ('HĐ',   'Hợp đồng',              'hanh_chinh', 24),
  ('CV',   'Công văn',              'hanh_chinh', 25),
  ('CĐ',   'Công điện',             'hanh_chinh', 26),
  ('GM',   'Giấy mời',              'hanh_chinh', 27),
  ('GUQ',  'Giấy ủy quyền',         'hanh_chinh', 28),
  ('GGT',  'Giấy giới thiệu',       'hanh_chinh', 29),
  ('GNP',  'Giấy nghỉ phép',        'hanh_chinh', 30),
  ('GBN',  'Giấy biên nhận',        'hanh_chinh', 31),
  ('GCN',  'Giấy chứng nhận',       'hanh_chinh', 32),
  ('GP',   'Giấy phép',             'hanh_chinh', 33),
  ('PC',   'Phiếu chuyển',          'hanh_chinh', 34),
  ('PG',   'Phiếu gửi',             'hanh_chinh', 35),
  ('PB',   'Phiếu báo',             'hanh_chinh', 36),
  ('KL',   'Kết luận',              'hanh_chinh', 37),
  ('SL',   'Sao lục',               'hanh_chinh', 38),
  ('ĐL',   'Điều lệ',               'hanh_chinh', 39),
  ('TL',   'Tài liệu',              'hanh_chinh', 40),
  ('LT',   'Lịch tuần',             'hanh_chinh', 41)
ON CONFLICT (kind_code) DO UPDATE
  SET kind_name = EXCLUDED.kind_name,
      kind_group = EXCLUDED.kind_group,
      sort_order = EXCLUDED.sort_order;

-- Most frequent issuing bodies of tenant doit.phuyen, names inferred from
-- the codes — hence is_confirmed = false on every row. Confirmation needs
-- /api/publishers, which sits behind a JWT we do not yet hold.
INSERT INTO refdata.issuing_body (body_code, body_name, body_level, is_confirmed) VALUES
  ('UBND',    'UBND tỉnh Phú Yên',                          'tinh',          false),
  ('VPUBND',  'Văn phòng UBND tỉnh',                        'tinh',          false),
  ('HĐND',    'HĐND tỉnh Phú Yên',                          'tinh',          false),
  ('TU',      'Tỉnh ủy Phú Yên',                            'dang_doan_the', false),
  ('ĐU',      'Đảng ủy',                                    'dang_doan_the', false),
  ('STC',     'Sở Tài chính',                               'so_nganh',      false),
  ('SCT',     'Sở Công Thương',                             'so_nganh',      false),
  ('SNV',     'Sở Nội vụ',                                  'so_nganh',      false),
  ('SXD',     'Sở Xây dựng',                                'so_nganh',      false),
  ('SYT',     'Sở Y tế',                                    'so_nganh',      false),
  ('STP',     'Sở Tư pháp',                                 'so_nganh',      false),
  ('SNN',     'Sở Nông nghiệp và PTNT',                     'so_nganh',      false),
  ('SNNMT',   'Sở Nông nghiệp và Môi trường',               'so_nganh',      false),
  ('STNMT',   'Sở Tài nguyên và Môi trường',                'so_nganh',      false),
  ('SKHCN',   'Sở Khoa học và Công nghệ',                   'so_nganh',      false),
  ('SKHĐT',   'Sở Kế hoạch và Đầu tư',                      'so_nganh',      false),
  ('SGDĐT',   'Sở Giáo dục và Đào tạo',                     'so_nganh',      false),
  ('SVHTTDL', 'Sở Văn hóa, Thể thao và Du lịch',            'so_nganh',      false),
  ('CAT',     'Công an tỉnh',                               'so_nganh',      false),
  ('BCHQS',   'Bộ Chỉ huy Quân sự tỉnh',                    'so_nganh',      false),
  ('BCT',     'Bộ Công Thương',                             'trung_uong',    false),
  ('QLTT',    'Cục Quản lý thị trường',                     'so_nganh',      false),
  ('QLTTPY',  'Cục QLTT Phú Yên',                           'so_nganh',      false),
  ('KKT',     'Ban Quản lý Khu kinh tế',                    'so_nganh',      false),
  ('PYPC',    'Công ty Điện lực Phú Yên',                   'khac',          false),
  ('TTTN',    'Trung tâm',                                  'khac',          false)
ON CONFLICT (body_code) DO NOTHING;
