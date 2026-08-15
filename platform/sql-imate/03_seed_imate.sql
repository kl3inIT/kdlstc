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
  ('L',    'Luat',                  'quy_pham', 1),
  ('PL',   'Phap lenh',             'quy_pham', 2),
  ('NĐ',   'Nghi dinh',             'quy_pham', 3),
  ('TT',   'Thong tu',              'quy_pham', 4),
  ('TTLT', 'Thong tu lien tich',    'quy_pham', 5),
  -- van ban hanh chinh (ND 30/2020, Phu luc I)
  ('NQ',   'Nghi quyet',            'hanh_chinh', 10),
  ('QĐ',   'Quyet dinh',            'hanh_chinh', 11),
  ('QD',   'Quyet dinh',            'hanh_chinh', 11),  -- viet khong dau van gap
  ('CT',   'Chi thi',               'hanh_chinh', 12),
  ('QC',   'Quy che',               'hanh_chinh', 13),
  ('TB',   'Thong bao',             'hanh_chinh', 14),
  ('HD',   'Huong dan',             'hanh_chinh', 15),
  ('CTR',  'Chuong trinh',          'hanh_chinh', 16),
  ('KH',   'Ke hoach',              'hanh_chinh', 17),
  ('PA',   'Phuong an',             'hanh_chinh', 18),
  ('ĐA',   'De an',                 'hanh_chinh', 19),
  ('DA',   'Du an',                 'hanh_chinh', 20),
  ('BC',   'Bao cao',               'hanh_chinh', 21),
  ('BB',   'Bien ban',              'hanh_chinh', 22),
  ('TTR',  'To trinh',              'hanh_chinh', 23),
  ('HĐ',   'Hop dong',              'hanh_chinh', 24),
  ('CV',   'Cong van',              'hanh_chinh', 25),
  ('CĐ',   'Cong dien',             'hanh_chinh', 26),
  ('GM',   'Giay moi',              'hanh_chinh', 27),
  ('GUQ',  'Giay uy quyen',         'hanh_chinh', 28),
  ('GGT',  'Giay gioi thieu',       'hanh_chinh', 29),
  ('GNP',  'Giay nghi phep',        'hanh_chinh', 30),
  ('GBN',  'Giay bien nhan',        'hanh_chinh', 31),
  ('GCN',  'Giay chung nhan',       'hanh_chinh', 32),
  ('GP',   'Giay phep',             'hanh_chinh', 33),
  ('PC',   'Phieu chuyen',          'hanh_chinh', 34),
  ('PG',   'Phieu gui',             'hanh_chinh', 35),
  ('PB',   'Phieu bao',             'hanh_chinh', 36),
  ('KL',   'Ket luan',              'hanh_chinh', 37),
  ('SL',   'Sao luc',               'hanh_chinh', 38),
  ('ĐL',   'Dieu le',               'hanh_chinh', 39),
  ('TL',   'Tai lieu',              'hanh_chinh', 40),
  ('LT',   'Lich tuan',             'hanh_chinh', 41)
ON CONFLICT (kind_code) DO UPDATE
  SET kind_name = EXCLUDED.kind_name,
      kind_group = EXCLUDED.kind_group,
      sort_order = EXCLUDED.sort_order;

-- Most frequent issuing bodies of tenant doit.phuyen, names inferred from
-- the codes — hence is_confirmed = false on every row. Confirmation needs
-- /api/publishers, which sits behind a JWT we do not yet hold.
INSERT INTO refdata.issuing_body (body_code, body_name, body_level, is_confirmed) VALUES
  ('UBND',    'UBND tinh Phu Yen',                          'tinh',          false),
  ('VPUBND',  'Van phong UBND tinh',                        'tinh',          false),
  ('HĐND',    'HDND tinh Phu Yen',                          'tinh',          false),
  ('TU',      'Tinh uy Phu Yen',                            'dang_doan_the', false),
  ('ĐU',      'Dang uy',                                    'dang_doan_the', false),
  ('STC',     'So Tai chinh',                               'so_nganh',      false),
  ('SCT',     'So Cong Thuong',                             'so_nganh',      false),
  ('SNV',     'So Noi vu',                                  'so_nganh',      false),
  ('SXD',     'So Xay dung',                                'so_nganh',      false),
  ('SYT',     'So Y te',                                    'so_nganh',      false),
  ('STP',     'So Tu phap',                                 'so_nganh',      false),
  ('SNN',     'So Nong nghiep va PTNT',                     'so_nganh',      false),
  ('SNNMT',   'So Nong nghiep va Moi truong',               'so_nganh',      false),
  ('STNMT',   'So Tai nguyen va Moi truong',                'so_nganh',      false),
  ('SKHCN',   'So Khoa hoc va Cong nghe',                   'so_nganh',      false),
  ('SKHĐT',   'So Ke hoach va Dau tu',                      'so_nganh',      false),
  ('SGDĐT',   'So Giao duc va Dao tao',                     'so_nganh',      false),
  ('SVHTTDL', 'So Van hoa, The thao va Du lich',            'so_nganh',      false),
  ('CAT',     'Cong an tinh',                               'so_nganh',      false),
  ('BCHQS',   'Bo Chi huy Quan su tinh',                    'so_nganh',      false),
  ('BCT',     'Bo Cong Thuong',                             'trung_uong',    false),
  ('QLTT',    'Cuc Quan ly thi truong',                     'so_nganh',      false),
  ('QLTTPY',  'Cuc QLTT Phu Yen',                           'so_nganh',      false),
  ('KKT',     'Ban Quan ly Khu kinh te',                    'so_nganh',      false),
  ('PYPC',    'Cong ty Dien luc Phu Yen',                   'khac',          false),
  ('TTTN',    'Trung tam',                                  'khac',          false)
ON CONFLICT (body_code) DO NOTHING;
