-- =========================================================================
-- Reference data + mapping rules for the QL Gia source.
--
-- Identifiers are English; the VALUES stay as they are in the real world —
-- Vietnamese commodity and locality names, and the source system's own codes.
-- =========================================================================

-- ── Commodities under regular price survey ───────────────────────────────
INSERT INTO refdata.commodity
    (commodity_code, commodity_name, commodity_group, unit_of_measure, specification)
VALUES
  ('CMD001','Gao te thuong',       'Luong thuc',  'kg',   'Loai 1'),
  ('CMD002','Gao tam thom',        'Luong thuc',  'kg',   'Loai 1'),
  ('CMD003','Thit lon hoi',        'Thuc pham',   'kg',   'Hoi'),
  ('CMD004','Thit bo than',        'Thuc pham',   'kg',   'Loai 1'),
  ('CMD005','Duong kinh trang',    'Thuc pham',   'kg',   'RS'),
  ('CMD006','Xi mang PCB40',       'Vat lieu XD', 'tan',  'Bao 50kg'),
  ('CMD007','Thep xay dung CB300', 'Vat lieu XD', 'tan',  'Phi 12'),
  ('CMD008','Cat vang xay dung',   'Vat lieu XD', 'm3',   NULL),
  ('CMD009','Phan bon Ure',        'Nong nghiep', 'tan',  'Bao 50kg'),
  ('CMD010','Xang RON95-III',      'Nhien lieu',  'lit',  NULL),
  ('CMD011','Dau DO 0,05S',        'Nhien lieu',  'lit',  NULL),
  ('CMD012','Gas LPG dan dung',    'Nhien lieu',  'binh', 'Binh 12kg')
ON CONFLICT (commodity_code) DO NOTHING;

-- ── Administrative units of Hung Yen province ────────────────────────────
INSERT INTO refdata.locality
    (locality_code, locality_name, admin_level, parent_code)
VALUES
  ('LOC00','Tinh Hung Yen',    'province', NULL),
  ('LOC01','TP Hung Yen',      'district', 'LOC00'),
  ('LOC02','Huyen Van Lam',    'district', 'LOC00'),
  ('LOC03','Huyen Van Giang',  'district', 'LOC00'),
  ('LOC04','Huyen Yen My',     'district', 'LOC00'),
  ('LOC05','Thi xa My Hao',    'district', 'LOC00'),
  ('LOC06','Huyen An Thi',     'district', 'LOC00'),
  ('LOC07','Huyen Khoai Chau', 'district', 'LOC00'),
  ('LOC08','Huyen Kim Dong',   'district', 'LOC00'),
  ('LOC09','Huyen Tien Lu',    'district', 'LOC00'),
  ('LOC10','Huyen Phu Cu',     'district', 'LOC00')
ON CONFLICT (locality_code) DO NOTHING;

-- ── Source vocabulary -> warehouse vocabulary ────────────────────────────
-- Two commodity codes are DELIBERATELY absent so the pipeline has something
-- real to put in metadata.mapping_rejections instead of an empty demo table.
INSERT INTO metadata.mapping_rules
    (source_code, code_type, source_value, standard_value)
VALUES
  ('qlgia','commodity','QLG-GAO-TE',     'CMD001'),
  ('qlgia','commodity','QLG-GAO-TAM',    'CMD002'),
  ('qlgia','commodity','QLG-LON-HOI',    'CMD003'),
  ('qlgia','commodity','QLG-BO-THAN',    'CMD004'),
  ('qlgia','commodity','QLG-DUONG',      'CMD005'),
  ('qlgia','commodity','QLG-XM-PCB40',   'CMD006'),
  ('qlgia','commodity','QLG-THEP-CB300', 'CMD007'),
  ('qlgia','commodity','QLG-CAT-VANG',   'CMD008'),
  ('qlgia','commodity','QLG-URE',        'CMD009'),
  ('qlgia','commodity','QLG-RON95',      'CMD010'),
  -- MISSING ON PURPOSE: QLG-DAU-DO -> CMD011, QLG-GAS -> CMD012

  ('qlgia','locality','QLG-TPHY',   'LOC01'),
  ('qlgia','locality','QLG-VLAM',   'LOC02'),
  ('qlgia','locality','QLG-VGIANG', 'LOC03'),
  ('qlgia','locality','QLG-YMY',    'LOC04'),
  ('qlgia','locality','QLG-MHAO',   'LOC05'),
  ('qlgia','locality','QLG-ATHI',   'LOC06'),
  ('qlgia','locality','QLG-KCHAU',  'LOC07'),
  ('qlgia','locality','QLG-KDONG',  'LOC08'),
  ('qlgia','locality','QLG-TLU',    'LOC09'),
  ('qlgia','locality','QLG-PCU',    'LOC10')
ON CONFLICT DO NOTHING;

-- ── Source registration ──────────────────────────────────────────────────
INSERT INTO ingestion.sources
    (source_code, description, ingest_method, load_mode,
     business_key, cursor_column, data_owner)
VALUES
  ('qlgia',
   'Commodity prices by survey period, Price Management system',
   'rest_api',
   'incremental',
   'commodity x locality x survey_period',
   'lastModified',
   'Phong Gia')
ON CONFLICT (source_code) DO NOTHING;
