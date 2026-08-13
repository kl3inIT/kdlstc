-- =========================================================================
-- Reference data for the revenue side of TABMIS.
--
-- Revenue shares curated.fact_budget with expenditure, so this file adds what
-- the revenue rows need: the collecting agencies, the revenue half of the
-- chart of accounts, tax types and revenue sources — plus the "not applicable"
-- row in every dimension that only one flow uses.
-- =========================================================================

-- ── "Not applicable" rows ────────────────────────────────────────────────
-- A foreign key pointing at an explicit not-applicable row instead of NULL.
-- The difference shows up the first time somebody groups by tax type: with a
-- sentinel the expenditure rows land in a visible bucket and the total still
-- reconciles; with NULL they vanish from the result and the report quietly
-- disagrees with every other report.
INSERT INTO refdata.funding_source (funding_code, funding_name, funding_group)
VALUES ('NKP00','Khong ap dung (dong thu NSNN)','n/a')
ON CONFLICT (funding_code) DO NOTHING;

INSERT INTO refdata.expense_sector (sector_code, sector_name, sort_order)
VALUES ('LV00','Khong ap dung (dong thu NSNN)', 0)
ON CONFLICT (sector_code) DO NOTHING;

-- ── Tax types ────────────────────────────────────────────────────────────
INSERT INTO refdata.tax_type (tax_type_code, tax_type_name, sort_order) VALUES
  ('TX00','Khong ap dung (dong chi NSNN)',            0),
  ('TX01','Thue gia tri gia tang',                     1),
  ('TX02','Thue thu nhap doanh nghiep',                2),
  ('TX03','Thue thu nhap ca nhan',                     3),
  ('TX04','Thue tieu thu dac biet',                    4),
  ('TX05','Thue tai nguyen',                           5),
  ('TX06','Thue su dung dat phi nong nghiep',          6),
  ('TX07','Le phi truoc ba',                           7),
  ('TX08','Thue bao ve moi truong',                    8),
  ('TX09','Phi va le phi',                             9),
  ('TX10','Thu tien su dung dat, thue dat',           10),
  ('TX11','Thu khac ngan sach',                       11)
ON CONFLICT (tax_type_code) DO NOTHING;

-- ── Revenue sources ──────────────────────────────────────────────────────
INSERT INTO refdata.revenue_source (revenue_source_code, revenue_source_name) VALUES
  ('NT00','Khong ap dung (dong chi NSNN)'),
  ('NT01','Thu noi dia'),
  ('NT02','Thu tu hoat dong xuat nhap khau'),
  ('NT03','Thu tu dau tho'),
  ('NT04','Thu vien tro khong hoan lai')
ON CONFLICT (revenue_source_code) DO NOTHING;

-- ── Revenue half of the chart of accounts, fiscal year 2026 ──────────────
-- Revenue and expenditure occupy disjoint code ranges (revenue in the 1000s
-- and 2000s, expenditure in the 6000s upward), which is why one budget_line
-- table serves both and flow_type can be trusted as a discriminator.
INSERT INTO refdata.budget_line
  (line_code, fiscal_year, category_code, subcategory_code, category_name,
   item_code, item_name, line_name, flow_type)
VALUES
  ('1001',2026,'010','011','Thue thu tu hang hoa, dich vu','1000','Thue gia tri gia tang','Thue GTGT hang san xuat kinh doanh trong nuoc','revenue'),
  ('1002',2026,'010','011','Thue thu tu hang hoa, dich vu','1000','Thue gia tri gia tang','Thue GTGT hang nhap khau','revenue'),
  ('1004',2026,'010','011','Thue thu tu hang hoa, dich vu','1000','Thue gia tri gia tang','Thue GTGT hoat dong xay dung co ban','revenue'),
  ('1052',2026,'010','012','Thue thu tu thu nhap','1050','Thue thu nhap doanh nghiep','Thue TNDN tu hoat dong san xuat kinh doanh','revenue'),
  ('1053',2026,'010','012','Thue thu tu thu nhap','1050','Thue thu nhap doanh nghiep','Thue TNDN tu chuyen nhuong bat dong san','revenue'),
  ('1151',2026,'010','012','Thue thu tu thu nhap','1150','Thue thu nhap ca nhan','Thue TNCN tu tien luong, tien cong','revenue'),
  ('1154',2026,'010','012','Thue thu tu thu nhap','1150','Thue thu nhap ca nhan','Thue TNCN tu chuyen nhuong bat dong san','revenue'),
  ('1156',2026,'010','012','Thue thu tu thu nhap','1150','Thue thu nhap ca nhan','Thue TNCN tu hoat dong san xuat kinh doanh','revenue'),
  ('1301',2026,'010','011','Thue thu tu hang hoa, dich vu','1300','Thue tieu thu dac biet','Thue TTDB hang san xuat trong nuoc','revenue'),
  ('1401',2026,'010','013','Thue thu tu tai san, tai nguyen','1400','Thue tai nguyen','Thue tai nguyen (tru dau, khi)','revenue'),
  ('1551',2026,'010','013','Thue thu tu tai san, tai nguyen','1550','Thue su dung dat phi nong nghiep','Thue su dung dat phi nong nghiep','revenue'),
  ('1601',2026,'010','013','Thue thu tu tai san, tai nguyen','1600','Le phi truoc ba','Le phi truoc ba nha, dat','revenue'),
  ('1602',2026,'010','013','Thue thu tu tai san, tai nguyen','1600','Le phi truoc ba','Le phi truoc ba o to, xe may','revenue'),
  ('1701',2026,'010','014','Thue bao ve moi truong','1700','Thue bao ve moi truong','Thue bao ve moi truong doi voi xang, dau','revenue'),
  ('2001',2026,'020','021','Thu tu dat, tai san cong','2000','Thu tien su dung dat','Thu tien su dung dat','revenue'),
  ('2011',2026,'020','021','Thu tu dat, tai san cong','2010','Thu tien thue dat','Thu tien thue dat, thue mat nuoc','revenue'),
  ('2801',2026,'030','031','Phi, le phi','2800','Phi thuoc linh vuc kinh te','Phi thuoc linh vuc tai nguyen, moi truong','revenue'),
  ('2803',2026,'030','031','Phi, le phi','2800','Phi thuoc linh vuc kinh te','Le phi quan ly nha nuoc','revenue'),
  ('4902',2026,'090','091','Thu khac','4900','Thu khac ngan sach','Thu tien phat vi pham hanh chinh','revenue'),
  ('4949',2026,'090','091','Thu khac','4900','Thu khac ngan sach','Cac khoan thu khac','revenue')
ON CONFLICT (line_code, fiscal_year) DO NOTHING;

-- ── Collecting agencies ──────────────────────────────────────────────────
-- On a revenue row, unit_code is the agency that collected the money, not a
-- unit that spent it. Same column, different meaning per flow — which is
-- exactly what flow_type is there to keep straight.
INSERT INTO refdata.budget_unit
  (unit_code, unit_name, short_name, unit_level, parent_code, locality_code, org_type)
VALUES
  ('1054301','Cuc Thue tinh Hung Yen',        'CT tinh','department','1054001','LOC00','state_admin'),
  ('1054320','Chi cuc Hai quan Hung Yen',     'CCHQ',   'department','1054001','LOC00','state_admin'),
  ('1054330','Kho bac Nha nuoc tinh Hung Yen','KBNN',   'department','1054001','LOC00','state_admin')
ON CONFLICT (unit_code) DO NOTHING;

-- One district tax office per district, mirroring the finance offices.
INSERT INTO refdata.budget_unit
  (unit_code, unit_name, short_name, unit_level, parent_code, locality_code, org_type)
SELECT
  '10543' || lpad((40 + row_number() OVER (ORDER BY l.locality_code))::text, 2, '0'),
  'Chi cuc Thue ' || l.locality_name,
  'CCT ' || l.locality_code,
  'district',
  '1054301',
  l.locality_code,
  'state_admin'
FROM refdata.locality l
WHERE l.admin_level = 'district'
ON CONFLICT (unit_code) DO NOTHING;
