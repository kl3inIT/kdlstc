-- =========================================================================
-- Reference data for TABMIS.
--
-- Identifiers are English; the VALUES are Vietnamese because that is what they
-- are in the real world — unit names, chart-of-accounts wording, sectors.
-- Codes follow the shape of the real state budget chart of accounts closely
-- enough to be recognisable, without claiming to be an authoritative copy.
-- =========================================================================

-- ── Spending sectors ─────────────────────────────────────────────────────
INSERT INTO refdata.expense_sector (sector_code, sector_name, sort_order) VALUES
  ('LV01','Quoc phong',                        1),
  ('LV02','An ninh va trat tu an toan xa hoi', 2),
  ('LV03','Giao duc - dao tao va day nghe',    3),
  ('LV04','Khoa hoc va cong nghe',             4),
  ('LV05','Y te, dan so va gia dinh',          5),
  ('LV06','Van hoa thong tin',                 6),
  ('LV07','Phat thanh, truyen hinh, thong tan',7),
  ('LV08','The duc the thao',                  8),
  ('LV09','Bao ve moi truong',                 9),
  ('LV10','Cac hoat dong kinh te',            10),
  ('LV11','Hoat dong cua co quan quan ly nha nuoc', 11),
  ('LV12','Bao dam xa hoi',                   12)
ON CONFLICT (sector_code) DO NOTHING;

-- ── Funding sources ──────────────────────────────────────────────────────
INSERT INTO refdata.funding_source (funding_code, funding_name, funding_group) VALUES
  ('NKP01','Nguon ngan sach nha nuoc trong nuoc', 'domestic'),
  ('NKP02','Nguon thu su nghiep duoc de lai',     'domestic'),
  ('NKP03','Nguon vien tro, ODA',                 'aid'),
  ('NKP04','Nguon trai phieu chinh quyen dia phuong','bond'),
  ('NKP05','Nguon cai cach tien luong',           'domestic'),
  ('NKP06','Nguon khac',                          'other')
ON CONFLICT (funding_code) DO NOTHING;

-- ── Chapters — the managing body axis ────────────────────────────────────
INSERT INTO refdata.budget_chapter (chapter_code, chapter_name, admin_level) VALUES
  ('400','Cac co quan chinh quyen dia phuong',      'province'),
  ('422','So Giao duc va Dao tao',                  'province'),
  ('423','So Y te',                                 'province'),
  ('424','So Van hoa, The thao va Du lich',         'province'),
  ('426','So Khoa hoc va Cong nghe',                'province'),
  ('428','So Lao dong - Thuong binh va Xa hoi',     'province'),
  ('430','So Tai chinh',                            'province'),
  ('432','So Ke hoach va Dau tu',                   'province'),
  ('434','So Nong nghiep va Phat trien nong thon',  'province'),
  ('436','So Giao thong van tai',                   'province'),
  ('438','So Xay dung',                             'province'),
  ('440','So Tai nguyen va Moi truong',             'province'),
  ('442','So Cong Thuong',                          'province'),
  ('444','So Noi vu',                               'province'),
  ('446','So Tu phap',                              'province'),
  ('448','Thanh tra tinh',                          'province'),
  ('460','Cong an tinh',                            'province'),
  ('462','Bo Chi huy Quan su tinh',                 'province'),
  ('470','Cac don vi cap huyen',                    'district')
ON CONFLICT (chapter_code) DO NOTHING;

-- ── Budget lines — economic nature, fiscal year 2026 ─────────────────────
INSERT INTO refdata.budget_line
  (line_code, fiscal_year, category_code, subcategory_code, category_name,
   item_code, item_name, line_name, flow_type)
VALUES
  ('6001',2026,'340','341','Hoat dong cua co quan nha nuoc','6000','Tien luong','Luong ngach bac theo quy luong duoc duyet','expense'),
  ('6003',2026,'340','341','Hoat dong cua co quan nha nuoc','6000','Tien luong','Luong hop dong','expense'),
  ('6051',2026,'340','341','Hoat dong cua co quan nha nuoc','6050','Tien cong','Tien cong tra cho lao dong thuong xuyen theo hop dong','expense'),
  ('6101',2026,'340','341','Hoat dong cua co quan nha nuoc','6100','Phu cap luong','Phu cap chuc vu','expense'),
  ('6103',2026,'340','341','Hoat dong cua co quan nha nuoc','6100','Phu cap luong','Phu cap tham nien vuot khung','expense'),
  ('6112',2026,'340','341','Hoat dong cua co quan nha nuoc','6100','Phu cap luong','Phu cap uu dai nghe','expense'),
  ('6115',2026,'340','341','Hoat dong cua co quan nha nuoc','6100','Phu cap luong','Phu cap lam dem, lam them gio','expense'),
  ('6201',2026,'340','341','Hoat dong cua co quan nha nuoc','6200','Tien thuong','Thuong thuong xuyen','expense'),
  ('6251',2026,'340','341','Hoat dong cua co quan nha nuoc','6250','Phuc loi tap the','Tro cap kho khan thuong xuyen','expense'),
  ('6301',2026,'340','341','Hoat dong cua co quan nha nuoc','6300','Cac khoan dong gop','Bao hiem xa hoi','expense'),
  ('6302',2026,'340','341','Hoat dong cua co quan nha nuoc','6300','Cac khoan dong gop','Bao hiem y te','expense'),
  ('6303',2026,'340','341','Hoat dong cua co quan nha nuoc','6300','Cac khoan dong gop','Kinh phi cong doan','expense'),
  ('6304',2026,'340','341','Hoat dong cua co quan nha nuoc','6300','Cac khoan dong gop','Bao hiem that nghiep','expense'),
  ('6401',2026,'340','341','Hoat dong cua co quan nha nuoc','6400','Thanh toan khac cho ca nhan','Cac khoan thanh toan khac cho ca nhan','expense'),
  ('6501',2026,'340','341','Hoat dong cua co quan nha nuoc','6500','Thanh toan dich vu cong cong','Tien dien','expense'),
  ('6502',2026,'340','341','Hoat dong cua co quan nha nuoc','6500','Thanh toan dich vu cong cong','Tien nuoc','expense'),
  ('6503',2026,'340','341','Hoat dong cua co quan nha nuoc','6500','Thanh toan dich vu cong cong','Nhien lieu','expense'),
  ('6504',2026,'340','341','Hoat dong cua co quan nha nuoc','6500','Thanh toan dich vu cong cong','Ve sinh moi truong','expense'),
  ('6551',2026,'340','341','Hoat dong cua co quan nha nuoc','6550','Vat tu van phong','Van phong pham','expense'),
  ('6552',2026,'340','341','Hoat dong cua co quan nha nuoc','6550','Vat tu van phong','Mua sam cong cu, dung cu van phong','expense'),
  ('6601',2026,'340','341','Hoat dong cua co quan nha nuoc','6600','Thong tin, lien lac','Cuoc phi dien thoai','expense'),
  ('6605',2026,'340','341','Hoat dong cua co quan nha nuoc','6600','Thong tin, lien lac','Cuoc phi Internet','expense'),
  ('6617',2026,'340','341','Hoat dong cua co quan nha nuoc','6600','Thong tin, lien lac','Sach, bao, tap chi thu vien','expense'),
  ('6651',2026,'340','341','Hoat dong cua co quan nha nuoc','6650','Hoi nghi','In, mua tai lieu','expense'),
  ('6652',2026,'340','341','Hoat dong cua co quan nha nuoc','6650','Hoi nghi','Boi duong giang vien, bao cao vien','expense'),
  ('6657',2026,'340','341','Hoat dong cua co quan nha nuoc','6650','Hoi nghi','Chi phi thue hoi truong','expense'),
  ('6701',2026,'340','341','Hoat dong cua co quan nha nuoc','6700','Cong tac phi','Tien ve may bay, tau xe','expense'),
  ('6702',2026,'340','341','Hoat dong cua co quan nha nuoc','6700','Cong tac phi','Phu cap cong tac phi','expense'),
  ('6703',2026,'340','341','Hoat dong cua co quan nha nuoc','6700','Cong tac phi','Tien thue phong ngu','expense'),
  ('6751',2026,'340','341','Hoat dong cua co quan nha nuoc','6750','Chi phi thue muon','Thue phuong tien van chuyen','expense'),
  ('6757',2026,'340','341','Hoat dong cua co quan nha nuoc','6750','Chi phi thue muon','Thue lao dong trong nuoc','expense'),
  ('6758',2026,'340','341','Hoat dong cua co quan nha nuoc','6750','Chi phi thue muon','Thue dao tao lai can bo','expense'),
  ('6901',2026,'340','341','Hoat dong cua co quan nha nuoc','6900','Sua chua, duy tu tai san','Sua chua nha cua','expense'),
  ('6907',2026,'340','341','Hoat dong cua co quan nha nuoc','6900','Sua chua, duy tu tai san','Sua chua thiet bi tin hoc','expense'),
  ('6912',2026,'340','341','Hoat dong cua co quan nha nuoc','6900','Sua chua, duy tu tai san','Sua chua phuong tien van tai','expense'),
  ('7001',2026,'340','341','Hoat dong cua co quan nha nuoc','7000','Chi phi nghiep vu chuyen mon','Chi mua hang hoa, vat tu','expense'),
  ('7003',2026,'340','341','Hoat dong cua co quan nha nuoc','7000','Chi phi nghiep vu chuyen mon','Chi mua sach, tai lieu chuyen mon','expense'),
  ('7004',2026,'340','341','Hoat dong cua co quan nha nuoc','7000','Chi phi nghiep vu chuyen mon','Chi in an, photo tai lieu','expense'),
  ('7012',2026,'340','341','Hoat dong cua co quan nha nuoc','7000','Chi phi nghiep vu chuyen mon','Chi phi nghiep vu chuyen mon khac','expense'),
  ('7756',2026,'340','341','Hoat dong cua co quan nha nuoc','7750','Chi khac','Chi cac khoan khac','expense'),
  ('9051',2026,'340','341','Hoat dong cua co quan nha nuoc','9050','Mua sam tai san','Mua sam thiet bi, may moc','expense'),
  ('9062',2026,'340','341','Hoat dong cua co quan nha nuoc','9050','Mua sam tai san','Mua sam phuong tien van tai','expense')
ON CONFLICT (line_code, fiscal_year) DO NOTHING;

-- ── Budget units — province level ────────────────────────────────────────
INSERT INTO refdata.budget_unit
  (unit_code, unit_name, short_name, unit_level, parent_code, locality_code, org_type)
VALUES
  ('1054001','Uy ban nhan dan tinh Hung Yen','UBND tinh','province',NULL,'LOC00','state_admin'),
  ('1054010','So Giao duc va Dao tao',           'So GDDT','department','1054001','LOC00','state_admin'),
  ('1054011','So Y te',                          'So YT',  'department','1054001','LOC00','state_admin'),
  ('1054012','So Van hoa, The thao va Du lich',  'So VHTTDL','department','1054001','LOC00','state_admin'),
  ('1054013','So Khoa hoc va Cong nghe',         'So KHCN','department','1054001','LOC00','state_admin'),
  ('1054014','So Lao dong - Thuong binh va Xa hoi','So LDTBXH','department','1054001','LOC00','state_admin'),
  ('1054015','So Tai chinh',                     'So TC',  'department','1054001','LOC00','state_admin'),
  ('1054016','So Ke hoach va Dau tu',            'So KHDT','department','1054001','LOC00','state_admin'),
  ('1054017','So Nong nghiep va Phat trien nong thon','So NNPTNT','department','1054001','LOC00','state_admin'),
  ('1054018','So Giao thong van tai',            'So GTVT','department','1054001','LOC00','state_admin'),
  ('1054019','So Xay dung',                      'So XD',  'department','1054001','LOC00','state_admin'),
  ('1054020','So Tai nguyen va Moi truong',      'So TNMT','department','1054001','LOC00','state_admin'),
  ('1054021','So Cong Thuong',                   'So CT',  'department','1054001','LOC00','state_admin'),
  ('1054022','So Noi vu',                        'So NV',  'department','1054001','LOC00','state_admin'),
  ('1054023','So Tu phap',                       'So TP',  'department','1054001','LOC00','state_admin'),
  ('1054024','Thanh tra tinh',                   'TTr tinh','department','1054001','LOC00','state_admin'),
  ('1054025','Van phong Uy ban nhan dan tinh',   'VP UBND','department','1054001','LOC00','state_admin'),
  ('1054026','Cong an tinh Hung Yen',            'CA tinh','department','1054001','LOC00','state_admin'),
  ('1054027','Bo Chi huy Quan su tinh',          'BCHQS', 'department','1054001','LOC00','state_admin')
ON CONFLICT (unit_code) DO NOTHING;

-- ── Budget units — one finance office per district ───────────────────────
INSERT INTO refdata.budget_unit
  (unit_code, unit_name, short_name, unit_level, parent_code, locality_code, org_type)
SELECT
  '10541' || lpad((row_number() OVER (ORDER BY l.locality_code))::text, 2, '0'),
  'Phong Tai chinh - Ke hoach ' || l.locality_name,
  'PTCKH ' || l.locality_code,
  'district',
  '1054001',
  l.locality_code,
  'state_admin'
FROM refdata.locality l
WHERE l.admin_level = 'district'
ON CONFLICT (unit_code) DO NOTHING;

-- ── Budget units — the long tail of public service providers ─────────────
-- Generated rather than typed out: the point is realistic VOLUME and a
-- realistic spread across districts and sectors, not authentic names.
INSERT INTO refdata.budget_unit
  (unit_code, unit_name, short_name, unit_level, parent_code, locality_code, org_type)
SELECT
  '10542' || lpad(n::text, 2, '0'),
  CASE
    WHEN n <= 30 THEN 'Truong THPT ' || d.locality_name
    WHEN n <= 55 THEN 'Truong THCS ' || d.locality_name || ' so ' || ((n - 30) % 3 + 1)
    WHEN n <= 70 THEN 'Trung tam Y te ' || d.locality_name
    WHEN n <= 82 THEN 'Trung tam Van hoa - The thao ' || d.locality_name
    ELSE 'Ban Quan ly du an ' || d.locality_name
  END,
  NULL,
  'public_service',
  CASE
    WHEN n <= 55 THEN '1054010'   -- schools sit under the education department
    WHEN n <= 70 THEN '1054011'   -- health centres under health
    WHEN n <= 82 THEN '1054012'
    ELSE '1054001'
  END,
  d.locality_code,
  'public_service'
FROM generate_series(1, 95) AS n
CROSS JOIN LATERAL (
  SELECT locality_code, locality_name
  FROM refdata.locality
  WHERE admin_level = 'district'
  ORDER BY locality_code
  OFFSET ((n - 1) % 10) LIMIT 1
) AS d
ON CONFLICT (unit_code) DO NOTHING;

-- ── A mid-year merger, planted on purpose ────────────────────────────────
-- Two lower-secondary schools merge from 2026-07. The old unit stops being
-- valid but is NOT deleted: reports for the first half of the year still have
-- to resolve it, and superseded_by is what lets a roll-up follow the budget
-- into its successor. This is the case that breaks any dimension without SCD-2.
UPDATE refdata.budget_unit
   SET valid_to      = DATE '2026-06-30',
       superseded_by = '10542' || lpad('32', 2, '0')
 WHERE unit_code = '10542' || lpad('31', 2, '0');
