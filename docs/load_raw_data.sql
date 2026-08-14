-- Sandbox database for learning Airflow with PostgreSQL
TRUNCATE raw_thu_chi;

INSERT INTO raw_thu_chi (id, ma_don_vi, loai, so_tien, ngay, ghi_chu, ngay_den) VALUES
  ('TC001', 'DV001', 'thu', 1500000, '2026-07-12', 'Thu quy', '2026-07-13'),
  ('TC002', 'DV001', 'thu', 2000000, '2026-07-12', 'Thu HKD', '2026-07-13'),
  ('TC003', 'DV002', 'chi', -500000, '2026-07-12', NULL, '2026-07-13'),
  ('TC001', 'DV001', 'thu', 1500000, '2026-07-12', 'TRUNG - ban ghi trung', '2026-07-13'),
  ('TC004', 'DV003', 'thu', NULL, '2026-07-12', 'Thieu so tien', '2026-07-13'),
  ('TC006', 'DV004', 'thu', 3000000, '2026-07-13', 'Thu ngay sau', '2026-07-13');

SELECT '=== Raw data loaded ===' AS info;
SELECT id, ma_don_vi, loai, so_tien, ngay FROM raw_thu_chi ORDER BY id;
SELECT '--- Note: 2 ban ghi co van de: TC001 trung, TC004 NULL tien ---' AS info;
