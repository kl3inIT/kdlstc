-- BUỔI 4: Tạo một version nguồn chưa sạch để kiểm chứng transform.
-- Điều kiện updated_at giúp script idempotent: chạy lại sẽ không update lần nữa.

UPDATE nguon_thu_chi
SET ma_don_vi = ' dv001 ',
    loai = ' CHI ',
    so_tien = -550000,
    ghi_chu = '  Dieu chinh Chi Q2  ',
    updated_at = TIMESTAMP '2026-07-02 09:00:00'
WHERE id = 'Q2-002'
  AND updated_at < TIMESTAMP '2026-07-02 09:00:00';

SELECT id, ma_don_vi, loai, so_tien, ghi_chu, updated_at
FROM nguon_thu_chi
WHERE id = 'Q2-002';
