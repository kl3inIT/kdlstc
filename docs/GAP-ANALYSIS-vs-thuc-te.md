# Phân tích khoảng cách: mô phỏng hiện tại vs structure THẬT

Nguồn đối chiếu: repo `git.dth.com.vn/jmix-team/research/stc-hungyen-baocao`
— metadata đặc tả **358 báo cáo** thật của Sở Tài chính Hưng Yên (4.844 cột, 8.407 dòng chỉ tiêu).

Đọc ngày 2026-07-23. Nhóm sát pipeline nhất: **20 biểu điều hành thu/chi** (`DHTC_THU_01..10`, `DHTC_CHI_01..10`).

---

## 1. Kết luận nhanh

Mô phỏng của chúng ta (DWH-01/02/03) **đúng hướng về kiến trúc**, nhưng **thiếu chiều sâu về mô hình dữ liệu**. Ba khoảng cách lớn:

| # | Khoảng cách | Mức quan trọng |
|---|---|---|
| **G1** | Đối soát thật là **3 chiều** (Thuế ↔ Kho bạc ↔ **TABMIS**), ta chỉ làm 2 chiều | 🔴 Cao — đây là milestone reconciliation |
| **G2** | Thiếu ~10 dimension mà mọi báo cáo đều dùng (địa bàn, cơ quan thu, KBNN, nguồn KP, lĩnh vực chi, dự án…) | 🔴 Cao — không có thì không sinh được báo cáo nào |
| **G3** | Thiếu tầng **chỉ tiêu phân tích** (cùng kỳ, tăng/giảm, tỷ trọng, tiến độ chuẩn, dự báo, mức cảnh báo) | 🟠 Trung — là phần "giá trị" của báo cáo điều hành |

Điểm tích cực: **fact grain ta chọn đúng**, và ta tự suy ra đúng các cột kỹ thuật thật cũng có (`nguon_du_lieu`, `ngay_nap`).

---

## 2. Fact grain — ta chọn ĐÚNG

Biểu `DHTC_THU_01` / `DHTC_CHI_01` là báo cáo chi tiết giao dịch, hạt dữ liệu:

> *"1 dòng = 1 chứng từ thu NSNN (chi tiết tới từng tiểu mục nếu chứng từ nhiều dòng)"*

→ Trùng khớp `fact_thu_ngan_sach` / `fact_chi_ngan_sach` của ta.

### So cột: THU_01 (23 cột thật) vs `fact_thu_ngan_sach`

| Cột thật | Ta có? | Ghi chú |
|---|---|---|
| `so_chung_tu` | ❌ | **thiếu** — khóa nghiệp vụ của giao dịch |
| `ngay_chung_tu` | ❌ | thiếu — ngày trên chứng từ |
| `ngay_hach_toan` | ⚠️ | ta có `ngay` (chưa phân biệt 2 loại ngày) |
| `ma_co_quan_thu` | ❌ | thiếu |
| `ma_kbnn` | ❌ | thiếu |
| `ten_nguoi_nop`, `ma_so_thue` | ✅ | `dim_nnt` |
| `ma_dia_ban` | ❌ | thiếu |
| `ma_chuong/loai/khoan/muc/tieu_muc` | ✅ | `dim_muc_luc_nsnn` (đủ 5 cấp) |
| `noi_dung_thu` | ❌ | thiếu |
| `ma_nguon_thu` (sắc thuế) | ⚠️ | có ở Oracle `sac_thue`, chưa lên DWH |
| `so_tien` | ✅ | |
| `trang_thai_hach_toan` | ❌ | thiếu |
| `ma_cap_ns` | ❌ | thiếu (có trong `danh-muc.json`) |
| `ty_le_phan_chia` | ❌ | thiếu — % phân chia giữa các cấp NS |
| `so_ct_goc` | ❌ | thiếu — tham chiếu chứng từ gốc |
| `nguon_du_lieu` | ✅ | `_source_system` — **ta tự suy ra đúng** |
| `ngay_nap` | ✅ | `_ngay_nap` / `_loaded_at` — **đúng** |

### CHI_01 (23 cột) — thêm các chiều riêng của chi

`ma_dvqhns` (⚠️ ta gọi `ma_dvsdns`), `noi_dung_chi`, `ma_nguon_kp`, `trang_thai_thanh_toan`,
`ma_hinh_thuc_chi`, `ma_linh_vuc_chi`, `ma_du_an`, `ma_ctmt` (chương trình mục tiêu) — **đều thiếu**.

> ⚠️ Lưu ý thuật ngữ: thật dùng **`ma_dvqhns`** (đơn vị *quan hệ* ngân sách), ta đặt `ma_dvsdns` (đơn vị *sử dụng* ngân sách). Nên đổi theo thật.

---

## 3. G1 — Đối soát 3 chiều (khoảng cách quan trọng nhất)

Biểu `DHTC_THU_07` "Báo cáo đối chiếu chứng từ thu NSNN":

> *"1 dòng = 1 giao dịch thu được đối chiếu giữa **Thuế ↔ Kho bạc ↔ TABMIS**"*

Biểu `DHTC_CHI_08`: *"đối chiếu giữa **Đơn vị SDNS ↔ Kho bạc ↔ TABMIS**"*.

### Cột đối soát thật

**Số tiền theo từng nguồn:** `so_tien_thue`, `so_tien_kb`, `so_tien_tabmis`
**Chênh lệch từng cặp:** `cl_thue_kb`, `cl_kb_tabmis`, `cl_thue_tabmis`
**Cờ tồn tại:** `co_o_thue`, `co_o_kb`, `co_o_tabmis` ← phát hiện *thiếu ở nguồn nào*
**Phân loại:** `trang_thai_doi_chieu`, `muc_do_cl`, `ma_nguyen_nhan_cl`, `lech_ma_hach_toan`
**Workflow xử lý:** `so_ngay_ton`, `nguoi_xu_ly`, `ngay_xu_ly`, `noi_dung_giai_trinh`, `so_ct_khac_phuc`

### So với `doi_soat_thu` của ta

| | Ta có | Thật cần |
|---|---|---|
| Số nguồn đối soát | 2 (TMS, KBNN) | **3** (+ TABMIS) |
| Mức đối soát | tổng toàn kỳ | **từng chứng từ** |
| Cờ tồn tại theo nguồn | ❌ | ✅ `co_o_*` |
| Nguyên nhân lệch | ❌ | ✅ danh mục `ma_nguyen_nhan_cl` |
| Workflow xử lý lệch | ❌ | ✅ người xử lý, giải trình, chứng từ khắc phục |
| Tuổi tồn đọng | ❌ | ✅ `so_ngay_ton` |

**TABMIS** = Hệ thống thông tin quản lý Ngân sách và Kho bạc — hệ thống nguồn thứ 3, là "sổ cái" của ngành tài chính. Đây là nguồn ta **chưa mô phỏng**.

> 💡 Bài học: đối soát thật không chỉ trả lời *"lệch bao nhiêu"* mà còn *"lệch ở đâu, vì sao, ai đang xử lý, tồn bao lâu"*. Đối soát là **quy trình**, không phải một con số.

---

## 4. G2 — Dimension còn thiếu

Trích từ mã xuất hiện trong 20 biểu DHTC:

| Dimension | Mã trong biểu | Ta có? | Dùng ở biểu |
|---|---|---|---|
| Mục lục NSNN (5 cấp) | `ma_chuong/loai/khoan/muc/tieu_muc` | ✅ | mọi biểu |
| Người nộp thuế | `ma_so_thue` | ✅ | THU_01,04,09,10 |
| Đơn vị QHNS | `ma_dvqhns` | ⚠️ đổi tên | CHI_01,02,07,08,10 |
| **Địa bàn** | `ma_dia_ban` | ❌ | THU_01,03,04,10 · CHI_01,04 |
| **Cơ quan thu** | `ma_co_quan_thu` | ❌ | THU_01,02,03,05,06,10 |
| **Kho bạc** | `ma_kbnn` | ❌ | THU_01,07 · CHI_01,08 |
| **Nguồn thu / sắc thuế** | `ma_nguon_thu`, `ma_sac_thue`, `ma_nhom_nguon_thu` | ⚠️ chỉ ở Oracle | THU_01,02,03,06,10 |
| **Nguồn kinh phí** | `ma_nguon_kp` | ❌ | CHI_01,02,04,05,06,09,10 |
| **Lĩnh vực chi** | `ma_linh_vuc_chi` | ❌ | CHI_01,03,05,09,10 |
| **Cấp ngân sách** | `ma_cap_ns` | ❌ | THU_01 · CHI_01 |
| **Dự án / CTMT** | `ma_du_an`, `ma_ctmt` | ❌ | CHI_01,07 |
| **Hình thức chi** | `ma_hinh_thuc_chi` | ❌ | CHI_01,08 |
| Ngành nghề, loại hình DN | `ma_nganh_nghe`, `ma_loai_hinh_dn` | ❌ | THU_04,09 |
| Nguyên nhân (lệch/chậm) | `ma_nguyen_nhan_cl`, `ma_nguyen_nhan_cham` | ❌ | THU_07,10 · CHI_07,08,10 |
| Quy tắc cảnh báo | `ma_quy_tac` | ❌ | THU_10 · CHI_10 |

`danh-muc.json` của repo đã cho **bộ mã chuẩn** cho: `cap_ngan_sach` (7), `ky_bao_cao` (5),
`giai_doan_chu_trinh` (5), `hinh_thuc_trinh_bay` (5), `don_vi_tinh` (6), `vai_tro_luong` (5).

---

## 5. G3 — Tầng chỉ tiêu phân tích (derived metrics)

18 trong 20 biểu DHTC **không phải** danh sách giao dịch thô — chúng là **tổng hợp + phân tích**.
Pattern cột lặp lại ở gần như mọi biểu:

| Nhóm | Cột | Ý nghĩa |
|---|---|---|
| Kế hoạch vs thực tế | `du_toan`, `thuc_hien_ky`, `luy_ke`, `con_lai`/`con_phai_thu` | ta đã có nền (DWH-03) |
| Tỷ lệ | `ty_le_hoan_thanh` (thu), `ty_le_giai_ngan` (chi) | ta đã có |
| So sánh kỳ | `cung_ky`, `tang_giam_td` (tuyệt đối), `tang_giam_tl` (tương đối), `tt_ky_truoc` | ❌ chưa có |
| Cơ cấu | `ty_trong`, `ty_trong_cung_ky`, `bien_dong_ty_trong`, `dong_gop_tang_truong` | ❌ chưa có |
| Tiến độ | `tien_do_chuan` (chuẩn theo thời gian đã trôi), `chenh_tien_do` | ❌ chưa có |
| Dự báo | `du_bao_ca_nam`, `nguy_co_khong_dung_het`, `kha_nang_ht`, `he_so_no_luc` | ❌ chưa có |
| Cảnh báo | `muc_canh_bao`, `diem_rui_ro`, `nhom_rui_ro`, `xu_huong`, `xep_hang` | ⚠️ ta chỉ có `VUOT_DU_TOAN` |
| Đếm | `so_luong_ct`, `so_nguoi_nop`, `so_don_vi`, `so_lan_nop` | ❌ chưa có |

> 💡 `tien_do_chuan` là khái niệm hay: nếu đã qua 6/12 tháng thì tiến độ chuẩn = 50%; giải ngân 30% → `chenh_tien_do` = −20% → cảnh báo chậm. Đây là logic điều hành thật, không phải chỉ so dự toán.

---

## 6. Nguồn dữ liệu thật (suy ra từ metadata)

| Hệ thống | Vai trò | Xuất hiện ở |
|---|---|---|
| **Thuế (TMS)** | số phải thu, đã thu theo NNT | `so_tien_thue`, `ma_co_quan_thu` |
| **Kho bạc (KBNN)** | tiền thực vào/ra quỹ NSNN | `so_tien_kb`, `ma_kbnn` |
| **TABMIS** | sổ cái ngân sách — chuẩn hạch toán | `so_tien_tabmis` |
| Đơn vị SDNS | số liệu đơn vị tự khai | `so_tien_dv` (CHI_08) |
| Hệ thống dự toán | dự toán giao, điều chỉnh | `du_toan_dau_nam`, `dieu_chinh_trong_nam` |

→ Khớp với ghi nhận trước đó: **~5 database nguồn**.

---

## 7. Đề xuất lộ trình thu hẹp khoảng cách

Không thể (và không nên) làm hết 358 biểu. Đề xuất theo thứ tự giá trị:

### Ưu tiên 1 — Đối soát 3 chiều (G1)
Thêm nguồn **TABMIS** (engine thứ 3, ví dụ MySQL) → `doi_soat_thu_ct` cấp **chứng từ** với
`so_tien_thue/kb/tabmis`, `co_o_*`, `trang_thai_doi_chieu`, `ma_nguyen_nhan_cl`, workflow xử lý.
→ Trực tiếp phục vụ milestone *data consistency & reconciliation*.

### Ưu tiên 2 — Bổ sung dimension (G2)
Thêm: `dim_dia_ban`, `dim_co_quan_thu`, `dim_kbnn`, `dim_nguon_thu`, `dim_nguon_kinh_phi`,
`dim_linh_vuc_chi`, `dim_cap_ngan_sach`, `dim_nguyen_nhan`. Mở rộng `fact_*` cho đủ cột THU_01/CHI_01.

### Ưu tiên 3 — Chỉ tiêu phân tích (G3)
Bắt đầu bằng: `cung_ky`, `tang_giam_td/tl`, `ty_trong`, `tien_do_chuan`, `chenh_tien_do`.
Đủ để sinh được `DHTC_THU_02/03/06` và `DHTC_CHI_02/03`.

### Ưu tiên 4 — Sinh thử 1 báo cáo thật
Chọn `DHTC_THU_01` (chi tiết chứng từ, 23 cột) — sinh đúng bố cục từ kho → chứng minh
pipeline phục vụ được báo cáo thật.

---

## 8. Ghi chú kỹ thuật khi đọc repo

- Metadata sinh từ `../specs/` (repo khác, chưa clone) — đặc tả gốc dạng markdown từng biểu.
- Đọc JSON phải chỉ định UTF-8: terminal Windows mặc định cp1252 sẽ lỗi với tiếng Việt.
- `manifest.json` = tổng quan; `danh-muc.json` = bộ chiều chuẩn; `bieu/*.json` = 358 biểu.
- Repo đã clone tạm ở scratchpad, **chưa** đưa vào repo `kdlstc`.
