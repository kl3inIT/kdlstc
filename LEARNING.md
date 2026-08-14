# Learning Mode — Data Engineering & Airflow (DTH Kho dữ liệu)

Track học DE thật qua dự án Kho dữ liệu Sở Tài chính Hưng Yên.
Learning Mode **bật mặc định** trừ khi bạn nói "chỉ làm, đừng dạy".

## Behavior (cách dạy)

- Bắt đầu bằng kiểm tra bạn đã hiểu gì trước khi dạy tiếp.
- Dạy từng bước nhỏ, không giảng bài dài.
- Luôn nói cả **lý do cấp cao** lẫn **cơ chế cấp thấp**.
- So sánh với thứ bạn đã biết (Java/Spring, SQL, service backend) khi hữu ích.
- Ưu tiên bằng chứng thật: file SQL, UI Airflow, logs, kết quả query — không lý thuyết suông.
- Chỉ tick `[x]` khi bạn **giải thích lại được bằng lời của mình**, không phải khi "chạy xong".

## Depth Dials (gọi tên để chỉnh độ sâu)

- `eli5` — ví von siêu đơn giản.
- `eli14` — giải thích nhẹ nhàng, có vài thuật ngữ thật.
- `eli-eng` — mức kỹ sư: đánh đổi, khi nào dùng, khi nào không.

## Curriculum — Pipeline thật đầu-cuối

Resume từ mục chưa tick đầu tiên trừ khi bạn đổi hướng.

### Nền tảng (đã xong qua bài tập Airflow)
- [x] Vòng đời DAG → Task → Log (Bài 1)
- [x] TaskFlow API & XCom truyền dữ liệu (Bài 2)
- [x] Retry, branch, trigger_rule, rerun đúng bước (Bài 3)
- [x] Idempotency là gì & vì sao cốt lõi
- [x] Kiến trúc các thành phần Airflow (scheduler/worker/metadata DB/...)

### Xây pipeline thật (sandbox `learn_dwh`)
- [x] **Buổi 1 — Nguồn giả + Watermark** *(hiểu: watermark = giá trị mốc; cùng DB để nạp data + đẩy mốc trong 1 transaction, rollback cả hai khi lỗi; 2PC tồn tại nhưng tránh vì chậm/giòn)*
  - Watermark column (`updated_at`)
  - Watermark table đặt trong kho dữ liệu (không phải metadata Airflow)
  - Incremental filter `WHERE updated_at > last_value`
- [x] **Buổi 2 — DAG incremental** *(hiểu: `INSERT ... SELECT` đổ phần dữ liệu mới vào raw; insert + update watermark cùng transaction nên lỗi bước sau sẽ rollback cả hai; run thứ hai nạp 0 dòng)*
- [x] **Buổi 3 — Load Raw bất biến** *(hiểu: Raw giữ lịch sử các version gần audit trail dữ liệu; nguồn sửa thì append version mới, không UPDATE/DELETE bản cũ; WAL là log kỹ thuật khác)*
- [ ] **Buổi 4 — Transform: clean + dedup → staging**
- [ ] **Buổi 5 — Data-quality gate + quarantine**
- [ ] **Buổi 6 — Warehouse idempotent MERGE + reconciliation**

### Bài toán thực tế — Kho dữ liệu tài chính tỉnh (multi-source, 2026-07-16)
Mô phỏng VN: mượn *hình dạng* AdventureWorksDW (FactFinance/DimScenario) + *pattern bẩn* Olist + multi-engine (Oracle+Postgres). Dữ liệu tự sinh (`docs/gen_data.py`, seed cố định 20260716).

**Nguồn đã dựng (verified):**
- Oracle `TMS` (user tms/tms_dev_2026 @ XEPDB1): `sac_thue`(4), `nnt`(40), `to_khai`(82), `khoan_nop`(73). Cột `updated_at` làm watermark.
- Postgres `kbnn_src` (@ pgvector): `don_vi`(5), `giao_dich_thu`(72), `giao_dich_chi`(18).

**Độ bẩn seed (bắt chước lỗi thật, verified):**
- MST lệch định dạng: KBNN có gạch `0100-000002` vs TMS `0100000002` → chuẩn hóa
- Tên NNT viết khác giữa 2 nguồn (HOA/thường) → dedup
- khoan_nop đến sau to_khai vài ngày → late-arriving
- Đối soát: tổng thu TMS 2.720.000.000 vs KBNN 2.710.000.000 = lệch 10tr (thiếu 1 khoản)

**Kho đích:** `learn_dwh` — sẽ dựng Mục lục NSNN (Chương/Loại/Khoản/Mục/Tiểu mục) + Fact thu/chi + dim_kich_ban (Dự toán/Thực hiện).

**Kho đích đã dựng** (`docs/ds03_dwh_schema.sql`, 15 bảng): raw_* (bất biến) · dim_muc_luc_nsnn (Chương→Loại→Khoản→Mục→Tiểu mục, 9 tiểu mục thật) · dim_nnt/dim_don_vi/dim_kich_ban(DT/TH)/dim_thoi_gian(365) · fact_thu/chi_ngan_sach · quarantine_thu · doi_soat_thu · etl_watermark_ms (theo source_system+table).

**Provider Oracle:** đã build image `kdlstc-airflow:3.3.0-oracle` (Dockerfile + `pip install apache-airflow-providers-oracle` → 4.6.2, oracledb 4.0.2 thin mode). `.env` đã trỏ image này.

**Connections:** `oracle_tms` (Oracle), `pg_kbnn` (Postgres KBNN), `postgres_learn_dwh` (kho đích, user `airflow_learn`).

- [x] **DWH-01 — Ingest 2 nguồn đa engine → Raw** (`dags/dwh01_ingest_multisource.py`)
  - Run 1 nạp 40 nnt / 73 khoan_nop (Oracle) + 72 giao_dich_thu (Postgres); run 2 nạp 0 dòng → incremental đúng.
  - Watermark theo từng (source_system, source_table); INSERT + đẩy mốc cùng 1 transaction.

  **🐛 BẪY MULTI-ENGINE đã gặp & fix (bài học quan trọng nhất):**
  Driver `oracledb` bind Python `datetime` thành kiểu **Oracle DATE** — mà DATE **không có phần thập phân giây**. Watermark `15:13:58.517696` bị cắt còn `15:13:58` → mọi dòng đều "lớn hơn" → **nạp lại toàn bộ, nhân đôi Raw** (40→80, 73→146). Postgres không dính vì psycopg giữ nguyên microseconds.
  → **Fix:** truyền watermark dạng **chuỗi** + ép Oracle parse bằng `TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS.FF6')`.
  → **Bài học:** cùng một logic watermark có thể đúng trên engine này nhưng sai trên engine kia. Multi-source **bắt buộc** test idempotency riêng cho từng engine.

  **Quyền:** user `airflow_learn` — Raw chỉ `SELECT, INSERT` (giữ bất biến); `etl_watermark_ms` thêm `UPDATE`; dim/fact/quarantine full.

- [x] **DWH-02 — Transform + DQ + Đối soát** (`dags/dwh02_transform_reconcile.py`)
  - `dim_nnt`: 40 NNT, chuẩn hóa MST (bỏ gạch), lấy bản mới nhất mỗi MST từ Raw append-only. Tên chuẩn ưu tiên nguồn TMS.
  - **16/40 bản ghi có `_co_xung_dot`** — Kho bạc ghi tên viết HOA khác TMS (vd `HKD Ha Cuong` vs `HKD HA CUONG`).
  - `dim_don_vi`: 5 đơn vị. `quarantine_thu`: 0 dòng (dữ liệu sinh đều hợp lệ về kiểu).
  - `fact_thu_ngan_sach`: **72 dòng**, MST đã chuẩn hóa (0 dòng còn gạch).
  - **Đối soát tự phát hiện:** TMS 2.720.000.000 (73 bản ghi) vs KBNN 2.710.000.000 (72) → **lệch 10.000.000đ, trạng thái LECH** ✅
  - Idempotent: run 3 lần → fact vẫn 72, doi_soat vẫn 1 dòng.

  **🐛 Bug đã gặp & fix:** ban đầu so sánh `UPPER(a) <> UPPER(b)` → xung đột hoa/thường bị coi là giống nhau, `_co_xung_dot = 0`. Sửa thành so sánh nguyên văn `TRIM(a) <> TRIM(b)` → phát hiện đúng 16 ca.
  → **Bài học:** khi so khớp giữa 2 nguồn, "chuẩn hóa quá tay" sẽ **giấu mất** bất nhất cần rà soát. Chuẩn hóa để *join* (MST) khác với so sánh để *phát hiện xung đột* (tên).

- [x] **DWH-03 — Chi ngân sách + Dự toán vs Thực hiện** (`dags/dwh03_chi_dutoan.py`, setup `docs/ds04_dwh03_setup.sql`)
  - Bảng mới: `raw_kbnn_giao_dich_chi` (Raw bất biến), `du_toan_ngan_sach` (12 dòng, 3,95 tỷ), `doi_soat_chi`.
  - Ingest chi: 18 dòng, watermark riêng cho `giao_dich_chi`.
  - **`dim_kich_ban` dùng thật:** cùng bảng `fact_chi_ngan_sach` chứa CẢ dự toán (`DT`, 12 dòng) lẫn thực hiện (`TH`, 18 dòng) — mượn ý tưởng `DimScenario` của AdventureWorksDW.
  - **Đối soát DT vs TH phát hiện 2 đơn vị VƯỢT DỰ TOÁN:**
    | Đơn vị | Dự toán | Thực hiện | Tỷ lệ | Trạng thái |
    |---|---:|---:|---:|---|
    | Sở Giáo dục và Đào tạo | 300tr | 700tr | 233% | VUOT_DU_TOAN |
    | UBND TP Hưng Yên | 500tr | 1.000tr | 200% | VUOT_DU_TOAN |
    | Sở Y tế | 900tr | 600tr | 67% | TRONG_DU_TOAN |
    | Sở Tài chính | 850tr | 400tr | 47% | TRONG_DU_TOAN |
    | Văn phòng UBND tỉnh | 1.400tr | 200tr | 14% | TRONG_DU_TOAN |
  - Idempotent: run 2 giữ nguyên 18/12/18/5 dòng.

  **🐛 Bug đã gặp (lần 2 cùng loại):** `InsufficientPrivilege` — bảng mới tạo nhưng `GRANT` trong file SQL chạy TRƯỚC `CREATE TABLE` của bảng đó.
  → **Bài học:** trong script setup, `GRANT` phải đặt SAU `CREATE TABLE`. Với DB có role hạn chế (least privilege), **mỗi bảng mới đều phải cấp quyền** — dễ quên, nên gom GRANT về cuối file.

### Tổng kết pipeline hiện có
```
Oracle TMS ─┐                  ┌─► dim_nnt (dedup, 16 xung đột tên)
            ├─► DWH-01 ─► Raw ─┼─► fact_thu (72) ─► đối soát THU: lệch 10tr
Postgres ───┘      (bất biến)  ├─► fact_chi DT/TH ─► đối soát CHI: 2 đv vượt dự toán
KBNN                           └─► quarantine (DQ gate)
```

**Bước tiếp có thể làm:** (a) Assets để DWH-02/03 tự chạy khi Raw đổi; (b) alerting khi phát hiện LECH/VUOT_DU_TOAN; (c) semantic layer/view tổng hợp phục vụ AI báo cáo; (d) backfill & rollback theo partition.

## Từ vựng đã lưu (Northstar flashcards)
idempotent · reconciliation · watermark · backfill · orchestration

## Resource
- Airflow official tutorial (3.x) — nguồn số 1, đúng version
- Astronomer Guides — best practices
- Sách: "Fundamentals of Data Engineering" (tư duy), "Data Pipelines with Apache Airflow" (đọc lấy ý, code tra doc 3.x)
