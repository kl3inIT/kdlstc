# Bản phác contract "viên gạch API" — làm ngược từ metadata 358 biểu

> Bản nháp v1 (2026-07-31) — phục vụ prototype 2 API nguồn Thu + Chi để validate
> chuỗi: API tổng hợp → MCP tools → agent → ẩn cột theo quyền.
> Nguồn phân tích: `stc-hungyen-baocao/public/metadata` (358 biểu, 4.844 cột, 237 cột công thức).

## 1. Phát hiện chính từ metadata

1. **42 biểu điều hành nội bộ (DHTC 20, DHXDCB 10, DHVB 7, DHNV 5) có phân loại
   `kieu_bao_cao` rõ ràng**; 316 biểu pháp quy (TT26/TT343/TT131/ND85/TT77/TT120/TT29/TT35)
   không có — chủ yếu là biểu năm/công khai, ưu tiên sau.
2. **Các biểu kiểu B (Tổng hợp theo chiều) dùng chung một khuôn**: cùng bộ measure,
   chỉ khác chiều `group_by`. Ví dụ 5 biểu Thu (THU_02/03/04/05/06) = cùng số liệu thu,
   group theo: cơ quan thu / địa bàn / người nộp thuế / MLNS / sắc thuế.
3. **Kiểu D (time series) = chính API đó, group theo kỳ thời gian.**
   **Kiểu E (xếp hạng) = consumer sort.** → không cần API riêng.
4. **Kiểu A (danh sách chứng từ), C (đối chiếu liên hệ thống), F (cảnh báo rule engine)
   KHÔNG làm được từ API tổng hợp** — cần API chi tiết hoặc một bên tính riêng
   (6-7 biểu/phân hệ, đã nêu trong GAP-ANALYSIS).
5. **Cột dẫn xuất (tỷ lệ, cùng kỳ, tiến độ, dự báo) tính ở tầng consumer** — API chỉ trả
   số đo thô. Điều này khớp nguyên tắc ẩn-cột-lan-truyền: cột dẫn xuất bị ẩn khi
   cột nguồn của nó bị ẩn.

## 2. Phân bố kiểu báo cáo (trong 42 biểu điều hành)

| Kiểu | Nghĩa | Số biểu | Làm từ API tổng hợp? |
|---|---|---|---|
| B | Tổng hợp theo chiều (kể cả cây, ma trận) | ~22 | ✅ |
| D | Xu hướng thời gian | 2 | ✅ (group_by kỳ) |
| E | Xếp hạng / tiến độ | 4 | ✅ (consumer sort) |
| G | Dashboard | 1 | ✅ (ghép nhiều call) |
| A | Chi tiết giao dịch | 2 | ❌ cần API chi tiết |
| C | Đối chiếu liên hệ thống | 2 | ❌ cần dữ liệu 3 nguồn từng dòng |
| F | Cảnh báo & rủi ro (rule engine) | 4 | ❌ cần engine riêng |
| — | Văn bản (DHVB, dạng danh sách) | 7 | ❌ cần API danh sách văn bản |

## 3. Viên gạch API — app THU (mock 1)

### 3.1 `GET /rest/thu/tong-hop`

| Tham số | Kiểu | Ghi chú |
|---|---|---|
| `group_by` | enum | `co_quan_thu` \| `dia_ban` \| `nguoi_nop_thue` \| `mlns` \| `sac_thue` \| `nguon_thu` \| `ky_thoi_gian` |
| `tu_ngay`, `den_ngay` | date | kỳ dữ liệu — hỗ trợ tới NGÀY (44 biểu kỳ ngày) |
| `buoc_ky` | enum? | chỉ khi `group_by=ky_thoi_gian`: `NGAY`/`THANG`/`QUY` |
| `ma_dia_ban` | string? | filter |
| `ma_co_quan_thu` | string? | filter |
| `ma_nguon_thu` | string? | filter |
| `ma_chuong`..`ma_tieu_muc` | string? | filter theo nút MLNS bất kỳ |
| `ma_cap_ns` | enum? | NSNN/NSTW/NSDP/NS_CAP_TINH/... |
| `muc_mlns` | enum? | chỉ khi `group_by=mlns`: trả cây tới cấp nào |

**Trả về** (mảng dòng, mỗi dòng 1 giá trị chiều):

```json
{
  "khoa": "1054", "ten": "Chi cục Thuế khu vực X",
  "thuc_hien": 123456789, "so_luong_ct": 42, "so_nguoi_nop": 17
}
```

Chỉ **số đo cộng dồn được** (additive). Không trả tỷ lệ, không trả cùng kỳ.

### 3.2 `GET /rest/thu/du-toan`

`nam`, `group_by` (cùng enum trên, trừ `ky_thoi_gian`/`nguoi_nop_thue`) → `{khoa, du_toan}`.

### 3.3 `GET /rest/danh-muc/{loai}`

`loai` = `co_quan_thu` | `dia_ban` | `mlns` | `nguon_thu` | `sac_thue` — trả mã + tên + cây cha-con.
(Dùng chung cho cả 2 app; prototype đặt ở app THU.)

## 4. Viên gạch API — app CHI (mock 2)

### 4.1 `GET /rest/chi/tong-hop`

| Tham số | Kiểu | Ghi chú |
|---|---|---|
| `group_by` | enum | `don_vi` \| `dia_ban` \| `linh_vuc` \| `mlns` \| `nguon_kp` \| `du_an` \| `ky_thoi_gian` |
| `tu_ngay`, `den_ngay` | date | |
| filter | | `ma_dvqhns`, `ma_dia_ban`, `ma_linh_vuc_chi`, `ma_nguon_kp`, `ma_chuong`..`ma_tieu_muc`, `ma_cap_ns` |

**Trả về**: `{khoa, ten, thuc_chi, du_tam_ung, so_luong_ct}` (luy_ke_chi = thuc_chi + du_tam_ung — chốt định nghĩa khi dựng mock).

### 4.2 `GET /rest/chi/du-toan`

`nam`, `group_by` → `{khoa, du_toan_dau_nam, dieu_chinh_trong_nam}` (du_toan hiện hành = cộng 2 cột — dẫn xuất).

## 5. Cột dẫn xuất — tính ở tầng khai thác (KHÔNG nằm trong API)

| Cột trong biểu | Công thức | Cần call |
|---|---|---|
| `ty_le_hoan_thanh`, `ty_le_giai_ngan` | `luy_ke / du_toan × 100` | tong-hop + du-toan |
| `con_phai_thu`, `con_lai` | `du_toan − luy_ke` | tong-hop + du-toan |
| `cung_ky`, `tang_giam_td`, `tang_giam_tl` | call lần 2 với kỳ năm trước, trừ nhau | tong-hop ×2 |
| `ty_trong` | `giá trị dòng / tổng` | 1 call |
| `tien_do_chuan` | `% ngày đã trôi qua của năm` | không call |
| `chenh_tien_do` | `ty_le_hoan_thanh − tien_do_chuan` | dẫn xuất bậc 2 |
| `du_bao_ca_nam` | `luy_ke / tien_do_chuan` (tuyến tính, v1) | dẫn xuất bậc 2 |
| `bq_moi_ct`, `bq_moi_nnt` | `luy_ke / so_luong_ct hoặc so_nguoi_nop` | 1 call |
| `can_doi`, `ty_le_tu_can_doi` (CHI_04) | `thu_dia_ban − luy_ke_chi`; `thu/chi × 100` | **2 app** |

Quy tắc ẩn lan truyền: ẩn 1 số đo thô → ẩn mọi cột ở bảng này có nó trong công thức (đệ quy).

## 6. Biểu demo prototype: `DHTC_CHI_04` — Chi theo địa bàn

Chọn vì là biểu **thật sự cần cả 2 app**:

| Nhóm cột | Nguồn |
|---|---|
| `du_toan`, `luy_ke_chi`, `so_luong_ct`, `chi_thuong_xuyen`, `chi_dau_tu`... | app CHI |
| `thu_dia_ban` | app THU (`/thu/tong-hop?group_by=dia_ban`) |
| `can_doi`, `ty_le_tu_can_doi` | dẫn xuất từ CẢ HAI |
| `ty_le_giai_ngan`, `tien_do_chuan`, `chenh_tien_do`, `cung_ky`... | dẫn xuất |

Kịch bản nghiệm thu ẩn cột: user bị thu quyền app THU → `thu_dia_ban` ẩn,
`can_doi` + `ty_le_tu_can_doi` **ẩn theo** (lan truyền), toàn bộ cột CHI vẫn hiện,
agent nói rõ "không có quyền dữ liệu thu".

## 7. MCP tools (tầng của mình — phác)

| Tool | Wrap | Ghi chú |
|---|---|---|
| `thu_tong_hop(group_by, tu_ngay, den_ngay, filters)` | 3.1 + 3.2 | gộp du-toan qua flag `kem_du_toan` |
| `chi_tong_hop(group_by, tu_ngay, den_ngay, filters)` | 4.1 + 4.2 | như trên |
| `danh_muc(loai, tim)` | 3.3 | agent tra mã ↔ tên |
| `tra_cuu_bieu(ma_bieu | tu_khoa)` | metadata 358 biểu | trả: biểu này = API nào + công thức nào + grain |
| `dung_bieu(ma_bieu, ky, filters)` | orchestrate | gọi các API theo recipe, tính cột dẫn xuất, áp ẩn cột |

5 tools — dưới ngưỡng loạn (~40). Chức năng (C): hỏi đáp tự do dùng tool 1-3;
tìm/dựng biểu dùng tool 4-5.

## 8. Quyền & ẩn cột (nhắc lại quy tắc đã chốt)

- Token user forward nguyên vẹn xuống API nguồn qua SSO Keycloak.
- **403 từ 1 API → ẩn toàn bộ nhóm cột lấy từ API đó + cột dẫn xuất (lan truyền)**;
  kết quả kèm `nguon_bi_an: [...]`, agent bắt buộc khai báo trong câu trả lời.
- **Lỗi khác 403 (5xx/timeout) → báo lỗi biểu, KHÔNG ẩn im lặng.** Fail closed.
- Map cột→API cho biểu demo nằm ở mục 6; bản đầy đủ sinh dần từ metadata + recipe.

## 9. Checklist nghiệm thu prototype (định nghĩa "validate xong")

1. [ ] Login SSO 1 lần → token đi xuyên agent → MCP → cả 2 API nguồn.
2. [ ] `DHTC_CHI_04` dựng được từ ≥2 API của 2 app khác nhau.
3. [ ] Cột công thức (`can_doi`, `ty_le_giai_ngan`, `cung_ky`) tính đúng ở tầng khai thác.
4. [ ] Thu quyền API THU → `thu_dia_ban` + dẫn xuất biến mất, agent khai báo thiếu nguồn.
5. [ ] Tắt hẳn 1 app → hiện LỖI, không bị nhầm thành "không có quyền".

## 10. Việc còn treo (không chặn prototype)

- Kiểu A/C/F + DHVB (19 biểu grain chi tiết, 2 biểu đối chiếu, 4 biểu cảnh báo):
  cần quyết ai làm API chi tiết / rule engine — nêu với team khi validate xong.
- 5 domain thật của 5 API nguồn (bản đồ tạm từ metadata: Thu | Chi | Đầu tư công &
  nguồn vốn | Giá | Tài sản công);
  văn bản (DHVB) + dự toán/công khai (TT26/TT343) chưa rõ thuộc con nào).
- Danh mục dùng chung (MLNS, địa bàn, đơn vị) đặt ở đâu khi lên 5 app thật.
