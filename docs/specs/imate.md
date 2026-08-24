# Spec — lát cắt iMate

Nguồn văn bản điều hành. Lát cắt duy nhất chạy trên **dữ liệu thật**.

## Nguồn

| | |
|---|---|
| Địa chỉ | `http://10.123.123.199`, tiêu đề `Host: imate.local` |
| Tenant | `doit.phuyen` — `2d4a7629-0620-504b-a70f-083958673534` |
| Xác thực | Không cần cho các endpoint đang dùng |
| Kiểu nạp | merge-upsert theo khoá tự nhiên `globalId` |
| Quy mô | 6.141 văn bản, 99.214 lượt chuyển |

### Ba cái bẫy của API này

**Lỗi trả HTTP 200.** Tham số sai thì máy chủ trả mã 200 kèm `success: false`
và phần thân rỗng; riêng lỗi xác thực mới dùng 401. Nhìn mã trạng thái thôi là
đọc lỗi thành "không có dữ liệu".

**Không lọc được theo khoảng thời gian.** `filter[updatedAt]` chỉ khớp đúng một
mốc; thêm tiền tố so sánh thì trả 400. Xem
[quyết định 0003](../decisions/0003-quet-moi-nhat-truoc.md).

**Định tuyến theo tên miền ảo.** DNS cụm không phân giải `imate.local`, phải
gọi bằng IP nhưng vẫn ghi tên vào tiêu đề `Host`.

Ngoài ra: `pageSize` trần 100, và tham số `userGeneratedAttachments` bị bỏ qua
lặng lẽ.

## Hợp đồng danh sách

Tám trường bắt buộc, kiểm **trước khi ghi bất cứ thứ gì**:

```
globalId  tenantId  documentId  documentNo
uploadedAt  createdAt  updatedAt  processStatus
```

Hợp đồng kiểm đủ trường bắt buộc và đúng `tenantId` trước khi ghi. Kiểu dữ
liệu của các trường hợp đồng được Apicurio đối chiếu theo schema suy từ mẫu
thật; thay đổi phá vỡ đưa lượt chạy vào `schema_blocked`.

## Hai trục thời gian

| Trường | Ý nghĩa | Dùng vào |
|---|---|---|
| `uploadedAt` | Ngày nghiệp vụ — văn bản được đưa lên | Trục đếm của báo cáo, 169 ngày có dữ liệu |
| `updatedAt` | Mốc kỹ thuật — lần sửa gần nhất | Con trỏ phát hiện thay đổi, đo độ tươi |
| `createdAt` | Thời điểm trình thu thập ghi nhận | Giữ nguyên văn ở Silver-1, chưa dùng |

Trộn hai trục này là lỗi kinh điển: đếm theo `updatedAt` sẽ ra 12 ngày thay vì
169, vì đó là nhịp của trình thu thập chứ không phải nhịp của nghiệp vụ.

## Bóc tách số hiệu văn bản

Theo Nghị định 30/2020, ba quy ước:

| Mẫu | Dạng | Ví dụ |
|---|---|---|
| A | `số/[năm/]KÝHIỆU-CƠQUAN` | `1234/2025/QĐ-UBND` |
| B | `số-KÝHIỆU/CƠQUAN[#mã]` | `56-QĐ/TU` |
| C | `số/CƠQUAN` | `789/STC` |

**Công văn không mang ký hiệu loại** — phần đứng ở vị trí ký hiệu chính là cơ
quan. Nhận nhầm chỗ này từng sinh ra 186 loại văn bản rác, trong đó có "UBND".

Danh mục **loại** là danh mục đóng theo Phụ lục I. Danh mục **cơ quan** là mở,
tự đăng ký với `is_confirmed = false`; cái không bóc tách được vào
`metadata.mapping_rejections` kèm số lượng và lý do.

## Bảng

```
ingestion.doc_worklist   sổ công việc — discovered → landed → staged
                         → mapped → published | failed
staging.stg_imate__*     bốn bảng toàn chuỗi, nguyên văn
refdata.document_kind    danh mục đóng, 39 loại
refdata.issuing_body     danh mục mở, 340 cơ quan — toàn bộ chưa xác nhận
refdata.imate_unit       danh bạ đơn vị, chỉ thêm không xoá
refdata.imate_contact    danh bạ người, chỉ thêm không xoá
curated.fact_document    khoá chính global_id — 6.141 dòng
curated.fact_routing     khoá (global_id, seq) — 99.214 dòng
curated.dim_date         504 ngày, sinh đủ cả ngày không có văn bản
curated.dim_document_kind / dim_issuing_body    thành viên −1 "Chưa xác định"
```

Hai bảng dữ kiện **cố ý ở hai grain khác nhau**: mỗi văn bản có khoảng 16 lượt
chuyển, join vào mà không tổng hợp trước là nhân số lên 16 lần.

## Giới hạn đã biết

| Giới hạn | Số đo |
|---|---|
| Ba API danh bạ nằm sau JWT chưa được cấp | Định tuyến phân giải 79% — 96.652 người / 2.133 đơn vị / 429 không rõ |
| Tên cơ quan đều là suy đoán từ mã | 340/340 `is_confirmed = false` |
| Nguồn xoá văn bản thì không thấy | Chỉ đo được bằng chênh lệch tổng số |
| Lỗ hổng dữ liệu nguồn | Tháng 05/2025 và từ 07/2025; độ tươi chậm 96 ngày |
