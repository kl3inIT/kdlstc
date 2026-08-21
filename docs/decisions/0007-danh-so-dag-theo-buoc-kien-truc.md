# 0007 — Số DAG bám số bước kiến trúc

**Trạng thái:** Hiệu lực · **Ngày:** 21/08/2026

## Bối cảnh

Bảy DAG đầu tiên được đánh số 01 đến 07 theo thứ tự viết code. Khi đối chiếu
với sơ đồ bảy bước của kiến trúc, phát hiện chúng chỉ phủ **sáu trong bảy bước**:

- Cổng chất lượng được tách thành DAG riêng, khiến mọi số sau đó lệch một nấc.
- Bước 6 (Serving) **không tồn tại** — pipeline nhảy thẳng từ Gold sang báo cáo.

Con số bảy DAG trùng bảy bước là trùng hợp, không phải khớp kiến trúc.

## Quyết định

**Số tệp bám số bước kiến trúc.** Một bước bị chẻ làm nhiều DAG thì DAG thứ hai
mang chữ cái, không mang số mới:

```
01 discover  02 land_bronze  03 silver_one  04 silver_two + 04b quality_gate
05 publish   06 serving      07 report
```

Bổ sung DAG `imate_06_serving` cho bước còn trống.

## Phương án đã loại

**Gộp cổng chất lượng vào Silver-2 để giữ đúng bảy DAG** — khớp con số, nhưng
mất lý do kỹ thuật thật của việc tách: cổng chấm trên toàn bộ ảnh chụp dữ liệu,
nên phải chạy **sau khi** Silver-2 ghi xong, không nằm trong cùng giao dịch.

**Giữ nguyên và giải trình bằng bảng đối chiếu** — rẻ nhất, nhưng để lại một
bước thiếu và một cách đánh số mà người đọc phải tra bảng mới hiểu.

## Hệ quả

Hai DAG đổi định danh nên mất lịch sử chạy trong Airflow — chấp nhận được ở
giai đoạn kiểm chứng, và bản ghi thật vẫn nằm đủ trong `ingestion.runs`.

Điều các bên liên quan cần là **đủ bảy bước**, không phải đúng con số bảy DAG.
Chẻ nhỏ một bước là chi tiết kỹ thuật; thiếu một bước mới là lệch kiến trúc.
