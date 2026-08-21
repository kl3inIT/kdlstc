# 0005 — Hai chỉ số chất lượng không bao giờ trộn làm một

**Trạng thái:** Hiệu lực · **Ngày:** 15/08/2026

## Bối cảnh

Sau bước ánh xạ, có hai loại vấn đề khác hẳn nhau về bản chất:

- Bản ghi **không tra được danh mục** — thường vì nguồn dùng quy ước đánh số
  mới mà kho chưa có quy tắc. Đây là bài tập còn nợ.
- Bản ghi **hỏng cấu trúc** — ví dụ không có ngày nào dùng được. Đây là sự cố.

Gộp hai thứ này thành một điểm "chất lượng" thì một chỉ số cao che mất chỉ số
thấp, và người đọc không biết nên đi bổ sung quy tắc hay đi gọi cho đội nguồn.

## Quyết định

Hai chỉ số riêng, không bao giờ lấy trung bình:

| Chỉ số | Ngưỡng | Ý nghĩa khi rớt |
|---|---|---|
| `mapping_coverage` | ≥ 0,50 | Thiếu quy tắc ánh xạ — bài tập |
| `quality_score` | ≥ 0,95 | Dữ liệu hỏng — sự cố, chặn lô |

Chấm trên **toàn bộ ảnh chụp dữ liệu hiện tại**, không riêng lô vừa tới — vì
báo cáo đọc cả bảng, nên cổng phải chấm đúng thứ người đọc sẽ thấy.

## Phương án đã loại

**Một điểm tổng có trọng số** — gọn khi trình bày, nhưng che mất thông tin cần
thiết nhất là *nên gọi ai*.

**Chấm theo từng lô** — rẻ hơn, nhưng một lô sạch không nói gì về chất lượng
bảng mà báo cáo thực sự đọc.

## Hệ quả

Đến 21/08/2026, `mapping_coverage` **đang đo sai**: hai điều kiện "khớp loại"
và "khớp cơ quan" là cùng một biểu thức, nên con số công bố 95,47% thực chất
chỉ là tỉ lệ bóc tách được số hiệu. Xem [../roadmap.md](../roadmap.md), nhóm 1.

Ngưỡng hiện nằm cứng trong mã Python, chưa phải luật có phiên bản trong cơ sở
dữ liệu — đây là lý do kiến trúc chỉ định GX Core.
