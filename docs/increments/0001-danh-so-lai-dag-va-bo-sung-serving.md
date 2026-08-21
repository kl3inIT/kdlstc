# Đợt 0001 — Đánh số lại DAG và bổ sung bước Serving

**Trạng thái:** Hoàn thành · **Ngày:** 21/08/2026 ·
**Quyết định:** [0007](../decisions/0007-danh-so-dag-theo-buoc-kien-truc.md)

## Vấn đề

Bảy DAG chỉ phủ sáu trong bảy bước kiến trúc. Bước 4 bị chẻ làm hai khiến mọi
số sau đó lệch một nấc, và bước 6 Serving không tồn tại — pipeline nhảy thẳng
từ Gold sang báo cáo.

Hệ quả thật: báo cáo CSV và Superset đi hai đường SQL riêng cho cùng một chỉ
tiêu, không có nơi nào là định nghĩa gốc để phân xử khi hai bên lệch nhau.

## Đã làm

- `imate_05_quality_gate` → `imate_04b_quality_gate`
- `imate_06_publish` → `imate_05_publish`
- Thêm `imate_06_serving`
- `imate_07_report` nghe bước 6 thay vì đọc thẳng Gold
- Thêm `reader_cursor()` và Asset `imate://serving/documents`
- Thêm script `create-imate-reader-connection.sh`, sao mật khẩu vai trò đọc từ
  secret của Superset thay vì sinh mật khẩu thứ hai cho cùng vai trò

## Bước 6 làm gì

Ba phép kiểm mỗi lượt, đều qua vai trò `imate_reader`:

| Phép kiểm | Bắt lỗi gì |
|---|---|
| Đếm từng bảng curated | Thiếu `GRANT` — nếu không sẽ thành dashboard rỗng không ai đọc ra lỗi |
| Thử ghi, bắt buộc bị từ chối | Migration lỡ cấp quyền ghi cho vai trò đọc |
| Đo độ tươi từ văn bản mới nhất | Pipeline chạy đúng giờ trên một nguồn đã ngừng gửi |

## Kiểm chứng

Chạy thật ngày 21/08/2026:

```
exposed        fact_document 6.141 · fact_routing 99.214
               dim_date 504 · dim_issuing_body 340 · dim_document_kind 39
write_denied   true
staleness      96 ngày (đúng với lỗ hổng dữ liệu nguồn đã biết)
```

## Chưa xong

Cube — một định nghĩa chỉ tiêu dùng chung cho mọi kênh khai thác. DAG 06 là chỗ
Cube cắm vào, và Asset nó phát ra chính là mối nối đó. Xem
[../roadmap.md](../roadmap.md), nhóm 2.
