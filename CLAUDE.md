# Kho dữ liệu tổng hợp ngành Tài chính — Sở Tài chính Hưng Yên

Tệp này là **bản đồ dẫn**, không phải nơi chứa nội dung. Mỗi sự việc chỉ có
một chỗ ở; đọc sâu bằng cách đi theo liên kết, không nạp sẵn mọi thứ.

## Bắt đầu từ đâu

| Câu hỏi | Đọc |
|---|---|
| Hệ thống **đang** có gì, chạy ở đâu, gõ lệnh nào | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Hệ thống **định** trở thành gì, vì sao chọn kiến trúc này | [docs/vision.md](docs/vision.md) |
| Còn thiếu gì, làm theo thứ tự nào | [docs/roadmap.md](docs/roadmap.md) |
| Một lát cắt nguồn hoạt động ra sao | [docs/specs/](docs/specs/) |
| Cơ chế dùng chung giữa các lát cắt | [docs/guidelines/](docs/guidelines/) |
| Vì sao ngày ấy chọn thế, đã loại phương án nào | [docs/decisions/](docs/decisions/) |

## Quy ước trong kho mã này

**Định danh bằng tiếng Anh, tài liệu bằng tiếng Việt.** Tên bảng, tên cột, tên
DAG, tên biến đều tiếng Anh không dấu. Tài liệu, chú giải hướng người đọc, và
tên hiển thị trên báo cáo thì tiếng Việt có dấu đầy đủ. Mã danh mục giữ ASCII.

**Không có mật khẩu nào trong kho mã.** Mọi bí mật sinh ra trong cụm và nằm
trong Secret của Kubernetes. Script dựng lại được, giá trị thì không.

**Số DAG bám số bước kiến trúc**, không bám thứ tự viết code. Xem
[docs/decisions/0007-danh-so-dag-theo-buoc-kien-truc.md](docs/decisions/0007-danh-so-dag-theo-buoc-kien-truc.md).

**Tài liệu lệch mã nguồn là lỗi, không phải nợ.** Sửa mã mà không sửa tài liệu
thì lần sau có người tin vào một năng lực không tồn tại — đã xảy ra ba lần,
ghi trong [docs/roadmap.md](docs/roadmap.md).

## Tài liệu chuyên sâu dạng trang

Các trang HTML dưới `docs/` là hồ sơ trình bày, đọc bằng trình duyệt:

- `docs/ho-so-pipeline-imate.html` — hồ sơ đầy đủ lát cắt iMate, 10 chương
- `docs/dag-imate-01-discover.html` — mổ xẻ bước 1
- `docs/ra-soat-cong-cu-7-buoc.html` — rà soát khoảng trống công cụ
- `docs/pipeline-ingest-qlgia.html` — lát cắt QL Giá
- `docs/kien-thuc-nen-kho-du-lieu.html` — kiến thức nền
