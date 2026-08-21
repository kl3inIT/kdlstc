# Lộ trình — khoảng cách giữa ý định và hiện trạng

So [vision.md](vision.md) với [../ARCHITECTURE.md](../ARCHITECTURE.md). Bản rà
soát đầy đủ có sơ đồ: `ra-soat-cong-cu-7-buoc.html`.

Cập nhật: 21/08/2026.

## Nhóm 1 — Chỗ tài liệu và mã nguồn đang nói khác nhau

Nguy hiểm hơn "chưa làm", vì người đọc có thể tin vào năng lực không tồn tại.
Xử lý trước mọi việc bổ sung công cụ. Không cần công cụ mới.

| Hạng mục | Triệu chứng | Trạng thái |
|---|---|---|
| `schema_blocked` không đạt được | Nguồn đổi cấu trúc thì pod chết câm, vé kẹt ở `received` | Chưa vá |
| `mapping_coverage` đo sai | Hai điều kiện khớp là cùng một biểu thức, nên 95,47% lạc quan hơn thực chất | Chưa vá |
| Phát lại từ Bronze | Hồ sơ nói làm được, chưa có mã thực hiện | Chưa vá |
| Không lưu phản hồi danh sách | Không trả lời được "vì sao pipeline cho rằng văn bản này đã đổi" | Chưa vá |
| Không kiểm giá trị tenant | Chỉ kiểm trường có mặt, không kiểm đúng đơn vị | Chưa vá |
| Hai cầu chì im lặng | Chạm trần 200 trang và độ lệch khi nguồn xoá — có đo, không báo | Chưa vá |

## Nhóm 2 — Công cụ kiến trúc đã gọi tên mà chưa có

| Công cụ | Bù vào bước | Trạng thái |
|---|---|---|
| Apicurio Registry | 1, 3 — mới kiểm trường có mặt, chưa kiểm kiểu | Chưa triển khai |
| GX Core | 4b — ngưỡng nằm cứng trong Python | Chưa triển khai |
| **Cube** | 6, 7 — mỗi kênh tự viết SQL, cùng chỉ tiêu có thể ra hai số | Chưa triển khai |
| dbt cho lát cắt iMate | 5 — dựng hình sao bằng SQL tay | Đã chạy ở QL Giá |
| dlt tầng nạp | 3 — mới dùng tầng trích xuất | Chưa dùng |
| APISIX | 6 — chưa có cửa ra IOC/LGSP | Chưa triển khai |
| OpenSearch, RabbitMQ, giám sát | — | Chưa triển khai |

Bước 6 Serving **đã có** từ 21/08: kiểm cửa đọc bằng vai trò `imate_reader`,
thử ghi để bắt buộc bị từ chối, ghi độ tươi. Phần còn thiếu là Cube.

## Nhóm 3 — Năm tình huống kiểm thử

Chưa dựng tình huống nào bên lát cắt iMate. Xem [vision.md](vision.md) để biết
mỗi tình huống phải chứng minh điều gì.

Lưu ý bối cảnh: nhóm QLVBĐH đã dựng xong cả năm tình huống trên nguồn mô phỏng
của họ (nhánh `test/gia-lap-tinh-huong-nghiep-vu`). Giá trị riêng của lát cắt
iMate là chạy được **trên dữ liệu thật quy mô thật**, nên chỉ nên dựng lại
tình huống nào tận dụng được lợi thế đó — rõ nhất là tình huống 3 (sửa hồi tố),
vì Bronze content-addressed đã sẵn mọi phiên bản.

## Việc cần bên ngoài nhóm kỹ thuật

| Việc | Chặn cái gì |
|---|---|
| Xin JWT cho ba API danh bạ iMate | 339 cơ quan đang mang tên suy từ mã, `is_confirmed = false`; độ phân giải định tuyến dừng ở 79% |
| Xin danh mục mã đơn vị chuẩn của tỉnh | Không có đích đối chiếu cho tình huống 4 |
| Đối chất với đội nguồn iOffice | Lỗ hổng dữ liệu tháng 05/2025 và từ 07/2025; độ tươi hiện chậm 96 ngày |

## Nợ kỹ thuật đã biết

- Bộ lọc "Đơn vị gửi" trên báo cáo đang lọc theo tên hiển thị, nên dùng mã và
  hiển thị tên thì đúng hơn.
- Danh mục tham chiếu của hai lát cắt cũ trong `stc_dwh` vẫn chưa có dấu tiếng
  Việt, trong khi `stc_imate` đã Việt hoá.
- Namespace mồ côi `stc-hy-superset` còn sót, chờ xoá.
- Bố cục báo cáo Superset dựng bằng lệnh gọi API, chưa xuất thành tệp cấu hình
  đưa vào kho mã, nên chưa dựng lại được từ mã nguồn.
