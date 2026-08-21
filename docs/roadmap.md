# Lộ trình — khoảng cách giữa ý định và hiện trạng

So [vision.md](vision.md) với [../ARCHITECTURE.md](../ARCHITECTURE.md). Bản rà
soát đầy đủ có sơ đồ: `ra-soat-cong-cu-7-buoc.html`.

Cập nhật: 21/08/2026 (sau đợt bổ sung Apicurio, GX Core, dbt).

## Đã khép trong đợt 21/08/2026

| Hạng mục | Kết quả | Kiểm chứng hiện tại |
|---|---|---|
| `mapping_coverage` đo sai | Tách thành bốn luật độc lập: ngày đăng, loại văn bản, cơ quan phân giải và cơ quan đã xác nhận | GX chạy bốn luật trên dữ liệu thật; chuỗi 04b → 05 → 06 → 07 giữ nguyên 6.141 / 99.214 |
| Ngưỡng chất lượng đóng cứng | GX Core 1.21.0 thực thi expectation; luật, ngưỡng, owner, SLA và phiên bản nằm trong `metadata.quality_rules` | Image Airflow mới đã rollout, worker Ready |
| Không có Schema Registry | Apicurio Registry 3.1.7 lưu PostgreSQL, bước 1 đăng ký và đối chiếu schema mỗi lượt | **Đã kiểm đủ 5 tình huống**: lần đầu `NONE` v1 · y nguyên `NONE` v2 · thêm trường `ADDITIVE` v3 · đổi kiểu `BREAKING` · bỏ trường bắt buộc `BREAKING` |
| `schema_blocked` không đạt được | Pod bắt lỗi hợp đồng/schema và tự ghi `schema_blocked` vào sổ cái trước khi thoát | Nhánh `BREAKING` đã kiểm chứng trên registry thật — tình huống 1 nay chặn được |
| Gold dựng bằng SQL viết tay | Chuyển sang dbt: project riêng `platform/dbt-imate`, 27 phép kiểm chạy mỗi lượt build | Đối chiếu schema riêng trước khi cắt sang: **0 dòng lệch** trên cả 5 bảng; chạy thật giữ nguyên 6.141 / 99.214 |
| Một chỉ tiêu, hai con số | Cube v1.7.24 làm lớp ngữ nghĩa; bước 7 hỏi Cube thay vì tự viết SQL | Tổng khớp tuyệt đối 6.141; CSV 3.839 dòng · 504 ngày · 335 dòng số 0 |
| 7 bước thiếu bước Serving | Thêm `imate_06_serving`: kiểm cửa đọc bằng vai trò `imate_reader`, thử ghi bắt buộc bị từ chối, ghi độ tươi | Chạy thật: 5 bảng đọc được, ghi bị từ chối, độ tươi 96 ngày |

## Nhóm 1 — Chỗ tài liệu và mã nguồn còn nói khác nhau

Nguy hiểm hơn "chưa làm", vì người đọc có thể tin vào năng lực không tồn tại.
Xử lý trước mọi việc bổ sung công cụ. Không cần công cụ mới.

| Hạng mục | Triệu chứng | Trạng thái |
|---|---|---|
| Phát lại từ Bronze | Hồ sơ nói làm được, chưa có mã thực hiện | Chưa vá |
| Không lưu phản hồi danh sách | Không trả lời được "vì sao pipeline cho rằng văn bản này đã đổi" | Chưa vá |
| Không kiểm giá trị tenant | Chỉ kiểm trường có mặt, không kiểm đúng đơn vị | Chưa vá |
| Hai cầu chì im lặng | Chạm trần 200 trang và độ lệch khi nguồn xoá — có đo, không báo | Chưa vá |

## Nhóm 2 — Công cụ kiến trúc

| Công cụ | Bù vào bước | Trạng thái |
|---|---|---|
| Apicurio Registry | 1 — lưu phiên bản, phân loại schema drift | Đã triển khai 3.1.7; tích hợp bước 1 đang kiểm chứng |
| GX Core | 4b — expectation chuẩn, luật điều hành nằm trong DB | Đã triển khai 1.21.0 |

| dlt tầng nạp | 3 — mới dùng tầng trích xuất | Chưa dùng |
| APISIX | 6 — chưa có cửa ra IOC/LGSP | Chưa triển khai |
| OpenSearch, RabbitMQ, giám sát | — | Chưa triển khai |

Bước 6 Serving **đã có** từ 21/08: kiểm cửa đọc bằng vai trò `imate_reader`,
thử ghi để bắt buộc bị từ chối, ghi độ tươi. Phần còn thiếu là Cube.

Bước 6 nay đã đủ lớp ngữ nghĩa. Còn thiếu PostgREST và APISIX — chỉ cần khi
tỉnh thật sự yêu cầu đẩy số sang IOC/LGSP; Superset vẫn cần chuyển sang đọc qua
Cube để hai kênh khai thác dùng chung một định nghĩa.

Ghi chú về Apicurio: bản mới nhất 3.3.1 **không chạy được** trên phần cứng hiện
có — từ 3.2 trở đi ảnh biên dịch với baseline x86-64-v3 mà CPU các node không hỗ
trợ. Đang dùng 3.1.7, là bản mới nhất chạy được. Khi thay phần cứng thì nâng.

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
- Secret bootstrap `stc-hy/keycloak-admin` vẫn tồn tại nhưng credential trong
  đó không đăng nhập được Admin CLI. Cần đối soát/rotate trước khi export realm
  hoặc tự động hoá tạo client mới; không dùng lại secret client của Airflow,
  Superset hay SeaweedFS cho app khai thác.
- `khaithac` vẫn là prototype nối realm mock `stc-mock`; khi đưa lên Rancher
  phải đăng ký client riêng trong realm `khodl` và bơm issuer/client-secret từ
  cấu hình môi trường/Kubernetes Secret.
