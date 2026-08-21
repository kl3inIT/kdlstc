# 0004 — Mỗi lát cắt một cơ sở dữ liệu và một vai trò riêng

**Trạng thái:** Hiệu lực · **Ngày:** 15/08/2026

## Bối cảnh

Lát cắt iMate là bài toán kiểm chứng kiến trúc, chạy song song với hai lát cắt
đã ổn định (QL Giá, TABMIS). Một lỗi trong lát cắt mới không được phép làm hỏng
dữ liệu của hai lát cắt kia.

## Quyết định

iMate có **cơ sở dữ liệu riêng** `stc_imate`, **vai trò riêng** `imate_etl`,
**Airflow Connection riêng** `imate_dwh`, **bucket S3 riêng** `imate`, và thư
mục schema riêng `platform/sql-imate/`.

Thêm vai trò chỉ-đọc `imate_reader` cho công cụ BI và cho bước 6.

## Phương án đã loại

**Dùng chung `stc_dwh`, tách bằng schema** — đơn giản hơn, nhưng một lệnh
`DROP` gõ nhầm vẫn với tới được dữ liệu lát cắt khác, và nhật ký kiểm toán
không phân biệt được ai ghi.

**Tách vai trò nhưng chung cơ sở dữ liệu** — đã cân nhắc và bị bác: phạm vi
ảnh hưởng vẫn là toàn bộ instance khi có sự cố.

## Hệ quả

Bán kính thiệt hại của lát cắt mới đúng bằng danh sách quyền của `imate_etl`.

Đổi lại, không join trực tiếp được giữa dữ liệu iMate và hai lát cắt kia — khi
làm báo cáo ghép nguồn sẽ phải giải quyết. Đây là chi phí đã biết trước và chấp
nhận, vì giai đoạn này ưu tiên cách ly hơn tiện lợi.
