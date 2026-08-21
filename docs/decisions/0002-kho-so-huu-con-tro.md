# 0002 — Kho sở hữu con trỏ, công cụ vận chuyển không giữ trạng thái

**Trạng thái:** Hiệu lực · **Ngày:** 12/08/2026, mở rộng cho iMate 19/08/2026

## Bối cảnh

dlt có sẵn cơ chế lưu trạng thái incremental của riêng nó. Dùng cơ chế đó thì
tiện, nhưng khi ấy tồn tại **hai bản trạng thái**: một của dlt, một trong bảng
`ingestion.cursors` / `ingestion.doc_worklist` của kho.

## Quyết định

Kho là chủ duy nhất của con trỏ. dlt chỉ làm **lớp vỏ HTTP**: phiên có thử
lại, có backoff, tái dùng kết nối. Nó không lưu trạng thái, không chuẩn hoá,
không quyết định khi nào dừng.

## Phương án đã loại

**Để dlt tự quản trạng thái incremental** — hai bản trạng thái sớm muộn lệch
nhau, và lúc đó không có cách nào biết bên nào đúng. Chạy lại một lượt hỏng
trở thành thao tác phải sửa ở hai chỗ.

## Hệ quả

Đổi công cụ vận chuyển sau này không đụng tới logic nghiệp vụ, vì logic nằm cả
trong kho. Đổi lại, phần phân trang và điều kiện dừng phải tự viết — với iMate
là quét giảm dần dừng sau ba trang sạch (xem [0003](0003-quet-moi-nhat-truoc.md)).

Phiên bản dlt được ghi vào mọi bản tóm tắt lượt chạy (`dlt-rest-client/1.21.0`),
nên truy được đối tượng Bronze nào do phiên bản nào sinh ra.
