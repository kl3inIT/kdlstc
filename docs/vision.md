# Ý định — kiến trúc hướng tới

Tệp này mô tả **điều dự án định làm**, và giữ nguyên như vậy kể cả sau khi đã
làm xong. Điều đã thành hiện thực ghi ở [../ARCHITECTURE.md](../ARCHITECTURE.md);
khoảng cách giữa hai tệp chính là [roadmap](roadmap.md).

## Bài toán

Sở Tài chính cần một chỗ duy nhất trả lời được các câu hỏi tổng hợp bắc qua
nhiều hệ thống nghiệp vụ — mỗi hệ thống do một đơn vị khác vận hành, dùng danh
mục khác nhau, và không hệ thống nào được phép sửa từ phía kho.

Ràng buộc định hình mọi thứ còn lại: **kho chỉ đọc, không bao giờ ghi ngược về
nguồn**, và **mọi con số trên báo cáo phải lần ngược được về byte gốc**.

## Sáu lớp

1. **Nguồn dữ liệu** — TABMIS, QLVBĐH, MISA, Mua sắm tập trung, Quản lý Giá.
2. **Thu thập & Điều phối** — connector theo từng nguồn, Airflow lo lịch và
   thử lại, dlt lo tầng vận chuyển HTTP.
3. **Lưu trữ & Xử lý** — Raw Zone trên SeaweedFS → Schema Registry (Apicurio)
   → Mapping Engine → Quality Gate (GX Core) → Curated Store hình sao.
4. **Phục vụ & Khai thác** — Cube làm lớp ngữ nghĩa, Superset đọc qua Cube,
   APISIX làm cổng ra IOC và LGSP.
5. **AI / Trợ lý ảo** — ngoài phạm vi giai đoạn hiện tại.
6. **Nền tảng dùng chung** — Keycloak, Redis, RabbitMQ, giám sát.

## Bảy bước

Hành trình của một bản ghi, mỗi bước làm đúng một việc:

| Bước | Tên | Việc |
|---|---|---|
| 1 | Nguồn | Hỏi nguồn: có gì mới hoặc vừa sửa? Lập danh sách việc |
| 2 | Bronze | Tải về, cất nguyên byte, bất biến |
| 3 | Silver-1 | Trải phẳng thành bảng, vẫn nguyên văn, chưa ép kiểu |
| 4 | Silver-2 | Ép kiểu, tra danh mục, chấm chất lượng |
| 5 | Gold | Dựng lược đồ hình sao để đếm |
| 6 | Serving | Mở cửa cho bên ngoài đọc, một định nghĩa chỉ tiêu duy nhất |
| 7 | Khai thác | Báo cáo, biểu đồ, API cho hệ thống khác |

Vì sao tách bảy bước thay vì một luồng liền: mỗi bước hỏng theo cách khác nhau
và cần cách chữa khác nhau. Nguồn chập chờn thì thử lại bước 1; ánh xạ sai thì
sửa quy tắc rồi chạy lại từ bước 4 mà không cần gọi lại nguồn.

## Năm tình huống phải chứng minh được

Lãnh đạo nêu năm tình huống làm thước đo kiến trúc có dùng được hay không:

1. **Nguồn đổi cấu trúc dữ liệu** — phân biệt thêm trường với đổi kiểu; đổi
   kiểu phải chặn được lô, có nhật ký, có người nhận.
2. **Trường dữ liệu rỗng** — Bronze giữ nguyên trạng; báo cáo hiện "Chưa xác
   định" chứ không tự biến thành 0; đo được tỉ lệ rỗng theo từng trường.
3. **Nguồn khai báo sửa hồi tố** — trả lời được *áp dụng từ bao giờ*, và giải
   thích được vì sao báo cáo in hôm nay khác bản in tháng trước.
4. **Nhiều cơ quan cùng tên khác mã** — không nhân đôi số liệu; trường hợp
   chưa phân giải được vào hàng đợi ngoại lệ có người phụ trách và hạn xử lý.
5. **Báo cáo ghép hai nguồn** — thống nhất grain và kỳ; hiện rõ nguồn gốc và
   độ tươi từng nguồn; một nguồn thiếu thì nói ra chứ không lặng lẽ cộng thiếu.

Cả năm đều dựng trên **nguồn mô phỏng**; iMate là nguồn thật duy nhất và giữ
vai trò đối chứng — thêm cơ chế mới xong, số liệu iMate phải không đổi.

## Nguyên tắc không đánh đổi

**Bronze là bằng chứng.** Không ép kiểu, không chuẩn hoá, không sửa một byte.
Mọi thứ sau đó dựng lại được từ đây.

**Kho sở hữu con trỏ.** Công cụ vận chuyển không giữ bản trạng thái thứ hai —
hai bản trạng thái thì sớm muộn lệch nhau và không biết tin bên nào.

**Không có gì biến mất im lặng.** Bản ghi hỏng nằm lại có lý do và đếm được;
số không phân giải được hiện thành "Chưa xác định" chứ không bị lọc bỏ.

**Đo cái mình không biết.** Nguồn xoá bản ghi thì quét dừng sớm không thấy —
nên báo bằng một con số chênh lệch thay vì giả vờ là đủ.
