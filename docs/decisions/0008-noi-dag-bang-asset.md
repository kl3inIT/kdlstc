# 0008 — Nối DAG bằng Asset, không gọi tên nhau

**Trạng thái:** Hiệu lực · **Ngày:** 14/08/2026

## Bối cảnh

Bảy bước là bảy DAG riêng. Chúng phải chạy nối tiếp, và cần một cơ chế để
DAG sau biết DAG trước đã xong.

## Quyết định

Dùng Asset của Airflow 3. DAG trước khai báo `outlets=[X]`, DAG sau khai báo
`schedule=[X]`. **Không DAG nào nhắc tên DAG nào.**

Danh sách Asset nằm ở `platform/dags/imate_assets.py`, đặt tên theo **bước kiến
trúc mà nó kết thúc**, không theo DAG phát ra nó.

## Phương án đã loại

**TriggerDagRunOperator** — DAG trước phải gọi đích danh mọi DAG sau. Thêm một
DAG tiêu thụ nghĩa là phải sửa DAG sản xuất; một bước sau này nuôi ba DAG thì
sửa ba lần. Với Asset, bên tiêu thụ tự đăng ký, bên sản xuất không đổi dòng nào.

**ExternalTaskSensor** — DAG sau phải đoán đúng thời điểm chạy của DAG trước,
và tốn slot chờ.

## Hệ quả

**Tác vụ bị bỏ qua thì không phát sự kiện.** Đây là cơ chế quan trọng nhất:
khi bước 1 không tìm thấy gì mới, tác vụ đóng vé chủ động ném ngoại lệ bỏ qua,
và sáu DAG phía sau tiếp tục ngủ thay vì chạy rỗng. Nhờ vậy bước 1 chạy được
10 phút một lần mà gần như không tốn gì.

Bàn giao dữ liệu **không đi qua Asset**. Asset chỉ là chuông báo; danh sách
việc thật nằm trong bảng `ingestion.doc_worklist` — 6.141 dòng không phải là
thứ nhét vừa XCom, và danh sách ấy đằng nào cũng phải bền qua các lượt chạy.
