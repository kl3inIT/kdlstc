# 0006 — Pod hostNetwork riêng cho tác vụ chạm mạng

**Trạng thái:** Hiệu lực · **Ngày:** 14/08/2026

## Bối cảnh

API iMate nằm ở `10.123.123.199` cổng 80. Pod thường của Airflow mang địa chỉ
dải `10.42.0.0/16`, và dải này bị lọc ở cổng 80. Đo được: cổng 6443 trên đúng
máy chủ đó vẫn trả lời, nên đây là **lọc theo cổng, không phải thiếu đường mạng**.

## Quyết định

Hai tác vụ thật sự cần mạng (bước 1 và bước 2) chạy trong
`KubernetesPodOperator` với `hostnetwork=True` và
`dnspolicy="ClusterFirstWithHostNet"`. Pod mượn không gian mạng của node nên
địa chỉ nguồn là `10.123.123.x` và lọt qua bộ lọc.

Đặc quyền này **không cấp cho worker Airflow**.

## Phương án đã loại

**Bật hostNetwork cho worker** — một dòng cấu hình, xong ngay. Bị bác vì
hostNetwork cũng vô hiệu hoá NetworkPolicy và phơi các cổng nội bộ của node ra
cho tiến trình trong pod. Cấp cho worker là cấp cho **mọi DAG trong cụm**, kể
cả DAG người khác viết sau này.

**Xin mở tường lửa cho dải pod** — sạch nhất về mặt kiến trúc, nhưng phụ thuộc
đội hạ tầng bên ngoài và không kiểm soát được tiến độ. Vẫn là đường lui nếu
hostNetwork bị chính sách cụm chặn.

## Hệ quả

Đặc quyền bị nhốt vào một pod sống vài chục giây rồi tự xoá. Bù lại, mọi thứ
khác được siết: bỏ toàn bộ capability, cấm leo thang đặc quyền, không gắn token
tài khoản dịch vụ.

Pod hostNetwork dùng bộ phân giải tên miền của **node**, vốn không biết tên
dịch vụ trong cụm. Nên địa chỉ S3 được tra sẵn ra IP ở phía worker rồi truyền
vào pod qua biến môi trường.

Mã nguồn đến pod bằng đúng con đường đến DAG: ConfigMap `airflow-dags` gắn
chỉ-đọc vào `/src`. Một đường giao hàng, không có bản sao thứ hai để lệch.
