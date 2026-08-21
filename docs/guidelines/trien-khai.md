# Quy ước triển khai

## Namespace phải sinh ra đã thuộc project

Rancher gắn namespace vào project bằng annotation. Tạo namespace bằng `kubectl`
thuần rồi annotate sau **không có tác dụng** — namespace thành mồ côi, không có
quyền gì và không hiện trong giao diện Rancher.

```bash
kubectl create ns <ten> --dry-run=client -o yaml \
  | kubectl annotate -f - --local -o yaml \
      field.cattle.io/projectId=local:p-vs2td \
  | kubectl apply -f -
```

Hạn mức theo namespace là thật: `limits.cpu` và số PVC. Một Job không tạo được
pod vì vượt hạn mức sẽ nằm im ở trạng thái "Running" mà **không có pod nào**,
và bộ điều khiển lặng lẽ thử lại theo cấp số nhân.

## Bí mật sinh trong cụm, không nằm trong kho mã

Script dựng hạ tầng phải **idempotent** và tự sinh mật khẩu nếu chưa có, đọc
lại nếu đã có. Kho mã giữ cách dựng lại, không giữ giá trị.

Một vai trò chỉ có **một mật khẩu**. Cần dùng ở nơi khác thì sao chép từ secret
gốc, đừng sinh cái thứ hai — hai mật khẩu cho cùng một vai trò nghĩa là hai chỗ
phải xoay vòng và chắc chắn có một chỗ sai.

## Đồng bộ DAG

Mã nguồn đến worker và đến pod bằng cùng một ConfigMap:

```bash
kubectl -n stc-hy-airflow create configmap airflow-dags \
  --from-file=platform/dags --dry-run=client -o yaml | kubectl apply -f -
```

Đổi tên DAG thì bản ghi cũ vẫn nằm trong cơ sở dữ liệu Airflow. Xoá bằng
`airflow dags delete <id> --yes`, nếu không danh sách sẽ có cả tên cũ lẫn tên mới.

Sau khi áp ConfigMap, kubelet mất tới khoảng một phút để gắn lại tệp, rồi
dag-processor mới quét. Kiểm tra bằng cách nhìn tệp trong pod trước, đừng nhìn
danh sách DAG.

## Vai trò chỉ-đọc cho công cụ BI

Công cụ BI dùng vai trò riêng chỉ có `SELECT`. Bước Serving kiểm cửa này **từ
bên ngoài** bằng chính vai trò đó — kết nối bằng vai trò ghi thì không chứng
minh được gì, vì bên ghi đọc được mọi thứ theo định nghĩa.

Kiểm cả chiều ngược: thử ghi và **bắt buộc phải bị từ chối**. Cách ly là lời
tuyên bố cho tới khi có thứ gì đó thử phá nó.
