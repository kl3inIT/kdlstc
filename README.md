# KDLSTC

Prototype kho dữ liệu tài chính theo mô hình Bronze → Silver → Gold, dùng
Apache Airflow để điều phối, PostgreSQL làm warehouse, object storage giữ dữ
liệu Bronze và dbt cho các phép biến đổi SQL có kiểm thử.

Repository public chỉ chứa dữ liệu mô phỏng và cấu hình mẫu. IP, registry,
hostname SSO/Ingress, credential và values của môi trường thật không được lưu
trong Git.

## Thành phần chính

- `platform/dags/`: pipeline QL Giá và TABMIS.
- `platform/dbt/`: mô hình Silver được quản lý bằng dbt.
- `platform/sql/`: schema warehouse và các lát cắt nghiệp vụ.
- `platform/helm/`, `platform/k8s/`: template triển khai Kubernetes.
- `jmix-mocks/`: dịch vụ nguồn mô phỏng.
- `khaithac/`: prototype khai thác và trình bày dữ liệu.

Tài liệu vận hành chi tiết nằm tại [platform/README.md](platform/README.md).

## Airflow image

GitHub Actions build `platform/Dockerfile.airflow` và phát hành:

```text
ghcr.io/kl3init/kdlstc-airflow:3.2.2-dlt1.21.0-r1
```

Image kế thừa Airflow 3.2.2/Python 3.13 và cài sẵn các dependency được pin
trong `platform/requirements-airflow.txt`.

## Cấu hình môi trường

Các file trong Git sử dụng `example.com` và tên service chung. Khi deploy, đặt
giá trị thật trong `platform/helm/*-values.local.yaml` hoặc manifest `*.local.yaml`;
các file này đã được `.gitignore` loại trừ.

Không commit `.env`, kubeconfig, token, password, private key hoặc nội dung
Kubernetes Secret.

## License

Chưa cấp giấy phép sử dụng lại mã nguồn. Mọi quyền được bảo lưu cho chủ sở hữu.
