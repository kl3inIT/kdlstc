# Hiện trạng — những gì đã dựng và đang chạy

Tệp này chỉ ghi **sự việc đã thành hiện thực**. Ý định và thứ chưa làm nằm ở
[docs/vision.md](docs/vision.md) và [docs/roadmap.md](docs/roadmap.md).

Cập nhật: 21/08/2026.

## Nền tảng

| Thành phần | Phiên bản / vị trí |
|---|---|
| Kubernetes | Rancher, project `local:p-vs2td` |
| Airflow | 3.2.2, namespace `stc-hy-airflow` |
| Object storage | SeaweedFS (S3), namespace `stc-hy` |
| Warehouse | PostgreSQL — `stc_dwh` (QL Giá, TABMIS), `stc_imate` (iMate) |
| BI | Superset 6.1, namespace `stc-hy-bi` |
| SSO | Keycloak, realm `khodl` |
| Ảnh Airflow | `ghcr.io/kl3init/kdlstc-airflow:3.2.2-dlt1.21.0-r1` |

Namespace phải được tạo **kèm** annotation `field.cattle.io/projectId`, nếu không
sẽ thành namespace mồ côi không có quyền gì. Xem
[docs/guidelines/trien-khai.md](docs/guidelines/trien-khai.md).

## Ba lát cắt nguồn

| Lát cắt | Nguồn | Kiểu nạp | Trạng thái |
|---|---|---|---|
| **iMate** | API văn bản, tenant `doit.phuyen` | merge-upsert theo `globalId` | Chạy thật, 6.141 văn bản |
| **QL Giá** | API giá mô phỏng | incremental theo con trỏ | Chạy thật |
| **TABMIS** | Tệp Excel tải lên | thay thế theo kỳ | Chạy thật |

Chi tiết từng lát cắt: [docs/specs/](docs/specs/).

## Bảy bước của lát cắt iMate

Tám DAG phủ bảy bước; bước 4 được chẻ làm hai, ghi bằng chữ `b`.

```
01 discover     bước 1 · Nguồn
02 land_bronze  bước 2 · Bronze
03 silver_one   bước 3 · Silver-1
04 silver_two   bước 4 · Silver-2
04b quality_gate    ─┘ (cùng bước 4, chạy riêng)
05 publish      bước 5 · Gold
06 serving      bước 6 · Serving
07 report       bước 7 · Khai thác
```

Nối nhau bằng Asset của Airflow, không DAG nào gọi tên DAG nào. Xem
[docs/guidelines/dag-airflow.md](docs/guidelines/dag-airflow.md).

## Số liệu đang có (21/08/2026)

```
fact_document    6.141      dim_document_kind   39
fact_routing    99.214      dim_issuing_body   340
                            dim_date           504
```

Độ tươi: văn bản mới nhất 17/05/2026 — chậm 96 ngày, đúng với lỗ hổng dữ liệu
nguồn đã biết (xem [docs/roadmap.md](docs/roadmap.md)).

## Lệnh hay dùng

```bash
# Đồng bộ DAG lên cụm
kubectl -n stc-hy-airflow create configmap airflow-dags \
  --from-file=platform/dags --dry-run=client -o yaml | kubectl apply -f -

# Áp schema cho một lát cắt
SQL_DIR=sql-imate SECRET=imate-db CM=imate-sql ./platform/scripts/apply-sql.sh

# Dựng lại kết nối chỉ-đọc cho bước 6
./platform/scripts/create-imate-reader-connection.sh

# Xem sổ cái lượt chạy
psql -d stc_imate -c "SELECT run_id, status, row_count FROM ingestion.runs
                      ORDER BY started_at DESC LIMIT 10"
```

## Bố cục kho mã

```
platform/dags/       DAG và mã chạy trong pod
platform/sql/        schema stc_dwh (QL Giá, TABMIS)
platform/sql-imate/  schema stc_imate (iMate) — tách riêng để cách ly
platform/dbt/        mô hình dbt cho lát cắt QL Giá
platform/helm/       values Helm
platform/k8s/        manifest thuần
platform/scripts/    script dựng lại hạ tầng, idempotent
khaithac/            prototype backend khai thác
jmix-mocks/          nguồn mô phỏng Thu/Chi
```
