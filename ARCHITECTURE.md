# Hiện trạng — những gì đã dựng và đang chạy

Tệp này chỉ ghi **sự việc đã thành hiện thực**. Ý định và thứ chưa làm nằm ở
[docs/vision.md](docs/vision.md) và [docs/roadmap.md](docs/roadmap.md).

Cập nhật: 21/08/2026.

## Nền tảng

| Thành phần | Phiên bản / vị trí |
|---|---|
| Kubernetes | Rancher, project `local:p-vs2td` |
| Airflow | 3.2.2, CeleryExecutor, namespace `stc-hy-airflow` |
| Object storage | SeaweedFS (S3), namespace `stc-hy` |
| Schema Registry | Apicurio Registry 3.1.7, PostgreSQL storage, namespace `stc-hy` |
| Quality engine | Great Expectations Core 1.21.0, chạy trong task Airflow |
| Warehouse | PostgreSQL — `stc_dwh` (QL Giá, TABMIS), `stc_imate` (iMate) |
| BI | Superset 6.1, namespace `stc-hy-bi` |
| SSO | Keycloak 26.6.4, realm `khodl`, namespace `stc-hy` |
| Ảnh Airflow | `ghcr.io/kl3init/kdlstc-airflow:3.2.2-dlt1.21.0-gx1.21.0-r1` |

Checkpoint Rancher ngày 21/08/2026: Helm release Airflow revision 13 chạy
digest `sha256:f8bb40efd9554a27627ec5f2bac7c6e6fc8c8188c207274cdf62fa9924de959f`;
worker và các control-plane pod đều Ready. Apicurio 3.1.7 là bản mới nhất chạy
được trên CPU hiện tại; từ dòng 3.2, image yêu cầu x86-64-v3 và chết ngay khi
khởi động trên các node này.

## Xác thực dùng chung

Keycloak realm `khodl` là SSO của nền tảng. Trình duyệt đi qua issuer công
khai do values local cấu hình; các dịch vụ trong cụm dùng Service nội bộ để
tránh hairpin qua Ingress. Mỗi ứng dụng có client và Secret riêng, không dùng
chung client-secret:

| Ứng dụng | Client | Nơi giữ bí mật |
|---|---|---|
| Airflow | `airflow` | `stc-hy-airflow/keycloak-airflow` |
| Superset | `superset` | khóa `keycloak-client-secret` trong `stc-hy-bi/superset-secrets` |
| SeaweedFS qua oauth2-proxy | `seaweedfs` | `stc-hy/oauth2-proxy-sw` |

Tên Secret và tên khóa được ghi để vận hành; giá trị không được đưa vào tài
liệu hay Git. App khai thác mới phải đăng ký client riêng trong cùng realm,
không tái sử dụng ba client trên.

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

08 replay       ngoài bảy bước — dựng lại kho từ Bronze, chạy bằng tay
```

## Giao diện

| Công cụ | Địa chỉ | Xác thực |
|---|---|---|
| Airflow | `airflow-stc.<node>.nip.io` | đăng nhập riêng |
| Superset | `bi-stc.<node>.nip.io` | SSO Keycloak |
| Apicurio | `registry-stc.<node>.nip.io` | SSO qua oauth2-proxy |
| pgweb · SeaweedFS | `s3-stc.<node>.nip.io` | SSO qua oauth2-proxy |
| Keycloak | `sso-stc.<node>.nip.io` | riêng |

Cube không có giao diện: chế độ thật tắt Playground, chỉ còn API sau JWT.

Hostname thật nằm trong các tệp `*-values.local.yaml` không vào git. **Mọi lệnh
helm upgrade phải kèm tệp local tương ứng** — thiếu nó thì ingress bị ghi đè bằng
hostname che và giao diện mất đường vào mà không tác vụ nào đỏ.

Nối nhau bằng Asset của Airflow, không DAG nào gọi tên DAG nào. Xem
[docs/guidelines/dag-airflow.md](docs/guidelines/dag-airflow.md).

Bước 1 đã nối Apicurio để lưu phiên bản JSON Schema và phân biệt thay đổi
`NONE` / `ADDITIVE` / `BREAKING`. Thay đổi phá vỡ được ghi vào sổ cái dưới
trạng thái `schema_blocked`. Bước 4b dùng GX Core; luật, ngưỡng, mức
`blocker` / `scoring` / `informational`, chủ sở hữu và hạn xử lý nằm trong
`metadata.quality_rules`, không còn đóng cứng trong Python.

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
# Đồng bộ project dbt lên cụm (bắt buộc sau khi sửa model)
kubectl -n stc-hy-airflow create configmap imate-dbt   --from-file=platform/dbt-imate/dbt_project.yml   --from-file=platform/dbt-imate/profiles.yml   --from-file=platform/dbt-imate/models/marts/ --dry-run=client -o yaml | kubectl apply -f -

# Dựng lại Apicurio và Cube (đổi model Cube thì phải chạy lại — pod mới nạp)
./platform/scripts/deploy-apicurio.sh
./platform/scripts/deploy-cube.sh

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
