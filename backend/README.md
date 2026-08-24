# Backend điều hành kho dữ liệu

Backend dùng **Spring Boot 4.1.1 + Java 25 LTS + Gradle 9.7.0**; Liquibase
được pin ở bản stable `5.0.4`. React/Refine là UI duy nhất; backend là
BFF/control plane sở hữu session, phân quyền, run ledger và credential Airflow.

## Kiến trúc

Mã tổ chức theo feature:

```text
vn.dth.dwh
├── identity   # Spring Security, Keycloak, /api/me, /api/csrf
├── pipeline   # domain, use case, REST, JPA, Airflow adapter
├── overview   # KPI/read model cho trang tổng quan
└── shared     # ProblemDetail và cấu hình dùng chung
```

Luật phụ thuộc của mỗi feature:

```text
api → application → domain
          ↑
   infrastructure
```

Controller không gọi repository. HTTP gọi Airflow không nằm trong transaction
database dài. Trình duyệt không gọi trực tiếp Airflow.

## Stack

- Spring MVC, Spring Security, OAuth2 Client;
- Spring Data JPA/Hibernate;
- Liquibase XML, changelog append-only;
- PostgreSQL production, H2 PostgreSQL mode cho test;
- Actuator/Micrometer;
- Spring `RestClient` khóa HTTP/1.1 cho Airflow ingress.

## API

| Method | Endpoint | Quyền |
|---|---|---|
| `GET` | `/api/me` | authenticated |
| `GET` | `/api/csrf` | authenticated |
| `POST` | `/api/logout` | authenticated + CSRF |
| `GET` | `/api/platform/overview` | `platform.overview.read` |
| `GET` | `/api/pipelines/imate/runs` | `pipeline.read` |
| `GET` | `/api/pipelines/imate/runs/{id}` | `pipeline.read` |
| `GET` | `/api/pipelines/imate/runs/{id}/events` | `pipeline.read` |
| `POST` | `/api/pipelines/imate/runs` | `pipeline.trigger` + CSRF |

Lỗi API dùng RFC 9457 `ProblemDetail`. Request chưa đăng nhập dưới `/api/**`
nhận 401, không bị redirect sang HTML login.

## Run ledger

Liquibase tạo ba bảng:

- `pipeline_run`: trạng thái tổng hợp hiện tại;
- `pipeline_step_run`: một dòng cho mỗi DAG/phase;
- `pipeline_run_event`: audit append-only có idempotency key.

JPA entity và changelog phải được thay đổi cùng một lần; Hibernate chạy
`ddl-auto=validate`, không tự sửa schema.

DAG phát `correlation_id`, actor, scope và metrics trong `AssetEvent.extra`.
Reconciler đọc Airflow `/api/v2/assets/events` theo lịch, replay event theo thứ
tự thời gian và upsert ledger. GET của UI chỉ đọc PostgreSQL, nên lịch sử vẫn
hiển thị khi Airflow tạm mất kết nối.

## Xác thực

Spring OAuth2 Login giữ token phía server; browser chỉ giữ `JSESSIONID`. Claim
Keycloak `roles` hoặc `realm_access.roles` được ánh xạ:

- `quan_tri`, `chuyen_vien` → overview/read/trigger;
- `lanh_dao`, `ke_toan_dv` → overview/read.

CSRF không bị tắt. Frontend lấy token qua `/api/csrf` trước POST/logout.

## Chạy kiểm tra

```powershell
.\gradlew.bat --no-daemon clean test
```

Test profile dùng H2 in-memory và chạy đúng master Liquibase trước khi Hibernate
validate entity model.

## Chạy local với PostgreSQL

```powershell
$env:DWH_CONTROL_DB_URL = "jdbc:postgresql://localhost:5432/dwh_control"
$env:DWH_CONTROL_DB_USERNAME = "dwh_control"
$env:DWH_CONTROL_DB_PASSWORD = "..."
.\gradlew.bat bootRun
```

Mặc định `AIRFLOW_MODE=fixture`. Chế độ live cần `AIRFLOW_BASE_URL`,
`AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` và Keycloak client configuration từ
Kubernetes Secret/environment; không ghi secret vào repository.

## Messaging

Không dùng Kafka hoặc RabbitMQ cho control plane hiện tại. Chiều command dùng
HTTP đồng bộ Spring → Airflow; chiều trạng thái dùng Airflow Asset event →
reconciler → PostgreSQL ledger. Broker chỉ được thêm khi polling không còn đáp
ứng độ trễ hoặc có nhiều consumer độc lập.
