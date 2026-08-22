# Frontend điều hành kho dữ liệu

Frontend dùng React 19.2.8, Refine 5.0.12 và React Router 7.18.2; Router 8 chưa
được chọn vì adapter Refine 2.0.4 khai báo peer `react-router ^7`. Toolchain là
Vite 8.2.2, TypeScript 7.0.2, Tailwind CSS 4.3.3 và pnpm 11.22.0. Form dùng
TanStack Form 1.33.5 + Zod 4.4.3, không dùng React Hook Form hoặc Ant Design.

Trạng thái hiện tại dùng **primitive mặc định của shadcn/Radix** và
`@refine/layout-01` từ registry chính thức của Refine. Chưa có theme thương
hiệu, CSS tự viết hoặc block dashboard tùy biến; Tailwind chỉ được dùng để bố
trí các primitive theo responsive layout.

Lát cắt đầu đã dựng là **Tổng quan quy trình làm sạch dữ liệu 7 bước**:

- KPI chất lượng và backlog steward;
- timeline bảy bước đọc `PipelineRun`/`PipelineStepRun`, trong đó bước 4 có hai phase;
- trạng thái waiting/queued/running/success/partial/failed/stopped/schema-blocked;
- lịch sử lượt chạy và dialog drill-down metrics, DAG ID, run ID;
- dialog xác nhận trước khi chạy toàn bộ chuỗi;
- AppShell/sidebar/header chính thức của Refine và đăng nhập BFF/Keycloak.

## Chạy local

```bash
pnpm install
pnpm dev
```

Mặc định frontend dùng fixture và hiển thị nhãn `Dữ liệu minh hoạ`. Backend đã
hiện thực cùng contract với chế độ fixture/live. Để frontend gọi backend:

```bash
VITE_USE_FIXTURES=false pnpm dev
```

Vite chạy cổng `5174`, proxy `/api`, `/oauth2`, callback `/login/oauth2/**` và
`/logout` sang Spring Boot backend tại `http://localhost:8080`.

## Hợp đồng backend của lát cắt đầu

| Method | Endpoint | Công dụng |
|---|---|---|
| `GET` | `/api/me` | Người dùng và role hiện tại |
| `GET` | `/api/platform/overview` | KPI và trạng thái B1–B7 |
| `GET` | `/api/pipelines/imate/runs` | Các lượt chạy cùng công đoạn đã ghép theo correlation ID |
| `POST` | `/api/pipelines/imate/runs` | Ghi ledger và yêu cầu chạy toàn bộ chuỗi |

Frontend không gọi trực tiếp Airflow hoặc Cube. Spring Boot xác thực request,
sở hữu run ledger và gọi data-plane API bằng credential phía server. KPI
overview vẫn là dữ liệu minh họa; timeline/lịch sử dùng ledger thật khi tắt fixture.

Với POST/logout, frontend lấy CSRF token từ `GET /api/csrf` rồi gửi token trong
header/field mà Spring Security công bố. Không tắt CSRF cho BFF session.

## Ranh giới REST backend

- `/api/**` là use-case API cho read model và command; JPA entity không được
  expose thành generic CRUD.
- Spring Security ánh xạ role Keycloak thành authority nghiệp vụ và controller
  dùng `@PreAuthorize`; command ghi audit append-only ở server.
- Backend gọi Airflow bằng `RestClient` HTTP/1.1, timeout và credential phía
  server. Không chuyển credential data-plane xuống trình duyệt.
- UI dùng BFF/session cookie và CSRF. Client Keycloak/secret nằm trong
  Kubernetes Secret, không dùng lại client của Airflow hoặc Superset.

## Block official đã kiểm tra

| Registry | Block | Vai trò | Trạng thái |
|---|---|---|---|
| Refine | `@refine/layout-01` | Layout đọc resource để sinh sidebar/header | Đã dùng |
| Refine | `@refine/views` | Khung list/create/edit/show | Chỉ dry-run |
| Refine | `@refine/data-table` | Bảng có filter, sort, pagination | Chỉ dry-run |
| shadcn | `@shadcn/dashboard-01` | Dashboard/sidebar/chart/table tổng quát | Chỉ dry-run |

Các màn hiện tại dùng trực tiếp `Button`, `Card`, `Badge`, `Dialog`, `Alert` và
`Table` từ shadcn. Chỉ lấy `views` hoặc `data-table` khi xây màn CRUD/hàng đợi
có filter, sort và pagination. Không lấy `dashboard-01` nguyên khối vì nó mang
theo chart, DnD và nhiều dependency chưa cần cho lát cắt đầu.
