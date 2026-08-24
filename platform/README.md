# Nền tảng kho dữ liệu — STC Hưng Yên (Rancher)

Toàn bộ hạ tầng và pipeline chạy trên cụm Rancher. Thư mục này là nguồn sự
thật duy nhất: cụm được dựng lại từ đây, không phải từ lệnh gõ tay.

Thư mục `dags/`, `docs/`, `jmix-mocks/`, `khaithac/` ở gốc repo là bản
docker-compose đợt trước, không liên quan tới cụm này.

> Mọi định danh trong mã và cơ sở dữ liệu đều bằng tiếng Anh. Tài liệu và giải
> thích bằng tiếng Việt.

---

## 1. Kiến trúc

```
                    ┌─ nguồn giả lập ────────────┐
                    │  mock-qlgia  (REST, phân   │
                    │  trang, cursor lastModified)│
                    └──────────┬─────────────────┘
                               │  dlt REST client
                    ┌──────────▼─────────────────┐
   BRONZE           │ SeaweedFS S3               │  bất biến, nguyên văn
   bronze/qlgia/    │ page-00001.json            │  giữ tên trường của nguồn
   <period>/<run>/  │ _manifest.json             │  + checksum + số dòng
                    └──────────┬─────────────────┘
                               │  đọc NGƯỢC từ S3, không dùng lại HTTP response
                     ┌──────────▼─────────────────┐
   SILVER-1         │ staging.stg_qlgia__price   │  1-1 với nguồn, toàn text
                     │                            │  đổi tên camelCase → cột kho
                     └──────────┬─────────────────┘
                               │  Airflow tạo 1 dbt pod cho lô, xong tự xóa
                     ┌──────────▼─────────────────┐
   SILVER-2         │ stg_qlgia__price_typed     │  ép kiểu, khử trùng,
                     │                            │  ánh xạ mã, gắn cờ is_valid
                     └──────────┬─────────────────┘
                     ┌──────────▼─────────────────┐
   INTERMEDIATE     │ stg_qlgia__price_grain     │  tổng hợp theo mặt hàng ×
                     │                            │  địa bàn × kỳ khảo sát
                     └──────────┬─────────────────┘
                               │  Airflow kiểm tra chất lượng rồi công bố
                    ┌──────────▼─────────────────┐
   GOLD             │ curated.fact_price         │  merge-upsert theo khóa
                    │ curated.batch_summary      │  nghiệp vụ
                    └────────────────────────────┘
```

Hai quyết định không hiển nhiên, cần biết trước khi đọc mã:

**Silver-1 nạp từ Bronze, không dùng lại phản hồi HTTP đang giữ trong bộ nhớ.**
Tốn thêm một lượt đọc, đổi lại được khả năng phát lại: tháng sau đổi quy tắc
ánh xạ thì dựng lại đúng lô cũ từ byte đã lưu, không phải xin nguồn cấp lại số
mà có khi nguồn không còn tái tạo được.

**Dòng hỏng không bị vứt.** Chúng nằm lại Silver-2 với `reject_reason` và được
chép sang `metadata.quarantine_rows`. Câu hỏi "sao báo cáo tháng 7 thiếu Phù
Cừ" phải có câu trả lời chỉ được vào một dòng cụ thể.

---

## 2. Đang chạy ở đâu

| Namespace | Thành phần |
|---|---|
| `stc-hy` | Keycloak 26.6.4 · oauth2-proxy · SeaweedFS (master/volume/filer/s3/admin) · Apicurio Registry 3.1.7 · mock-qlgia · pgweb |
| `stc-hy-airflow` | Airflow 3.2.2 (api-server, scheduler, dag-processor, triggerer, worker) + PostgreSQL + Redis |
| `stc-hy-bi` | Superset 6.1 (web + worker) + PostgreSQL metadata + Redis |
| PostgreSQL ngoài cụm | `stc_dwh` — kho dữ liệu · `stc_keycloak` |

| Dịch vụ | Địa chỉ |
|---|---|
| Airflow | `http://airflow-stc.10.123.123.194.nip.io` |
| Keycloak | `http://sso-stc.10.123.123.194.nip.io` |
| SeaweedFS Admin | `https://storage.example.com` |
| PostgreSQL inspector (pgweb) | `https://storage.example.com/db/` |

Hai hostname `nip.io` trên là ingress đang chạy tại checkpoint 22/08/2026; các
hostname SeaweedFS bên dưới vẫn là placeholder trong values công khai. Đăng
nhập bằng Keycloak realm `khodl`.

---

## 3. Dựng lại từ đầu

```bash
export DWH_HOST=postgres.example.com DWH_DBNAME=stc_dwh DWH_USER=stc_dwh
export DWH_PASSWORD=...           # xem mục 6
export KC_DB_PASSWORD=... KC_ADMIN_USER=admin KC_ADMIN_PASSWORD=...
export KC_AIRFLOW_CLIENT_SECRET=... KC_SW_CLIENT_SECRET=...
export O2P_COOKIE_SECRET="$(openssl rand -base64 32 | tr -- '+/' '-_' | tr -d '=')"

# Tuỳ chọn: values thật nằm ngoài Git và ghi đè các placeholder public
export KEYCLOAK_VALUES_LOCAL="$PWD/helm/keycloak-values.local.yaml"
export OAUTH2_PROXY_VALUES_LOCAL="$PWD/helm/oauth2-proxy-values.local.yaml"
export AIRFLOW_VALUES_LOCAL="$PWD/helm/airflow-values.local.yaml"

./scripts/create-secrets.sh     # bắt buộc chạy trước
./scripts/deploy.sh             # helm cho toàn bộ, hoặc: ./deploy.sh airflow
./scripts/apply-sql.sh          # schema + danh mục cho kho
./scripts/create-jmix-airflow-service.sh  # user Op + Secret cho Jmix gọi API
./scripts/create-jmix-keycloak-client.sh  # client OIDC + roles mapper + Secret
```

`deploy.sh` ghim phiên bản chart. Nâng cấp không ghim là cách một cụm đang chạy
lặng lẽ biến thành một cụm khác cũng đang chạy.

---

## 4. Vòng lặp thường ngày

| Việc | Lệnh | Bao lâu thì có hiệu lực |
|---|---|---|
| Sửa DAG | `./scripts/sync-dags.sh` | ~3 phút (kubelet ~60s + `min_file_process_interval` 120s) |
| Sửa dbt model | `bash ./scripts/sync-dbt.sh` | job dbt kế tiếp |
| Sửa dependency Airflow | Linux: `bash ./scripts/build-airflow-image.sh`; Windows: `.\scripts\build-airflow-image.ps1` — sau đó deploy Airflow | sau rollout |
| Sửa nguồn giả lập | `./scripts/sync-source.sh` | ngay, có restart pod |
| Sửa schema kho | `./scripts/apply-sql.sh [file.sql]` | ngay |
| Sửa cấu hình helm | `./scripts/deploy.sh <component>` | theo rollout |
| Deploy database inspector | `./scripts/deploy.sh database-ui` | theo rollout |

`pgweb` là giao diện xem PostgreSQL nhẹ, chạy ở chế độ `read-only`, không cần
PVC và không chứa mật khẩu kho trong manifest. Các connection đã duyệt được
lưu thành bookmark TOML trong Secret `stc-hy/pgweb-bookmarks`; sau khi đăng
nhập Keycloak, người dùng chọn connection trong danh sách, không phải nhập lại.
Có thể lưu nhiều connection bằng nhiều key `*.toml` trong cùng Secret. pgweb
không tự lưu connection nhập từ UI như pgAdmin; danh sách này do platform quản
lý. `bookmarks-only` chặn kết nối tới host nội bộ ngoài danh sách đã duyệt.
Đường `/db/` dùng chung hostname với SeaweedFS để tái sử dụng oauth2-proxy và
callback Keycloak hiện có.

Hiện `read-only` được thực thi ở lớp pgweb. Tài khoản vận hành `stc_dwh` không
có `CREATEROLE`, nên chưa tạo được role PostgreSQL riêng cho inspector. DBA cần
tạo `stc_dwh_inspector` chỉ có `CONNECT`, `USAGE`, `SELECT` và đặt
`default_transaction_read_only=on`, sau đó xoay Secret `pgweb-database` sang
role đó để có lớp bảo vệ ở chính database.

DAG đi vào cụm bằng ConfigMap chứ không phải PVC hay gitSync. Lý do: chart
không mount gì cho DAG khi tắt cả `persistence` lẫn `gitSync`, mà namespace chỉ
còn đúng 1 khe PVC trong quota. ConfigMap không tốn quota và không cần dựng git
server — đổi lại trần 1MiB và không có lịch sử phiên bản. `sync-dags.sh` tự
chặn khi sắp chạm trần.

dbt Core không có Deployment chạy thường trực và không nằm trong image
Airflow. Task `dbt_transform` dùng `KubernetesPodOperator` tạo một pod từ
`ghcr.io/dbt-labs/dbt-postgres:1.9.0`, mount project từ hai ConfigMap
`airflow-dbt-project`/`airflow-dbt-models`, lấy kết nối kho từ Secret `dwh-db`,
chạy toàn bộ model gắn tag `qlgia`, rồi tự xóa pod. Image đã có trên node thì
chi phí khởi động quan sát được khoảng 10 giây; SQL vẫn chạy trong PostgreSQL.

Service account của worker tạo/đọc/log/xóa pod được nhưng không được list
Kubernetes Events. Provider KPO 10.17 luôn mở event watcher dù chỉ để chẩn đoán,
nên DAG dùng `ProjectMemberKubernetesPodOperator`: giữ nguyên vòng đời KPO và
chỉ bỏ event watcher, thay bằng polling trạng thái pod. Khi project-owner cấp
quyền list/watch Events cho worker service account thì có thể trở lại KPO gốc.

Image Airflow hiện tại là
`ghcr.io/kl3init/kdlstc-airflow:3.2.2-dlt1.21.0-gx1.21.0-r1`, build từ
`Dockerfile.airflow`. Image giữ nguyên Airflow 3.2.2/Python 3.13 và nướng sẵn
`dlt==1.21.0`, `openpyxl==3.1.5`, `great_expectations==1.21.0`. GitHub Actions build và publish package GHCR
public nên cluster có thể pull mà không cần secret. Nếu đổi registry, đặt lại
`AIRFLOW_IMAGE` và `REGISTRY_SECRET`. Các script build chỉ dùng Docker config
tạm và không ghi credential vào repo.

Checkpoint 2026-08-21: Airflow Helm revision 13 đang chạy digest
`sha256:f8bb40efd9554a27627ec5f2bac7c6e6fc8c8188c207274cdf62fa9924de959f`;
api-server, scheduler, dag-processor, triggerer và worker đều Ready. Chuỗi iMate
04b → 05 → 06 → 07 chạy trọn với 6.141 văn bản và 99.214 lượt chuyển.

Apicurio chạy ở `stc-hy`, dùng database PostgreSQL riêng và Secret
`apicurio-db`; pipeline chỉ nhận địa chỉ Service, không nhận credential DB.
Bản 3.1.7 được ghim vì là bản mới nhất chạy được trên CPU của cụm; image từ
3.2 trở đi yêu cầu x86-64-v3. Registry hiện chỉ mở nội bộ và chưa bật xác thực;
trước khi mở Ingress phải đặt sau Keycloak/oauth2-proxy hoặc bật OIDC trực tiếp.

QL Giá dùng `dlt` REST client cho HTTP, retry và pagination `nextPage`. Cursor
đã công bố vẫn chỉ có một nguồn sự thật là `ingestion.cursors`: Airflow truyền
`updatedSince` vào dlt và chỉ tiến cursor trong cùng transaction công bố Gold.
dlt không giữ checkpoint thứ hai. Response bytes của từng page được ghi nguyên
văn vào Bronze; manifest ghi thêm `extractor` và `dlt_version` để truy vết.

---

## 5. Pipeline `ingest_qlgia`

Chín task, các checkpoint nghiệp vụ đẩy `ingestion.runs.status` sang trạng thái mới. Lô dừng
giữa chừng thì giữ nguyên trạng thái nó dừng — đó là thứ làm sổ nhật ký đáng
đọc.

```
received → parsed → mapped → quality_passed → published
    └→ schema_blocked          └→ quality_failed
```

| Bước | Làm gì |
|---|---|
| `open_run` | Mở vé, đọc cursor. `run_id` suy từ Airflow run id nên chạy lại dùng lại vé cũ thay vì rải vé dở dang |
| `check_schema_contract` | Lấy mẫu 25 dòng, đối chiếu kiểu. Thiếu trường hoặc đổi kiểu → **DỪNG trước khi ghi gì**. Thừa trường lạ → ghi nhận, cho qua |
| `land_in_bronze` | Phân trang, ghi nguyên văn + `_manifest.json` + checksum |
| `load_silver_one` | Đọc ngược từ S3, đổi tên trường, giữ text |
| `dbt_transform` | Tạo pod dbt riêng; dựng Silver-2 và bảng tổng hợp, chạy 10 data tests, xong tự xóa pod |
| `summarize_silver_two` | Đếm kết quả dbt và xếp mã chưa ánh xạ vào hàng chờ xử lý |
| `quality_gate` | Tính hai chỉ số, quyết định công bố |
| `summarize_grain` | Kiểm tra nhóm bị mất hết điểm hoặc có quá ít nơi khảo sát |
| `publish` | Merge-upsert vào Gold, tiến cursor |

### Hai chỉ số, không phải một

Vì hai thứ này đòi hai cách xử lý khác nhau:

- **`mapping_coverage`** — kho hiểu được bao nhiêu phần của lô. Thấp nghĩa là
  ai đó còn nợ một quy tắc ánh xạ. Ngưỡng chặn 50%.
- **`quality_score`** — trong phần hiểu được, bao nhiêu là lành. Thấp nghĩa là
  dữ liệu sai. Ngưỡng công bố 95%.

Dòng loại vì **cấu trúc** (dòng TỔNG lẫn trong chi tiết) không tính vào chỉ số
nào. Nó không phải lỗi, chỉ là không thuộc hạt này. Tính nó vào điểm là đóng
đinh một cái trượt vĩnh viễn vào thước đo.

### Lỗi cài sẵn trong nguồn và thứ chúng chứng minh

| Lỗi | Chứng minh |
|---|---|
| 2/12 mặt hàng không có quy tắc ánh xạ | mã lạ thành câu hỏi nghiệp vụ có người phụ trách và hạn xử lý, không bị nuốt |
| Thiếu `areaCode` rải rác | ràng buộc bắt buộc |
| Giá × 1000 | phát hiện ngoài dải bằng trung vị theo mặt hàng |
| Giá âm | ràng buộc miền giá trị |
| Dòng trùng khóa, `lastModified` mới hơn | khử trùng giữ bản mới nhất |
| Dòng TỔNG lẫn chi tiết | lọc theo hạt, nếu không `SUM` ra gấp đôi |
| Phù Cừ khuyết kỳ `2026-06-K1` | khuyết kỳ phải hiện ra, không được render thành 0 |
| Kỳ khảo sát ≠ kỳ lịch (5/2026 có 2 kỳ) | mọi giả định "một tháng một kỳ" vỡ ở đây |

Đổi lược đồ theo yêu cầu để thử cổng chặn:

```bash
kubectl -n stc-hy exec deploy/mock-qlgia -- \
  python -c "import urllib.request as u; \
  print(u.urlopen(u.Request('http://localhost:8080/admin/schema-drift?mode=break', method='POST')).read())"
```

`mode=break` đổi `unitPrice` từ số sang chuỗi có dấu chấm phân cách → DAG phải
dừng ở `schema_blocked` và Bronze phải sạch. `mode=add` thêm cột mới → phải cho
qua và ghi nhận. `mode=reset` trả về bình thường.

---

## 6. Ràng buộc đang phải sống chung

**Quyền hiện tại quản lý được RBAC trong namespace, không quản lý RBAC toàn
cụm.** Vì vậy chart vẫn để `rbac.create=false`; các quyền bổ sung phải được
khai báo hẹp và có manifest trong repo. `k8s/airflow-kpo-rbac.yaml` chỉ cấp
`get/list/watch` cho `core/v1 Events` để `KubernetesPodOperator` theo dõi pod.
ClusterRole và CRD vẫn nằm ngoài phạm vi dự án.

**Nhưng quyền chạy pod thì ĐÃ được cấp.** Kiểm bằng cách tạo pod thật (HTTP 201),
Kubernetes nói rõ nguồn quyền:

```
RBAC: allowed by RoleBinding "airflow-pod-launcher/stc-hy-airflow"
      of ClusterRole "jmix-airflow-pod-launcher"
      to ServiceAccount "stc-airflow-worker/stc-hy-airflow"
```

Nên `KubernetesPodOperator` dùng được (dbt đang chạy kiểu đó), và
`KubernetesExecutor` cũng khả thi về mặt quyền. Vẫn giữ `CeleryExecutor` vì
chưa có lý do đủ để đổi, không phải vì bị chặn.

Cần phân biệt hai thứ hay bị gộp làm một: **tạo pod** thì được, **tạo đối tượng
RBAC** thì không. Cái sau mới là thứ chặn Operator.

**Pod hiện chưa tới được mạng chứa các nguồn bên ngoài cluster.** Đã xác minh
bằng kiểm tra lưu lượng; địa chỉ và dải mạng cụ thể không được ghi trong bản
public. Hệ quả:

- mọi lệnh gọi server-to-server phải đi Service nội bộ, không đi ingress.
  Đây là lý do Keycloak bật `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true` và
  oauth2-proxy dùng endpoint nội bộ cho backchannel
- **chưa nối được nguồn thật** (Oracle TMS, MISA, DMS, KBNN). Cần định tuyến từ
  đội hạ tầng trước khi rời khỏi nguồn giả lập

**Chưa có cert-manager** → toàn bộ chạy HTTP. `cookie-secure: "false"` ở
oauth2-proxy là hệ quả, phải đổi khi có TLS.

**Mật khẩu.** Không có mật khẩu nào trong repo. Chúng nằm trong Secret của
Kubernetes, tạo bằng `create-secrets.sh`. Đọc lại:

```bash
kubectl -n stc-hy-airflow get secret dwh-db -o jsonpath='{.data.password}' | base64 -d
```

Credential môi trường phải được rotate ngay nếu từng xuất hiện ngoài secret
manager; không ghi sự cố hoặc giá trị cụ thể vào repository public.

---

## 7. Bài học đã trả giá

Ghi lại để không phải trả lần nữa.

- **SeaweedFS `maxVolumes: 0`** nghĩa là "tự tính" = `đĩa ÷ volumeSizeLimitMB`.
  10Gi ÷ 1000MB = 9 khe. Mỗi bucket là một collection và xin khe riêng, nên
  vài bucket là cạn, báo `No writable volumes` **dù đĩa còn trống nguyên**.
  Phải khai số khe tường minh.
- **Đặt app sau oauth2-proxy thì phải tắt xác thực nội bộ của app đó.** Bật cả
  hai bắt người dùng đăng nhập hai lần, hoàn toàn vô nghĩa.
- **Chart SeaweedFS mặc định `hostPath /ssd`** — dữ liệu mất khi pod dời node.
  Phải đổi sang PVC.
- **Airflow 3 dùng `apiServer.apiServerConfig`**, không phải
  `webserver.webserverConfig` của Airflow 2.
- **`limits.cpu` quá chặt giết pod theo kiểu khó đoán.** api-server 500m không
  kịp startupProbe; triggerer 300m bị throttle đúng ở trần khiến heartbeat
  đứng. Cả hai trông như treo chứ không như thiếu CPU.
- **`workers`/`triggerer` mặc định xin 100Gi PVC mỗi cái** → vượt quota ngay.
- **`celery.worker_concurrency` mặc định 16** là thủ phạm OOM số một ở cụm nhỏ.
- **Đừng override `image.repository` của chart oauth2-proxy** — chart tự ghép
  registry, kết quả thành `quay.io/quay.io/...`.

---

## 8. Ứng dụng điều hành

Backend Spring Boot và frontend React có image production riêng trong
`backend/Dockerfile` và `frontend/Dockerfile`. Frontend được build với
`VITE_USE_FIXTURES=false`; Nginx chỉ phục vụ SPA, còn Ingress chuyển `/api`,
`/oauth2` và `/login/oauth2` thẳng tới backend.
GitLab CI build hai image và đẩy vào Container Registry của project
`data-warehouse/kho-so-tai-chinh`. Job dùng credential ngắn hạn
`CI_REGISTRY_USER`/`CI_REGISTRY_PASSWORD`; máy phát triển và repository không
giữ token push.

Manifest `k8s/kdlstc.yaml` ghim tag image, resource limit, security context và
ba probe. Host public trong manifest là placeholder; script deploy thay bằng
hostname thật trong tệp tạm, không ghi cấu hình môi trường vào Git:

```bash
KDLSTC_HOST=kdlstc-stc.10.123.123.194.nip.io ./scripts/deploy-kdlstc.sh
```

Script tạo database `kdlstc_control` cùng role riêng trong PostgreSQL hiện có,
lưu kết nối ở Secret `stc-hy/kdlstc-db`, cập nhật redirect URI của client
Keycloak `kdlstc`, áp manifest rồi chờ cả hai rollout. Secret
`stc-hy/jmix-airflow-api` vẫn giữ tên identity cũ vì đã được smoke-test với
Airflow; deployment mới chỉ đọc lại, không xoay credential.

Rollback không chạy ngược Liquibase. Trước hết trả hai Deployment về
ReplicaSet trước; changelog hiện chỉ có thay đổi cộng thêm nên binary cũ vẫn
đọc được:

```bash
kubectl -n stc-hy rollout undo deployment/kdlstc-backend
kubectl -n stc-hy rollout undo deployment/kdlstc-frontend
kubectl -n stc-hy rollout status deployment/kdlstc-backend --timeout=5m
kubectl -n stc-hy rollout status deployment/kdlstc-frontend --timeout=5m
```
Checkpoint 24/08/2026: backend và frontend đã rollout thành công từ GHCR public
vào namespace `stc-hy`. Backend chạy Spring Boot 4.1.1/Java 25, Liquibase xác
nhận đủ ba changeset trên PostgreSQL `kdlstc_control`; frontend production tắt
fixture. Ingress trả UI 200, `/api/me` chưa đăng nhập trả 401 và nút SSO chuyển
đúng realm `khodl` với callback production cùng PKCE S256.

---

## 9. Còn phải làm

- [ ] Lát cắt 2: nguồn thu/chi ngân sách theo hợp đồng "viên gạch API"
      (`docs/API-VIEN-GACH-DRAFT.md`) — 219/350 biểu thuộc vòng đời ngân sách,
      và `DHTC_CHI_04` đã có recipe chạy được để làm đích
- [ ] Chuyển phần publish Gold sang dbt khi chốt cơ chế công bố nguyên tử giữa
      Gold, `batch_summary` và cursor; hiện Airflow giữ ba cập nhật trong một transaction
- [ ] Export realm Keycloak ra JSON, commit vào repo
- [ ] Đối soát/rotate credential quản trị Keycloak: Secret bootstrap hiện
      không đăng nhập được Admin CLI, nên chưa thể export realm an toàn
- [ ] Bật xác thực cho S3 API
- [ ] Chuyển metadata DB của Airflow sang `jmix-ha`, bỏ `bitnamilegacy/postgresql`
      (kho archive, không còn được vá) và trả lại 1 khe PVC
- [ ] Phối hợp đội hạ tầng: định tuyến mạng nguồn, cert-manager, quyền `project-owner`
