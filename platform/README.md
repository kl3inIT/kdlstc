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
                               │  urllib
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
                    ┌──────────▼─────────────────┐
   SILVER-2         │ stg_qlgia__price_typed     │  ép kiểu, khử trùng,
                    │                            │  ánh xạ mã, gắn cờ is_valid
                    └──────────┬─────────────────┘
                               │  cổng chất lượng
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
| `stc-hy` | Keycloak · oauth2-proxy · SeaweedFS (master/volume/filer/s3/admin) · mock-qlgia · pgAdmin |
| `stc-hy-airflow` | Airflow 3.2.2 (api-server, scheduler, dag-processor, triggerer, worker) + PostgreSQL + Redis |
| `jmix-ha` (ngoài cụm, `10.1.50.103`) | `stc_dwh` — kho dữ liệu · `stc_keycloak` |

| Dịch vụ | Địa chỉ |
|---|---|
| Airflow | http://airflow-stc.10.123.123.194.nip.io |
| Keycloak | http://sso-stc.10.123.123.194.nip.io |
| SeaweedFS Admin | http://s3-stc.10.123.123.194.nip.io |

Cần VPN. Đăng nhập bằng Keycloak realm `khodl`.

`nip.io` tự phân giải `<gì đó>.10.123.123.194.nip.io` về chính IP đó, nên chưa
cần cấu hình DNS. Khi có tên miền thật thì sửa `host` trong `helm/` và
`k8s/`.

---

## 3. Dựng lại từ đầu

```bash
export DWH_HOST=10.1.50.103 DWH_DBNAME=stc_dwh DWH_USER=stc_dwh
export DWH_PASSWORD=...           # xem mục 6
export KC_DB_PASSWORD=... KC_ADMIN_USER=admin KC_ADMIN_PASSWORD=...
export KC_AIRFLOW_CLIENT_SECRET=... KC_SW_CLIENT_SECRET=...
export O2P_COOKIE_SECRET="$(openssl rand -base64 32 | tr -- '+/' '-_' | tr -d '=')"

./scripts/create-secrets.sh     # bắt buộc chạy trước
./scripts/deploy.sh             # helm cho toàn bộ, hoặc: ./deploy.sh airflow
./scripts/apply-sql.sh          # schema + danh mục cho kho
```

`deploy.sh` ghim phiên bản chart. Nâng cấp không ghim là cách một cụm đang chạy
lặng lẽ biến thành một cụm khác cũng đang chạy.

---

## 4. Vòng lặp thường ngày

| Việc | Lệnh | Bao lâu thì có hiệu lực |
|---|---|---|
| Sửa DAG | `./scripts/sync-dags.sh` | ~3 phút (kubelet ~60s + `min_file_process_interval` 120s) |
| Sửa nguồn giả lập | `./scripts/sync-source.sh` | ngay, có restart pod |
| Sửa schema kho | `./scripts/apply-sql.sh [file.sql]` | ngay |
| Sửa cấu hình helm | `./scripts/deploy.sh <component>` | theo rollout |

DAG đi vào cụm bằng ConfigMap chứ không phải PVC hay gitSync. Lý do: chart
không mount gì cho DAG khi tắt cả `persistence` lẫn `gitSync`, mà namespace chỉ
còn đúng 1 khe PVC trong quota. ConfigMap không tốn quota và không cần dựng git
server — đổi lại trần 1MiB và không có lịch sử phiên bản. `sync-dags.sh` tự
chặn khi sắp chạm trần.

---

## 5. Pipeline `ingest_qlgia`

Bảy bước, mỗi bước đẩy `ingestion.runs.status` sang trạng thái mới. Lô dừng
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
| `build_silver_two` | Ép kiểu, khử trùng theo `lastModified`, ánh xạ mã, phán mỗi dòng |
| `quality_gate` | Tính hai chỉ số, quyết định công bố |
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

**Quyền trên cụm là `project-member`.** Không tạo được Role, RoleBinding,
ClusterRole hay CRD. Hệ quả:

- mọi chart phải `rbac.create=false`; SeaweedFS phải `createClusterRole: false`
- Operator (Keycloak Operator, Strimzi…) bị chặn hoàn toàn — chỉ dùng được Helm
- Airflow không đổi sang `KubernetesExecutor` được, vì executor đó cần quyền
  tạo pod. Đang chạy `CeleryExecutor`

**Pod không tới được `10.123.123.0/24`.** Đã xác minh bằng tcpdump: không có
gói nào tới nơi, nguyên nhân là cụm nằm ở mạng khác chứ không phải firewall
trong cụm. Hệ quả:

- mọi lệnh gọi server-to-server phải đi Service nội bộ, không đi ingress VIP.
  Đây là lý do Keycloak bật `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true` và
  oauth2-proxy phải `skip-oidc-discovery` rồi chỉ tay từng endpoint
- **chưa nối được nguồn thật** (Oracle TMS, MISA, DMS, KBNN). Cần định tuyến từ
  Bình trước khi rời khỏi nguồn giả lập

**Chưa có cert-manager** → toàn bộ chạy HTTP. `cookie-secure: "false"` ở
oauth2-proxy là hệ quả, phải đổi khi có TLS.

**Mật khẩu.** Không có mật khẩu nào trong repo. Chúng nằm trong Secret của
Kubernetes, tạo bằng `create-secrets.sh`. Đọc lại:

```bash
kubectl -n stc-hy-airflow get secret dwh-db -o jsonpath='{.data.password}' | base64 -d
```

Mật khẩu superuser `postgres` của cụm `jmix-ha` đã từng bị dán vào chat —
**cần đổi**.

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

## 8. Còn phải làm

- [ ] Lát cắt 2: nguồn thu/chi ngân sách theo hợp đồng "viên gạch API"
      (`docs/API-VIEN-GACH-DRAFT.md`) — 219/350 biểu thuộc vòng đời ngân sách,
      và `DHTC_CHI_04` đã có recipe chạy được để làm đích
- [ ] dbt cho tầng curated (PyPI thông từ trong cụm, cài được không cần build image)
- [ ] Export realm Keycloak ra JSON, commit vào repo
- [ ] Bật xác thực cho S3 API
- [ ] Chuyển metadata DB của Airflow sang `jmix-ha`, bỏ `bitnamilegacy/postgresql`
      (kho archive, không còn được vá) và trả lại 1 khe PVC
- [ ] Hỏi Bình: định tuyến tới `10.123.123.0/24`, cert-manager, quyền `project-owner`
