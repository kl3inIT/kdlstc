# Jmix mocks — validate kiến trúc federation 5 app + AI

2 app Jmix 3.0.1 mô phỏng 2 trong 5 nguồn (scaffold từ `jmix-restds-oidc-sample`
nhánh `release_3`), phục vụ checklist nghiệm thu trong `docs/API-VIEN-GACH-DRAFT.md` (mục 9).

| App | Port | API | Role Keycloak cần |
|---|---|---|---|
| `mock-thu` | 8081 | `/rest/thu/tong-hop`, `/rest/thu/du-toan`, `/rest/danh-muc/{loai}` | `thu-api` |
| `mock-chi` | 8082 | `/rest/chi/tong-hop`, `/rest/chi/du-toan`, `/rest/danh-muc/{loai}` | `chi-api` |

## Chạy

```bash
cd jmix-mocks/mock-thu && ./gradlew bootRun   # JDK 21/25
cd jmix-mocks/mock-chi && ./gradlew bootRun
```

DB: HSQL file-based trong `.jmix/` của từng app — không cần hạ tầng.
Issuer mặc định: `https://auth.x2h.com.vn/realms/stc-mock` (public, không cần VPN);
override bằng env `KC_ISSUER_URI`.

## Keycloak (realm `stc-mock` trên dev — đã tạo 2026-07-31)

- Realm roles: `thu-api`, `chi-api` — code trùng `ThuApiRole.CODE` / `ChiApiRole.CODE`.
- Client `agent-mock`: public, bật direct access grant (chỉ để smoke test / dev).
- Mapper `realm-roles-claim` trên client: realm roles → claim `roles` (top-level,
  multivalued) — Jmix `DefaultClaimsRolesMapper` đọc claim này để gán role.
- User test: `user-thu` (chỉ `thu-api`), `user-full` (cả hai).
  **Password KHÔNG lưu trong repo** — xem `/home/ubuntu/app/keycloak/stc-mock-test-users.txt`
  (server dev, mode 600, cần SSH+VPN).

## Smoke test

```bash
# lay token (password grant — chi dung dev)
TOKEN=$(curl -s https://auth.x2h.com.vn/realms/stc-mock/protocol/openid-connect/token \
  -d grant_type=password -d client_id=agent-mock \
  -d username=user-thu -d password=... | jq -r .access_token)

# co quyen -> 200 + so lieu
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8081/rest/thu/tong-hop?group_by=dia_ban&tu_ngay=2026-01-01&den_ngay=2026-06-30"

# user-thu goi CHI -> 403 {"source":"CHI","required_role":"chi-api"} (an cot, khong phai loi)
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8082/rest/chi/tong-hop?group_by=dia_ban&tu_ngay=2026-01-01&den_ngay=2026-06-30"

# khong token -> 401
```

## Số liệu seed (deterministic — dùng để đối chiếu khi test)

- Thu H1/2026 = **3.300.000.000đ**; theo địa bàn: HY01 1.200tr, HY02 800tr,
  HY03 600tr, HY04 450tr, HY05 250tr. Cùng kỳ 2025 = 1.700tr.
- Chi H1/2026 = **2.750.000.000đ** (thực chi 1.900tr + tạm ứng 850tr);
  theo địa bàn: HY01 900tr, HY02 600tr, HY03 750tr, HY04 300tr, HY05 200tr.
- Điểm nhấn demo `DHTC_CHI_04`: **HY03 thu 600tr < chi 750tr → cân đối −150tr**;
  dự toán chi HY03 có điều chỉnh giữa năm +100tr.
- Dự toán thu 2026 mỗi chiều = 6.600tr; dự toán chi đầu năm mỗi chiều = 5.000tr.

## Quy ước 403 (hợp đồng với tầng khai thác)

- `403` **kèm body marker** `{error:"no_permission", source, required_role}` =
  không có quyền nguồn đó → ẩn nhóm cột của nguồn + cột dẫn xuất (lan truyền),
  agent khai báo thiếu nguồn.
- **Không tin status code trần**: 403/500 KHÔNG có marker = lỗi → báo lỗi, không ẩn.
  (Đã quan sát: filter `OidcResourceServerEventSecurityFilter` của jmix-oidc trả
  500 cho request không token; token sai/hết hạn thì 401 chuẩn.)
- Lỗi khác (5xx/timeout/connection refused) = sự cố → báo lỗi biểu, không ẩn im lặng.

## Demo tầng khai thác — `demo_dhtc_chi_04.py`

Dựng biểu `DHTC_CHI_04` (Chi theo địa bàn) ghép 3 API của 2 app + tính cột dẫn
xuất + ẩn lan truyền theo đồ thị công thức:

```bash
python demo_dhtc_chi_04.py user-full   # đủ quyền — biểu đầy đủ 11 cột
python demo_dhtc_chi_04.py user-chi    # thiếu THU — ẩn thu_dia_ban + can_doi
                                       # + ty_le_tu_can_doi, cột CHI giữ nguyên,
                                       # in khai báo nguồn bị ẩn
```

Kết quả nghiệm thu 2026-07-31: cả 2 kịch bản chạy đúng; điểm nhấn HY03 (Mỹ Hào)
giải ngân 75% vs tiến độ chuẩn 49,6% (+25,4) và cân đối **−150tr** hiện đúng.
