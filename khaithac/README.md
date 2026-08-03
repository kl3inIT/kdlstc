# khaithac — App khai thác dữ liệu tài chính (báo cáo + AI)

Spring Boot 4.1 / Java 25 / Spring AI 2.0 + React 19 (Vite, `@ai-sdk/react`, Tailwind 4,
streamdown) — kiến trúc học theo `D:\OrgMemory`. Đứng giữa người dùng/agent và các
app Jmix nguồn; **ẩn cột theo quyền luôn ở server**, browser chỉ giữ session cookie.

```
Browser (BFF cookie) ─┐                          ┌─ mock-thu :8081  (role thu-api)
Agent  (Bearer JWT) ──┼→ khaithac :8090 ─ relay ─┤
                      │   ├ baocao: RecipeEngine │└─ mock-chi :8082  (role chi-api)
                      │   └ ai: 7 @Tool + Chat   │
                      └── Keycloak auth.x2h.com.vn/realms/stc-mock
```

## Chạy

```bash
# 2 nguồn mock trước (jmix-mocks/README.md), rồi:
export KC_CLIENT_SECRET=$(ssh kdlstc-dev "cat /home/ubuntu/app/keycloak/stc-mock-khaithac-client.txt" | cut -d: -f2)
export OPENAI_API_KEY=$(ssh zm "grep '^OPENAI_API_KEY=' /apps/north-star/.env | cut -d= -f2-")
cd khaithac && ./gradlew bootRun          # http://localhost:8090 (web đã build sẵn trong static/)
```

Secrets chỉ nằm trong env — **không bao giờ** commit. Đổi model: env `AI_MODEL`
(mặc định `gpt-5.4-mini` — tránh tier reasoning từ chối tool). Đổi provider
(9Router/on-prem): đổi `spring.ai.openai.base-url` + key. Dữ liệu thật → LLM on-prem.

## Frontend (`web/`)

```bash
cd web && pnpm install
pnpm build      # xuất vào ../src/main/resources/static — chạy chung 8090
pnpm dev        # dev 5173, proxy /api /oauth2 /login /logout → 8090 (redirect 5173 đã khai với Keycloak)
```

## API

| Endpoint | Ghi chú |
|---|---|
| `GET /api/me` | tên user (session hoặc Bearer) |
| `GET /api/bao-cao` · `GET /api/bao-cao/{ma}?tu_ngay&den_ngay` | recipe engine, kèm `nguon_bi_an`/`khai_bao` |
| `POST /api/chat` `{message, history?}` | AI đồng bộ |
| `POST /api/chat/stream` | SSE — AI SDK UI Message Stream v1 (mẫu OrgMemory `UiMessageStream`) |

## Nguyên tắc đã cài cứng

1. **403 chỉ được tin khi kèm marker** `{error:"no_permission", source}` từ app nguồn;
   mọi lỗi khác → `NguonLoiException` → 502, không ẩn im lặng. Fail closed.
2. **Ẩn lan truyền**: cột mất khi nguồn mất; cột công thức mất khi bất kỳ tham số nào mất
   (fixpoint trong `RecipeEngine`) — chống rò rỉ suy luận `E = D − F`.
3. **Tool bake token theo request** (`AiToolsFactory`) — không dùng `SecurityContextHolder`
   trong tool vì streaming chạy trên thread reactor. Agent mù như user của nó.
4. Agent bắt buộc khai báo `nguon_bi_an`/`khai_bao`, cấm suy đoán số bị ẩn (system prompt).
5. Thêm biểu mới = thêm 1 file `src/main/resources/recipes/*.json` (map cột→nguồn→công thức),
   không sửa code.
