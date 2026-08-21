# Chuẩn trích xuất — dlt cho mọi nguồn REST

Quyết định nền: [0002](../decisions/0002-kho-so-huu-con-tro.md).

## Ranh giới

dlt là **lớp vỏ HTTP**, không phải pipeline.

| dlt làm | dlt không làm |
|---|---|
| Phiên có thử lại, backoff khi lỗi mạng hoặc 5xx | Giữ con trỏ hay bất kỳ trạng thái nào |
| Tái dùng kết nối qua nhiều lời gọi | Chuẩn hoá, suy schema, ép kiểu |
| Phân trang khi API có kiểu phân trang chuẩn | Quyết định khi nào dừng quét |

Bronze nhận **nguyên byte** từ nguồn. dlt chạm vào cấu trúc là Bronze hết còn
là bằng chứng.

## Mẫu dùng

Một client cho mỗi luồng — `requests.Session` không bảo đảm an toàn khi dùng
chung giữa các luồng, mà bước tải chi tiết chạy song song:

```python
_local = threading.local()

def _client():
    if getattr(_local, "client", None) is None:
        _local.client = RESTClient(base_url=API_ADDR, headers={...})
    return _local.client
```

Một cánh cửa duy nhất ra nguồn, để mọi cái bẫy của API chỉ phải chặn một lần:

```python
def api_get(path):
    payload = _client().get(path, timeout=HTTP_TIMEOUT).json()
    if not payload.get("success"):        # API trả lỗi bằng HTTP 200
        raise SourceRefusedError(...)
    return payload["body"]
```

## Ghi vào lineage

Phiên bản dlt vào mọi bản tóm tắt lượt chạy:

```python
"extractor": f"dlt-rest-client/{DLT_VERSION}"
```

Để sau này trả lời được: đối tượng Bronze này do phiên bản trình trích xuất nào
sinh ra.

## Chưa dùng

Tầng nạp của dlt (`dlt.pipeline`, `destination.postgres`, merge theo khoá) hiện
chưa dùng ở lát cắt nào — Silver-1 tự viết `INSERT ON CONFLICT`. Xem
[../roadmap.md](../roadmap.md), nhóm 2.
