"""
Schema Registry — nơi cấu trúc dữ liệu của nguồn được lưu lại và đối chiếu.

Hợp đồng tự viết trước đây chỉ trả lời được một câu: trường này có mặt không.
Nó không lưu phiên bản, nên không phân biệt được ba việc hoàn toàn khác nhau:

    NONE       nguồn trả về đúng hình dạng cũ — đi tiếp, không ghi gì
    ADDITIVE   nguồn THÊM trường — đi tiếp, nhưng ghi lại để còn biết mà dùng
    BREAKING   nguồn ĐỔI KIỂU hoặc BỎ trường bắt buộc — chặn lô ngay

Phân loại do Apicurio quyết định, không phải do mã ở đây so từng khoá: registry
giữ lịch sử phiên bản và luật tương thích, nên nó biết những thứ một phép so
dict không biết — ví dụ một trường optional chuyển thành required.

Luật tương thích chọn FORWARD: dữ liệu ghi theo schema MỚI phải đọc được bằng
schema CŨ. Đó đúng là điều kho cần — mã xử lý ở hạ nguồn viết theo hình dạng đã
biết, và phải tiếp tục chạy được khi nguồn đổi.
"""

import json
import os
import urllib.error
import urllib.request


HTTP_TIMEOUT = 20


def _url():
    """
    Read at call time, not at import.

    The registry address arrives differently depending on where this runs: the
    fetch pod gets it as an env var resolved to an IP, the worker gets it from
    the deployment. Reading it at import froze whichever value existed when the
    module first loaded, which in the worker was none at all — every check
    silently reported SKIPPED and nobody noticed, because SKIPPED looks like a
    deliberate choice.
    """
    return os.environ.get("APICURIO_URL", "")


def _group():
    return os.environ.get("APICURIO_GROUP", "imate")


class SchemaBreakingChange(RuntimeError):
    """Nguồn đổi cấu trúc theo kiểu hạ nguồn không đọc nổi."""


def _call(method, path, payload=None, headers=None):
    url = f"{_url().rstrip('/')}/apis/registry/v3{path}"
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(url, data=body, method=method)
    request.add_header("Accept", "application/json")
    if body is not None:
        request.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
        raw = response.read().decode("utf-8")
        return response.status, (json.loads(raw) if raw else {})


def infer_schema(rows, required):
    """
    Suy JSON Schema từ một mẫu thật, không viết tay.

    Viết tay thì schema mô tả điều mình TƯỞNG nguồn trả về; suy từ mẫu thì nó mô
    tả điều nguồn ĐANG trả về. Chỉ khác nhau đúng vào lúc quan trọng nhất.

    Kiểu lấy từ giá trị đầu tiên khác null gặp được, nên một trường luôn null
    trong mẫu sẽ để ngỏ kiểu thay vì đoán bừa.
    """
    # CHỈ ràng buộc kiểu cho những trường hạ nguồn thật sự đọc.
    #
    # Bản trước ràng buộc kiểu cho MỌI trường nguồn trả về, và đo trên dữ liệu
    # thật thì lô nào cũng bị chặn: failureReason và readyAt là hai trường lúc có
    # giá trị lúc null, nên kiểu của chúng đổi theo việc trang vừa lấy tình cờ có
    # bản ghi nào điền chúng hay không. Nguồn không đổi gì cả — chỉ mẫu đổi.
    #
    # Trường ngoài hợp đồng vẫn được ghi nhận là CÓ TỒN TẠI, chỉ không hứa gì về
    # kiểu. Nhờ vậy nguồn thêm hay bỏ một trường không ai dùng thì registry vẫn
    # thấy và ghi lại, mà không dựng rào trước một lô hoàn toàn hợp lệ.
    required_set = set(required)
    properties = {}
    for row in rows:
        for key, value in row.items():
            if key not in required_set:
                properties.setdefault(key, {})
                continue
            if properties.get(key, {}).get("type"):
                continue
            if value is None:
                # Trường bắt buộc mà mẫu chỉ thấy null: ghi nhận có mặt, chưa
                # hứa kiểu — lô sau gặp giá trị thật sẽ không thành đổi kiểu.
                properties.setdefault(key, {})
                continue
            if isinstance(value, bool):
                kind = "boolean"
            elif isinstance(value, int):
                kind = "integer"
            elif isinstance(value, float):
                kind = "number"
            elif isinstance(value, list):
                kind = "array"
            elif isinstance(value, dict):
                kind = "object"
            else:
                kind = "string"
            properties[key] = {"type": kind}

    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "imate list row",
        "type": "object",
        "properties": {k: v for k, v in sorted(properties.items())},
        "required": sorted(required),
    }


def _ensure_forward_rule(artifact_id):
    """
    Đặt luật tương thích ngay sau khi tạo phiên bản đầu.

    Không đặt được ở lời gọi tạo: registry chỉ nhận luật khi artifact đã tồn tại.
    Lỗi 409 nghĩa là luật đã có sẵn — đúng ý, bỏ qua.
    """
    try:
        _call("POST", f"/groups/{_group()}/artifacts/{artifact_id}/rules",
              {"ruleType": "COMPATIBILITY", "config": "FORWARD"})
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise


def check(artifact_id, rows, required):
    """
    Đăng ký hình dạng vừa nhận được và trả về phân loại thay đổi.

    Trả về dict gồm drift, version, contentId — đủ để ghi vào sổ cái và để lần
    sau đối chiếu. Ném SchemaBreakingChange khi registry từ chối.
    """
    if not _url():
        return {"drift": "SKIPPED", "reason": "chua cau hinh APICURIO_URL"}

    schema = infer_schema(rows, required)
    body = {
        "artifactId": artifact_id,
        "artifactType": "JSON",
        "firstVersion": {
            "content": {
                "content": json.dumps(schema, ensure_ascii=False),
                "contentType": "application/json",
            }
        },
    }

    try:
        status, result = _call(
            "POST", f"/groups/{_group()}/artifacts?ifExists=CREATE_VERSION", body)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:400]
        # 409 hoặc 400 ở đây nghĩa là registry TỪ CHỐI phiên bản này.
        #
        # Không dò chuỗi trong thân phản hồi để nhận diện: đo thật thì Apicurio
        # trả 409 với thân rỗng khi luật tương thích bị vi phạm, nên một phép so
        # chuỗi sẽ để lọt đúng trường hợp cần bắt. Bản thân mã trạng thái đã đủ:
        # nội dung y hệt KHÔNG bị từ chối (đã đo), nên bị từ chối nghĩa là hình
        # dạng đã đổi theo kiểu hạ nguồn không đọc nổi.
        if exc.code in (400, 409):
            raise SchemaBreakingChange(
                f"{artifact_id}: nguon doi cau truc theo kieu pha vo tuong thich "
                f"(HTTP {exc.code}) {detail}".strip()) from exc
        raise

    version = (result.get("version") or {})
    version_number = str(version.get("version", "?"))
    content_id = version.get("contentId")

    if version_number == "1":
        _ensure_forward_rule(artifact_id)
        return {"drift": "NONE", "version": "1", "contentId": content_id,
                "note": "phien ban dau tien"}

    # So với phiên bản liền trước: cùng contentId nghĩa là hình dạng không đổi.
    try:
        _, versions = _call(
            "GET", f"/groups/{_group()}/artifacts/{artifact_id}/versions?limit=2&order=desc")
        listed = versions.get("versions", [])
        previous = listed[1].get("contentId") if len(listed) > 1 else None
    except urllib.error.HTTPError:
        previous = None

    drift = "NONE" if previous is not None and previous == content_id else "ADDITIVE"
    return {"drift": drift, "version": version_number, "contentId": content_id}
