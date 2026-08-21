"""
Client cho Cube — lớp ngữ nghĩa.

Vì sao mọi kênh khai thác phải đi qua đây thay vì tự viết SQL: trước tệp này,
bước 7 và Superset mỗi bên tự viết truy vấn cho cùng một chỉ tiêu. Hôm nay chúng
trùng nhau là may. Ngày ai đó sửa một bên mà quên bên kia, hai nơi ra hai con số
và không có chỗ nào là định nghĩa gốc để phân xử — mà báo cáo tài chính thì
không có chỗ cho "tuỳ anh tin bên nào".

Chỉ tiêu định nghĩa một lần trong platform/cube/model. Ở đây chỉ còn việc hỏi.
"""

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.parse
import urllib.request


CUBE_URL = os.environ.get("CUBE_URL", "http://cube.stc-hy.svc.cluster.local")
HTTP_TIMEOUT = 90


def _token():
    """
    Ký một JWT ngắn hạn thay vì giữ token dài hạn ở đâu đó.

    Cube xác thực bằng JWT ký bằng khoá chung. Tự ký tại chỗ, hạn 10 phút: không
    có token nào nằm lại trong biến môi trường, trong log hay trong XCom.
    """
    secret = os.environ.get("CUBE_API_SECRET", "")
    if not secret:
        raise RuntimeError(
            "thieu CUBE_API_SECRET — khong ky duoc token cho lop ngu nghia")

    def part(payload):
        return base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":")).encode()).rstrip(b"=")

    head = part({"alg": "HS256", "typ": "JWT"})
    body = part({"exp": int(time.time()) + 600})
    signature = hmac.new(secret.encode(), head + b"." + body,
                         hashlib.sha256).digest()
    return (head + b"." + body + b"."
            + base64.urlsafe_b64encode(signature).rstrip(b"=")).decode()


def query(spec):
    """
    Hỏi Cube một câu và trả về danh sách bản ghi.

    Bên gọi chỉ nêu ĐO CÁI GÌ và CHIA THEO CHIỀU NÀO — không viết SQL, không biết
    tên bảng, không tự join. Đó là điểm mấu chốt: một chỗ đổi định nghĩa chỉ tiêu
    thì mọi kênh khai thác đổi theo, không sót kênh nào.
    """
    url = (f"{CUBE_URL.rstrip('/')}/cubejs-api/v1/load?query="
           + urllib.parse.quote(json.dumps(spec, ensure_ascii=False)))
    request = urllib.request.Request(url, headers={"Authorization": _token()})
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
        return json.load(response).get("data", [])


def rows(spec, *fields):
    """Lấy đúng các trường cần, theo thứ tự, thành tuple cho dễ ghi CSV."""
    return [tuple(record.get(f) for f in fields) for record in query(spec)]
