import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from qlgia_source import iter_price_pages


class _QLGiaHandler(BaseHTTPRequestHandler):
    requests = []

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        type(self).requests.append((parsed.path, query))

        page = int(query["page"][0])
        payload = {
            "items": [{"id": page, "lastModified": f"2026-08-0{page}T00:00:00Z"}],
            "nextPage": page + 1 if page == 1 else None,
        }
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


class QLGiaSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _QLGiaHandler.requests = []
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _QLGiaHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def test_uses_external_cursor_and_follows_next_page(self):
        base_url = f"http://127.0.0.1:{self.server.server_port}"
        pages = list(
            iter_price_pages(
                base_url,
                updated_since="2026-07-31T23:59:59Z",
                page_size=500,
                timeout=5,
            )
        )

        self.assertEqual([[row["id"] for row in page] for page in pages], [[1], [2]])
        self.assertEqual(len(_QLGiaHandler.requests), 2)

        first_path, first_query = _QLGiaHandler.requests[0]
        second_path, second_query = _QLGiaHandler.requests[1]
        self.assertEqual(first_path, "/api/prices")
        self.assertEqual(second_path, "/api/prices")
        self.assertEqual(first_query["page"], ["1"])
        self.assertEqual(second_query["page"], ["2"])
        self.assertEqual(first_query["pageSize"], ["500"])
        self.assertEqual(
            first_query["updatedSince"],
            ["2026-07-31T23:59:59Z"],
        )
        self.assertEqual(
            second_query["updatedSince"],
            ["2026-07-31T23:59:59Z"],
        )


if __name__ == "__main__":
    unittest.main()
