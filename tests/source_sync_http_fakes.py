from __future__ import annotations

import json


class FakeResponse:
    def __init__(self, status: int, payload: dict, headers: dict | None = None):
        self._status = int(status)
        self._payload = dict(payload)
        self.headers = dict(headers or {})

    def getcode(self):
        return self._status

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class Recorder:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self._uploaded_shards: dict[str, str] = {}

    def __call__(self, req, timeout=20):  # noqa: ANN001
        url = req.full_url
        method = req.get_method()
        body = req.data.decode("utf-8") if isinstance(req.data, bytes) else ""
        self.calls.append(
            {
                "url": url,
                "method": method,
                "headers": dict(req.header_items()),
                "body": body,
                "timeout": timeout,
            }
        )
        if method == "GET" and url.split("?ref=", 1)[0].endswith("/source-sync/shards"):
            return FakeResponse(404, {"message": "Not Found"})
        if "/source-sync/shards/" in url:
            key = url.split("?ref=", 1)[0]
            if method == "PUT":
                self._uploaded_shards[key] = str(json.loads(body).get("content") or "")
                return FakeResponse(201, {"content": {"sha": "shard-sha"}})
            if method == "GET" and key in self._uploaded_shards:
                return FakeResponse(200, {"content": self._uploaded_shards[key]})
        if not self.responses:
            raise AssertionError("No fake responses left")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item
