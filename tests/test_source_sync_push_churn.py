from __future__ import annotations

import base64
import json
from urllib.error import HTTPError, URLError

import pytest

from src import source_sync as sync
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401


class _FakeResponse:
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


class _Recorder:
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
            return _FakeResponse(404, {"message": "Not Found"})
        if "/source-sync/shards/" in url:
            key = url.split("?ref=", 1)[0]
            if method == "PUT":
                self._uploaded_shards[key] = str(json.loads(body).get("content") or "")
                return _FakeResponse(201, {"content": {"sha": "shard-sha"}})
            if method == "GET" and key in self._uploaded_shards:
                return _FakeResponse(200, {"content": self._uploaded_shards[key]})
        if not self.responses:
            raise AssertionError("No fake responses left")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_no_op_push_skips_write_when_content_unchanged(source_sync_test_root, monkeypatch):
    source_sync_test_root.write_packaged_config()
    local = {
        "active": [{"adapter": "static", "listing_url": "https://noop.example/jobs"}],
        "pending": [{"adapter": "teamtailor", "name": "Noop Pending"}],
        "rejected": [],
    }
    monkeypatch.setattr(sync, "now_iso", lambda: "2026-04-09T20:55:07.978053+00:00")
    remote_snapshot = sync.build_snapshot(local, source_label="admin_bridge")
    monkeypatch.setattr(sync, "now_iso", lambda: "2026-04-09T21:05:07.978053+00:00")
    encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(200, {"sha": "s1", "content": encoded}),
            _FakeResponse(201, {"content": {"sha": "manifest-sha"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"] is True
    assert result["skipped"] is False
    assert result["skipReason"] == ""
    assert result["remoteSha"] == "manifest-sha"
    assert result["snapshot"]["generatedAt"] == "2026-04-09T21:05:07.978053+00:00"
    assert result["counters"]["totalPushes"] == 1
    assert result["counters"]["noOpSkips"] == 0
    assert result["snapshotFormat"] == "sharded-v3"


def test_identity_collision_across_buckets_rejected(
    source_sync_test_root,
):
    source_sync_test_root.write_packaged_config()
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    local = {
        "active": [
            {"adapter": "static", "listing_url": "https://duplicate.example/jobs"},
            {
                "adapter": "static",
                "listing_url": "https://duplicate.example/jobs",
                "local_note": "second copy",
            },
        ],
        "pending": [],
        "rejected": [],
    }
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        with pytest.raises(sync.SyncOperationError) as ctx:
            sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert ctx.value.code == "duplicate_source_identity"
    assert "local active/pending snapshot" in str(ctx.value)
    assert len(opener.calls) == 3


def test_content_hash_stable_excluding_volatile_fields(source_sync_test_root, monkeypatch):
    source_sync_test_root.write_packaged_config()
    local = {
        "active": [
            {
                "adapter": "static",
                "listing_url": "https://example.com/jobs",
                "sourceId": "s1",
            }
        ],
        "pending": [],
        "rejected": [],
    }
    remote_snapshot = {
        "schemaVersion": 2,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "source": {"name": "discovery-run-a"},
        "active": [
            {"adapter": "static", "listing_url": "https://example.com/jobs", "sourceId": "s1"}
        ],
        "pending": [],
        "rejected": [],
    }
    remote_encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
    monkeypatch.setattr(sync, "now_iso", lambda: "2026-03-10T10:00:00+00:00")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(200, {"sha": "s1", "content": remote_encoded}),
            _FakeResponse(201, {"content": {"sha": "s2"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"]
    assert result["remoteSha"] == "s2"
    assert result["skipReason"] == ""
    assert result["snapshotFormat"] == "sharded-v3"


def test_idempotent_put_retry_re_reads_sha_on_transient_failure(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    remote_snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "active": [],
        "pending": [],
        "rejected": [],
    }
    remote_encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(200, {"sha": "s1", "content": remote_encoded}),
            URLError("transient socket close"),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(200, {"sha": "s1", "content": remote_encoded}),
            _FakeResponse(201, {"content": {"sha": "s2"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    local = {"active": [], "pending": [], "rejected": []}
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"] is True
    assert result["remoteSha"] == "s2"
    assert result["snapshotFormat"] == "sharded-v3"


def test_put_retry_detects_concurrent_write_as_conflict(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    remote_snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "active": [],
        "pending": [],
        "rejected": [],
    }
    remote_encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
    conflict_payload = {"message": "Update is not a fast-forward"}
    concurrent_payload = {
        "schemaVersion": 2,
        "generatedAt": "2026-03-09T11:00:00+00:00",
        "active": [{"adapter": "teamtailor", "listing_url": "https://other.example/jobs"}],
        "pending": [],
        "rejected": [],
    }
    concurrent_encoded = base64.b64encode(json.dumps(concurrent_payload).encode("utf-8")).decode(
        "ascii"
    )
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(200, {"sha": "s1", "content": remote_encoded}),
            _FakeResponse(409, conflict_payload),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(200, {"sha": "s3", "content": concurrent_encoded}),
            _FakeResponse(201, {"content": {"sha": "s4"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    local = {
        "active": [{"adapter": "static", "listing_url": "https://mine.example/jobs"}],
        "pending": [],
        "rejected": [],
    }
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"] is True
    assert result["remoteSha"] == "s4"


def test_transient_get_error_retries_with_backoff(source_sync_test_root, monkeypatch):
    source_sync_test_root.write_packaged_config()
    sleep_calls: list[float] = []
    monkeypatch.setattr(
        sync._source_sync_snapshot.time, "sleep", lambda seconds: sleep_calls.append(seconds)
    )
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            URLError("connection reset"),
            _FakeResponse(200, {"sha": "s1", "content": ""}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.read_remote_snapshot(cfg, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["exists"] is False
    assert result["sha"] == "s1"
    assert sleep_calls == [1.0]
