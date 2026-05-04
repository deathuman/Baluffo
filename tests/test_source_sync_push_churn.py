from __future__ import annotations

import base64
import json
from urllib.error import HTTPError

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

    def __call__(self, req, timeout=20):  # noqa: ANN001
        self.calls.append(
            {
                "url": req.full_url,
                "method": req.get_method(),
                "headers": dict(req.header_items()),
                "body": req.data.decode("utf-8") if isinstance(req.data, bytes) else "",
                "timeout": timeout,
            }
        )
        if not self.responses:
            raise AssertionError("No fake responses left")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_push_sources_snapshot_skips_noop_remote_write(source_sync_test_root, monkeypatch):
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
            _FakeResponse(200, {"sha": "s1", "content": encoded}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert not result["pushed"]
    assert result["skipped"] is True
    assert result["skipReason"] == "no_meaningful_change"
    assert result["remoteSha"] == "s1"
    assert result["snapshot"]["generatedAt"] == "2026-04-09T21:05:07.978053+00:00"
    assert len(opener.calls) == 2


def test_push_sources_snapshot_rejects_duplicate_canonical_identity_collision(
    source_sync_test_root,
):
    source_sync_test_root.write_packaged_config()
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
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
    assert len(opener.calls) == 2
