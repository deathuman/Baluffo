from __future__ import annotations

import json
from datetime import UTC, datetime
from urllib.error import HTTPError

import pytest

from src import source_sync as sync
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401
from tests.test_source_sync_push_churn import _FakeResponse, _Recorder


def test_dry_run_returns_diff_without_side_effects(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
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
    local = {
        "active": [{"adapter": "static", "listing_url": "https://dryrun.example/jobs"}],
        "pending": [],
        "rejected": [],
    }
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, dry_run=True, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"] is False
    assert result["dryRun"] is True
    assert result["wouldChange"] is True
    assert result["remoteSha"] == ""
    assert result["skipReason"] == "dryRun"
    assert len(opener.calls) == 3


def test_daily_counters_reset_on_date_boundary(source_sync_test_root, monkeypatch):
    source_sync_test_root.write_packaged_config()
    original_now_utc = sync.now_utc
    try:
        monkeypatch.setattr(sync, "now_utc", lambda: datetime(2026, 5, 4, tzinfo=UTC))
        first = sync.record_sync_counters(
            totalPushes=7,
            totalPulls=2,
            noOpSkips=1,
            conflictsDetected=1,
            conflictsResolved=1,
            tombstonesSuppressed=3,
            sourcesAdded=4,
            sourcesRemoved=5,
        )
        assert first["date"] == "2026-05-04"
        assert first["totalPushes"] == 7
        assert first["totalPulls"] == 2
        monkeypatch.setattr(sync, "now_utc", lambda: datetime(2026, 5, 5, tzinfo=UTC))
        second = sync.record_sync_counters(totalPulls=1)
    finally:
        monkeypatch.setattr(sync, "now_utc", original_now_utc)
    assert second["date"] == "2026-05-05"
    assert second["totalPushes"] == 0
    assert second["totalPulls"] == 1
    assert second["noOpSkips"] == 0
    assert second["conflictsDetected"] == 0
    assert second["conflictsResolved"] == 0


def test_snapshot_size_warning_and_rejection(source_sync_test_root, monkeypatch):
    source_sync_test_root.write_packaged_config()
    huge_url = "https://example.com/jobs/" + ("a" * 256)
    local = {
        "active": [
            {
                "adapter": "static",
                "listing_url": f"{huge_url}/{idx}",
                "name": f"{'x' * 512}-{idx}",
            }
            for idx in range(5_000)
        ],
        "pending": [],
        "rejected": [],
    }
    fixed_now = "2026-04-09T20:55:07.978053+00:00"
    monkeypatch.setattr(sync, "now_iso", lambda: fixed_now)
    snapshot = sync.build_snapshot(local)
    snapshot_size_bytes = len(
        json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    )
    assert snapshot_size_bytes > sync.SNAPSHOT_SIZE_WARN_BYTES
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
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    cfg.max_snapshot_size_bytes = snapshot_size_bytes + 1
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"] is True
    assert result["sizeWarning"] is True
    assert result["sizeBytes"] == snapshot_size_bytes
    assert result["maxSnapshotSizeBytes"] == snapshot_size_bytes + 1
    assert result["snapshotFormat"] == "sharded-v3"

    with sync._AUTH_MANAGER_LOCK:  # noqa: SLF001
        sync._AUTH_MANAGER.clear()  # noqa: SLF001
    monkeypatch.setattr(sync, "DEFAULT_SOURCE_SYNC_SHARD_SIZE_BYTES", 100)
    rejection_opener = _Recorder(
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
    cfg.max_snapshot_size_bytes = snapshot_size_bytes - 1
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        with pytest.raises(sync.SyncOperationError) as ctx:
            sync.push_sources_snapshot(cfg, local, opener=rejection_opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert ctx.value.code == "snapshot_too_large"
    assert ctx.value.fields["sizeBytes"] == snapshot_size_bytes
    assert ctx.value.fields["maxSnapshotSizeBytes"] == snapshot_size_bytes - 1
    assert ctx.value.fields["sizeWarning"] is True
    assert len(rejection_opener.calls) == 3


def test_default_snapshot_limit_allows_large_runtime_snapshot(source_sync_test_root, monkeypatch):
    source_sync_test_root.write_packaged_config()
    old_limit = 5 * 1024 * 1024
    huge_url = "https://example.com/jobs/" + ("a" * 256)
    local = {
        "active": [
            {
                "adapter": "static",
                "listing_url": f"{huge_url}/{idx}",
                "name": f"{'x' * 900}-{idx}",
            }
            for idx in range(6_000)
        ],
        "pending": [],
        "rejected": [],
    }
    fixed_now = "2026-05-08T10:55:07.978053+00:00"
    monkeypatch.setattr(sync, "now_iso", lambda: fixed_now)
    snapshot = sync.build_snapshot(local)
    snapshot_size_bytes = len(
        json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    )
    assert old_limit < snapshot_size_bytes < sync.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
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
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
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
    assert result["sizeBytes"] == snapshot_size_bytes
    assert result["maxSnapshotSizeBytes"] == sync.DEFAULT_MAX_SNAPSHOT_SIZE_BYTES
