from __future__ import annotations

import base64
import json

from src import source_sync as sync
from src.source_sync_shard import build_sharded_snapshot_bundle
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401
from tests.source_sync_http_fakes import FakeResponse, Recorder


def _start_recorder() -> Recorder:
    return Recorder(
        [
            FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            FakeResponse(404, {"message": "Not Found"}),
            FakeResponse(404, {"message": "Not Found"}),
        ]
    )


def test_push_fast_path_reads_only_manifest_when_local_and_remote_unchanged(
    source_sync_test_root,
):
    source_sync_test_root.write_packaged_config()
    local = {
        "active": [{"adapter": "static", "listing_url": "https://fast.example/jobs"}],
        "pending": [],
        "rejected": [],
    }
    opener = _start_recorder()
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"
        first = sync.push_sources_snapshot(cfg, local, opener=opener)
        manifest_sha = opener.manifest_sha
        assert first["pushed"] is True
        assert first["remoteSha"] == manifest_sha
        manifest_payload = {
            "sha": manifest_sha,
            "content": base64.b64encode(opener.blobs[manifest_sha]).decode("ascii"),
        }
        opener.responses.append(FakeResponse(200, manifest_payload))
        calls_before = len(opener.calls)
        second = sync.push_sources_snapshot(
            cfg,
            local,
            known_remote_sha=manifest_sha,
            known_local_revision=first["localRevision"],
            opener=opener,
        )
        fast_path_calls = opener.calls[calls_before:]
        opener.responses.extend(
            [FakeResponse(200, manifest_payload), FakeResponse(200, manifest_payload)]
        )
        third = sync.push_sources_snapshot(
            cfg,
            {**local, "pending": [{"adapter": "teamtailor", "name": "Fast Pending"}]},
            known_remote_sha=manifest_sha,
            known_local_revision=first["localRevision"],
            opener=opener,
        )
    finally:
        sync.build_app_jwt = original_build_jwt

    assert second["pushed"] is False
    assert second["skipped"] is True
    assert second["skipReason"] == "remote_and_local_unchanged"
    assert second["remoteReadSkipped"] is True
    assert second["remoteSha"] == manifest_sha
    assert second["counters"]["noOpSkips"] == 1
    assert len(fast_path_calls) == 1
    assert "shards/" not in fast_path_calls[0]["url"]
    assert "/git/" not in fast_path_calls[0]["url"]
    assert third["pushed"] is True
    assert "remoteReadSkipped" not in third


def test_push_fast_path_reports_remote_counts_when_local_lacks_merged_rows(
    source_sync_test_root,
) -> None:
    source_sync_test_root.write_packaged_config()
    local = {
        "active": [{"adapter": "static", "listing_url": "https://fast.example/jobs"}],
        "pending": [],
        "rejected": [],
    }
    opener = _start_recorder()
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"
        first = sync.push_sources_snapshot(cfg, local, opener=opener)
        remote_snapshot = {
            "schemaVersion": 2,
            "generatedAt": "2026-05-12T10:01:00+00:00",
            "source": {"name": "admin_bridge"},
            "active": [
                *local["active"],
                {"adapter": "static", "listing_url": "https://remote.example/jobs"},
            ],
            "pending": [],
        }
        manifest = build_sharded_snapshot_bundle(remote_snapshot, max_shard_size=10_000)["manifest"]
        remote_manifest_sha = "remote-manifest-sha"
        opener.responses.append(
            FakeResponse(
                200,
                {
                    "sha": remote_manifest_sha,
                    "content": base64.b64encode(json.dumps(manifest).encode("utf-8")).decode(
                        "ascii"
                    ),
                },
            )
        )
        result = sync.push_sources_snapshot(
            cfg,
            local,
            known_remote_sha=remote_manifest_sha,
            known_local_revision=first["localRevision"],
            opener=opener,
        )
    finally:
        sync.build_app_jwt = original_build_jwt

    # The local registry never received the row another client merged into the
    # remote, but the preview must still report the remote generation's counts.
    assert result["skipReason"] == "remote_and_local_unchanged"
    assert result["counts"] == {"active": 2, "pending": 0, "rejected": 0}
