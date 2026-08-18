from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Any

import pytest

import src.source_sync_shard as shard_mod


class _FakeSyncModule:
    def __init__(self, responses: list[tuple[int, dict[str, Any]] | BaseException]):
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def _github_api_base(self) -> str:
        return "https://api.github.test"

    def validate_sync_config(self, config: Any) -> None:
        assert config.repo == "owner/repo"

    def _request_json(self, **kwargs: Any) -> tuple[int, dict[str, Any], dict[str, str]]:
        self.calls.append(dict(kwargs))
        if not self.responses:
            raise AssertionError("No fake responses left")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        status, payload = response
        return status, payload, {}


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        repo="owner/repo",
        branch="main",
        path="baluffo/source-sync.json",
        timeout_s=20,
    )


def _row() -> dict[str, str]:
    return {
        "id": "static:listing_url:https://studio.example/jobs",
        "adapter": "static",
        "listing_url": "https://studio.example/jobs",
        "name": "Studio",
    }


def _shard() -> shard_mod.Shard:
    return shard_mod.build_shards([_row()], max_size=10_000)[0]


def _encoded_bytes(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def test_push_shard_existing_path_maps_expected_verify_failure() -> None:
    shard = _shard()
    module = _FakeSyncModule(
        [
            (422, {"message": "Invalid request. sha was not supplied."}),
            RuntimeError("verify unavailable"),
        ]
    )

    with pytest.raises(RuntimeError, match="sha was not supplied") as ctx:
        shard_mod.push_shard(module, _config(), shard, opener=lambda *_a, **_kw: None)

    assert isinstance(ctx.value.__cause__, RuntimeError)
    assert str(ctx.value.__cause__) == "verify unavailable"


def test_push_shard_existing_path_does_not_mask_unexpected_verify_bug() -> None:
    shard = _shard()
    module = _FakeSyncModule([(422, {"message": "existing immutable path"})])

    with pytest.raises(AssertionError, match="No fake responses left"):
        shard_mod.push_shard(module, _config(), shard, opener=lambda *_a, **_kw: None)


def test_push_changed_shard_records_expected_push_failure() -> None:
    shard = _shard()
    module = _FakeSyncModule([RuntimeError("network down")])

    result = shard_mod._push_and_verify_changed_shard(  # noqa: SLF001
        module,
        _config(),
        shard,
        opener=lambda *_a, **_kw: None,
    )

    assert result["ok"] is False
    assert isinstance(result["exception"], RuntimeError)
    assert result["remoteRequests"][0]["operation"] == "pushShard"
    assert result["remoteRequests"][0]["ok"] is False


def test_push_changed_shard_does_not_mask_unexpected_push_bug() -> None:
    shard = _shard()
    module = _FakeSyncModule([AssertionError("unexpected push bug")])

    with pytest.raises(AssertionError, match="unexpected push bug"):
        shard_mod._push_and_verify_changed_shard(  # noqa: SLF001
            module,
            _config(),
            shard,
            opener=lambda *_a, **_kw: None,
        )


def test_push_changed_shard_records_expected_verify_failure() -> None:
    shard = _shard()
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            RuntimeError("verify down"),
        ]
    )

    result = shard_mod._push_and_verify_changed_shard(  # noqa: SLF001
        module,
        _config(),
        shard,
        opener=lambda *_a, **_kw: None,
    )

    assert result["ok"] is False
    assert isinstance(result["exception"], RuntimeError)
    assert [row["operation"] for row in result["remoteRequests"]] == [
        "pushShard",
        "verifyShard",
    ]
    assert result["remoteRequests"][-1]["ok"] is False


def test_push_sharded_snapshot_records_expected_manifest_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shard = _shard()
    module = _FakeSyncModule(
        [
            (200, {"content": {"sha": "shard-sha"}}),
            (200, {"content": _encoded_bytes(shard.payload_bytes)}),
            RuntimeError("manifest down"),
        ]
    )
    original_remote_timing_row = shard_mod._remote_timing_row  # noqa: SLF001
    timing_rows: list[dict[str, Any]] = []

    def record_remote_timing_row(**kwargs: Any) -> dict[str, Any]:
        row = original_remote_timing_row(**kwargs)
        timing_rows.append(row)
        return row

    monkeypatch.setattr(shard_mod, "_remote_timing_row", record_remote_timing_row)

    with pytest.raises(RuntimeError, match="manifest down"):
        shard_mod.push_sharded_snapshot(
            module,
            _config(),
            {
                "schemaVersion": 2,
                "generatedAt": "2026-06-18T00:00:00+00:00",
                "source": {"name": "admin_bridge"},
                "active": [_row()],
                "pending": [],
            },
            max_shard_size=10_000,
            opener=lambda *_a, **_kw: None,
        )

    assert timing_rows[-1]["operation"] == "pushManifest"
    assert timing_rows[-1]["ok"] is False
