from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from src.source_sync_shard import build_shards, read_shard


class _FakeSyncModule:
    def __init__(self, payload: dict[str, Any], raw_bytes: bytes):
        self.payload = payload
        self.raw_bytes = raw_bytes
        self.calls: list[dict[str, Any]] = []

    def _github_api_base(self) -> str:
        return "https://api.github.test"

    def validate_sync_config(self, _config: Any) -> None:
        return None

    def _request_json(self, **kwargs: Any) -> tuple[int, dict[str, Any], dict[str, str]]:
        self.calls.append({"method": "GET", **kwargs})
        return 200, self.payload, {}

    def _request_raw_bytes(self, **kwargs: Any) -> tuple[int, bytes, dict[str, str]]:
        self.calls.append({"method": "GET_RAW", **kwargs})
        return 200, self.raw_bytes, {}


def test_read_shard_downloads_raw_bytes_when_github_omits_large_content() -> None:
    row = {"adapter": "demo", "id": "source-1", "name": "Example"}
    shard = build_shards([row], max_size=10_000)[0]
    module = _FakeSyncModule(
        {"content": "", "encoding": "none", "download_url": "https://raw.example/shard"},
        shard.payload_bytes,
    )
    config = SimpleNamespace(repo="owner/repo", branch="main", timeout_s=30)

    result = read_shard(module, config, shard.manifest_entry(), opener=object())

    assert result["rows"] == [row]
    assert [call["method"] for call in module.calls] == ["GET", "GET_RAW"]
    assert module.calls[1]["url"] == "https://raw.example/shard"
    assert module.calls[1]["headers"] == {"Accept": "application/octet-stream"}
