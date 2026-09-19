"""Regression coverage for `push_changed_shards` remote-request aggregation.

Every changed shard contributes its own `remoteRequests` timing rows to the
returned payload. The aggregation must read each shard's own output; reusing a
stale loop variable duplicates one shard's rows and silently drops the others.
"""

from __future__ import annotations

import base64
import threading
from typing import Any

import src.source_sync_shard as shard_mod
from tests.helpers.report_state import simple_config

_config = simple_config


class _PathKeyedSyncModule:
    """Serves each shard's PUT/GET from that shard's own path.

    `push_changed_shards` drives a `ThreadPoolExecutor`, so a shared
    first-in-first-out response queue would hand one shard's response to
    another. Keying every response by request path keeps each shard's round
    trip independent of completion order, so the test cannot go flaky.
    """

    def __init__(self, shards: list[shard_mod.Shard]) -> None:
        self._by_path = {shard.path: shard for shard in shards}
        self._lock = threading.Lock()
        self.calls: list[dict[str, Any]] = []

    def _github_api_base(self) -> str:
        return "https://api.github.test"

    def validate_sync_config(self, config: Any) -> None:
        assert config.repo == "owner/repo"

    def _request_json(self, **kwargs: Any) -> tuple[int, dict[str, Any], dict[str, str]]:
        with self._lock:
            self.calls.append(dict(kwargs))
        url = str(kwargs.get("url") or "")
        shard = next((item for path, item in self._by_path.items() if path in url), None)
        if shard is None:
            raise AssertionError(f"unexpected sync URL: {url}")
        if str(kwargs.get("method") or "").upper() == "PUT":
            return 200, {"content": {"sha": f"{shard.key}-remote-sha"}}, {}
        return 200, {"content": _encoded_bytes(shard.payload_bytes)}, {}


def _row(index: int) -> dict[str, str]:
    return {
        "id": f"static:listing_url:https://studio{index}.example/jobs",
        "adapter": "static",
        "listing_url": f"https://studio{index}.example/jobs",
        "name": f"Studio {index}",
    }


def _distinct_shards(count: int) -> list[shard_mod.Shard]:
    """Build `count` content-addressed shards, one per distinct shard key."""
    rows: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    index = 0
    while len(rows) < count:
        row = _row(index)
        key = shard_mod.shard_key(row)
        if key not in seen_keys:
            seen_keys.add(key)
            rows.append(row)
        index += 1
    shards = shard_mod.content_addressed_shards(shard_mod.build_shards(rows, max_size=10_000))
    assert len(shards) == count
    return shards


def _encoded_bytes(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def test_push_changed_shards_aggregates_each_shard_remote_requests_once() -> None:
    shards = _distinct_shards(3)
    module = _PathKeyedSyncModule(shards)

    result = shard_mod.push_changed_shards(
        module,
        _config(),
        shards,
        None,
        opener=lambda *_a, **_kw: None,
    )

    expected_rows = [
        (operation, shard.path) for shard in shards for operation in ("pushShard", "verifyShard")
    ]
    actual_rows = [(row["operation"], row["path"]) for row in result["remoteRequests"]]

    assert result["changedShardCount"] == len(shards)
    assert len(result["pushResults"]) == len(shards)
    assert len(result["verifiedShards"]) == len(shards)
    # Row *count* is 2 per shard on a successful round trip either way, so the
    # length check alone cannot discriminate a duplicated-shard bug. The
    # per-path multiset below is the assertion that actually fails when one
    # shard's rows are reused for every other shard.
    assert len(result["remoteRequests"]) == len(expected_rows)
    assert sorted(actual_rows) == sorted(expected_rows)
    assert len(set(actual_rows)) == len(expected_rows)
    assert all(row["ok"] for row in result["remoteRequests"])
