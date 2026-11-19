"""Regression tests for the shard GC retention window.

A pull reads the committed manifest first and its shards afterward. A push that
commits a new generation in that window must leave the older generation's
shards readable; these tests drive a real two-generation commit history through
the fake Git Data branch and then read the old manifest's shards.
"""

from typing import Any

import pytest

import src.source_sync_shard as shard_mod
from src.source_sync_shard import (
    build_sharded_snapshot_bundle,
    prune_unreferenced_shards,
    push_sharded_snapshot,
)
from tests.helpers.git_data_sync_module import GitDataSyncModule
from tests.helpers.report_state import simple_config


def _snapshot(*, url: str, name: str, generated_at: str) -> dict[str, Any]:
    return {
        "schemaVersion": 2,
        "generatedAt": generated_at,
        "source": {"name": "admin_bridge"},
        "active": [
            {
                "id": f"static:listing_url:{url}",
                "adapter": "static",
                "listing_url": url,
                "name": name,
            }
        ],
        "pending": [],
    }


def _push(module: GitDataSyncModule, snapshot: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    return push_sharded_snapshot(
        module,
        simple_config(),
        snapshot,
        max_shard_size=10_000,
        opener=lambda *_args, **_kwargs: None,
        **kwargs,
    )


def _first_generation(module: GitDataSyncModule) -> tuple[dict[str, Any], list[str]]:
    result = _push(
        module,
        _snapshot(
            url="https://first.example/jobs",
            name="First Studio",
            generated_at="2026-05-12T10:00:00+00:00",
        ),
    )
    return result["manifest"], [entry["path"] for entry in result["manifest"]["shards"]]


def test_pull_reading_an_older_manifest_survives_the_prune(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = GitDataSyncModule()
    first_manifest, first_paths = _first_generation(module)
    assert first_paths

    second = _push(
        module,
        _snapshot(
            url="https://second.example/jobs",
            name="Second Studio",
            generated_at="2026-05-12T10:01:00+00:00",
        ),
    )

    # The previous generation's shards are retained for the pull that read them.
    assert second["gc"]["deletedCount"] == 0
    assert second["gc"]["retainedGenerationCount"] == 1
    assert second["gc"]["retainedShardCount"] == len(first_paths)
    assert module.deleted_paths == []
    assert all(path in module.branch_tree() for path in first_paths)

    # The pull that captured the first manifest still reads every shard it names.
    monkeypatch.setattr(
        shard_mod,
        "read_manifest",
        lambda *_args, **_kwargs: {
            "manifest": first_manifest,
            "sha": "first-manifest-sha",
            "manifestSizeBytes": 1,
        },
    )
    snapshot = shard_mod.read_sharded_snapshot(
        module,
        simple_config(),
        max_workers=1,
        opener=lambda *_args, **_kwargs: None,
    )

    assert snapshot is not None
    assert [row["name"] for row in snapshot["active"]] == ["First Studio"]


def test_prune_window_is_bounded_by_retain_generations() -> None:
    module = GitDataSyncModule()
    _first_manifest, first_paths = _first_generation(module)
    _push(
        module,
        _snapshot(
            url="https://second.example/jobs",
            name="Second Studio",
            generated_at="2026-05-12T10:01:00+00:00",
        ),
    )

    third = _push(
        module,
        _snapshot(
            url="https://third.example/jobs",
            name="Third Studio",
            generated_at="2026-05-12T10:02:00+00:00",
        ),
        gc_retain_generations=1,
    )

    # One generation of retention means the generation before the previous one
    # is outside the window and can be pruned.
    assert set(first_paths) & set(third["gc"]["deletedPaths"])
    assert all(path not in module.branch_tree() for path in first_paths)


@pytest.mark.parametrize("invalid_history_row", ["not-a-history-row", {"sha": ""}])
def test_prune_fails_closed_on_invalid_history_tail_after_retention_window(
    invalid_history_row: Any,
) -> None:
    module = GitDataSyncModule()
    _first_manifest, first_paths = _first_generation(module)
    _push(
        module,
        _snapshot(
            url="https://second.example/jobs",
            name="Second Studio",
            generated_at="2026-05-12T10:01:00+00:00",
        ),
    )
    prior_commit_sha = str(module.commits[1]["sha"])
    module.commit_history_payload = [
        {"sha": prior_commit_sha},
        invalid_history_row,
    ]

    third = _push(
        module,
        _snapshot(
            url="https://third.example/jobs",
            name="Third Studio",
            generated_at="2026-05-12T10:02:00+00:00",
        ),
        gc_retain_generations=1,
    )

    # The valid prior generation is inside the one-generation window, but the
    # malformed tail makes the whole history untrustworthy. GC must not retain
    # only that prior generation and then prune the older generation as if the
    # window were complete.
    assert third["pushed"] is True
    assert third["gc"]["deletedCount"] == 0
    assert third["gc"]["retainedGenerationCount"] == 0
    assert any("retention unavailable" in warning for warning in third["warnings"])
    assert module.deleted_paths == []
    assert all(path in module.branch_tree() for path in first_paths)


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("commit_history_status", 500),
        ("manifest_ref_status", 404),
        ("commit_history_payload", []),
        ("commit_history_payload", ["not-a-history-row"]),
        ("commit_history_payload", [{"sha": ""}]),
    ],
)
def test_prune_leaves_shards_alone_when_retention_is_unavailable(
    attribute: str, value: Any
) -> None:
    module = GitDataSyncModule()
    _first_manifest, first_paths = _first_generation(module)
    setattr(module, attribute, value)

    second = _push(
        module,
        _snapshot(
            url="https://second.example/jobs",
            name="Second Studio",
            generated_at="2026-05-12T10:01:00+00:00",
        ),
    )

    # Without the prior generations there is no safe deletion set, so GC fails
    # closed rather than risk a 404 in an in-flight pull.
    assert second["pushed"] is True
    assert second["gc"]["deletedCount"] == 0
    assert second["gc"]["retainedGenerationCount"] == 0
    assert any("retention unavailable" in warning for warning in second["warnings"])
    assert module.deleted_paths == []
    assert all(path in module.branch_tree() for path in first_paths)


def test_gc_lists_shards_from_the_git_tree_past_the_contents_cap() -> None:
    """The branch tree, not the capped Contents listing, is the deletion set."""
    module = GitDataSyncModule()
    old_paths = [
        f"baluffo/source-sync/shards/active/a/old-{index:04d}.json.gz" for index in range(1001)
    ]
    for path in old_paths:
        module.tree_by_sha["base-tree"][path] = f"old-sha-{path.rsplit('/', 1)[-1]}"

    result = _push(
        module,
        _snapshot(
            url="https://cap.example/jobs",
            name="Cap Studio",
            generated_at="2026-05-12T10:00:00+00:00",
        ),
        gc_delete_limit=2000,
    )

    assert result["pushed"] is True
    assert result["gc"]["deletedCount"] == len(old_paths)
    assert set(module.deleted_paths) == set(old_paths)
    assert not [
        call
        for call in module.calls
        if str(call.get("method")) == "GET"
        and "/contents/baluffo/source-sync/shards" in str(call.get("url") or "")
    ]
    assert any(
        str(call.get("method")) == "GET" and "/git/trees/" in str(call.get("url") or "")
        for call in module.calls
    )


def test_gc_refuses_a_truncated_tree_listing() -> None:
    module = GitDataSyncModule()
    module.tree_listing_truncated = True
    manifest = build_sharded_snapshot_bundle(
        _snapshot(
            url="https://truncated.example/jobs",
            name="Truncated Studio",
            generated_at="2026-05-12T10:00:00+00:00",
        ),
        max_shard_size=10_000,
    )["manifest"]

    with pytest.raises(RuntimeError, match="truncated"):
        prune_unreferenced_shards(
            module,
            simple_config(),
            manifest,
            retain_generations=0,
            opener=lambda *_args, **_kwargs: None,
        )

    assert module.deleted_paths == []


def test_push_warns_when_the_gc_listing_is_truncated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_path = "baluffo/source-sync/shards/active/old/oldsha.json.gz"
    module = GitDataSyncModule()
    module.tree_by_sha["base-tree"][old_path] = "old-remote-sha"

    def truncated_listing(*_args, **_kwargs) -> list[dict[str, Any]]:
        raise RuntimeError("source-sync shard GC listing is truncated")

    monkeypatch.setattr("src.source_sync_shard_gc._list_shard_tree", truncated_listing)
    result = _push(
        module,
        _snapshot(
            url="https://truncated-warning.example/jobs",
            name="Truncated Warning Studio",
            generated_at="2026-05-12T10:00:00+00:00",
        ),
    )

    assert result["pushed"] is True
    assert result["gc"]["deletedCount"] == 0
    assert result["gc"]["deletedPaths"] == []
    assert any("truncated" in warning for warning in result["warnings"])
    assert module.deleted_paths == []
