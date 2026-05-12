from __future__ import annotations

from src.storage.baluffo_store import BaluffoStore
from src.storage.source_registry_runtime import (
    SourceRegistryRuntimeStore,
    source_registry_state_hash,
    source_registry_tombstone_hash,
)
from tests.helpers.temp_paths import workspace_tmpdir


def _state() -> dict[str, list[dict[str, object]]]:
    return {
        "active": [
            {"id": "a", "name": "Active A", "adapter": "static", "listing_url": "https://a.test"},
            {"id": "b", "name": "Active B", "adapter": "greenhouse", "slug": "b"},
        ],
        "pending": [
            {"id": "p", "name": "Pending", "adapter": "lever", "account": "pending"},
        ],
        "rejected": [
            {"id": "r", "name": "Rejected", "adapter": "workable", "account": "reject"},
        ],
    }


def test_source_registry_replace_state_round_trips_rows_and_tombstones() -> None:
    with workspace_tmpdir("source-registry-runtime") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRegistryRuntimeStore(store)
            tombstones = {
                "dead": {"id": "dead", "reason": "manual_delete"},
                "old": {"id": "old", "reason": "dedupe"},
            }

            summary = runtime.replace_state(
                state=_state(),
                tombstones=tombstones,
                generation="registry-test",
                reason="unit-test",
            )

            assert summary.published is True
            assert summary.active_count == 2
            assert summary.pending_count == 1
            assert summary.rejected_count == 1
            assert summary.tombstone_count == 2
            assert runtime.current_generation() == "registry-test"
            assert runtime.current_state() == _state()
            assert runtime.current_tombstones() == tombstones
            assert runtime.current_summary()["stateHash"] == source_registry_state_hash(_state())
            assert runtime.current_summary()["reason"] == "unit-test"


def test_source_registry_stage_is_invisible_until_publish() -> None:
    with workspace_tmpdir("source-registry-runtime-stage") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRegistryRuntimeStore(store)
            first = {"active": [{"id": "first", "name": "First"}], "pending": [], "rejected": []}
            second = {"active": [{"id": "second", "name": "Second"}], "pending": [], "rejected": []}
            runtime.replace_state(state=first, tombstones={}, generation="first")

            staged = runtime.stage_state(state=second, tombstones={}, generation="second")

            assert staged.published is False
            assert runtime.current_generation() == "first"
            assert runtime.current_state() == first

            runtime.publish_generation(
                "second",
                expected_state_hash=staged.state_hash,
                expected_tombstone_hash=staged.tombstone_hash,
            )

            assert runtime.current_generation() == "second"
            assert runtime.current_state() == second


def test_source_registry_hashes_are_deterministic_for_payload_key_order() -> None:
    left = {"active": [{"id": "a", "name": "A"}], "pending": [], "rejected": []}
    right = {"pending": [], "rejected": [], "active": [{"name": "A", "id": "a"}]}
    left_tombstones = {"z": {"reason": "delete", "id": "z"}, "a": {"id": "a", "reason": "manual"}}
    right_tombstones = {"a": {"reason": "manual", "id": "a"}, "z": {"id": "z", "reason": "delete"}}

    assert source_registry_state_hash(left) == source_registry_state_hash(right)
    assert source_registry_tombstone_hash(left_tombstones) == source_registry_tombstone_hash(
        right_tombstones
    )


def test_source_registry_cleanup_deletes_only_old_generations() -> None:
    with workspace_tmpdir("source-registry-runtime-cleanup") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRegistryRuntimeStore(store)
            for generation in ("one", "two", "three"):
                runtime.replace_state(
                    state={"active": [{"id": generation}], "pending": [], "rejected": []},
                    tombstones={generation: {"id": generation}},
                    generation=generation,
                )

            deleted = runtime.cleanup_old_generations(delete_cap=1)

            assert deleted == 1
            assert runtime.current_generation() == "three"
            assert runtime.current_state()["active"] == [{"id": "three"}]
            assert runtime.current_tombstones() == {"three": {"id": "three"}}
