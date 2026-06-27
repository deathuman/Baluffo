from __future__ import annotations

from src.storage.baluffo_store import BaluffoStore
from src.storage.source_registry_runtime import (
    SourceRegistryRuntimeStore,
    _row_identity,
    _tombstone_key,
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


def test_source_registry_current_table_rows_are_limited_by_bucket() -> None:
    with workspace_tmpdir("source-registry-runtime-table-rows") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRegistryRuntimeStore(store)
            state = {
                "active": [{"id": f"a{index}", "name": f"Active {index}"} for index in range(4)],
                "pending": [{"id": f"p{index}", "name": f"Pending {index}"} for index in range(3)],
                "rejected": [{"id": "r0", "name": "Rejected 0"}],
            }
            runtime.replace_state(state=state, tombstones={}, generation="registry-table-test")

            rows = runtime.table_rows_for_current_generation(
                buckets=["active", "pending"],
                limit_per_bucket=2,
            )

            assert [row["id"] for row in rows["active"]] == ["a0", "a1"]
            assert [row["id"] for row in rows["pending"]] == ["p0", "p1"]
            assert "rejected" not in rows


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


def test_source_registry_identity_fallbacks_are_stable() -> None:
    assert _row_identity({}, "active", 3) == _row_identity({}, "pending", 99)
    assert _row_identity({}, "active", 3).startswith(":unknown:")
    assert _tombstone_key({}, "", 4) == "tombstone:4"
    assert _tombstone_key({"sourceId": "src-1"}, "fallback", 4) == "src-1"
    assert _tombstone_key({"id": "dead"}, "fallback", 4) == "dead"


def test_source_registry_empty_state_summary_and_publish_guard() -> None:
    with workspace_tmpdir("source-registry-runtime-empty") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRegistryRuntimeStore(store)

            assert runtime.current_generation() == ""
            assert runtime.current_state() == {"active": [], "pending": [], "rejected": []}
            assert runtime.current_tombstones() == {}
            assert runtime.state_for_generation("") == {
                "active": [],
                "pending": [],
                "rejected": [],
            }
            assert runtime.tombstones_for_generation("") == {}
            assert runtime.current_summary() == {
                "generation": "",
                "reason": "",
                "activeCount": 0,
                "pendingCount": 0,
                "rejectedCount": 0,
                "tombstoneCount": 0,
                "stateHash": "",
                "tombstoneHash": "",
                "publishedAt": "",
                "updatedAt": "",
            }

            try:
                runtime.publish_generation("")
            except ValueError as exc:
                assert "requires a generation" in str(exc)
            else:  # pragma: no cover - defensive assertion branch
                raise AssertionError("publish_generation accepted a blank generation")


def test_source_registry_publish_hash_guards_and_parity_hash() -> None:
    with workspace_tmpdir("source-registry-runtime-hash-guards") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRegistryRuntimeStore(store)
            staged = runtime.stage_state(
                state={"active": [{"id": "a"}], "pending": [], "rejected": []},
                tombstones={"dead": {"id": "dead"}},
                generation="staged",
            )

            try:
                runtime.publish_generation("staged", expected_state_hash="wrong")
            except ValueError as exc:
                assert "state hash mismatch" in str(exc)
            else:  # pragma: no cover - defensive assertion branch
                raise AssertionError("publish_generation accepted a wrong state hash")

            try:
                runtime.publish_generation(
                    "staged",
                    expected_state_hash=staged.state_hash,
                    expected_tombstone_hash="wrong",
                )
            except ValueError as exc:
                assert "tombstone hash mismatch" in str(exc)
            else:  # pragma: no cover - defensive assertion branch
                raise AssertionError("publish_generation accepted a wrong tombstone hash")

            runtime.publish_generation(
                "staged",
                expected_state_hash=staged.state_hash,
                expected_tombstone_hash=staged.tombstone_hash,
            )

            assert runtime.parity_hash() == {
                "stateHash": staged.state_hash,
                "tombstoneHash": staged.tombstone_hash,
            }
            assert runtime.parity_hash(
                state={"active": [], "pending": [], "rejected": []},
                tombstones={},
            ) == {
                "stateHash": source_registry_state_hash(
                    {"active": [], "pending": [], "rejected": []}
                ),
                "tombstoneHash": source_registry_tombstone_hash({}),
            }


def test_source_registry_stage_normalizes_partial_state_and_cleanup_cap_zero() -> None:
    with workspace_tmpdir("source-registry-runtime-partial-state") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRegistryRuntimeStore(store)
            first = runtime.replace_state(
                state={"active": [{"id": "first"}]},
                tombstones={"": {"ignored": True}, "plain": "deleted"},
                generation="first",
            )
            runtime.replace_state(
                state={"active": [{"id": "second"}]},
                tombstones={},
                generation="second",
            )

            assert first.tombstone_count == 1
            assert runtime.tombstones_for_generation("first") == {"plain": {"value": "deleted"}}
            assert runtime.cleanup_old_generations(delete_cap=0) == 0
            assert runtime.state_for_generation("first")["active"] == [{"id": "first"}]
