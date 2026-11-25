"""Discovery approval and an admin demotion must compose, not overwrite each other."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import pytest

from src.bridge.registry_service import RegistryPaths, RegistryService
from src.bridge.routes import post_routes_admin
from src.bridge.routes.post_routes_admin import handle_post
from src.bridge.storage_health import close_storage_stores, get_storage_store
from src.source_registry import (
    REGISTRY_REASON_REPEATED_ZERO_JOBS,
    hide_repeated_zero_job_pending,
    load_json_array,
    save_json_atomic,
    save_registry_state_atomic,
)
from src.source_registry_authority_apply import apply_discovery_approval_to_authority
from src.source_registry_identity import source_identity, source_url_fingerprint
from src.source_registry_write_lock import (
    SourceRegistryWriteLockTimeout,
    source_registry_write_lock,
)
from src.storage.baluffo_store import BaluffoStore
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore
from tests.bridge.test_admin_post_route_capability import MinimalAdminPostRouteApi
from tests.bridge.test_discovery_service_hardening import _make_service
from tests.helpers.bridge_api import FakeHandler

_NOW = "2026-07-01T10:00:00+00:00"

# A was demoted by the admin for zero kept jobs, so the approval policy never
# re-approves it: the demotion is the only writer that can move it.
_A_ACTIVE = {
    "id": "a",
    "adapter": "static",
    "name": "Studio A",
    "careersUrl": "https://a.example/careers",
    "jobsFound": 0,
}
_B_PENDING = {
    "id": "b",
    "adapter": "static",
    "name": "Studio B",
    "careersUrl": "https://b.example/careers",
    "jobsFound": 3,
}
_X_ZERO_JOB_PENDING = {
    "id": "x",
    "adapter": "static",
    "name": "Studio X",
    "careersUrl": "https://x.example/careers",
    "jobsFound": 0,
    "deferCount": 0,
}
_C_DELETED = {
    "id": "c",
    "adapter": "static",
    "name": "Studio C",
    "careersUrl": "https://c.example/careers",
    "jobsFound": 2,
}


def _paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "active": tmp_path / "source-registry-active.json",
        "pending": tmp_path / "source-registry-pending.json",
        "rejected": tmp_path / "source-registry-rejected.json",
    }


def _report() -> dict[str, Any]:
    return {
        "runId": "discovery_concurrent_1",
        "summary": {"queuedCandidateCount": 1},
        "runtime": {},
        "candidates": [dict(_B_PENDING)],
    }


def _seed_sqlite_authority(tmp_path: Path) -> None:
    store = get_storage_store(tmp_path)
    SourceRegistryRuntimeStore(store).replace_state(
        state={"active": [dict(_A_ACTIVE)], "pending": [dict(_B_PENDING)], "rejected": []},
        tombstones={},
        reason="test_seed",
    )


def _apply_approval(
    tmp_path: Path,
    *,
    pending_rows: list[dict[str, Any]],
    auto_approve_enabled: bool = True,
    tombstones: dict[str, Any] | None = None,
    lock_timeout_s: float = 10.0,
    merge_audit: dict[str, int] | None = None,
    merge_conflicts: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], int, str]:
    paths = _paths(tmp_path)
    return apply_discovery_approval_to_authority(
        active_path=paths["active"],
        pending_path=paths["pending"],
        rejected_path=paths["rejected"],
        pending_rows=pending_rows,
        report=_report(),
        auto_approve_enabled=auto_approve_enabled,
        tombstones=tombstones or {},
        approval_state_path=tmp_path / "source-approval-state.json",
        now_iso_fn=lambda: _NOW,
        save_registry_state_atomic=save_registry_state_atomic,
        save_json_atomic=save_json_atomic,
        load_json_array=load_json_array,
        lock_timeout_s=lock_timeout_s,
        merge_audit=merge_audit,
        merge_conflicts=merge_conflicts,
    )


def _run_pending() -> list[dict[str, Any]]:
    return [dict(_B_PENDING)]


def _current_authority_state(tmp_path: Path) -> dict[str, list[dict[str, Any]]]:
    with BaluffoStore(tmp_path, defer_maintenance=True) as store:
        return SourceRegistryRuntimeStore(store).current_state()


def test_discovery_approval_publishes_through_sqlite_authority(tmp_path: Path) -> None:
    try:
        _seed_sqlite_authority(tmp_path)

        state, approved, authority = _apply_approval(tmp_path, pending_rows=_run_pending())

        assert authority == "sqlite"
        assert approved == 1
        assert [source_identity(row) for row in state["active"]] == ["a", "b"]
        assert state["pending"] == []

        current = _current_authority_state(tmp_path)
        assert sorted(source_identity(row) for row in current["active"]) == ["a", "b"]
        assert current["pending"] == []
        exports = load_json_array(_paths(tmp_path)["active"], [])
        assert sorted(source_identity(row) for row in exports) == ["a", "b"]
    finally:
        close_storage_stores()


def test_discovery_approval_and_concurrent_admin_demotion_both_survive(
    tmp_path: Path, monkeypatch: Any
) -> None:
    publish_entered = threading.Event()
    release_publish = threading.Event()
    admin_done = threading.Event()
    errors: list[BaseException] = []
    import src.source_registry_authority_apply as authority_apply

    real_publish = authority_apply._publish_authority_state

    def blocked_publish(*, runtime: Any, state: Any, tombstones: Any) -> None:
        publish_entered.set()
        assert release_publish.wait(10.0)
        real_publish(runtime=runtime, state=state, tombstones=tombstones)

    monkeypatch.setattr(authority_apply, "_publish_authority_state", blocked_publish)

    try:
        _seed_sqlite_authority(tmp_path)
        # The cached store belongs to this thread; let each worker open its own.
        close_storage_stores()

        def run_discovery() -> None:
            try:
                _apply_approval(tmp_path, pending_rows=_run_pending())
            except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion below
                errors.append(exc)

        def run_admin_demotion() -> None:
            try:
                with source_registry_write_lock(tmp_path, timeout_s=10.0):
                    service = RegistryService(
                        paths=RegistryPaths(**_paths(tmp_path)),
                        default_active=[],
                        normalize_manual_static=lambda row: row,
                    )
                    state = service.load_state()
                    demoted = [row for row in state["active"] if source_identity(row) == "a"]
                    state["active"] = [
                        row for row in state["active"] if source_identity(row) != "a"
                    ]
                    state["pending"] = [*demoted, *state["pending"]]
                    service.persist_state(state)
            except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion below
                errors.append(exc)
            finally:
                admin_done.set()

        discovery_thread = threading.Thread(target=run_discovery, name="discovery-approval")
        discovery_thread.start()
        assert publish_entered.wait(10.0) is True

        admin_thread = threading.Thread(target=run_admin_demotion, name="admin-demotion")
        admin_thread.start()
        # The admin write is a read-modify-write too: it must wait for the
        # discovery publish, then read the state that publish left behind.
        assert admin_done.wait(0.3) is False

        release_publish.set()
        discovery_thread.join(10.0)
        admin_thread.join(10.0)
        assert discovery_thread.is_alive() is False
        assert admin_thread.is_alive() is False
        assert errors == []

        current = _current_authority_state(tmp_path)
        assert sorted(source_identity(row) for row in current["active"]) == ["b"]
        assert sorted(source_identity(row) for row in current["pending"]) == ["a"]
    finally:
        close_storage_stores()


def test_run_pending_rows_do_not_resurrect_rejected_or_deleted_sources(tmp_path: Path) -> None:
    try:
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [dict(_A_ACTIVE)], "pending": [], "rejected": [dict(_B_PENDING)]},
            tombstones={},
            reason="test_seed",
        )
        tombstone = {
            "c": {
                "sourceId": "c",
                "sourceUrlFingerprint": source_url_fingerprint(_C_DELETED),
            }
        }

        state, approved, _ = _apply_approval(
            tmp_path,
            pending_rows=[dict(_B_PENDING), dict(_C_DELETED)],
            auto_approve_enabled=False,
            tombstones=tombstone,
        )

        assert approved == 0
        assert state["pending"] == []
        assert [source_identity(row) for row in state["active"]] == ["a"]
        assert [source_identity(row) for row in state["rejected"]] == ["b"]
        current = _current_authority_state(tmp_path)
        assert [source_identity(row) for row in current["active"]] == ["a"]
        assert current["pending"] == []
        assert [source_identity(row) for row in current["rejected"]] == ["b"]
    finally:
        close_storage_stores()


def test_run_zero_job_hide_reaches_existing_pending_without_overwriting_authority_fields(
    tmp_path: Path,
) -> None:
    try:
        current = {
            **_X_ZERO_JOB_PENDING,
            "name": "Admin Studio X",
            "careersUrl": "https://admin.example/x",
            "stateChangedAt": "2026-06-01T09:00:00+00:00",
            "stateChangedBy": "admin",
            "lastDemotedAt": "2026-06-01T09:00:00+00:00",
        }
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [], "pending": [current], "rejected": []},
            tombstones={},
            reason="test_seed",
        )
        hidden = hide_repeated_zero_job_pending(
            {
                **_X_ZERO_JOB_PENDING,
                "name": "Run Studio X",
                "careersUrl": "https://run.example/x",
                "deferCount": 3,
                "lastDemotedAt": _NOW,
            },
            at=_NOW,
        )

        state, approved, _ = _apply_approval(
            tmp_path,
            pending_rows=[hidden],
            auto_approve_enabled=False,
        )

        assert approved == 0
        [row] = state["pending"]
        assert row["hiddenFromDefault"] is True
        assert row["candidateState"] == "hidden"
        assert row["pendingReason"] == REGISTRY_REASON_REPEATED_ZERO_JOBS
        assert row["deferCount"] == 0
        assert row["name"] == "Admin Studio X"
        assert row["careersUrl"] == "https://admin.example/x"
        assert row["stateChangedAt"] == "2026-06-01T09:00:00+00:00"
        assert row["stateChangedBy"] == "admin"
        assert row["lastDemotedAt"] == "2026-06-01T09:00:00+00:00"
        [published] = _current_authority_state(tmp_path)["pending"]
        assert published["hiddenFromDefault"] is True
        assert published["deferCount"] == 0
    finally:
        close_storage_stores()


def test_registry_write_lock_times_out_on_thread_contention(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with source_registry_write_lock(tmp_path, timeout_s=10.0):
            entered.set()
            assert release.wait(10.0)

    holder = threading.Thread(target=hold, name="lock-holder")
    holder.start()
    try:
        assert entered.wait(10.0) is True
        with pytest.raises(SourceRegistryWriteLockTimeout):
            with source_registry_write_lock(tmp_path, timeout_s=0.1):
                pass
    finally:
        release.set()
        holder.join(10.0)
    assert holder.is_alive() is False


def test_registry_write_lock_is_reentrant_within_a_thread(tmp_path: Path) -> None:
    with source_registry_write_lock(tmp_path, timeout_s=1.0):
        with source_registry_write_lock(tmp_path, timeout_s=1.0):
            pass


def test_registry_mutation_route_takes_the_registry_write_lock(
    tmp_path: Path, monkeypatch: Any
) -> None:
    entered: list[str] = []
    real_lock = post_routes_admin.source_registry_write_lock

    @contextmanager
    def recording_lock(data_dir: Path | str, *, timeout_s: float | None = None) -> Iterator[None]:
        entered.append(str(Path(data_dir).resolve()))
        with real_lock(data_dir, timeout_s=timeout_s):
            yield

    monkeypatch.setattr(post_routes_admin, "source_registry_write_lock", recording_lock)
    api = cast(Any, MinimalAdminPostRouteApi(tmp_path))
    handler = FakeHandler()

    assert (
        handle_post(
            handler,
            api=api,
            path="/registry/demote-active",
            payload={"ids": ["active-1"]},
        )
        is True
    )

    assert entered == [str(tmp_path.resolve())]


def test_run_observations_update_existing_pending_but_authority_fields_win(
    tmp_path: Path,
) -> None:
    try:
        current = {
            **_B_PENDING,
            "jobsFound": 3,
            "sampleCount": 3,
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
            "lastProbeStatus": "error",
            "lastProbeError": "old probe failed",
        }
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [], "pending": [current], "rejected": []},
            tombstones={},
            reason="test_seed",
        )
        run = {
            **current,
            "jobsFound": 7,
            "sampleCount": 7,
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
            "lastProbeStatus": "ok",
            "lastProbeError": "",
            "confidence": "high",
            "weakSignal": False,
            "name": "Studio B Refreshed",
            "careersUrl": "https://b.example/refreshed",
        }

        state, approved, _ = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=False,
        )

        assert approved == 0
        [row] = state["pending"]
        assert row["jobsFound"] == 7
        assert row["sampleCount"] == 7
        assert row["lastProbedAt"] == "2026-07-01T10:00:00+00:00"
        assert row["lastProbeStatus"] == "ok"
        assert row["lastProbeError"] == ""
        assert row["confidence"] == "high"
        assert row["weakSignal"] is False
        assert row["name"] == "Studio B"
        assert row["careersUrl"] == "https://b.example/careers"
        [published] = _current_authority_state(tmp_path)["pending"]
        assert published["jobsFound"] == 7
    finally:
        close_storage_stores()


def test_run_cannot_take_over_a_non_blank_adapter(tmp_path: Path) -> None:
    """The adapter decides how the board is fetched, so only the authority owns it.

    A run may observe a different adapter on the same identity (an old registry
    row, a URL patch, a provider migration). Carrying that observation would
    silently reroute every later probe, so a non-blank authority adapter wins
    even while the run's measurements land.
    """
    try:
        current = {
            **_B_PENDING,
            "adapter": "greenhouse",
            "jobsFound": 3,
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
        }
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [], "pending": [current], "rejected": []},
            tombstones={},
            reason="test_seed",
        )
        run = {
            **current,
            "adapter": "lever",
            "jobsFound": 8,
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
        }

        state, _, _ = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=False,
        )

        [row] = state["pending"]
        assert row["adapter"] == "greenhouse"
        assert row["jobsFound"] == 8
        [published] = _current_authority_state(tmp_path)["pending"]
        assert published["adapter"] == "greenhouse"
    finally:
        close_storage_stores()


def test_stale_run_cannot_roll_back_observations_or_refill_blank_fields(
    tmp_path: Path,
) -> None:
    try:
        current = {
            **_B_PENDING,
            "name": "",
            "studio": "",
            "careersUrl": "",
            "jobsFound": 9,
            "lastProbedAt": "2026-07-01T11:00:00+00:00",
        }
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [], "pending": [current], "rejected": []},
            tombstones={},
            reason="test_seed",
        )
        run = {
            **current,
            "name": "Filled Name",
            "studio": "Filled Studio",
            "careersUrl": "https://b.example/filled",
            "jobsFound": 1,
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
        }

        state, _, _ = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=False,
        )

        [row] = state["pending"]
        assert row["jobsFound"] == 9
        assert row["lastProbedAt"] == "2026-07-01T11:00:00+00:00"
        # The admin cleared these after the run's probe; the stale snapshot may
        # not put them back.
        assert row["name"] == ""
        assert row["studio"] == ""
        assert row["careersUrl"] == ""
    finally:
        close_storage_stores()


def test_failed_probe_carries_error_without_overwriting_jobs_count(tmp_path: Path) -> None:
    try:
        current = {
            **_B_PENDING,
            "jobsFound": 5,
            "lastProbedAt": "2026-07-01T09:00:00+00:00",
        }
        store = get_storage_store(tmp_path)
        SourceRegistryRuntimeStore(store).replace_state(
            state={"active": [], "pending": [current], "rejected": []},
            tombstones={},
            reason="test_seed",
        )
        run = {
            **current,
            "jobsFound": 0,
            "sampleCount": 0,
            "lastProbedAt": "2026-07-01T10:00:00+00:00",
            "lastProbeStatus": "error",
            "lastProbeError": "timeout",
        }

        state, _, _ = _apply_approval(
            tmp_path,
            pending_rows=[run],
            auto_approve_enabled=False,
        )

        [row] = state["pending"]
        assert row["jobsFound"] == 5
        assert row["lastProbedAt"] == "2026-07-01T10:00:00+00:00"
        assert row["lastProbeStatus"] == "error"
        assert row["lastProbeError"] == "timeout"
    finally:
        close_storage_stores()


def test_failed_publish_carries_run_observations_onto_existing_pending(tmp_path: Path) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    current = {
        **_B_PENDING,
        "jobsFound": 3,
        "lastProbedAt": "2026-07-01T09:00:00+00:00",
        "lastProbeError": "old",
    }
    run = {
        **current,
        "jobsFound": 8,
        "lastProbedAt": "2026-07-01T10:00:00+00:00",
        "lastProbeStatus": "ok",
        "lastProbeError": "",
        "name": "Studio B Refreshed",
        "careersUrl": "https://b.example/refreshed",
    }
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_pending_update",
                "startedAt": "2026-07-01T09:30:00+00:00",
                "finishedAt": "2026-07-01T10:00:00+00:00",
                "summary": {"queuedCandidateCount": 1},
                "runtime": {
                    "autoApproval": {
                        "enabled": False,
                        "approvedCount": 0,
                        "status": "completed",
                    },
                    "registryFinalization": {
                        "status": "failed",
                        "error": "authority write failed",
                        "pendingCount": 1,
                        "pendingRows": [run],
                    },
                },
                "candidates": [],
            }
        ),
        encoding="utf-8",
    )
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []
    persist_reasons: list[str] = []
    events: list[str] = []
    state = {"active": [], "pending": [current], "rejected": []}

    def persist(
        next_state: dict[str, list[dict[str, Any]]], **kwargs: Any
    ) -> dict[str, list[dict[str, Any]]]:
        persist_reasons.append(str(kwargs.get("reason") or ""))
        persisted_states.append(json.loads(json.dumps(next_state)))
        return next_state

    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-07-01T10:05:00+00:00",
        bridge_log=lambda level, message, **fields: events.append(str(message)),
        load_state=lambda: json.loads(json.dumps(state)),
        persist_state_and_auto_sync=persist,
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted_states) == 1
    # A field-only carry updates rows that are already pending; naming it a
    # restage claimed a row had been added.
    assert persist_reasons == ["discovery_pending_update"]
    [row] = persisted_states[0]["pending"]
    assert row["jobsFound"] == 8
    assert row["lastProbedAt"] == "2026-07-01T10:00:00+00:00"
    assert row["lastProbeStatus"] == "ok"
    assert row["lastProbeError"] == ""
    assert row["name"] == "Studio B"
    assert row["careersUrl"] == "https://b.example/careers"
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    finalization = saved["runtime"]["registryFinalization"]
    assert finalization["pendingUpdatedCount"] == 1
    assert finalization["pendingUpdatedAt"] == "2026-07-01T10:05:00+00:00"
    assert "pendingRestagedCount" not in finalization
    assert "discovery_registry_pending_updated" in events


def test_failed_publish_restage_does_not_resurrect_a_tombstoned_source(
    tmp_path: Path,
) -> None:
    """A source deleted while the run was going must stay deleted.

    The run's pending rows predate the admin's delete, so restaging them would
    put the board back. Both the identity and a re-slugged row with the same URL
    fingerprint must be dropped.
    """
    report_path = tmp_path / "source-discovery-report.json"
    deleted = {
        "id": "deleted-during-run",
        "adapter": "static",
        "name": "Deleted During Run",
        "listing_url": "https://deleted.example/careers",
        "jobsFound": 2,
    }
    reimported = {
        **deleted,
        "id": "deleted-reimported",
        "name": "Deleted Reimported",
        "listing_url": "https://deleted.example/careers/",
        "jobsFound": 1,
    }
    live = {
        "id": "live-new",
        "adapter": "static",
        "name": "Live New",
        "listing_url": "https://live.example/careers",
        "jobsFound": 3,
    }
    report_path.write_text(
        json.dumps(
            {
                "runId": "discovery_deleted_during_run",
                "startedAt": "2026-03-20T12:00:00Z",
                "finishedAt": "2026-03-20T12:05:00Z",
                "summary": {"queuedCandidateCount": 3},
                "runtime": {
                    "autoApproval": {
                        "enabled": False,
                        "approvedCount": 0,
                        "status": "completed",
                    },
                    "registryFinalization": {
                        "status": "failed",
                        "error": "authority write failed",
                        "pendingCount": 3,
                        "pendingRows": [deleted, reimported, live],
                    },
                },
                "candidates": [],
            }
        ),
        encoding="utf-8",
    )
    tombstones_path = tmp_path / "source-registry-tombstones.json"
    tombstones_path.write_text(
        json.dumps(
            {
                "deleted-during-run": {
                    "sourceId": "deleted-during-run",
                    "sourceUrlFingerprint": source_url_fingerprint(deleted),
                    "deletedAt": "2026-03-20T12:02:00Z",
                    "deletedBy": "admin",
                    "reason": "registry_delete",
                    "bucket": "pending",
                }
            }
        ),
        encoding="utf-8",
    )
    persisted_states: list[dict[str, list[dict[str, Any]]]] = []

    def persist_state_and_auto_sync(
        next_state: dict[str, list[dict[str, Any]]], **_kwargs: Any
    ) -> dict[str, list[dict[str, Any]]]:
        persisted = {bucket: [dict(row) for row in rows] for bucket, rows in next_state.items()}
        persisted_states.append(persisted)
        return persisted

    service = _make_service(
        tmp_path,
        report_path=report_path,
        now_iso="2026-03-20T12:06:00Z",
        bridge_log=lambda *_args, **_kwargs: None,
        load_state=lambda: {"active": [], "pending": [], "rejected": []},
        persist_state_and_auto_sync=persist_state_and_auto_sync,
    )

    service.reconcile_terminal_discovery_report_from_state()

    assert len(persisted_states) == 1
    persisted = persisted_states[0]
    assert [row["id"] for row in persisted["pending"]] == ["live-new"]
    assert "deleted-during-run" not in {
        row["id"] for bucket in persisted.values() for row in bucket
    }
    assert "deleted-reimported" not in {
        row["id"] for bucket in persisted.values() for row in bucket
    }
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["runtime"]["registryFinalization"]["pendingRestagedCount"] == 1
    assert "deleted-during-run" in json.loads(tombstones_path.read_text(encoding="utf-8"))


def test_authority_apply_reports_why_a_run_row_was_dropped(tmp_path: Path) -> None:
    """The normal publish path fills the same audit the restage path reports."""
    try:
        _seed_sqlite_authority(tmp_path)
        audit: dict[str, int] = {}

        state, approved, authority = _apply_approval(
            tmp_path,
            pending_rows=[{**_A_ACTIVE, "jobsFound": 9}],
            auto_approve_enabled=False,
            merge_audit=audit,
        )

        assert authority == "sqlite"
        assert approved == 0
        assert [row["id"] for row in state["pending"]] == ["b"]
        assert audit == {"rowsDroppedPlaced": 1}
    finally:
        close_storage_stores()


def test_authority_apply_reports_fill_only_conflicts(tmp_path: Path) -> None:
    """The normal publish path exposes a run's renamed source to the caller."""
    try:
        _seed_sqlite_authority(tmp_path)
        audit: dict[str, int] = {}
        conflicts: list[dict[str, Any]] = []

        state, approved, authority = _apply_approval(
            tmp_path,
            pending_rows=[
                {
                    **_B_PENDING,
                    "name": "Migrated Studio B",
                    "lastProbedAt": _NOW,
                }
            ],
            auto_approve_enabled=False,
            merge_audit=audit,
            merge_conflicts=conflicts,
        )

        assert authority == "sqlite"
        assert approved == 0
        [pending] = state["pending"]
        assert pending["name"] == "Studio B"
        assert audit["fieldConflicts"] == 1
        assert conflicts == [
            {
                "identity": source_identity(_B_PENDING),
                "field": "name",
                "authorityValue": "Studio B",
                "runValue": "Migrated Studio B",
            }
        ]
    finally:
        close_storage_stores()
