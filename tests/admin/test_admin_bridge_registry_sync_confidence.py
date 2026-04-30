from __future__ import annotations

from src import admin_bridge
from src.bridge.registry_sync_summary import derive_registry_sync_summary


def test_derive_registry_sync_summary_counts_local_registry_and_sync_history() -> None:
    summary = derive_registry_sync_summary(
        state={
            "active": [{"id": "active-1"}],
            "pending": [
                {"id": "hidden", "hiddenFromDefault": True},
                {"id": "deferred", "deferReason": "domain_cap"},
                {
                    "id": "duplicate",
                    "pendingReason": "duplicate_family_weaker_variant",
                    "duplicateOfSourceId": "active-1",
                },
                "not-a-row",
            ],
            "rejected": [{"id": "rejected-1"}],
        },
        tombstones={"deleted-1": {"sourceId": "deleted-1"}},
        sync_status={
            "runtime": {
                "lastPullAt": "2026-04-30T10:00:00+00:00",
                "lastResult": "ok",
                "lastAction": "pull",
            }
        },
        history=[
            {
                "type": "sync",
                "status": "ok",
                "finishedAt": "2026-04-30T10:00:00+00:00",
                "summary": {
                    "action": "pull",
                    "activeCount": 3,
                    "pendingCount": 2,
                    "changed": True,
                },
            }
        ],
    )

    assert summary["activeCount"] == 1
    assert summary["pendingCount"] == 3
    assert summary["rejectedCount"] == 1
    assert summary["tombstoneCount"] == 1
    assert summary["hiddenPendingCount"] == 1
    assert summary["deferredPendingCount"] == 1
    assert summary["duplicatePendingCount"] == 1
    assert summary["ignoredRejectedCount"] == 1
    assert summary["ignoredTombstonedCount"] == 1
    assert summary["localOnlyCount"] == 2
    assert summary["remoteActiveCount"] == 3
    assert summary["remotePendingCount"] == 2
    assert summary["pulledCount"] == 1
    assert summary["pushedCount"] == 0
    assert summary["invalidRowsCount"] == 1
    assert summary["lastSyncStatus"] == "ok"


def test_derive_registry_sync_summary_reports_missing_sync_as_never() -> None:
    summary = derive_registry_sync_summary(state={}, tombstones={}, sync_status={}, history=[])

    assert summary["lastSyncAt"] == ""
    assert summary["lastSyncStatus"] == "never"
    assert summary["remoteOnlyCount"] == 0
    assert summary["conflictCount"] == 0


def test_ops_health_exposes_registry_sync_confidence(admin_bridge_entrypoint_root):
    admin_bridge.save_json_atomic(
        admin_bridge.ACTIVE_PATH,
        [{"id": "active-1", "adapter": "static", "listing_url": "https://a.example/jobs"}],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.PENDING_PATH,
        [
            {
                "id": "pending-hidden",
                "adapter": "static",
                "listing_url": "https://p.example/jobs",
                "hiddenFromDefault": True,
            }
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.REJECTED_PATH,
        [{"id": "rejected-1", "adapter": "static", "listing_url": "https://r.example/jobs"}],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.TOMBSTONES_PATH,
        {"deleted-1": {"sourceId": "deleted-1", "reason": "registry_delete"}},
    )
    admin_bridge.append_run_history(
        {
            "id": "sync-1",
            "runId": "sync-1",
            "type": "sync",
            "status": "ok",
            "startedAt": "2026-04-30T10:00:00+00:00",
            "finishedAt": "2026-04-30T10:01:00+00:00",
            "durationMs": 60000,
            "summary": {"action": "push", "activeCount": 1, "pendingCount": 1},
        }
    )

    health = admin_bridge.compute_ops_health()
    registry_sync = (health.get("kpis") or {}).get("registrySync") or {}

    assert registry_sync["activeCount"] == 1
    assert registry_sync["pendingCount"] == 1
    assert registry_sync["rejectedCount"] == 1
    assert registry_sync["tombstoneCount"] == 1
    assert registry_sync["hiddenPendingCount"] == 1
    assert registry_sync["ignoredRejectedCount"] == 1
    assert registry_sync["ignoredTombstonedCount"] == 1
    assert registry_sync["pushedCount"] == 1
