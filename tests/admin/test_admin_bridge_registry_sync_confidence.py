from __future__ import annotations

from pathlib import Path

from src import admin_bridge
from src.bridge.registry_conflicts import load_registry_conflicts_payload
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

    health = admin_bridge.compute_ops_dashboard_health()
    registry_sync = (health.get("kpis") or {}).get("registrySync") or {}

    assert registry_sync["activeCount"] == 1
    assert registry_sync["pendingCount"] == 1
    assert registry_sync["rejectedCount"] == 1
    assert registry_sync["tombstoneCount"] == 1
    assert registry_sync["summaryExact"] is False
    assert registry_sync["countBasis"] == "storage"
    assert registry_sync["hiddenPendingCount"] == 0
    assert registry_sync["ignoredRejectedCount"] == 1
    assert registry_sync["ignoredTombstonedCount"] == 1
    assert registry_sync["pushedCount"] == 1


def test_registry_conflicts_payload_joins_source_health_aliases(admin_bridge_entrypoint_root):
    source_state_path = Path(admin_bridge.JOBS_FETCH_REPORT_PATH).with_name(
        "jobs-source-state.json"
    )
    admin_bridge.save_json_atomic(
        admin_bridge.ACTIVE_PATH,
        [
            {
                "id": "winner-1",
                "name": "Winner Source",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "candidateState": "live",
                "rankScore": 25,
                "score": 25,
                "status": "ok",
            }
        ],
    )
    admin_bridge.save_json_atomic(
        admin_bridge.PENDING_PATH,
        [
            {
                "id": "loser-1",
                "name": "Loser Source",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
                "candidateState": "validated",
                "rankScore": 1,
                "score": 1,
                "status": "ok",
            }
        ],
    )
    admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
    admin_bridge.save_json_atomic(
        source_state_path,
        {
            "schemaVersion": 1,
            "sources": {
                "Winner Source": {
                    "health": "healthy",
                    "healthReason": "steady",
                    "lastSuccessfulFetchAt": "2026-05-01T10:00:00Z",
                    "lastSeenInFetchAt": "2026-05-01T10:00:00Z",
                    "lastJobsKept": 9,
                    "lastKeptCount": 9,
                    "failureCount": 0,
                    "zeroJobStreak": 0,
                },
                "Loser Source": {
                    "health": "warning",
                    "healthReason": "stale",
                    "lastSuccessfulFetchAt": "2026-04-30T10:00:00Z",
                    "lastSeenInFetchAt": "2026-05-01T09:00:00Z",
                    "lastJobsKept": 1,
                    "lastKeptCount": 1,
                    "failureCount": 2,
                    "zeroJobStreak": 3,
                },
            },
        },
    )

    payload = load_registry_conflicts_payload(
        load_state=admin_bridge.load_state,
        load_json_object=admin_bridge.load_json_object,
        source_state_path=source_state_path,
    )

    assert payload["summary"]["conflictCount"] == 1
    card = payload["conflicts"][0]
    assert card["winner"]["name"] == "Winner Source"
    assert card["winner"]["health"] == "healthy"
    assert card["winner"]["healthReason"] == "steady"
    assert card["winner"]["lastSuccessfulFetchAt"] == "2026-05-01T10:00:00Z"
    assert card["winner"]["actions"][0]["route"] == "/registry/demote-active"
    assert card["losers"][0]["actions"][0]["route"] == "/registry/approve"
    assert card["losers"][0]["actions"][1]["route"] == "/registry/reject"
    assert card["diffs"]


def test_registry_conflicts_payload_enriches_provider_rows_from_fetch_report_details(
    admin_bridge_entrypoint_root,
):
    source_state_path = Path(admin_bridge.JOBS_FETCH_REPORT_PATH).with_name(
        "jobs-source-state.json"
    )
    admin_bridge.save_json_atomic(
        admin_bridge.ACTIVE_PATH,
        [
            {
                "id": "smartrecruiters:company_id:epochgames",
                "name": "Epoch Games (SmartRecruiters)",
                "studio": "Epoch Games",
                "adapter": "smartrecruiters",
                "registryState": "active",
                "jobsFound": 10,
            },
            {
                "id": "static:listing_url:https://careers.smartrecruiters.com/epochgames",
                "name": "Epoch Games (Sheet)",
                "studio": "Epoch Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 13,
            },
        ],
    )
    admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [])
    admin_bridge.save_json_atomic(admin_bridge.REJECTED_PATH, [])
    admin_bridge.save_json_atomic(
        source_state_path,
        {
            "schemaVersion": 1,
            "sources": {
                "Epoch Games (SmartRecruiters)": {
                    "health": "unknown",
                    "lastRunAt": "2026-05-08T10:00:00Z",
                    "lastSuccessfulFetchAt": "2026-04-10T10:00:00Z",
                    "lastJobsKept": 0,
                }
            },
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        {
            "sources": [
                {
                    "adapter": "smartrecruiters",
                    "status": "ok",
                    "lastRunAt": "2026-05-09T10:00:00Z",
                    "details": [
                        {
                            "adapter": "smartrecruiters",
                            "status": "ok",
                            "name": "Epoch Games (SmartRecruiters)",
                            "studio": "Epoch Games",
                            "providerUrl": (
                                "https://api.smartrecruiters.com/v1/companies/EpochGames/postings"
                            ),
                            "fetchedCount": 10,
                            "keptCount": 10,
                        }
                    ],
                }
            ]
        },
    )

    payload = load_registry_conflicts_payload(
        load_state=admin_bridge.load_state,
        load_json_object=admin_bridge.load_json_object,
        source_state_path=source_state_path,
    )

    provider = next(
        row
        for row in payload["conflicts"][0]["rows"]
        if row["id"] == "smartrecruiters:company_id:epochgames"
    )
    assert provider["health"] == "healthy"
    assert provider["healthReason"] == "last fetch kept jobs"
    assert provider["lastSuccessfulFetchAt"] == "2026-05-09T10:00:00Z"
    assert provider["lastJobsKept"] == 10
