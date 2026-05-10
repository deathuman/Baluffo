from __future__ import annotations

from src.bridge.registry_conflicts import derive_registry_conflict_queue


def test_registry_conflicts_suppresses_adjudicated_independent_provider_boards() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:slug:siei",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 55,
            },
            {
                "id": "greenhouse:slug:pdi",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 19,
            },
            {
                "id": "greenhouse:slug:naughtydog",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 15,
            },
        ],
        "pending": [
            {
                "id": "static:listing_url:https://www.playstation.com",
                "name": "Sony Computer Entertainment (GameDevMap)",
                "studio": "Sony Computer Entertainment",
                "adapter": "static",
                "registryState": "pending",
                "jobsFound": 1,
            }
        ],
        "rejected": [],
    }
    adjudication = {
        "finishedAt": "2026-05-10T10:00:00Z",
        "families": [
            {
                "familyKey": "sony computer entertainment",
                "status": "keep_both",
                "winnerSourceId": "greenhouse:slug:siei",
                "checkedSourceIds": [
                    "greenhouse:slug:siei",
                    "greenhouse:slug:pdi",
                    "greenhouse:slug:naughtydog",
                ],
                "decisions": [
                    {
                        "sourceId": "greenhouse:slug:pdi",
                        "status": "keep_both",
                        "reason": "both sources are live and job sets differ",
                    },
                    {
                        "sourceId": "greenhouse:slug:naughtydog",
                        "status": "keep_both",
                        "reason": "both sources are live and job sets differ",
                    },
                ],
            }
        ],
    }

    payload = derive_registry_conflict_queue(state, adjudication_payload=adjudication)

    assert payload["conflicts"] == []


def test_registry_conflicts_keeps_unadjudicated_multiple_provider_boards() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:slug:siei",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 55,
            },
            {
                "id": "greenhouse:slug:pdi",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 19,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["conflicts"][0]["reviewQueue"] == "p0_multi_active_provider"
