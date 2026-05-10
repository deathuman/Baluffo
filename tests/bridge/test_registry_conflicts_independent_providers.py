from __future__ import annotations

from src.bridge.registry_conflicts import derive_registry_conflict_queue


def _sony_greenhouse_state() -> dict[str, list[dict]]:
    return {
        "active": [
            {
                "id": "greenhouse:slug:siei",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "slug": "siei",
                "registryState": "active",
                "jobsFound": 55,
            },
            {
                "id": "greenhouse:slug:pdi",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "slug": "pdi",
                "registryState": "active",
                "jobsFound": 19,
            },
            {
                "id": "greenhouse:slug:naughtydog",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "slug": "naughtydog",
                "registryState": "active",
                "jobsFound": 15,
            },
            {
                "id": "greenhouse:slug:haven",
                "name": "Sony Computer Entertainment (Greenhouse)",
                "studio": "Sony Computer Entertainment",
                "adapter": "greenhouse",
                "slug": "haven",
                "registryState": "active",
                "jobsFound": 3,
            },
        ],
        "pending": [],
        "rejected": [],
    }


def _greenhouse_job_row(slug: str, job_id: str) -> dict:
    return {
        "sourceBundle": [
            {
                "source": f"greenhouse:{slug}",
                "sourceJobId": f"greenhouse:{slug}:{job_id}",
                "jobLink": f"https://job-boards.greenhouse.io/{slug}/jobs/{job_id}",
                "adapter": "greenhouse",
            }
        ]
    }


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
    audit = payload["suppressedIndependentProviderBoards"]
    assert audit["summary"] == {"familyCount": 1, "rowCount": 2}
    assert audit["families"][0]["familyKey"] == "sony computer entertainment"
    assert audit["families"][0]["adapter"] == "greenhouse"
    assert audit["families"][0]["sourceIds"] == [
        "greenhouse:slug:pdi",
        "greenhouse:slug:naughtydog",
    ]
    assert audit["families"][0]["evidenceReason"] == ("live_adjudication_keep_both_job_sets_differ")


def test_registry_conflicts_suppresses_current_fetch_independent_greenhouse_boards() -> None:
    payload = derive_registry_conflict_queue(
        _sony_greenhouse_state(),
        job_rows=[
            _greenhouse_job_row("siei", "100001"),
            _greenhouse_job_row("siei", "100002"),
            _greenhouse_job_row("pdi", "200001"),
            _greenhouse_job_row("naughtydog", "300001"),
            _greenhouse_job_row("haven", "400001"),
        ],
    )

    assert payload["conflicts"] == []
    audit = payload["suppressedIndependentProviderBoards"]
    assert audit["summary"] == {"familyCount": 1, "rowCount": 4}
    assert audit["families"][0]["sourceIds"] == [
        "greenhouse:slug:siei",
        "greenhouse:slug:pdi",
        "greenhouse:slug:naughtydog",
        "greenhouse:slug:haven",
    ]
    assert audit["families"][0]["evidenceReason"] == (
        "current_fetch_job_identity_overlap_below_threshold"
    )


def test_registry_conflicts_keeps_overlapping_greenhouse_boards_for_review() -> None:
    payload = derive_registry_conflict_queue(
        _sony_greenhouse_state(),
        job_rows=[
            _greenhouse_job_row("siei", "100001"),
            _greenhouse_job_row("pdi", "100001"),
            _greenhouse_job_row("naughtydog", "300001"),
            _greenhouse_job_row("haven", "400001"),
        ],
    )

    assert payload["summary"]["conflictCount"] == 1
    assert payload["conflicts"][0]["reviewQueue"] == "p0_multi_active_provider"
    assert payload["suppressedIndependentProviderBoards"]["summary"] == {
        "familyCount": 0,
        "rowCount": 0,
    }


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
