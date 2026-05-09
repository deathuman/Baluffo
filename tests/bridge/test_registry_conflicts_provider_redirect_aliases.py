from __future__ import annotations

from src.bridge.registry_conflicts import derive_registry_conflict_queue


def test_registry_conflicts_demotes_provider_redirect_alias_and_weaker_static_rows() -> None:
    canonical = "teamtailor:listing_url:https://career.paradoxplaza.com/jobs"
    redirected = "teamtailor:listing_url:https://paradox-interactive.teamtailor.com/jobs"
    static_landing = "static:listing_url:https://www.paradoxinteractive.com/career"
    static_partial = "static:listing_url:https://career.paradoxplaza.com/#jobs"
    final_url = "https://career.paradoxplaza.com/jobs"
    state = {
        "active": [
            {
                "id": static_partial,
                "name": "Paradox Interactive (Sheet)",
                "studio": "Paradox Interactive",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 21,
                "liveJobsFound": 5,
                "liveProbeFinalUrl": "https://career.paradoxplaza.com/",
            },
            {
                "id": canonical,
                "name": "Paradox Interactive (Teamtailor)",
                "studio": "Paradox Interactive",
                "adapter": "teamtailor",
                "registryState": "active",
                "jobsFound": 20,
                "lastJobsKept": 20,
                "liveJobsFound": 25,
                "liveProbeFinalUrl": final_url,
            },
            {
                "id": redirected,
                "name": "Paradox Interactive (Teamtailor)",
                "studio": "Paradox Interactive",
                "adapter": "teamtailor",
                "registryState": "active",
                "jobsFound": 16,
                "lastJobsKept": 0,
                "liveJobsFound": 25,
                "liveProbeFinalUrl": final_url,
            },
            {
                "id": static_landing,
                "name": "Paradox Interactive (GameDevMap)",
                "studio": "Paradox Interactive",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "liveJobsFound": 0,
                "liveProbeFinalUrl": "https://www.paradoxinteractive.com/career",
            },
            {
                "id": "static:listing_url:https://www.paradoxinteractive.com/careers",
                "name": "Paradox Interactive (Manual Website)",
                "studio": "Paradox Interactive",
                "adapter": "static",
                "registryState": "rejected",
                "jobsFound": 0,
                "liveJobsFound": 0,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_provider_redirect_static_aliases"
    assert automation["targetIds"] == [redirected, static_partial, static_landing]
