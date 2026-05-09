from __future__ import annotations

from src.bridge.registry_conflicts import derive_registry_conflict_queue


def test_registry_conflicts_safe_automation_allows_matching_positive_provider_alias() -> None:
    state = {
        "active": [
            {
                "id": "recruitee:api_url:https://focushomeinteractive.recruitee.com/api/offers/",
                "name": "Focus Entertainment (Recruitee)",
                "studio": "Focus Entertainment",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 41,
                "score": 24,
            },
            {
                "id": "recruitee:api_url:https://focusentertainment.recruitee.com/api/offers/",
                "name": "Focus Entertainment (Recruitee)",
                "studio": "Focus Entertainment",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 33,
                "score": 25,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_same_adapter_provider_alias"
    assert automation["targetIds"] == [
        "recruitee:api_url:https://focusentertainment.recruitee.com/api/offers/"
    ]


def test_registry_conflicts_safe_automation_skips_mismatched_positive_loser_evidence() -> None:
    state = {
        "active": [
            {
                "id": "recruitee:api_url:https://studio-a.recruitee.com/api/offers/",
                "name": "Studio (Recruitee)",
                "studio": "Studio",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 5,
                "rankScore": 41,
                "score": 24,
            },
            {
                "id": "recruitee:api_url:https://studio-b.recruitee.com/api/offers/",
                "name": "Studio (Recruitee)",
                "studio": "Studio",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 33,
                "score": 25,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is False
    assert "loser_has_positive_evidence" in automation["blockedReasons"]


def test_registry_conflicts_safe_automation_demotes_weaker_provider_static() -> None:
    provider_static_state = {
        "active": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 1,
            },
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 0,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(provider_static_state)
    automation = payload["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_provider_static_weaker_source"
    assert automation["targetIds"] == ["static:listing_url:https://studio.example/careers"]
    assert payload["automation"]["summary"] == {"eligibleCount": 1, "demotableCount": 1}


def test_registry_conflicts_safe_automation_demotes_provider_static_when_jobs_equal() -> None:
    provider_static_state = {
        "active": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 4,
            },
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 4,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(provider_static_state)["conflicts"][0][
        "safeAutomation"
    ]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_provider_static_weaker_source"
    assert automation["targetIds"] == ["static:listing_url:https://studio.example/careers"]


def test_registry_conflicts_safe_automation_demotes_multiple_weaker_static_aliases() -> None:
    provider_static_state = {
        "active": [
            {
                "id": "recruitee:api_url:https://blooberteam.recruitee.com/api/offers/",
                "name": "Bloober Team (Recruitee)",
                "studio": "Bloober Team",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 6,
            },
            {
                "id": "static:listing_url:https://careers.blooberteam.com/jobs",
                "name": "Bloober Team (Manual Website)",
                "studio": "Bloober Team",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 6,
            },
            {
                "id": "static:listing_url:https://careers.blooberteam.com/careers",
                "name": "Bloober Team (Manual Website)",
                "studio": "Bloober Team",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 6,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(provider_static_state)["conflicts"][0][
        "safeAutomation"
    ]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_provider_static_weaker_source"
    assert automation["targetIds"] == [
        "static:listing_url:https://careers.blooberteam.com/jobs",
        "static:listing_url:https://careers.blooberteam.com/careers",
    ]


def test_registry_conflicts_safe_automation_lets_equal_provider_beat_higher_scored_static() -> None:
    provider_static_state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "score": 28,
            },
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 1,
                "score": 26,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    conflict = derive_registry_conflict_queue(provider_static_state)["conflicts"][0]
    automation = conflict["safeAutomation"]

    assert conflict["winner"]["id"] == "greenhouse:slug:studio"
    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_provider_static_weaker_source"
    assert automation["targetIds"] == ["static:listing_url:https://studio.example/careers"]


def test_registry_conflicts_safe_automation_keeps_higher_static_in_review() -> None:
    provider_static_state = {
        "active": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 4,
                "rankScore": 100,
                "score": 100,
            },
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 5,
                "rankScore": 0,
                "score": 0,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    conflict = derive_registry_conflict_queue(provider_static_state)["conflicts"][0]
    automation = conflict["safeAutomation"]

    assert conflict["winner"]["id"] == "static:listing_url:https://studio.example/careers"
    assert automation["eligible"] is False


def test_registry_conflicts_demotes_higher_weak_static_count_against_provider() -> None:
    provider_static_state = {
        "active": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 1,
            },
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 5,
                "lastReliableJobsFound": 1,
                "lastProbeWeakSignal": True,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    conflict = derive_registry_conflict_queue(provider_static_state)["conflicts"][0]
    automation = conflict["safeAutomation"]

    assert conflict["winner"]["id"] == "greenhouse:slug:studio"
    assert automation["eligible"] is True
    assert automation["targetIds"] == ["static:listing_url:https://studio.example/careers"]


def test_registry_conflicts_uses_complete_live_adjudication_counts_for_winner() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://azragames.com/careers/",
                "name": "Azra Games (GameDevMap)",
                "studio": "Azra Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 5,
                "rankScore": 52,
            },
            {
                "id": "greenhouse:slug:azragames",
                "name": "Azra Games (Greenhouse)",
                "studio": "Azra Games",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 48,
            },
        ],
        "pending": [],
        "rejected": [],
    }
    adjudication = {
        "families": [
            {
                "familyKey": "azra games",
                "status": "needs_review",
                "probes": [
                    {
                        "sourceId": "static:listing_url:https://azragames.com/careers/",
                        "httpStatus": 200,
                        "ok": True,
                        "jobsFound": 0,
                        "finalUrl": "https://azragames.com/careers/",
                    },
                    {
                        "sourceId": "greenhouse:slug:azragames",
                        "httpStatus": 200,
                        "ok": True,
                        "jobsFound": 1,
                        "finalUrl": "https://boards-api.greenhouse.io/v1/boards/azragames/jobs",
                    },
                ],
            }
        ]
    }

    conflict = derive_registry_conflict_queue(state, adjudication_payload=adjudication)[
        "conflicts"
    ][0]

    assert conflict["winner"]["id"] == "greenhouse:slug:azragames"
    assert conflict["effectiveWinnerSource"] == "live_adjudication"
    assert conflict["liveAdjudicationComplete"] is True
    loser = conflict["losers"][0]
    assert loser["registryJobsFound"] == 5
    assert loser["liveJobsFound"] == 0
    assert loser["jobsFound"] == 0


def test_registry_conflicts_safe_automation_promotes_pending_provider_with_more_jobs() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
            },
        ],
        "pending": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "pending",
                "jobsFound": 8,
            },
        ],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_promote_pending_provider_higher_jobs"
    assert automation["targetIds"] == ["greenhouse:slug:studio"]


def test_registry_conflicts_safe_automation_skips_pending_provider_when_active_jobs_missing() -> (
    None
):
    state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
            },
        ],
        "pending": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "pending",
                "jobsFound": 3,
            },
        ],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is False
    assert "active_missing_jobs_found" in automation["blockedReasons"]


def test_registry_conflicts_suppresses_pending_provider_when_active_has_more_jobs() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 4,
            },
        ],
        "pending": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "pending",
                "jobsFound": 3,
            },
        ],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["summary"]["conflictCount"] == 0
    audit = payload["automation"]["audit"]["safePendingProviderLowerJobs"]
    assert audit["summary"] == {"familyCount": 1, "rowCount": 1}
    assert audit["families"][0]["rows"][0]["id"] == "greenhouse:slug:studio"


def test_registry_conflicts_promotes_pending_provider_when_static_jobs_are_equal() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 0,
            },
        ],
        "pending": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "pending",
                "jobsFound": 0,
            },
        ],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_promote_pending_provider_higher_jobs"
    assert automation["targetIds"] == ["greenhouse:slug:studio"]


def test_registry_conflicts_promotes_pending_bamboohr_host_over_weak_static_count() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://www.beamdog.com/careers/",
                "name": "Beamdog (GameDevMap)",
                "studio": "Beamdog",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "lastProbeWeakSignal": True,
                "rankScore": 39,
                "score": 21,
            },
        ],
        "pending": [
            {
                "id": "static:listing_url:https://beamdog.bamboohr.com/careers",
                "name": "Beamdog (Sheet)",
                "studio": "Beamdog",
                "adapter": "static",
                "registryState": "pending",
                "jobsFound": 0,
                "rankScore": 0,
                "score": 23,
            },
        ],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_promote_pending_provider_higher_jobs"
    assert automation["targetIds"] == ["static:listing_url:https://beamdog.bamboohr.com/careers"]


def test_registry_conflicts_suppresses_pending_static_homepage_when_active_careers_wins() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://www.adictiz.com/en/careers/",
                "name": "Adictiz (GameDevMap)",
                "studio": "Adictiz",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 39,
                "score": 21,
            },
        ],
        "pending": [
            {
                "id": "static:listing_url:https://adictiz.com",
                "name": "Adictiz (GameDevMap)",
                "studio": "Adictiz",
                "adapter": "static",
                "registryState": "pending",
                "jobsFound": 1,
                "rankScore": 15,
                "score": 18,
                "weakSignal": True,
            },
        ],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["summary"]["conflictCount"] == 0
    audit = payload["automation"]["audit"]["safePendingStaticAlias"]
    assert audit["summary"] == {"familyCount": 1, "rowCount": 1}
    assert audit["families"][0]["rows"][0]["id"] == "static:listing_url:https://adictiz.com"
