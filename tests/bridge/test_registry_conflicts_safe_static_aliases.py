from __future__ import annotations

from src.bridge.registry_conflicts import derive_registry_conflict_queue


def test_registry_conflicts_safe_automation_marks_static_listing_variant_eligible() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://www.rockstargames.com/careers/openings",
                "name": "Rockstar Games",
                "studio": "Rockstar Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 13,
                "rankScore": 60,
                "score": 27,
            },
            {
                "id": "static:listing_url:https://www.rockstargames.com/careers",
                "name": "Rockstar Games",
                "studio": "Rockstar Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 10,
                "score": 7,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_static_same_host_listing_variant"
    assert automation["targetIds"] == ["static:listing_url:https://www.rockstargames.com/careers"]


def test_registry_conflicts_prefers_career_source_over_stale_homepage_alias() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://careers.10chambers.com/jobs",
                "name": "10 Chambers (Sheet)",
                "studio": "10 Chambers",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 0,
                "rankScore": 51,
                "score": 36,
            },
            {
                "id": "static:listing_url:https://10chambers.com",
                "name": "10 Chambers (GameDevMap)",
                "studio": "10 Chambers",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 35,
                "score": 22,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    card = derive_registry_conflict_queue(state)["conflicts"][0]

    assert card["winner"]["id"] == "static:listing_url:https://careers.10chambers.com/jobs"
    automation = card["safeAutomation"]
    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_static_same_host_listing_variant"
    assert automation["targetIds"] == ["static:listing_url:https://10chambers.com"]


def test_registry_conflicts_prefers_provider_career_source_over_stale_homepage_alias() -> None:
    state = {
        "active": [
            {
                "id": "teamtailor:listing_url:https://careers.10chambers.com/jobs",
                "name": "10 Chambers (Sheet)",
                "studio": "10 Chambers",
                "adapter": "teamtailor",
                "registryState": "active",
                "jobsFound": 0,
                "rankScore": 51,
                "score": 36,
            },
            {
                "id": "static:listing_url:https://10chambers.com",
                "name": "10 Chambers (GameDevMap)",
                "studio": "10 Chambers",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 35,
                "score": 22,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    card = derive_registry_conflict_queue(state)["conflicts"][0]

    assert card["winner"]["id"] == "teamtailor:listing_url:https://careers.10chambers.com/jobs"
    automation = card["safeAutomation"]
    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_provider_static_weaker_source"
    assert automation["targetIds"] == ["static:listing_url:https://10chambers.com"]


def test_registry_conflicts_safe_automation_skips_broad_same_host_listing_variant() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://www.ycombinator.com/jobs",
                "name": "Gym Class VR",
                "studio": "Gym Class VR",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 80,
                "rankScore": 100,
                "score": 30,
            },
            {
                "id": (
                    "static:listing_url:"
                    "https://www.ycombinator.com/companies/gym-class-by-irl-studios/jobs"
                ),
                "name": "Gym Class VR",
                "studio": "Gym Class VR",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 12,
                "rankScore": 40,
                "score": 20,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is False
    assert automation["targetIds"] == []


def test_registry_conflicts_suppresses_safe_auto_demoted_pending_aliases() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static A",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 40,
                "score": 20,
            }
        ],
        "pending": [
            {
                "id": "static:listing_url:https://www.studio.example/careers",
                "name": "Studio Static B",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "pending",
                "pendingReason": "registry_conflict_safe_auto_demote",
                "stateChangedBy": "registry_conflict_safe_auto_demote",
            }
        ],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["summary"]["conflictCount"] == 0
    audit = payload["automation"]["audit"]["safeAutoDemotedPending"]
    assert audit["summary"] == {"familyCount": 1, "rowCount": 1}
    assert audit["families"][0]["familyKey"] == "static studio"
    assert audit["families"][0]["rows"][0]["id"] == (
        "static:listing_url:https://www.studio.example/careers"
    )


def test_registry_conflicts_suppresses_adjudication_auto_demoted_pending_aliases() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static A",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 40,
                "score": 20,
            }
        ],
        "pending": [
            {
                "id": "static:listing_url:https://www.studio.example/careers",
                "name": "Studio Static B",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "pending",
                "pendingReason": "registry_conflict_adjudication_auto_demote",
                "stateChangedBy": "registry_conflict_adjudication_auto_demote",
            }
        ],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["summary"]["conflictCount"] == 0
    audit = payload["automation"]["audit"]["safeAutoDemotedPending"]
    assert audit["summary"] == {"familyCount": 1, "rowCount": 1}
    assert audit["families"][0]["familyKey"] == "static studio"
    assert audit["families"][0]["rows"][0]["pendingReason"] == (
        "registry_conflict_adjudication_auto_demote"
    )


def test_registry_conflicts_keeps_unresolved_rows_when_suppressing_safe_pending_aliases() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static A",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 40,
                "score": 20,
            },
            {
                "id": "static:listing_url:https://studio.example/jobs",
                "name": "Studio Static C",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 30,
                "score": 18,
            },
        ],
        "pending": [
            {
                "id": "static:listing_url:https://www.studio.example/careers",
                "name": "Studio Static B",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "pending",
                "pendingReason": "registry_conflict_safe_auto_demote",
                "stateChangedBy": "registry_conflict_safe_auto_demote",
            }
        ],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["summary"]["conflictCount"] == 1
    assert payload["conflicts"][0]["rowCount"] == 2
    assert {row["id"] for row in payload["conflicts"][0]["rows"]} == {
        "static:listing_url:https://studio.example/careers",
        "static:listing_url:https://studio.example/jobs",
    }
    assert payload["automation"]["audit"]["safeAutoDemotedPending"]["summary"] == {
        "familyCount": 1,
        "rowCount": 1,
    }


def test_registry_conflicts_suppresses_safe_pending_static_weaker_aliases() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://gameestudio.com/careers/",
                "name": "Gamee Studio",
                "studio": "Gamee Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 45,
                "score": 23,
            }
        ],
        "pending": [
            {
                "id": "static:listing_url:https://gameestudio.com/hiring/",
                "name": "Gamee Studio",
                "studio": "Gamee Studio",
                "adapter": "static",
                "registryState": "pending",
                "jobsFound": 1,
                "rankScore": 20,
                "score": 10,
            }
        ],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["summary"]["conflictCount"] == 0
    audit = payload["automation"]["audit"]["safePendingStaticAlias"]
    assert audit["summary"] == {"familyCount": 1, "rowCount": 1}
    assert audit["families"][0]["familyKey"] == "gamee studio"
    assert audit["families"][0]["rows"][0]["id"] == (
        "static:listing_url:https://gameestudio.com/hiring/"
    )


def test_registry_conflicts_safe_automation_marks_generated_static_listing_variants_eligible() -> (
    None
):
    state = {
        "active": [
            {
                "id": "static:listing_url:https://dragondropper.com/work-with-us/",
                "name": "Dragon Dropper (GameDevMap)",
                "studio": "Dragon Dropper",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 4,
                "rankScore": 60,
                "score": 30,
            },
            {
                "id": "static:listing_url:https://dragondropper.com/join-us/",
                "name": "Dragon Dropper (GameDevMap)",
                "studio": "Dragon Dropper",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 4,
                "rankScore": 50,
                "score": 20,
            },
            {
                "id": "static:listing_url:https://dragondropper.com/jobs/",
                "name": "Dragon Dropper (GameDevMap)",
                "studio": "Dragon Dropper",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 50,
                "score": 20,
            },
            {
                "id": "static:listing_url:https://dragondropper.com/careers/",
                "name": "Dragon Dropper (GameDevMap)",
                "studio": "Dragon Dropper",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 2,
                "rankScore": 50,
                "score": 20,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    automation = payload["conflicts"][0]["safeAutomation"]
    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_static_generated_listing_variants"
    assert automation["targetIds"] == [
        "static:listing_url:https://dragondropper.com/join-us/",
        "static:listing_url:https://dragondropper.com/jobs/",
        "static:listing_url:https://dragondropper.com/careers/",
    ]
    assert payload["automation"]["summary"]["eligibleCount"] == 1
    assert payload["automation"]["summary"]["demotableCount"] == 3


def test_registry_conflicts_safe_automation_skips_generated_static_listing_cross_domain_boards() -> (
    None
):
    state = {
        "active": [
            {
                "id": "static:listing_url:https://www.capcom.co.jp/recruit/",
                "name": "Capcom",
                "studio": "Capcom",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 5,
                "rankScore": 60,
                "score": 30,
            },
            {
                "id": "static:listing_url:https://jobs.jobvite.com/capcomusa",
                "name": "Capcom",
                "studio": "Capcom",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 4,
                "rankScore": 50,
                "score": 20,
            },
            {
                "id": "static:listing_url:https://www.capcom-games.com/careers/",
                "name": "Capcom",
                "studio": "Capcom",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 50,
                "score": 20,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["conflicts"][0]["safeAutomation"]["eligible"] is False
    assert payload["automation"]["summary"]["eligibleCount"] == 0
    assert payload["automation"]["summary"]["demotableCount"] == 0
