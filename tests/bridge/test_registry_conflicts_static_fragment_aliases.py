from __future__ import annotations

from src.bridge.registry_conflicts import derive_registry_conflict_queue


def test_registry_conflicts_demotes_bare_static_alias_when_jobs_fragment_exists() -> None:
    bare = "static:listing_url:https://athinkingape.com/careers/"
    jobs_fragment = "static:listing_url:https://www.athinkingape.com/careers/#positions"
    state = {
        "active": [
            {
                "id": bare,
                "name": "A Thinking Ape (GameDevMap)",
                "studio": "A Thinking Ape",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "lastJobsKept": 2,
            },
            {
                "id": jobs_fragment,
                "name": "A Thinking Ape (Sheet)",
                "studio": "A Thinking Ape",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 2,
                "lastJobsKept": 2,
            },
            {
                "id": "greenhouse:slug:athinkingape",
                "name": "A Thinking Ape (Greenhouse)",
                "studio": "A Thinking Ape",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 1,
                "lastJobsKept": 1,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_static_normalized_url_alias"
    assert automation["targetIds"] == [bare]


def test_registry_conflicts_demotes_stale_bare_static_alias_when_provider_matches_join_fragment() -> (
    None
):
    bare = "static:listing_url:https://www.bandainamcoent.com/careers"
    join_fragment = "static:listing_url:https://www.bandainamcoent.com/careers#join"
    state = {
        "active": [
            {
                "id": bare,
                "name": "Bandai Namco Entertainment America Inc. (Sheet)",
                "studio": "Bandai Namco Entertainment America Inc.",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 11,
            },
            {
                "id": "greenhouse:slug:bandainamco",
                "name": "Bandai Namco Entertainment America (Greenhouse)",
                "studio": "Bandai Namco Entertainment America Inc.",
                "adapter": "greenhouse",
                "registryState": "active",
                "lastJobsKept": 7,
                "health": "healthy",
            },
            {
                "id": join_fragment,
                "name": "Bandai Namco Entertainment America Inc. (Sheet)",
                "studio": "Bandai Namco Entertainment America Inc.",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 7,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_static_normalized_url_alias"
    assert automation["targetIds"] == [bare]


def test_registry_conflicts_promotes_pending_static_open_roles_fragment_over_active_bare_alias() -> (
    None
):
    bare = "static:listing_url:https://www.theorycraftgames.com/careers"
    open_roles = "static:listing_url:https://www.theorycraftgames.com/careers#open-roles"
    state = {
        "active": [
            {
                "id": bare,
                "name": "Theorycraft Games (Sheet)",
                "studio": "Theorycraft Games",
                "adapter": "static",
                "registryState": "active",
                "lastJobsKept": 1,
                "health": "healthy",
            },
        ],
        "pending": [
            {
                "id": open_roles,
                "name": "Theorycraft Games (Sheet)",
                "studio": "Theorycraft Games",
                "adapter": "static",
                "registryState": "pending",
                "lastJobsKept": 1,
                "health": "healthy",
            },
        ],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_promote_pending_static_jobs_fragment"
    assert automation["targetIds"] == [open_roles]


def test_registry_conflicts_rejects_pending_bare_static_alias_when_jobs_fragment_is_active() -> (
    None
):
    fragment = "static:listing_url:https://www.overwolf.com/careers/#position"
    bare = "static:listing_url:https://www.overwolf.com/careers"
    state = {
        "active": [
            {
                "id": fragment,
                "name": "Overwolf (Sheet)",
                "studio": "Overwolf",
                "adapter": "static",
                "registryState": "active",
                "lastJobsKept": 4,
                "health": "healthy",
            },
        ],
        "pending": [
            {
                "id": bare,
                "name": "Overwolf (Manual Website)",
                "studio": "Overwolf",
                "adapter": "static",
                "registryState": "pending",
            },
        ],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_reject_pending_static_bare_alias"
    assert automation["targetIds"] == [bare]
