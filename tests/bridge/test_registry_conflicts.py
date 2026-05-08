from __future__ import annotations

import json
from pathlib import Path

from src.bridge.registry_conflicts import derive_registry_conflict_queue
from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_registry_conflicts_route_joins_source_health_aliases(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    source_state_path.write_text(
        json.dumps(
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
            }
        ),
        encoding="utf-8",
    )
    api.load_state = lambda: {
        "active": [
            {
                "id": "winner-1",
                "name": "Winner Source",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "candidateState": "live",
                "status": "ok",
            }
        ],
        "pending": [
            {
                "id": "loser-1",
                "name": "Loser Source",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
                "candidateState": "validated",
                "status": "ok",
            }
        ],
        "rejected": [],
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/registry/conflicts", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["ok"] is True
    assert payload["summary"]["conflictCount"] == 1
    assert payload["conflicts"][0]["winner"]["health"] == "healthy"
    assert payload["conflicts"][0]["losers"][0]["actions"][0]["route"] == "/registry/approve"
    assert payload["registrySummary"]["activeCount"] == 1
    assert payload["triage"]["summary"]["totalConflictCount"] == 1
    assert payload["review"]["summary"]["totalConflictCount"] == 1
    assert "priorityCounts" in payload["review"]["summary"]


def _conflict_bucket(state: dict[str, list[dict]]) -> str:
    payload = derive_registry_conflict_queue(state)
    assert payload["summary"]["conflictCount"] == 1
    return str(payload["conflicts"][0]["triageBucket"])


def _conflict_review(state: dict[str, list[dict]]) -> dict:
    payload = derive_registry_conflict_queue(state)
    assert payload["summary"]["conflictCount"] == 1
    card = payload["conflicts"][0]
    return {
        "queue": str(card["reviewQueue"]),
        "priority": int(card["reviewPriority"]),
        "label": str(card["reviewLabel"]),
        "reason": str(card["reviewReason"]),
        "disposition": str(card["suggestedDisposition"]),
        "confidence": str(card["suggestedConfidence"]),
        "flags": list(card["evidenceFlags"]),
    }


def test_registry_conflicts_prefers_static_row_with_more_live_job_evidence() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio (Greenhouse)",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "candidateState": "live",
                "jobsFound": 1,
                "rankScore": 80,
            },
            {
                "id": "static:listing_url:https://studio.example/careers/jobs/",
                "name": "Studio (Website)",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "candidateState": "live",
                "listing_url": "https://studio.example/careers/jobs/",
                "jobsFound": 30,
                "rankScore": 65,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["summary"]["conflictCount"] == 1
    card = payload["conflicts"][0]
    assert card["winner"]["id"] == "static:listing_url:https://studio.example/careers/jobs/"
    assert card["winnerScore"]["lastKeptCount"] == 30


def test_registry_conflicts_triage_exact_duplicate_source_identity() -> None:
    state = {
        "active": [
            {
                "id": "static:studio",
                "name": "Studio Careers",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
            }
        ],
        "pending": [
            {
                "id": "static:studio",
                "name": "Studio Careers Copy",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
            }
        ],
        "rejected": [],
    }

    assert _conflict_bucket(state) == "exact_duplicate_auto_healable"


def test_registry_conflicts_triage_active_active_likely_duplicate() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
            },
            {
                "id": "static:studio",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
            },
        ],
        "pending": [],
        "rejected": [],
    }

    assert _conflict_bucket(state) == "active_active_likely_duplicate"


def test_registry_conflicts_triage_pending_duplicate_of_active() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
            }
        ],
        "pending": [
            {
                "id": "static:studio",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
            }
        ],
        "rejected": [],
    }

    assert _conflict_bucket(state) == "pending_duplicate_of_active"


def test_registry_conflicts_triage_rejected_historical_noise() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
            }
        ],
        "pending": [],
        "rejected": [
            {
                "id": "static:studio",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "rejected",
            }
        ],
    }

    assert _conflict_bucket(state) == "rejected_historical_noise"


def test_registry_conflicts_triage_pending_only_is_manual_review() -> None:
    state = {
        "active": [],
        "pending": [
            {
                "id": "static:studio-a",
                "name": "Studio Careers A",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
            },
            {
                "id": "static:studio-b",
                "name": "Studio Careers B",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
            },
        ],
        "rejected": [],
    }

    assert _conflict_bucket(state) == "ambiguous_manual_review"


def test_registry_conflicts_review_active_provider_static_queue() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
            },
            {
                "id": "static:studio",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
            },
        ],
        "pending": [],
        "rejected": [],
    }

    review = _conflict_review(state)

    assert review["queue"] == "p1_active_provider_static"
    assert review["priority"] == 1
    assert review["disposition"] == "Review provider/static replacement"


def test_registry_conflicts_review_active_static_variants_queue() -> None:
    state = {
        "active": [
            {
                "id": "static:studio-a",
                "name": "Studio Careers A",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
            },
            {
                "id": "static:studio-b",
                "name": "Studio Careers B",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
            },
        ],
        "pending": [],
        "rejected": [],
    }

    review = _conflict_review(state)

    assert review["queue"] == "p2_static_url_variant_active"
    assert review["priority"] == 2
    assert "active_static_rows:2" in review["flags"]


def test_registry_conflicts_review_multi_active_provider_queue() -> None:
    state = {
        "active": [
            {
                "id": "ashby:board_url:https://jobs.ashbyhq.com/studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "ashby",
                "registryState": "active",
            },
            {
                "id": "greenhouse:studio",
                "name": "Studio Provider Other",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
            },
        ],
        "pending": [],
        "rejected": [],
    }

    review = _conflict_review(state)

    assert review["queue"] == "p0_multi_active_provider"
    assert review["priority"] == 0
    assert review["confidence"] == "high"


def test_registry_conflicts_review_same_adapter_active_variant_queue() -> None:
    state = {
        "active": [
            {
                "id": "manual:studio-a",
                "name": "Studio Careers A",
                "studio": "Studio",
                "adapter": "manual",
                "registryState": "active",
            },
            {
                "id": "manual:studio-b",
                "name": "Studio Careers B",
                "studio": "Studio",
                "adapter": "manual",
                "registryState": "active",
            },
        ],
        "pending": [],
        "rejected": [],
    }

    review = _conflict_review(state)

    assert review["queue"] == "p2_same_adapter_active_variant"
    assert review["priority"] == 2
    assert "same_active_adapter:manual" in review["flags"]


def test_registry_conflicts_review_pending_provider_against_active_queue() -> None:
    state = {
        "active": [
            {
                "id": "static:studio",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
            }
        ],
        "pending": [
            {
                "id": "lever:account:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "lever",
                "registryState": "pending",
            }
        ],
        "rejected": [],
    }

    review = _conflict_review(state)

    assert review["queue"] == "p1_pending_provider_against_active"
    assert review["priority"] == 1
    assert "pending_provider_rows:1" in review["flags"]


def test_registry_conflicts_review_pending_only_intake_queue() -> None:
    state = {
        "active": [],
        "pending": [
            {
                "id": "static:studio-a",
                "name": "Studio Careers A",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
            },
            {
                "id": "static:studio-b",
                "name": "Studio Careers B",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "pending",
            },
        ],
        "rejected": [],
    }

    review = _conflict_review(state)

    assert review["queue"] == "p3_pending_only_intake"
    assert review["priority"] == 3
    assert review["disposition"] == "Pending-only intake"


def test_registry_conflicts_safe_automation_marks_same_provider_alias_eligible() -> None:
    state = {
        "active": [
            {
                "id": "recruitee:api_url:https://crazygames.recruitee.com/api/offers/",
                "name": "CrazyGames (Recruitee)",
                "studio": "CrazyGames",
                "adapter": "recruitee",
                "registryState": "active",
                "jobsFound": 6,
                "rankScore": 51,
                "score": 29,
            },
            {
                "id": "recruitee:api_url:https://jobs.crazygames.com/api/offers/",
                "name": "CrazyGames (Recruitee)",
                "studio": "CrazyGames",
                "adapter": "recruitee",
                "registryState": "active",
                "rankScore": 0,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)
    automation = payload["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_same_adapter_provider_alias"
    assert automation["targetIds"] == ["recruitee:api_url:https://jobs.crazygames.com/api/offers/"]
    assert payload["automation"]["summary"] == {"eligibleCount": 1, "demotableCount": 1}


def test_registry_conflicts_safe_automation_skips_positive_loser_evidence() -> None:
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

    assert automation["eligible"] is False
    assert "loser_has_positive_evidence" in automation["blockedReasons"]


def test_registry_conflicts_safe_automation_skips_provider_static() -> None:
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
            },
        ],
        "pending": [],
        "rejected": [],
    }

    assert (
        derive_registry_conflict_queue(provider_static_state)["conflicts"][0]["safeAutomation"][
            "eligible"
        ]
        is False
    )


def test_registry_conflicts_safe_automation_marks_static_normalized_url_alias_eligible() -> None:
    static_state = {
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
            },
            {
                "id": "static:listing_url:https://www.studio.example/careers",
                "name": "Studio Static B",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 20,
                "score": 10,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(static_state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_static_normalized_url_alias"
    assert automation["targetIds"] == ["static:listing_url:https://www.studio.example/careers"]


def test_registry_conflicts_safe_automation_skips_static_normalized_url_alias_ties() -> None:
    static_state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static A",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 20,
                "score": 10,
            },
            {
                "id": "static:listing_url:https://www.studio.example/careers",
                "name": "Studio Static B",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 20,
                "score": 10,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(static_state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is False
    assert "loser_has_equal_or_stronger_evidence" in automation["blockedReasons"]


def test_registry_conflicts_safe_automation_skips_multi_row_cards() -> None:
    state = {
        "active": [
            {
                "id": "teamtailor:listing_url:https://paradox-interactive.teamtailor.com/jobs",
                "name": "Paradox Provider",
                "studio": "Paradox",
                "adapter": "teamtailor",
                "registryState": "active",
                "jobsFound": 16,
            },
            {
                "id": "teamtailor:listing_url:https://career.paradoxplaza.com/jobs",
                "name": "Paradox Provider Alias",
                "studio": "Paradox",
                "adapter": "teamtailor",
                "registryState": "active",
                "jobsFound": 20,
            },
            {
                "id": "static:listing_url:https://career.paradoxplaza.com/#jobs",
                "name": "Paradox Static",
                "studio": "Paradox",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 21,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is False
    assert "requires_exactly_two_rows" in automation["blockedReasons"]


def test_registry_conflicts_triage_summary_keeps_ordered_buckets() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:active-active",
                "name": "Active Provider",
                "studio": "Active Studio",
                "adapter": "greenhouse",
                "registryState": "active",
            },
            {
                "id": "static:active-active",
                "name": "Active Static",
                "studio": "Active Studio",
                "adapter": "static",
                "registryState": "active",
            },
            {
                "id": "greenhouse:active-pending",
                "name": "Pending Provider",
                "studio": "Pending Studio",
                "adapter": "greenhouse",
                "registryState": "active",
            },
        ],
        "pending": [
            {
                "id": "static:active-pending",
                "name": "Pending Static",
                "studio": "Pending Studio",
                "adapter": "static",
                "registryState": "pending",
            }
        ],
        "rejected": [],
    }

    payload = derive_registry_conflict_queue(state)

    assert payload["triage"]["summary"]["bucketCounts"] == {
        "exact_duplicate_auto_healable": 0,
        "active_active_likely_duplicate": 1,
        "pending_duplicate_of_active": 1,
        "rejected_historical_noise": 0,
        "ambiguous_manual_review": 0,
    }
    assert [row["bucket"] for row in payload["triage"]["buckets"]] == [
        "exact_duplicate_auto_healable",
        "active_active_likely_duplicate",
        "pending_duplicate_of_active",
        "rejected_historical_noise",
        "ambiguous_manual_review",
    ]
    assert [row["queue"] for row in payload["review"]["queues"]] == [
        "p0_multi_active_provider",
        "p1_active_provider_static",
        "p1_pending_provider_against_active",
        "p2_same_adapter_active_variant",
        "p2_static_url_variant_active",
        "p2_pending_static_variant",
        "p3_pending_only_intake",
        "p3_low_signal_manual",
    ]
    assert payload["conflicts"][0]["reviewPriority"] <= payload["conflicts"][1]["reviewPriority"]
