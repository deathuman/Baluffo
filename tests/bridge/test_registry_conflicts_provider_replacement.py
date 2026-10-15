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


def test_registry_conflicts_demotes_static_when_fresh_source_state_contradicts_stale_count() -> (
    None
):
    state = {
        "active": [
            {
                "id": "greenhouse:slug:mojangab",
                "name": "Mojang Studios (Greenhouse)",
                "studio": "Mojang Studios",
                "adapter": "greenhouse",
                "registryState": "active",
                "lastStatus": "ok",
                "lastJobsFound": 1,
                "lastKeptCount": 1,
                "jobsFound": 1,
            },
            {
                "id": "static:listing_url:https://www.minecraft.net/mojang-careers",
                "name": "Mojang Studios (Xbox Game Studios) (GameDevMap)",
                "studio": "Mojang Studios (Xbox Game Studios)",
                "adapter": "static",
                "registryState": "active",
                "lastStatus": "ok",
                "health": "warning",
                "healthReason": "latest fetch kept no jobs",
                "lastJobsFound": 0,
                "lastKeptCount": 0,
                "jobsFound": 25,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    conflict = derive_registry_conflict_queue(state)["conflicts"][0]
    automation = conflict["safeAutomation"]

    assert conflict["winner"]["id"] == "greenhouse:slug:mojangab"
    assert conflict["winnerScore"]["lastKeptCount"] == 1
    assert (
        conflict["losers"][0]["id"] == "static:listing_url:https://www.minecraft.net/mojang-careers"
    )
    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_provider_static_weaker_source"
    assert automation["targetIds"] == [
        "static:listing_url:https://www.minecraft.net/mojang-careers"
    ]


def test_registry_conflicts_does_not_treat_failed_latest_static_fetch_as_zero() -> None:
    state = {
        "active": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "lastStatus": "ok",
                "lastJobsFound": 1,
            },
            {
                "id": "static:listing_url:https://studio.example/careers",
                "name": "Studio Static",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "lastStatus": "error",
                "health": "broken",
                "lastJobsFound": 0,
                "jobsFound": 25,
            },
        ],
        "pending": [],
        "rejected": [],
    }

    conflict = derive_registry_conflict_queue(state)["conflicts"][0]
    automation = conflict["safeAutomation"]

    assert conflict["winner"]["id"] == "static:listing_url:https://studio.example/careers"
    assert automation["eligible"] is False


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
        "finishedAt": "2026-05-09T10:00:00Z",
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
        ],
    }

    conflict = derive_registry_conflict_queue(state, adjudication_payload=adjudication)[
        "conflicts"
    ][0]

    assert conflict["winner"]["id"] == "greenhouse:slug:azragames"
    assert conflict["effectiveWinnerSource"] == "live_adjudication"
    assert conflict["liveAdjudicationComplete"] is True
    assert conflict["winner"]["health"] == "healthy"
    assert conflict["winner"]["healthReason"] == "live adjudication found jobs"
    assert conflict["winner"]["lastSuccessfulFetchAt"] == "2026-05-09T10:00:00Z"
    loser = conflict["losers"][0]
    assert loser["registryJobsFound"] == 5
    assert loser["liveJobsFound"] == 0
    assert loser["jobsFound"] == 0
    assert loser["health"] == "warning"
    assert loser["healthReason"] == "live adjudication found no jobs"


def test_registry_conflicts_uses_live_jazzhr_count_over_stale_static_count() -> None:
    state = {
        "active": [
            {
                "id": (
                    "static:listing_url:https://nextlevelgames.com/"
                    "jobs-at-next-level-games-subsidiary-of-nintendo-co-ltd/"
                ),
                "name": "Next Level Games (Sheet)",
                "studio": "Next Level Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 10,
                "rankScore": 48,
            },
            {
                "id": "jazzhr:board_url:https://nextlevelgames.applytojob.com/apply",
                "name": "Next Level Games (JazzHR)",
                "studio": "Next Level Games",
                "adapter": "jazzhr",
                "registryState": "active",
                "lastKeptCount": 5,
            },
        ],
        "pending": [],
        "rejected": [],
    }
    adjudication = {
        "finishedAt": "2026-05-09T10:00:00Z",
        "families": [
            {
                "familyKey": "next level games",
                "status": "recommended_demotion",
                "probes": [
                    {
                        "sourceId": (
                            "static:listing_url:https://nextlevelgames.com/"
                            "jobs-at-next-level-games-subsidiary-of-nintendo-co-ltd/"
                        ),
                        "httpStatus": 200,
                        "ok": True,
                        "jobsFound": 0,
                        "finalUrl": (
                            "https://nextlevelgames.com/"
                            "jobs-at-next-level-games-subsidiary-of-nintendo-co-ltd/"
                        ),
                    },
                    {
                        "sourceId": "jazzhr:board_url:https://nextlevelgames.applytojob.com/apply",
                        "httpStatus": 200,
                        "ok": True,
                        "jobsFound": 5,
                        "finalUrl": "https://nextlevelgames.applytojob.com/apply",
                    },
                ],
            }
        ],
    }

    conflict = derive_registry_conflict_queue(state, adjudication_payload=adjudication)[
        "conflicts"
    ][0]

    assert (
        conflict["winner"]["id"] == "jazzhr:board_url:https://nextlevelgames.applytojob.com/apply"
    )
    assert conflict["effectiveWinnerSource"] == "live_adjudication"
    assert conflict["winner"]["jobsFound"] == 5
    static_row = conflict["losers"][0]
    assert static_row["registryJobsFound"] == 10
    assert static_row["liveJobsFound"] == 0
    assert static_row["jobsFound"] == 0


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
                "lastStatus": "ok",
                "lastKeptCount": 8,
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
                "jobsFound": 2,
            },
        ],
        "pending": [
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio Provider",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "pending",
                "lastStatus": "ok",
                "lastKeptCount": 2,
            },
        ],
        "rejected": [],
    }

    automation = derive_registry_conflict_queue(state)["conflicts"][0]["safeAutomation"]

    assert automation["eligible"] is True
    assert automation["action"] == "auto_promote_pending_provider_higher_jobs"
    assert automation["targetIds"] == ["greenhouse:slug:studio"]


def test_registry_conflicts_blocks_pending_bamboohr_host_without_delivery_evidence() -> None:
    """F-2: a zero-delivery provider must not retire the active static fallback.

    The pending row is a static row whose host resolves to bamboohr (a provider
    identity). It was previously promoted over the weak active static count, but
    it has no delivery evidence (no fetch kept any job), which is exactly the
    09-07-style decision the delivery-evidence guard forbids. The card is no
    longer eligible for the provider-replacement promotion.
    """
    from src.bridge.registry_conflicts_automation_static import (
        _analyze_pending_provider_replacement_automation,
    )

    rows = [
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
    ]

    result = _analyze_pending_provider_replacement_automation(
        family_key="beamdog",
        rows=rows,
    )

    assert result["eligible"] is False
    assert "provider_missing_delivery_evidence" in result["blockedReasons"]


def test_registry_conflicts_promotes_pending_bamboohr_host_with_delivery_evidence() -> None:
    from src.bridge.registry_conflicts_automation_static import (
        _analyze_pending_provider_replacement_automation,
    )

    rows = [
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
        {
            "id": "static:listing_url:https://beamdog.bamboohr.com/careers",
            "name": "Beamdog (Sheet)",
            "studio": "Beamdog",
            "adapter": "static",
            "registryState": "pending",
            "jobsFound": 0,
            "lastStatus": "ok",
            "lastKeptCount": 1,
            "rankScore": 0,
            "score": 23,
        },
    ]

    result = _analyze_pending_provider_replacement_automation(
        family_key="beamdog",
        rows=rows,
    )

    assert result["eligible"] is True
    assert result["action"] == "auto_promote_pending_provider_higher_jobs"


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


def _pending_provider_rows(*, provider_extra: dict[str, object]) -> list[dict[str, object]]:
    """Active static fallback + pending provider replacement, analyzer-shaped."""
    provider: dict[str, object] = {
        "id": "greenhouse:slug:studio",
        "name": "Studio Provider",
        "studio": "Studio",
        "adapter": "greenhouse",
        "registryState": "pending",
    }
    provider.update(provider_extra)
    return [
        {
            "id": "static:listing_url:https://studio.example/careers",
            "name": "Studio Static",
            "studio": "Studio",
            "adapter": "static",
            "registryState": "active",
            "jobsFound": 2,
        },
        provider,
    ]


def test_pending_provider_blocked_when_only_probe_count_present() -> None:
    """F-2: a probe `jobsFound` count is not delivery evidence.

    The provider has a higher probe count than the active static row, which the
    old logic promoted on. With no fetched-and-kept run it must be blocked so the
    static fallback survives.
    """
    from src.bridge.registry_conflicts_automation_static import (
        _analyze_pending_provider_replacement_automation,
    )

    result = _analyze_pending_provider_replacement_automation(
        family_key="studio",
        rows=_pending_provider_rows(provider_extra={"jobsFound": 9}),
    )

    assert result["eligible"] is False
    assert "provider_missing_delivery_evidence" in result["blockedReasons"]


def test_pending_provider_blocked_when_excluded_status() -> None:
    """F-2: `excluded` means "not fetched this run" — no evidence, not disproof."""
    from src.bridge.registry_conflicts_automation_static import (
        _analyze_pending_provider_replacement_automation,
    )

    result = _analyze_pending_provider_replacement_automation(
        family_key="studio",
        rows=_pending_provider_rows(
            provider_extra={"lastStatus": "excluded", "lastKeptCount": 9},
        ),
    )

    assert result["eligible"] is False
    assert "provider_missing_delivery_evidence" in result["blockedReasons"]


def test_pending_provider_blocked_when_zero_kept() -> None:
    """F-2: a completed run that kept zero jobs is not delivery evidence."""
    from src.bridge.registry_conflicts_automation_static import (
        _analyze_pending_provider_replacement_automation,
    )

    result = _analyze_pending_provider_replacement_automation(
        family_key="studio",
        rows=_pending_provider_rows(provider_extra={"lastStatus": "ok", "lastKeptCount": 0}),
    )

    assert result["eligible"] is False
    assert "provider_missing_delivery_evidence" in result["blockedReasons"]


def test_pending_provider_allowed_with_kept_delivery() -> None:
    """F-2: a real fetch (ok + kept > 0) keeps the promotion eligible."""
    from src.bridge.registry_conflicts_automation_static import (
        _analyze_pending_provider_replacement_automation,
    )

    result = _analyze_pending_provider_replacement_automation(
        family_key="studio",
        rows=_pending_provider_rows(provider_extra={"lastStatus": "ok", "lastKeptCount": 3}),
    )

    assert result["eligible"] is True
    assert result["action"] == "auto_promote_pending_provider_higher_jobs"
    assert "provider_missing_delivery_evidence" not in result["blockedReasons"]


def test_has_delivery_evidence_requires_ok_status_and_positive_kept_count() -> None:
    from src.bridge.registry_conflicts_row import _has_delivery_evidence

    assert _has_delivery_evidence({"lastStatus": "ok", "lastKeptCount": 4}) is True
    assert _has_delivery_evidence({"lastStatus": "success", "lastKeptCount": 1}) is True
    # Probe count alone is not delivery evidence.
    assert _has_delivery_evidence({"jobsFound": 12}) is False
    # excluded / error statuses are no evidence.
    assert _has_delivery_evidence({"lastStatus": "excluded", "lastKeptCount": 4}) is False
    assert _has_delivery_evidence({"lastStatus": "error", "lastKeptCount": 4}) is False
    # ok but zero kept is no evidence.
    assert _has_delivery_evidence({"lastStatus": "ok", "lastKeptCount": 0}) is False
    # Legacy alias spelling still converges to the canonical counter.
    assert _has_delivery_evidence({"lastStatus": "ok", "lastJobsKept": 2}) is True
