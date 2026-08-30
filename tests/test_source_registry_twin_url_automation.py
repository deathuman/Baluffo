"""Runtime twin detection: conflict automations gated on canonicalize_careers_url.

The commit-time guardrail (WP20) fails when two active seed rows share a
canonicalized careers URL. These tests cover the runtime side: conflict cards
and safe automations must apply the *same* canonicalize_careers_url rule so a
www/apex/scheme/slash twin introduced by live discovery (like the Scopely
join-us pair WP19 reconciled in the seeds) is auto-demoted to pending rather
than double-emitting jobs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.bridge.registry_conflicts import (
    apply_registry_conflict_safe_demotions,
    derive_registry_conflict_queue,
)
from src.source_registry import (
    canonicalize_careers_url,
    demote_duplicate_active_variants,
    source_careers_url_key,
)
from src.source_registry_policy import duplicate_family_conflict_cards

ROOT = Path(__file__).resolve().parents[1]

_URL_TWIN_PREFIX = "url-twin:"


def _row(
    row_id: str,
    *,
    studio: str,
    url: str,
    adapter: str = "static",
    state: str = "active",
    jobs_found: int = 19,
    evidence_score: int = 40,
) -> dict:
    return {
        "id": row_id,
        "name": f"{studio} (GameDevMap)",
        "studio": studio,
        "adapter": adapter,
        "registryState": state,
        "candidateState": "live" if state == "active" else "validated",
        "listing_url": url,
        "jobsFound": jobs_found,
        "evidenceScore": evidence_score,
    }


_SCOPELY_APEX = _row(
    "static:listing_url:https://scopely.com/en/join-us",
    studio="Genjoy (Scopely)",
    url="https://scopely.com/en/join-us",
)
_SCOPELY_WWW = _row(
    "static:listing_url:https://www.scopely.com/en/join-us",
    studio="Omnidrone (Scopely)",
    url="https://www.scopely.com/en/join-us",
)


# ── the shared rule ----------------------------------------------------------


def test_canonicalize_rule_matches_guardrail_contract() -> None:
    assert canonicalize_careers_url("https://www.scopely.com/en/join-us/") == (
        "scopely.com/en/join-us"
    )
    assert canonicalize_careers_url("http://scopely.com/en/join-us/#openings") == (
        "scopely.com/en/join-us"
    )
    assert canonicalize_careers_url("https://WWW.IOI.DK/Careers/") == "ioi.dk/careers"
    assert canonicalize_careers_url("https://a.com/jobs?q=one") != canonicalize_careers_url(
        "https://a.com/jobs?q=two"
    )
    assert canonicalize_careers_url("") == ""


def test_source_careers_url_key_prefers_board_then_listing_then_url() -> None:
    assert (
        source_careers_url_key(
            {"board_url": "https://www.x.com/board", "listing_url": "https://x.com/list"}
        )
        == "x.com/board"
    )
    assert source_careers_url_key({"listing_url": "https://x.com/list"}) == "x.com/list"
    assert source_careers_url_key({"url": "https://x.com/jobs/"}) == "x.com/jobs"
    assert source_careers_url_key({}) == ""


# ── policy cards --------------------------------------------------------------


def test_cross_family_url_twin_raises_url_twin_card(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    cards = duplicate_family_conflict_cards([_SCOPELY_APEX, _SCOPELY_WWW])
    url_cards = [card for card in cards if card["familyKey"].startswith(_URL_TWIN_PREFIX)]
    assert len(url_cards) == 1
    card = url_cards[0]
    assert card["familyKey"] == f"{_URL_TWIN_PREFIX}scopely.com/en/join-us"
    assert card["winner"]["id"] == _SCOPELY_APEX["id"]
    assert [row["id"] for row in card["losers"]] == [_SCOPELY_WWW["id"]]


def test_baselined_url_never_raises_url_twin_card(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.source_registry_policy.known_twin_career_urls",
        lambda: {"scopely.com/en/join-us"},
    )
    cards = duplicate_family_conflict_cards([_SCOPELY_APEX, _SCOPELY_WWW])
    assert not any(card["familyKey"].startswith(_URL_TWIN_PREFIX) for card in cards)


def test_missing_baseline_disables_url_twin_automation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: None)
    cards = duplicate_family_conflict_cards([_SCOPELY_APEX, _SCOPELY_WWW])
    assert not any(card["familyKey"].startswith(_URL_TWIN_PREFIX) for card in cards)


def test_same_family_rows_only_produce_family_card(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    apex = _row(
        "static:listing_url:https://a4vr.com/jobs",
        studio="A4VR",
        url="https://a4vr.com/jobs",
    )
    www = _row(
        "static:listing_url:https://www.a4vr.com/jobs/",
        studio="A4VR",
        url="https://www.a4vr.com/jobs/",
    )
    cards = duplicate_family_conflict_cards([apex, www])
    assert [card["familyKey"] for card in cards] == ["a4vr"]
    assert not any(card["familyKey"].startswith(_URL_TWIN_PREFIX) for card in cards)


def test_pending_row_with_same_url_is_not_a_runtime_twin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    pending_twin = _row(
        "static:listing_url:https://www.scopely.com/en/join-us",
        studio="Omnidrone (Scopely)",
        url="https://www.scopely.com/en/join-us",
        state="pending",
    )
    cards = duplicate_family_conflict_cards([_SCOPELY_APEX, pending_twin])
    assert not any(card["familyKey"].startswith(_URL_TWIN_PREFIX) for card in cards)


def test_query_distinct_pages_are_not_twins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    rows = [
        _row("a", studio="Studio A", url="https://jobs.careers.microsoft.com/search?q=games"),
        _row("b", studio="Studio B", url="https://jobs.careers.microsoft.com/search?q=xbox"),
    ]
    cards = duplicate_family_conflict_cards(rows)
    assert not any(card["familyKey"].startswith(_URL_TWIN_PREFIX) for card in cards)


def test_no_studio_rows_twin_still_raises_url_twin_card(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    apex = dict(_SCOPELY_APEX, studio="")
    www = dict(_SCOPELY_WWW, studio="")
    cards = duplicate_family_conflict_cards([apex, www])
    assert any(card["familyKey"].startswith(_URL_TWIN_PREFIX) for card in cards)


# ── noise-cleanup demotion ---------------------------------------------------


def test_demote_duplicate_active_variants_keeps_canonical_winner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    next_active, demoted = demote_duplicate_active_variants([_SCOPELY_APEX, _SCOPELY_WWW])
    assert [row["id"] for row in next_active] == [_SCOPELY_APEX["id"]]
    assert [row["id"] for row in demoted] == [_SCOPELY_WWW["id"]]
    assert demoted[0]["pendingReason"] == "duplicate_family_weaker_variant"
    assert demoted[0]["duplicateOfSourceId"] == _SCOPELY_APEX["id"]


# ── safe automation + live auto-heal ------------------------------------------


def _state() -> dict:
    return {"active": [_SCOPELY_APEX, _SCOPELY_WWW], "pending": [], "rejected": []}


def test_safe_automation_marks_url_twin_eligible(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    payload = derive_registry_conflict_queue(_state())
    url_cards = [
        card for card in payload["conflicts"] if card["familyKey"].startswith(_URL_TWIN_PREFIX)
    ]
    assert len(url_cards) == 1
    automation = url_cards[0]["safeAutomation"]
    assert automation["eligible"] is True
    assert automation["action"] == "auto_demote_static_normalized_url_alias"
    assert automation["targetIds"] == [_SCOPELY_WWW["id"]]


def test_load_time_auto_heal_demotes_discovered_twin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    result = apply_registry_conflict_safe_demotions(_state())
    assert result["demoted"] == 1
    assert [row["id"] for row in result["state"]["active"]] == [_SCOPELY_APEX["id"]]
    pending = result["state"]["pending"]
    assert [row["id"] for row in pending] == [_SCOPELY_WWW["id"]]
    assert pending[0]["pendingReason"] == "registry_conflict_safe_auto_demote"


def test_url_twin_analyzer_requires_active_static_rows() -> None:
    from src.bridge.registry_conflicts_automation_static import _analyze_url_twin_automation

    provider_twin = _row(
        "greenhouse:slug:scopely",
        studio="Omnidrone (Scopely)",
        url="https://www.scopely.com/en/join-us",
        adapter="greenhouse",
    )
    family_key = f"{_URL_TWIN_PREFIX}scopely.com/en/join-us"
    result = _analyze_url_twin_automation(
        family_key=family_key,
        winner=provider_twin,
        losers=[_SCOPELY_APEX],
        rows=[provider_twin, _SCOPELY_APEX],
    )
    assert result["eligible"] is False
    assert "requires_active_static_rows" in result["blockedReasons"]

    result = _analyze_url_twin_automation(
        family_key=family_key,
        winner=_SCOPELY_APEX,
        losers=[_SCOPELY_WWW],
        rows=[_SCOPELY_APEX, _SCOPELY_WWW],
    )
    assert result["eligible"] is True
    assert result["targetIds"] == [_SCOPELY_WWW["id"]]


def test_url_twin_analyzer_ignores_non_url_twin_families() -> None:
    from src.bridge.registry_conflicts_automation_static import _analyze_url_twin_automation

    result = _analyze_url_twin_automation(
        family_key="scopely",
        winner=_SCOPELY_APEX,
        losers=[_SCOPELY_WWW],
        rows=[_SCOPELY_APEX, _SCOPELY_WWW],
    )
    assert result["eligible"] is False
    assert "requires_url_twin_family" in result["blockedReasons"]


# ── seed invariant -------------------------------------------------------------


def test_committed_seed_raises_no_url_twin_cards(monkeypatch: pytest.MonkeyPatch) -> None:
    """The baseline allowlist covers every current seed collision, so the
    runtime side must not raise any url-twin card from the tracked active seed.

    The baseline is read from the repo directly (like the guardrail test) so
    this invariant does not depend on the ambient DATA_DIR, which earlier
    suites legitimately rebind (test_source_registry_paths_honor_baluffo_data_dir_override).
    """
    from tools.repo_health.source_registry_duplicate_url_policy import _load_known_collisions

    known = _load_known_collisions(ROOT)
    assert len(known) > 0
    monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: known)
    rows = json.loads(
        (ROOT / "data/defaults/source-registry-active.seed.json").read_text(encoding="utf-8")
    )
    cards = duplicate_family_conflict_cards(rows)
    assert not any(card["familyKey"].startswith(_URL_TWIN_PREFIX) for card in cards)
