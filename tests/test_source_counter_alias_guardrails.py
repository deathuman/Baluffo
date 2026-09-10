"""Counter-collapse guardrails (docs/plans/source-health-counter-collapse-plan.md).

The alias map in the policy leaf owns every dual-written counter name. These
tests fail if an alias spelling re-enters the persistence layer unowned, or if
a wire emitter silently stops carrying the alias surface before Phase 5.
"""

from __future__ import annotations

import re
from pathlib import Path

from src.shared.source_counter_aliases import (
    CANONICAL_COUNTERS,
    COUNTER_ALIASES,
    emit_with_aliases,
    fill_canonical_counters,
    heal_counter_aliases,
    read_counter,
)


def test_alias_map_covers_exactly_the_three_dual_written_pairs() -> None:
    """Phase 6: every alias maps to a known canonical counter, and the
    canonical tuple covers every mapped target — a fourth alias (or a
    canonical counter without alias coverage) fails here unowned."""

    mapped_targets = set(COUNTER_ALIASES.values())
    assert mapped_targets == set(CANONICAL_COUNTERS)
    assert set(CANONICAL_COUNTERS) == {
        "lastKeptCount",
        "consecutiveZeroKept",
        "consecutiveFailures",
    }
    assert set(COUNTER_ALIASES) == {
        "lastJobsKept",
        "zeroJobStreak",
        "zeroKeptStreak",
        "failureCount",
    }


def test_derive_module_no_longer_writes_alias_keys_into_state() -> None:
    """Phase 4 single-writer invariant: the persistence derive returns only
    canonical counter keys — the alias spellings must not re-enter the
    returned payload (grep-guard against a regression reintroducing them)."""

    module = Path("src/jobs/state_source_records.py").read_text(encoding="utf-8")
    assert "heal_counter_aliases" not in module, (
        "persistence funnel must use fill_canonical_counters, not the alias-fill display heal"
    )
    derive_body = module.split("def derive_source_health_fields", 1)[1].split("\ndef ", 1)[0]
    for alias in ("lastJobsKept", "zeroJobStreak", "failureCount"):
        assert f'"{alias}"' not in derive_body, alias


def test_persistence_heal_fills_canonical_only_and_never_writes_aliases() -> None:
    """Persistence direction (the funnel used by the state normalizer/save):
    alias-only legacy rows gain canonical counters, and — the Phase-4
    single-writer invariant the live acceptance audit caught being violated —
    the persistence heal must NEVER write alias keys back, otherwise every
    normalize+save round-trip re-adds the aliases the derive stopped
    emitting."""

    legacy = {"lastJobsKept": 7, "zeroJobStreak": 2, "failureCount": 1}
    healed = fill_canonical_counters(dict(legacy))
    assert healed["lastKeptCount"] == 7
    assert healed["consecutiveZeroKept"] == 2
    assert healed["consecutiveFailures"] == 1
    for alias in ("lastJobsKept", "zeroJobStreak", "failureCount"):
        assert alias not in healed, alias

    # canonical-present rows are untouched (no alias re-add, no value change)
    fresh = {"lastKeptCount": 36, "consecutiveZeroKept": 0, "consecutiveFailures": 0}
    healed = fill_canonical_counters(dict(fresh))
    assert healed == fresh


def test_display_heal_is_alias_fill_and_canonical_wins() -> None:
    """Display/wire direction (bridge joined rows, Phase-5 surface): aliases
    are part of the visible contract, canonical wins over a stale alias, and
    an alias-only row gains the canonical name. This direction is what the
    persistence funnel must NOT use."""

    legacy = {"lastJobsKept": 7, "zeroJobStreak": 2, "failureCount": 1}
    healed = heal_counter_aliases(dict(legacy))
    assert healed["lastKeptCount"] == 7
    assert healed["consecutiveZeroKept"] == 2
    assert healed["consecutiveFailures"] == 1
    assert healed["lastJobsKept"] == 7

    stale = {"lastKeptCount": 36, "lastJobsKept": 0}
    healed = heal_counter_aliases(dict(stale))
    assert healed["lastJobsKept"] == 36


def test_wire_emit_with_aliases_still_carries_the_alias_surface() -> None:
    """Until Phase 5 lands (operator-approved contract change), wire emitters
    must keep filling alias keys from canonical — this guards the bridge emit
    surface against silent early removal."""

    emitted = emit_with_aliases(
        {"lastKeptCount": 5, "consecutiveZeroKept": 1, "consecutiveFailures": 0}
    )
    assert emitted["lastJobsKept"] == 5
    assert emitted["zeroJobStreak"] == 1
    assert emitted["failureCount"] == 0

    # emit never fabricates: absent canonical counters stay absent
    untouched = emit_with_aliases({"status": "ok"})
    assert "lastJobsKept" not in untouched


def test_read_counter_canonical_first_and_absent_aware() -> None:
    row = {"lastKeptCount": 3, "lastJobsKept": 99}
    assert read_counter(row, "lastKeptCount") == 3
    alias_only = {"lastJobsKept": 99}
    assert read_counter(alias_only, "lastKeptCount") == 99
    assert read_counter({}, "lastKeptCount") is None


def test_repo_reader_sites_omit_hand_rolled_alias_fallbacks() -> None:
    """Phase 3 convergence: the in-repo readers the plan listed must go through
    the leaf instead of hand-rolled alias-first fallback chains."""

    pattern = re.compile(
        r"(?:state|row|entry|parent|detail)\.get\(\"(?:lastJobsKept|zeroJobStreak|failureCount)\"\)"
    )
    scoped = [
        Path("src/source_registry_policy.py"),
        Path("src/jobs/pipeline_loader_selection.py"),
        Path("src/jobs/state_incremental.py"),
        Path("src/bridge/registry_conflicts_row_core.py"),
        Path("src/bridge/registry_conflicts_automation_provider.py"),
    ]
    for path in scoped:
        body = path.read_text(encoding="utf-8")
        hits = [
            line
            for line in body.splitlines()
            if pattern.search(line) and "audit" not in line.lower()
        ]
        assert not hits, (path, hits)
