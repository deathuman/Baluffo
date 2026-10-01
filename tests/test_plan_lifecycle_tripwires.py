"""Guardrails for the plan-lifecycle tripwires in tools/repo_health/release_docs_policy.py.

A plan is a temporary ledger: refine before execution, track to completion, then
delete it and let the durable lessons land in the regular docs. These tests pin the
detection rules and, more importantly, the *exempt* case -- a check that flags
compliant plans is worse than no check, because it trains people to skip it.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.repo_health.release_docs_policy import (  # noqa: E402
    PLAN_LIFECYCLE_EXEMPT,
    PLAN_LINE_TRIPWIRE,
    PLAN_TERMINAL_STATUS,
    PLAN_WORD_TRIPWIRE,
    _plan_lifecycle_findings,
)


def _plan(tmp_path: Path, name: str, body: str) -> Path:
    plans = tmp_path / "docs" / "plans"
    plans.mkdir(parents=True, exist_ok=True)
    path = plans / name
    path.write_text(body, encoding="utf-8")
    return path


def test_line_tripwire_flags_oversized_plan(tmp_path: Path) -> None:
    """Sized against a fixed reference, not the constant, so raising the tripwire
    cannot make the fixture grow with it and hide the break."""
    body = "> - **Status:** Active\n\n" + ("x\n" * 650)
    assert 652 > PLAN_LINE_TRIPWIRE, "fixture must start above the tripwire"
    _plan(tmp_path, "big.md", body)
    findings = _plan_lifecycle_findings(tmp_path / "docs" / "plans")
    assert any("big.md" in f and "652 lines" in f for f in findings)


def test_line_tripwire_leaves_a_600_line_plan_alone(tmp_path: Path) -> None:
    """A tripwire is a prompt to review, not a hard cap: sitting at the line is fine."""
    body = "> - **Status:** Active\n\n" + ("x\n" * 590)
    _plan(tmp_path, "atcap.md", body)
    assert not any("atcap.md" in f for f in _plan_lifecycle_findings(tmp_path / "docs" / "plans"))


def test_word_tripwire_catches_single_giant_line(tmp_path: Path) -> None:
    """The real shape that hid: 21,816 words across 65 lines, 97% on line one.

    Fixed size, so raising the tripwire turns this red instead of silently
    rescaling the fixture.
    """
    body = "> - **Status:** Active\n\n" + ("word " * 5200)
    assert len(body.split()) > PLAN_WORD_TRIPWIRE, "fixture must start above the tripwire"
    _plan(tmp_path, "wide.md", body)
    findings = _plan_lifecycle_findings(tmp_path / "docs" / "plans")
    assert any("wide.md" in f and f"{len(body.split())} words" in f for f in findings)
    # The point of the word rule: 3 lines, still over budget.
    assert len(body.splitlines()) < PLAN_LINE_TRIPWIRE


def test_word_tripwire_leaves_a_short_plan_alone(tmp_path: Path) -> None:
    body = "> - **Status:** Active\n\n" + ("word " * 4000)
    _plan(tmp_path, "narrow.md", body)
    assert not any("narrow.md" in f for f in _plan_lifecycle_findings(tmp_path / "docs" / "plans"))


@pytest.mark.parametrize(
    "status",
    [
        "Folded (2026-09-04) -- remainder moved elsewhere",
        "Closed -- no active follow-up",
        "Superseded by the current plan",
        "Fully executed - Phases 1-4",
        "**All four systemic fixes S1-S4 landed**",
        "Code work is complete and everything in this plan has landed",
    ],
)
def test_terminal_status_in_plans_is_flagged(tmp_path: Path, status: str) -> None:
    _plan(tmp_path, "done.md", f"> - **Status:** {status}\n\nShort body.\n")
    findings = _plan_lifecycle_findings(tmp_path / "docs" / "plans")
    assert any("done.md" in f and "terminal" in f for f in findings)


@pytest.mark.parametrize(
    "status",
    [
        "Active",
        "Active follow-up plan (draft for review)",
        "Planned -- every item is scoped against measured evidence",
        "Active -- standing rules, plus the current baseline",
        "In progress",
    ],
)
def test_live_status_is_not_flagged(tmp_path: Path, status: str) -> None:
    _plan(tmp_path, "live.md", f"> - **Status:** {status}\n\nShort body.\n")
    findings = _plan_lifecycle_findings(tmp_path / "docs" / "plans")
    assert not any("live.md" in f for f in findings)


def test_parked_with_a_trigger_is_kept(tmp_path: Path) -> None:
    """Deferral is legitimate when the status names what unblocks it.

    Real case: optional-playwright reads "Parked -- deferred until the next desktop
    portable release". That trigger is real and checkable, so the plan must survive.
    Condemning it would be a false positive in the enforcing phase.
    """
    _plan(
        tmp_path,
        "pw.md",
        "> - **Status:** Parked -- deferred until the next desktop portable release\n\nBody.\n",
    )
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_implemented_with_pending_acceptance_criteria_is_kept(tmp_path: Path) -> None:
    """Real case: task-abort-control reads 'Implemented baseline, refinement-ready'."""
    _plan(tmp_path, "abort.md", "> - **Status:** Implemented baseline, refinement-ready\n\nBody.\n")
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


@pytest.mark.parametrize(
    "status",
    ["Parked; deferred experiment", "Parked", "Deferred", "Implemented"],
)
def test_deferral_without_a_trigger_is_flagged_as_unscoped(tmp_path: Path, status: str) -> None:
    """ "Parked" with no stated reason to wait is indistinguishable from abandoned.

    Requiring the trigger is what stops "Parked" from becoming the new "Complete".
    """
    _plan(tmp_path, "vague.md", f"> - **Status:** {status}\n\nBody.\n")
    findings = _plan_lifecycle_findings(tmp_path / "docs" / "plans")
    assert any("vague.md" in f and "without naming what unblocks it" in f for f in findings)


@pytest.mark.parametrize(
    "status",
    [
        "Parked -- revisit before the next desktop runtime effort",
        "Parked until the pooled fallback proves insufficient",
        "Deferred until a fresh fetch returns",
        "Parked; revisit together with the release plan",
    ],
)
def test_various_phraseings_of_a_real_trigger_are_accepted(tmp_path: Path, status: str) -> None:
    _plan(tmp_path, "triggered.md", f"> - **Status:** {status}\n\nBody.\n")
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_parked_wins_over_a_later_terminal_word(tmp_path: Path) -> None:
    """A deferred status is judged on its trigger, not on a word further along.

    "Parked until the next release -- implemented behind a flag" is still deferred,
    because the opening clause is what states the plan's lifecycle.
    """
    _plan(
        tmp_path,
        "mixed.md",
        "> - **Status:** Parked until the next release -- implemented behind a flag\n\nBody.\n",
    )
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


@pytest.fixture
def pinned_today(monkeypatch: pytest.MonkeyPatch):
    """Pin 'today' so the expiry rule is deterministic instead of calendar-dependent."""
    import tools.repo_health.release_docs_policy as policy

    monkeypatch.setattr(policy, "_PLAN_TODAY_OVERRIDE", date(2026, 10, 1))
    return date(2026, 10, 1)


def test_lapsed_deadline_is_not_a_trigger(pinned_today: date, tmp_path: Path) -> None:
    """Real case: reliable-job-availability said "Remaining: bounded monitoring window
    through ~2026-08-31 ... then archive this plan" and sat un-flagged for a month,
    because "Remaining" satisfied the trigger test. A deadline in the past is not a
    reason to keep waiting -- it is an overdue action."""
    status = (
        "Live and enforced on the container -- verified 2026-08-28. Remaining: bounded "
        "monitoring window through ~2026-08-31 -- canary rechecks -- then archive this plan"
    )
    _plan(tmp_path, "lapsed.md", f"> - **Status:** {status}\n\nBody.\n")
    findings = _plan_lifecycle_findings(tmp_path / "docs" / "plans")
    assert any("lapsed.md" in f and "deferral has lapsed" in f for f in findings)


def test_future_deadline_is_still_a_live_trigger(pinned_today: date, tmp_path: Path) -> None:
    _plan(
        tmp_path,
        "future.md",
        "> - **Status:** Parked -- monitoring window through ~2026-12-01, then archive\n\nBody.\n",
    )
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_start_date_is_not_a_deadline(pinned_today: date, tmp_path: Path) -> None:
    """ "since 2026-05-30" is history, not an expiry. It must not read as a lapsed deadline."""
    _plan(
        tmp_path,
        "history.md",
        "> - **Status:** Active -- hardening implemented and validated since 2026-05-30; "
        "parser-noise classifier tightened 2026-08-21\n\nBody.\n",
    )
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_today_itself_is_not_yet_lapsed(pinned_today: date, tmp_path: Path) -> None:
    """A deadline landing today is still open; only a strictly past date has lapsed."""
    _plan(tmp_path, "today.md", "> - **Status:** Parked -- window through 2026-10-01\n\nBody.\n")
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_deadline_lookup_ignores_malformed_dates(pinned_today: date) -> None:
    """A bad date must not crash the gate; it just cannot prove an expiry."""
    import tools.repo_health.release_docs_policy as policy

    assert (
        policy._plan_status_deadlines_passed(
            "Parked -- through 2026-13-45 then review", today=pinned_today
        )
        is False
    )
    assert (
        policy._plan_status_deadlines_passed(
            "Parked -- through 2026-01-01 then review", today=pinned_today
        )
        is True
    )


def test_template_is_exempt(tmp_path: Path) -> None:
    """A plan-authoring template has no execution history, so lifecycle rules skip it."""
    assert "refactor-charter-template.md" in PLAN_LIFECYCLE_EXEMPT
    _plan(tmp_path, "refactor-charter-template.md", "> Placeholder\n\n" + ("x\n" * 700))
    findings = _plan_lifecycle_findings(tmp_path / "docs" / "plans")
    assert not any("refactor-charter-template.md" in f for f in findings)


def test_compliant_plan_produces_no_findings(tmp_path: Path) -> None:
    _plan(tmp_path, "ok.md", "> - **Status:** Active\n\n## Item 1\n\nOne line of work.\n")
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_terminal_status_reads_only_the_status_line(tmp_path: Path) -> None:
    """Body prose saying 'superseded' must not trip it -- only the header counts."""
    body = "> - **Status:** Active\n\nThis replaces the superseded W2 approach and folds the old plan.\n"
    _plan(tmp_path, "prose.md", body)
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_completed_work_in_the_middle_of_an_active_status_is_not_terminal(tmp_path: Path) -> None:
    """Real case: art-title reads 'Active follow-up plan, ... hardening implemented and
    validated on 2026-05-30'. The leading clause states the plan is still active, so the
    mid-sentence 'implemented' must not condemn it."""
    status = (
        "Active follow-up plan, parser/redirect/company hardening implemented and "
        "validated on 2026-05-30; shipped-artifact gate tooling also completed, with "
        "remaining blockers now isolated to static-source category-page rows."
    )
    _plan(tmp_path, "art-title.md", f"> - **Status:** {status}\n\nBody.\n")
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_live_status_outranks_a_terminal_phrase_later(tmp_path: Path) -> None:
    """'Live and enforced on the container' describes a shipped system, not a finished doc."""
    status = (
        "Live and enforced on the container -- `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1` "
        "verified via docker exec; desktop promotion follow-up: the packaged"
    )
    _plan(tmp_path, "availability.md", f"> - **Status:** {status}\n\nBody.\n")
    assert _plan_lifecycle_findings(tmp_path / "docs" / "plans") == []


def test_anchoring_not_clause_length_is_what_rejects_a_late_terminal_word(tmp_path: Path) -> None:
    """Deliberately pins WHICH guard does the work, and how much.

    A terminal word past the clause window is rejected by the anchored pattern -- the
    opening phrase "Ongoing review" is not terminal, and no later text can match a
    pattern that only reads the start. This is the guard that matters, because a
    172,891-character status line is real.
    """
    status = "Ongoing review; " + ("x " * 60) + " and finally Completed the migration"
    _plan(tmp_path, "long.md", f"> - **Status:** {status}\n\nBody.\n")
    assert not any("long.md" in f for f in _plan_lifecycle_findings(tmp_path / "docs" / "plans"))
    # The same status with a terminal OPENING is flagged, window or not.
    _plan(tmp_path, "long2.md", f"> - **Status:** Parked; {'y ' * 60}Completed\n\nBody.\n")
    assert any("long2.md" in f for f in _plan_lifecycle_findings(tmp_path / "docs" / "plans"))


def test_terminal_opening_is_flagged_however_long_the_status_runs(tmp_path: Path) -> None:
    """The clause window bounds the inspection, it does not excuse a terminal opening."""
    _plan(tmp_path, "parked.md", f"> - **Status:** Parked; {'y ' * 200}done\n\nBody.\n")
    assert any("parked.md" in f for f in _plan_lifecycle_findings(tmp_path / "docs" / "plans"))


def test_terminal_pattern_anchors_and_respects_word_boundaries() -> None:
    """'Unfinished' contains 'finished' but is not a terminal status, and the pattern
    is anchored so a word mid-sentence never matches."""
    assert not PLAN_TERMINAL_STATUS.search("Unfinished work remains")
    assert not PLAN_TERMINAL_STATUS.search("work is finished")
    assert PLAN_TERMINAL_STATUS.search("Finished -- nothing left")


def test_tripwires_are_tripwires_not_caps() -> None:
    """A plan one line over the threshold is flagged for review, not called a violation."""
    assert PLAN_LINE_TRIPWIRE == 600
    assert PLAN_WORD_TRIPWIRE > PLAN_LINE_TRIPWIRE
