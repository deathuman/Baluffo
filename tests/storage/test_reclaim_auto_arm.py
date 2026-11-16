"""Self-arming decisions: what a finished rehearsal justifies, and its evidence.

Auto mode enables itself on measurement, so the decision must be a pure function of
the rehearsal's own outcome and the anchor's age. These cases pin both the arm and
every way it is withheld: a thin gate, a failed copy, and a measurement that no
longer describes the database.
"""

from __future__ import annotations

from typing import Any

from src.storage.reclaim_policy import (
    RECLAIM_ANCHOR_STALE_AFTER_S,
    ReclaimContext,
    auto_arm_decision,
    reclaim_verdict,
)

DECIDED_AT = "2026-11-16T10:00:00Z"


def _result(**overrides: Any) -> dict[str, Any]:
    return {
        "runId": "rehearsal-1",
        "stagingBytes": 900 * 1024**2,
        "measuredGateBytes": None,
        "gateAdequate": None,
        **overrides,
    }


def test_a_completed_first_rehearsal_arms_the_host() -> None:
    decision = auto_arm_decision(
        status="complete", result=_result(), anchor_age_s=0, decided_at=DECIDED_AT
    )

    assert decision["armed"] is True
    assert decision["reason"] == "rehearsal-adequate"
    assert decision["decidedAt"] == DECIDED_AT
    # A first measurement had no gate to breach: `None` is not a failure.
    assert decision["evidence"] == {
        "runId": "rehearsal-1",
        "stagingBytes": 900 * 1024**2,
        "gateBytes": None,
        "gateAdequate": None,
        "anchorAgeS": 0,
    }


def test_an_adequate_rehearsal_over_a_measured_gate_arms() -> None:
    decision = auto_arm_decision(
        status="complete",
        result=_result(gateAdequate=True, measuredGateBytes=2 * 1024**3),
        anchor_age_s=60,
        decided_at=DECIDED_AT,
    )

    assert decision["armed"] is True
    assert decision["evidence"]["gateBytes"] == 2 * 1024**3
    assert decision["evidence"]["gateAdequate"] is True


def test_an_inadequate_rehearsal_withholds_the_arm() -> None:
    decision = auto_arm_decision(
        status="complete",
        result=_result(gateAdequate=False, measuredGateBytes=1024**3),
        anchor_age_s=0,
        decided_at=DECIDED_AT,
    )

    # The copy outgrew the gate it started on: the margin is thin, so arm later.
    assert decision["armed"] is False
    assert decision["reason"] == "rehearsal-inadequate"


def test_a_failed_rehearsal_withholds_the_arm() -> None:
    decision = auto_arm_decision(
        status="failed", result={}, anchor_age_s=None, decided_at=DECIDED_AT
    )

    assert decision["armed"] is False
    assert decision["reason"] == "rehearsal-failed"
    assert decision["evidence"]["stagingBytes"] == 0


def test_a_stale_anchor_withholds_the_arm() -> None:
    decision = auto_arm_decision(
        status="complete",
        result=_result(gateAdequate=True),
        anchor_age_s=RECLAIM_ANCHOR_STALE_AFTER_S + 1,
        decided_at=DECIDED_AT,
    )

    assert decision["armed"] is False
    assert decision["reason"] == "anchor-stale"


def test_an_unknown_anchor_age_withholds_the_arm() -> None:
    decision = auto_arm_decision(
        status="complete", result=_result(), anchor_age_s=None, decided_at=DECIDED_AT
    )

    # An unreadable stamp is unknown, not fresh.
    assert decision["armed"] is False
    assert decision["reason"] == "anchor-stale"


ARM_RECORD = {
    "armed": True,
    "reason": "rehearsal-adequate",
    "decidedAt": "2026-11-16T10:00:00Z",
    "evidence": {
        "runId": "rehearsal-arm",
        "stagingBytes": 900 * 1024**2,
        "gateBytes": 2 * 1024**3,
        "gateAdequate": True,
        "anchorAgeS": 0,
    },
}


def _verdict(context: ReclaimContext) -> dict[str, Any]:
    preview = {
        "currentGeneration": "gen-1",
        "currentGenerationEstimatedBytes": 40 * 1024**2,
        "historicalGenerationEstimatedBytes": 0,
        "historicalGenerationCount": 0,
        "estimatedReclaimableBytes": 0,
        "candidateGenerationCount": 0,
        "capReached": False,
    }
    return reclaim_verdict(
        preview,
        {"databaseBytes": 100 * 1024**2, "freelistBytes": 0},
        context=context,
    )


def test_the_verdict_reports_an_armed_host_with_its_evidence() -> None:
    result = _verdict(
        ReclaimContext(
            free_space_bytes=32 * 1024**3,
            auto_enabled=True,
            auto_switch=None,
            auto_arm=ARM_RECORD,
        )
    )

    assert result["autoStart"] == {
        "enabled": True,
        "source": "rehearsal",
        "switch": "unset",
        "armed": True,
        "reason": "rehearsal-adequate",
        "decidedAt": "2026-11-16T10:00:00Z",
        "evidence": ARM_RECORD["evidence"],
    }
    assert "auto-disabled" not in result["blockers"]


def test_the_verdict_says_when_nothing_has_armed_the_host() -> None:
    result = _verdict(
        ReclaimContext(free_space_bytes=32 * 1024**3, auto_enabled=False, auto_switch=None)
    )

    assert result["autoStart"] == {
        "enabled": False,
        "source": "none",
        "switch": "unset",
        "armed": False,
        "reason": "not-armed",
        "decidedAt": "",
        "evidence": None,
    }
    assert "auto-disabled" in result["blockers"]


def test_the_verdict_names_the_kill_switch_over_a_recorded_arm() -> None:
    result = _verdict(
        ReclaimContext(
            free_space_bytes=32 * 1024**3,
            auto_enabled=False,
            auto_switch=False,
            auto_arm=ARM_RECORD,
        )
    )

    # The measurement stands; the operator's switch is why auto is off.
    assert result["autoStart"]["enabled"] is False
    assert result["autoStart"]["source"] == "env"
    assert result["autoStart"]["switch"] == "off"
    assert result["autoStart"]["reason"] == "kill-switch"
    assert result["autoStart"]["armed"] is True
    assert result["autoStart"]["evidence"] == ARM_RECORD["evidence"]
    assert "auto-disabled" in result["blockers"]


def test_the_verdict_names_a_forced_on_switch() -> None:
    result = _verdict(
        ReclaimContext(free_space_bytes=32 * 1024**3, auto_enabled=True, auto_switch=True)
    )

    assert result["autoStart"]["source"] == "env"
    assert result["autoStart"]["switch"] == "on"
    assert result["autoStart"]["reason"] == "forced-on"
    assert result["autoStart"]["enabled"] is True
    assert "auto-disabled" not in result["blockers"]
