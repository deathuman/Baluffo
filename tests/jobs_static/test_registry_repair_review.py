"""Registry repair review: the human-approval gate (2026-09-25).

Recording a decision is not applying a repair. These tests pin that split: the
artifact stores intent, the audit annotates findings with it, and nothing in
this path mutates a registry row.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.jobs.common.contracts_registry_repair_review import (
    REGISTRY_REPAIR_ACTIONS,
    REGISTRY_REPAIR_REVIEW_DECISIONS,
    apply_registry_repair_review_action,
    normalize_registry_repair_review_artifact,
    read_registry_repair_review_artifact,
    registry_finding_fingerprint,
    registry_repair_review_key,
    registry_repair_review_status,
)
from src.jobs.registry_hygiene import registry_hygiene_audit

_UPDATED_AT = "2026-09-25T00:00:00+00:00"


def _fingerprint(**overrides: object) -> str:
    base: dict[str, Any] = {
        "source_id": "static:listing_url:https://gone.example/careers",
        "finding_kind": "repair_candidate",
        "registry_state": "active",
        "unreachable_evidence": "http_404",
        "affected_urls": ["https://gone.example/careers"],
    }
    base.update(overrides)
    return registry_finding_fingerprint(**base)


# fingerprint binding ------------------------------------------------------


def test_fingerprint_is_stable_and_order_independent() -> None:
    assert _fingerprint() == _fingerprint()
    assert _fingerprint(affected_urls=["b", "a"]) == _fingerprint(affected_urls=["a", "b"])


def test_review_survives_a_counter_increment() -> None:
    """A still-broken source keeps climbing; that must not void its review.

    The whole point of fingerprinting on defect identity rather than on counts
    is that a standing review does not need re-approving after every run.
    """
    source_id = "static:listing_url:https://gone.example/careers"

    def audit_with(failures: int) -> dict:
        return registry_hygiene_audit(
            [
                {
                    "id": source_id,
                    "registryState": "active",
                    "listing_url": "https://gone.example/careers",
                    "pages": ["https://gone.example/careers"],
                }
            ],
            source_state_rows={
                f"static_source::{source_id}": {
                    "lastStatus": "error",
                    "lastError": "HTTP 404 for https://gone.example/careers",
                    "consecutiveFailures": failures,
                    "lastSuccessAt": "2026-08-01T00:00:00+00:00",
                    "lastFailureAt": "2026-09-24T00:00:00+00:00",
                }
            },
            observed_at="2026-09-25T00:00:00+00:00",
        )

    approved_at_three = audit_with(3)
    approved_at_nine = audit_with(9)
    assert approved_at_three["repairCandidateCount"] == 1
    assert approved_at_nine["repairCandidateCount"] == 1
    assert approved_at_nine["sources"][0]["consecutiveFailures"] == 9

    fingerprint = registry_finding_fingerprint(
        source_id=source_id,
        finding_kind="repair_candidate",
        registry_state="active",
        unreachable_evidence="http_404",
        affected_urls=["https://gone.example/careers"],
    )
    artifact, _ = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": source_id,
            "findingKind": "repair_candidate",
            "decision": "acknowledged",
            "evidenceFingerprint": fingerprint,
        },
        updated_at=_UPDATED_AT,
    )
    # The same fingerprint still matches nine runs later: no re-approval needed.
    still_reviewed = registry_hygiene_audit(
        [
            {
                "id": source_id,
                "registryState": "active",
                "listing_url": "https://gone.example/careers",
                "pages": ["https://gone.example/careers"],
            }
        ],
        source_state_rows={
            f"static_source::{source_id}": {
                "lastStatus": "error",
                "lastError": "HTTP 404 for https://gone.example/careers",
                "consecutiveFailures": 9,
                "lastSuccessAt": "2026-08-01T00:00:00+00:00",
                "lastFailureAt": "2026-09-24T00:00:00+00:00",
            }
        },
        observed_at="2026-09-25T00:00:00+00:00",
        repair_review=artifact,
    )
    assert still_reviewed["sources"][0]["reviewState"] == "acknowledged"
    assert still_reviewed["staleReviewCount"] == 0


def test_fingerprint_changes_with_defect_identity() -> None:
    base = _fingerprint()
    assert _fingerprint(unreachable_evidence="dns_or_tls") != base
    assert _fingerprint(registry_state="pending") != base
    assert _fingerprint(affected_urls=["https://gone.example/other"]) != base
    assert _fingerprint(finding_kind="host_drift_candidate") != base


def test_review_key_is_per_finding() -> None:
    assert registry_repair_review_key(source_id="a", finding_kind="repair_candidate") == (
        "a||repair_candidate"
    )
    assert registry_repair_review_key(source_id="a", finding_kind="repair_candidate") != (
        registry_repair_review_key(source_id="a", finding_kind="host_drift_candidate")
    )
    assert registry_repair_review_key(source_id="", finding_kind="x") == ""
    assert registry_repair_review_key(source_id="a", finding_kind="") == ""


# normalizer ---------------------------------------------------------------


def test_normalizer_is_total_against_junk() -> None:
    artifact = normalize_registry_repair_review_artifact(
        {"rows": {"junk": "not-a-dict", "": {"decision": "acknowledged"}}}
    )
    assert artifact["rows"] == {}
    assert artifact["summary"] == {
        "acknowledged": 0,
        "repair_approved": 0,
        "snoozed": 0,
    }
    assert normalize_registry_repair_review_artifact(None)["rows"] == {}
    assert normalize_registry_repair_review_artifact(7)["rows"] == {}


def test_normalizer_rejects_unknown_decision_and_action() -> None:
    row = normalize_registry_repair_review_artifact(
        {
            "rows": {
                "k": {
                    "sourceId": "a",
                    "findingKind": "repair_candidate",
                    "decision": "definitely_repair_it",
                    "approvedAction": "rm_rf",
                }
            }
        }
    )
    assert row["rows"] == {}


def test_normalizer_drops_action_when_not_an_approval() -> None:
    artifact = normalize_registry_repair_review_artifact(
        {
            "rows": {
                "k": {
                    "sourceId": "a",
                    "findingKind": "host_drift_candidate",
                    "decision": "acknowledged",
                    "approvedAction": "repoint",
                    "approvedTarget": "https://x.example",
                }
            }
        }
    )
    row = next(iter(artifact["rows"].values()))
    assert row["decision"] == "acknowledged"
    assert row["approvedAction"] == ""
    assert row["approvedTarget"] == ""


def test_normalizer_drops_unparseable_timestamps() -> None:
    artifact = normalize_registry_repair_review_artifact(
        {
            "rows": {
                "k": {
                    "sourceId": "a",
                    "findingKind": "repair_candidate",
                    "decision": "snoozed",
                    "decisionAt": "whenever",
                    "snoozedUntil": "2026-13-45T99:99:99+00:00",
                }
            }
        }
    )
    row = next(iter(artifact["rows"].values()))
    assert row["decisionAt"] == ""
    assert row["snoozedUntil"] == ""


# reader ------------------------------------------------------------------


def test_reader_degrades_to_empty_with_a_warning(tmp_path: Path) -> None:
    missing, missing_warning = read_registry_repair_review_artifact(tmp_path / "nope.json")
    assert missing["rows"] == {}
    assert missing_warning == "missing_registry_repair_review"

    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not json", encoding="utf-8")
    corrupt_rows, corrupt_warning = read_registry_repair_review_artifact(corrupt)
    assert corrupt_rows["rows"] == {}
    assert corrupt_warning == "malformed_registry_repair_review"

    not_object = tmp_path / "list.json"
    not_object.write_text("[1, 2, 3]", encoding="utf-8")
    _, list_warning = read_registry_repair_review_artifact(not_object)
    assert list_warning == "malformed_registry_repair_review"


def test_reader_round_trips_a_written_artifact(tmp_path: Path) -> None:
    artifact, _ = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": "a",
            "findingKind": "repair_candidate",
            "decision": "acknowledged",
            "note": "known dead ATS, awaiting studio confirmation",
        },
        updated_at=_UPDATED_AT,
        default_decided_by="operator",
    )
    path = tmp_path / "registry-repair-review.json"
    path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    reloaded, warning = read_registry_repair_review_artifact(path)
    assert warning == ""
    assert reloaded == artifact
    assert reloaded["summary"]["acknowledged"] == 1


# action application ------------------------------------------------------


def test_apply_rejects_invalid_decisions_and_missing_keys() -> None:
    with pytest.raises(ValueError):
        apply_registry_repair_review_action(
            prior_artifact={},
            action_payload={"sourceId": "a", "findingKind": "x", "decision": "new"},
            updated_at=_UPDATED_AT,
        )
    with pytest.raises(ValueError):
        apply_registry_repair_review_action(
            prior_artifact={},
            action_payload={"sourceId": "", "findingKind": "x", "decision": "acknowledged"},
            updated_at=_UPDATED_AT,
        )
    with pytest.raises(ValueError):
        apply_registry_repair_review_action(
            prior_artifact={},
            action_payload={"sourceId": "a", "findingKind": "", "decision": "acknowledged"},
            updated_at=_UPDATED_AT,
        )


def test_apply_records_approval_intent_without_applying_it() -> None:
    artifact, row = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": "a",
            "findingKind": "repair_candidate",
            "decision": "repair_approved",
            "approvedAction": "repoint",
            "approvedTarget": "https://new-board.example/careers",
            "evidenceFingerprint": "abc123",
            "note": "board moved",
        },
        updated_at=_UPDATED_AT,
        default_decided_by="operator",
    )
    assert row["approvedAction"] == "repoint"
    assert row["approvedTarget"] == "https://new-board.example/careers"
    assert row["decidedBy"] == "operator"
    assert row["decisionAt"] == _UPDATED_AT
    # Recording intent must not itself change the artifact's other rows.
    assert artifact["summary"]["repair_approved"] == 1
    assert set(REGISTRY_REPAIR_ACTIONS) == {"repoint", "retire", "reclassify"}
    assert set(REGISTRY_REPAIR_REVIEW_DECISIONS) == {
        "new",
        "acknowledged",
        "repair_approved",
        "snoozed",
    }


def test_apply_preserves_a_prior_fingerprint_when_omitted() -> None:
    artifact, _ = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": "a",
            "findingKind": "repair_candidate",
            "decision": "acknowledged",
            "evidenceFingerprint": "keep-me",
        },
        updated_at=_UPDATED_AT,
    )
    updated, _ = apply_registry_repair_review_action(
        prior_artifact=artifact,
        action_payload={"sourceId": "a", "findingKind": "repair_candidate", "decision": "snoozed"},
        updated_at=_UPDATED_AT,
    )
    row = registry_repair_review_status(
        updated, source_id="a", finding_kind="repair_candidate", evidence_fingerprint="keep-me"
    )
    assert row["evidenceFingerprint"] == "keep-me"
    assert row["decision"] == "snoozed"


# lookup honors a decision only while the evidence matches -----------------


def test_lookup_honors_a_matching_fingerprint() -> None:
    artifact, _ = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": "a",
            "findingKind": "repair_candidate",
            "decision": "acknowledged",
            "evidenceFingerprint": "fp-1",
        },
        updated_at=_UPDATED_AT,
    )
    row = registry_repair_review_status(
        artifact, source_id="a", finding_kind="repair_candidate", evidence_fingerprint="fp-1"
    )
    assert row["decision"] == "acknowledged"
    assert row["isStale"] is False
    assert row["hasDecision"] is True


def test_lookup_refuses_a_decision_made_about_different_evidence() -> None:
    artifact, _ = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": "a",
            "findingKind": "repair_candidate",
            "decision": "repair_approved",
            "approvedAction": "repoint",
            "evidenceFingerprint": "fp-1",
        },
        updated_at=_UPDATED_AT,
    )
    row = registry_repair_review_status(
        artifact, source_id="a", finding_kind="repair_candidate", evidence_fingerprint="fp-CHANGED"
    )
    assert row["decision"] == "new"
    assert row["approvedAction"] == ""
    assert row["isStale"] is True
    assert row["hasDecision"] is True


def test_lookup_refuses_an_unfingerprinted_decision() -> None:
    """An approval with no evidence binding cannot be matched to anything."""
    artifact, _ = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": "a",
            "findingKind": "repair_candidate",
            "decision": "acknowledged",
        },
        updated_at=_UPDATED_AT,
    )
    row = registry_repair_review_status(
        artifact, source_id="a", finding_kind="repair_candidate", evidence_fingerprint="fp-1"
    )
    assert row["decision"] == "new"
    assert row["isStale"] is True


# audit integration --------------------------------------------------------


def _dead_row() -> dict[str, object]:
    return {
        "id": "static:listing_url:https://gone.example/careers",
        "name": "Gone (Static)",
        "registryState": "active",
        "listing_url": "https://gone.example/careers",
        "pages": ["https://gone.example/careers"],
    }


_DEAD_STATE = {
    "static_source::static:listing_url:https://gone.example/careers": {
        "lastStatus": "error",
        "lastError": "HTTP 404 for https://gone.example/careers",
        "consecutiveFailures": 6,
        "lastSuccessAt": "2026-08-01T00:00:00+00:00",
        "lastFailureAt": "2026-09-24T00:00:00+00:00",
    }
}


def _audit(repair_review: object) -> dict:
    return registry_hygiene_audit(
        [_dead_row()],
        source_state_rows=_DEAD_STATE,
        observed_at="2026-09-25T00:00:00+00:00",
        repair_review=repair_review,
    )


def test_audit_reports_unreviewed_by_default() -> None:
    audit = _audit({})
    assert audit["reviewedFindingCount"] == 0
    assert audit["staleReviewCount"] == 0
    assert audit["sources"][0]["reviewState"] == "new"
    assert audit["sources"][0]["approvedAction"] == ""


def test_audit_marks_a_matching_decision_reviewed() -> None:
    live = _audit({})
    fingerprint = registry_finding_fingerprint(
        source_id="static:listing_url:https://gone.example/careers",
        finding_kind="repair_candidate",
        registry_state="active",
        unreachable_evidence="http_404",
        affected_urls=["https://gone.example/careers"],
    )
    artifact, _ = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": "static:listing_url:https://gone.example/careers",
            "findingKind": "repair_candidate",
            "decision": "repair_approved",
            "approvedAction": "repoint",
            "approvedTarget": "https://new.example/careers",
            "evidenceFingerprint": fingerprint,
        },
        updated_at=_UPDATED_AT,
    )
    reviewed = _audit(artifact)
    assert reviewed["reviewedFindingCount"] == 1
    assert reviewed["staleReviewCount"] == 0
    assert reviewed["sources"][0]["reviewState"] == "repair_approved"
    assert reviewed["sources"][0]["approvedAction"] == "repoint"
    # The finding is still reported; review annotates it, it does not hide it.
    assert reviewed["repairCandidateCount"] == live["repairCandidateCount"] == 1


def test_audit_surfaces_a_stale_decision_when_evidence_changed() -> None:
    artifact, _ = apply_registry_repair_review_action(
        prior_artifact={},
        action_payload={
            "sourceId": "static:listing_url:https://gone.example/careers",
            "findingKind": "repair_candidate",
            "decision": "repair_approved",
            "approvedAction": "repoint",
            "evidenceFingerprint": "fingerprint-from-a-different-defect",
        },
        updated_at=_UPDATED_AT,
    )
    stale = _audit(artifact)
    assert stale["staleReviewCount"] == 1
    assert stale["reviewedFindingCount"] == 0
    assert stale["sources"][0]["reviewState"] == "stale"
    assert stale["sources"][0]["approvedAction"] == ""
    assert "no longer matches" in stale["sources"][0]["reviewStaleReason"]


def test_audit_survives_a_malformed_review_payload() -> None:
    audit = _audit({"rows": "not-a-mapping", "nonsense": 7})
    assert audit["sources"][0]["reviewState"] == "new"
    assert audit["reviewedFindingCount"] == 0
