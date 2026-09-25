"""Advisory registry hygiene audit (2026-09-25).

The monitor generalizes the existing asset-page report with bounded evidence
for duplicate candidates, repeated unreachable state, and host drift. It is
observability-only: no row is demoted, deleted, or rewritten.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.jobs.common.contracts_runtime import normalize_runtime_payload
from src.jobs.registry_hygiene import (
    REGISTRY_HYGIENE_ERROR_SAMPLE_LIMIT,
    REGISTRY_HYGIENE_SAMPLE_LIMIT,
    REGISTRY_HYGIENE_SOURCE_LIMIT,
    REGISTRY_REACHABILITY_MAX_OBSERVATION_AGE_DAYS,
    REGISTRY_REACHABILITY_MIN_FAILURES,
    REGISTRY_REACHABILITY_MIN_OUTAGE_DAYS,
    registry_hygiene_audit,
)


def _row(source_id: str, pages: list[str], *, listing_url: str = "") -> dict[str, object]:
    return {
        "id": source_id,
        "registryState": "active",
        "listing_url": listing_url,
        "pages": pages,
    }


def test_audit_reports_all_four_advisory_categories() -> None:
    source_id = "static:listing_url:https://board.example/careers"
    audit = registry_hygiene_audit(
        [
            _row(
                source_id,
                [
                    "https://board.example/careers",
                    "https://board.example/js/app.js",
                    "https://provider.example/jobs",
                ],
                listing_url="https://board.example/careers",
            ),
            _row(
                "static:listing_url:https://board.example/careers/",
                ["https://board.example/careers/"],
                listing_url="https://board.example/careers/",
            ),
        ],
        source_state_rows={
            f"static_source::{source_id}": {
                "lastStatus": "error",
                "lastError": "HTTP 404 not found",
                "consecutiveFailures": 3,
            }
        },
    )

    assert audit["sourceCount"] == 2
    assert audit["assetPageCount"] == 1
    assert audit["duplicateGroupCount"] == 1
    assert audit["duplicateRowCount"] == 2
    assert audit["unreachablePageCount"] == 1
    assert audit["hostDriftCount"] == 1
    first = audit["sources"][0]
    assert first["sourceId"] == source_id
    assert first["flags"] == [
        "asset_pages",
        "duplicate_candidate",
        "host_drift_candidate",
        "unreachable_page",
    ]
    assert first["sampleAssetPages"] == ["https://board.example/js/app.js"]
    assert first["unreachableEvidence"] == "http_404"


def test_audit_resolves_provider_state_by_registry_name() -> None:
    audit = registry_hygiene_audit(
        [
            {
                "id": "workable:account:ghost",
                "name": "Ghost Studio (Workable)",
                "registryState": "active",
                "api_url": "https://apply.workable.com/api/v1/widget/accounts/ghost",
            }
        ],
        source_state_rows={
            "Ghost Studio (Workable)": {
                "lastStatus": "error",
                "lastError": "Network error for https://ghost.example/career: [SSL: CERTIFICATE_VERIFY_FAILED]",
                "consecutiveFailures": 3,
            }
        },
    )

    assert audit["unreachablePageCount"] == 1
    assert audit["sources"][0]["unreachableEvidence"] == "dns_or_tls"
    assert audit["sources"][0]["sampleUnreachablePages"] == [
        "https://apply.workable.com/api/v1/widget/accounts/ghost"
    ]


def test_audit_distinguishes_not_found_from_http_404() -> None:
    rows = [
        _row(
            "static:listing_url:https://gone.example/careers",
            ["https://gone.example/careers"],
        ),
        _row("static:listing_url:https://sheet.example/careers", ["https://sheet.example/careers"]),
    ]
    audit = registry_hygiene_audit(
        rows,
        source_state_rows={
            "static_source::static:listing_url:https://gone.example/careers": {
                "lastStatus": "error",
                "lastError": "Remote end closed connection: not found",
                "consecutiveFailures": 2,
            },
            "static_source::static:listing_url:https://sheet.example/careers": {
                "lastStatus": "error",
                "lastError": "HTTP 404 for https://sheet.example/careers",
                "consecutiveFailures": 2,
            },
        },
    )

    evidence = {s["sourceId"]: s["unreachableEvidence"] for s in audit["sources"]}
    assert evidence["static:listing_url:https://gone.example/careers"] == "not_found"
    assert evidence["static:listing_url:https://sheet.example/careers"] == "http_404"


def test_audit_ignores_single_transient_failure() -> None:
    audit = registry_hygiene_audit(
        [
            _row(
                "static:listing_url:https://flaky.example/careers",
                ["https://flaky.example/careers"],
            )
        ],
        source_state_rows={
            "static_source::static:listing_url:https://flaky.example/careers": {
                "lastStatus": "error",
                "lastError": "HTTP 404 not found",
                "consecutiveFailures": 1,
            }
        },
    )

    assert audit["unreachablePageCount"] == 0
    assert audit["sources"] == []


def test_audit_separates_reviewed_collisions_from_new_drift() -> None:
    rows = [
        _row(
            "static:listing_url:https://ea.com/careers",
            ["https://ea.com/careers"],
            listing_url="https://ea.com/careers",
        ),
        _row(
            "static:listing_url:https://www.ea.com/careers",
            ["https://www.ea.com/careers"],
            listing_url="https://www.ea.com/careers",
        ),
        _row(
            "static:listing_url:https://newgame.example/jobs",
            ["https://newgame.example/jobs"],
            listing_url="https://newgame.example/jobs",
        ),
        _row(
            "static:listing_url:https://www.newgame.example/jobs",
            ["https://www.newgame.example/jobs"],
            listing_url="https://www.newgame.example/jobs",
        ),
    ]

    reviewed_only = registry_hygiene_audit(rows, known_collision_urls={"ea.com/careers"})
    assert reviewed_only["duplicateGroupCount"] == 2
    assert reviewed_only["duplicateRowCount"] == 4
    assert reviewed_only["knownCollisionGroupCount"] == 1
    assert reviewed_only["uncoveredDuplicateGroupCount"] == 1
    assert reviewed_only["uncoveredDuplicateRowCount"] == 2
    reviewed = next(s for s in reviewed_only["sources"] if s["sourceId"].endswith("ea.com/careers"))
    drifted = next(s for s in reviewed_only["sources"] if "newgame" in s["sourceId"])
    assert reviewed["uncoveredDuplicateGroupCount"] == 0
    assert drifted["uncoveredDuplicateGroupCount"] == 1

    # No baseline available: everything is uncovered so drift is never hidden.
    unknown = registry_hygiene_audit(rows)
    assert unknown["knownCollisionGroupCount"] == 0
    assert unknown["uncoveredDuplicateGroupCount"] == 2
    assert unknown["uncoveredDuplicateRowCount"] == 4

    # A junk baseline must not raise and must not mask duplicates.
    assert (
        registry_hygiene_audit(rows, known_collision_urls="not-a-set")[
            "uncoveredDuplicateGroupCount"
        ]
        == 2
    )


def _failing_state(
    *,
    failures: int,
    error: str = "HTTP 404 for https://gone.example/careers",
    last_success_at: str = "2026-09-01T00:00:00+00:00",
    last_failure_at: str = "2026-09-24T12:00:00+00:00",
) -> dict[str, object]:
    state: dict[str, object] = {
        "lastStatus": "error",
        "lastError": error,
        "consecutiveFailures": failures,
    }
    if last_success_at:
        state["lastSuccessAt"] = last_success_at
    if last_failure_at:
        state["lastFailureAt"] = last_failure_at
    return state


_OBSERVED_AT = "2026-09-25T00:00:00+00:00"


def test_repair_candidate_promotes_on_either_repetition_form() -> None:
    row = _row("static:listing_url:https://gone.example/careers", ["https://gone.example/careers"])
    source_key = "static_source::static:listing_url:https://gone.example/careers"

    # Repeated failures, short outage: promoted on the failure-count branch.
    by_failures = registry_hygiene_audit(
        [row],
        source_state_rows={
            source_key: _failing_state(
                failures=9,
                last_success_at="2026-09-24T00:00:00+00:00",
                last_failure_at="2026-09-24T12:00:00+00:00",
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert by_failures["repairCandidateCount"] == 1
    assert by_failures["sources"][0]["outageDays"] == 1

    # Long outage, few recorded failures: promoted on the duration branch.
    # This is the circuit-broken shape: a source dead for months stops being
    # retried, so consecutiveFailures understates how dead it actually is.
    by_duration = registry_hygiene_audit(
        [row],
        source_state_rows={
            source_key: _failing_state(
                failures=2,
                last_success_at="2026-04-10T00:00:00+00:00",
                last_failure_at="2026-09-09T00:00:00+00:00",
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert by_duration["repairCandidateCount"] == 1
    entry = by_duration["sources"][0]
    assert entry["consecutiveFailures"] == 2
    assert entry["outageDays"] == 168
    assert entry["observationAgeDays"] == 16
    assert "repair_candidate" in entry["flags"]
    assert "unreachable_page" in entry["flags"]
    assert entry["lastErrorSample"] == "HTTP 404 for https://gone.example/careers"

    # Neither form: two failures, one day down.
    neither = registry_hygiene_audit(
        [row],
        source_state_rows={
            source_key: _failing_state(
                failures=2,
                last_success_at="2026-09-24T00:00:00+00:00",
                last_failure_at="2026-09-24T12:00:00+00:00",
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert neither["unreachablePageCount"] == 1
    assert neither["repairCandidateCount"] == 0


def test_repair_candidate_requires_a_recent_observation() -> None:
    """A long outage nobody has re-checked is unproven, not dead."""
    row = _row(
        "static:listing_url:https://stale.example/careers", ["https://stale.example/careers"]
    )
    audit = registry_hygiene_audit(
        [row],
        source_state_rows={
            "static_source::static:listing_url:https://stale.example/careers": _failing_state(
                failures=9,
                last_success_at="2026-01-01T00:00:00+00:00",
                last_failure_at="2026-05-01T00:00:00+00:00",
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert audit["unreachablePageCount"] == 1
    assert audit["repairCandidateCount"] == 0
    assert audit["sources"][0]["observationAgeDays"] == 147


def test_repair_candidate_needs_a_recorded_observation() -> None:
    """No failure or run timestamp at all means no current evidence."""
    row = _row(
        "static:listing_url:https://undated.example/careers", ["https://undated.example/careers"]
    )
    audit = registry_hygiene_audit(
        [row],
        source_state_rows={
            "static_source::static:listing_url:https://undated.example/careers": _failing_state(
                failures=9, last_failure_at=""
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert audit["repairCandidateCount"] == 0
    assert audit["sources"][0]["observationAgeDays"] == -1


def test_repair_candidate_is_not_promoted_without_repeated_evidence() -> None:
    """A week-long lastSuccessAt with zero failures is stale bookkeeping."""
    row = _row(
        "static:listing_url:https://quiet.example/careers", ["https://quiet.example/careers"]
    )
    audit = registry_hygiene_audit(
        [row],
        source_state_rows={
            "static_source::static:listing_url:https://quiet.example/careers": {
                "lastStatus": "error",
                "lastError": "HTTP 404 not found",
                "consecutiveFailures": 0,
                "lastSuccessAt": "2026-09-01T00:00:00+00:00",
            }
        },
        observed_at=_OBSERVED_AT,
    )
    assert audit["repairCandidateCount"] == 0
    assert audit["sources"] == []


def test_repair_candidate_reports_no_outage_span_without_an_anchor() -> None:
    """A row that never succeeded has no outage length; refuse to invent one.

    40 consecutive permanent failures still promote on the failure-count
    branch, so only the span is asserted here.
    """
    row = _row("static:listing_url:https://new.example/careers", ["https://new.example/careers"])
    audit = registry_hygiene_audit(
        [row],
        source_state_rows={
            "static_source::static:listing_url:https://new.example/careers": _failing_state(
                failures=40, last_success_at=""
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert audit["unreachablePageCount"] == 1
    assert audit["sources"][0]["outageDays"] == 0
    assert audit["repairCandidateCount"] == 1


def test_repair_candidate_ignores_transient_error_classes() -> None:
    """A timeout repeated for a month is an outage, not a dead domain."""
    row = _row("static:listing_url:https://slow.example/careers", ["https://slow.example/careers"])
    audit = registry_hygiene_audit(
        [row],
        source_state_rows={
            "static_source::static:listing_url:https://slow.example/careers": _failing_state(
                failures=30, error="read timeout after 30s"
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert audit["unreachablePageCount"] == 0
    assert audit["repairCandidateCount"] == 0


def test_repair_candidate_bounds_the_error_sample() -> None:
    row = _row("static:listing_url:https://big.example/careers", ["https://big.example/careers"])
    audit = registry_hygiene_audit(
        [row],
        source_state_rows={
            "static_source::static:listing_url:https://big.example/careers": _failing_state(
                failures=9, error="HTTP 404 " + ("x" * 5000)
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert len(audit["sources"][0]["lastErrorSample"]) == REGISTRY_HYGIENE_ERROR_SAMPLE_LIMIT


def test_repair_candidate_never_reports_a_negative_outage_span() -> None:
    """A corrupt future lastSuccessAt clamps to zero rather than going negative."""
    row = _row(
        "static:listing_url:https://future.example/careers", ["https://future.example/careers"]
    )
    audit = registry_hygiene_audit(
        [row],
        source_state_rows={
            "static_source::static:listing_url:https://future.example/careers": _failing_state(
                failures=9, last_success_at="2027-01-01T00:00:00+00:00"
            )
        },
        observed_at=_OBSERVED_AT,
    )
    assert audit["sources"][0]["outageDays"] == 0


def test_repair_candidate_tolerates_junk_state_timestamps() -> None:
    """An unparseable failure counter is treated as no evidence, not as evidence."""
    row = _row("static:listing_url:https://junk.example/careers", ["https://junk.example/careers"])
    audit = registry_hygiene_audit(
        [row],
        source_state_rows={
            "static_source::static:listing_url:https://junk.example/careers": {
                "lastStatus": "error",
                "lastError": "HTTP 404 not found",
                "consecutiveFailures": "many",
                "lastSuccessAt": "not-a-timestamp",
            }
        },
        observed_at="also-not-a-timestamp",
    )
    assert audit["unreachablePageCount"] == 0
    assert audit["repairCandidateCount"] == 0
    assert audit["sources"] == []


def test_contracts_normalize_repair_candidate_fields() -> None:
    normalized = normalize_runtime_payload(
        {
            "registryHygieneAudit": {
                "repairCandidateCount": "3",
                "repairCandidateMinFailures": 3,
                "repairCandidateMinOutageDays": 7,
                "repairCandidateMaxObservationAgeDays": 30,
                "sources": [
                    {
                        "sourceId": "x",
                        "flags": ["repair_candidate", "bogus_flag"],
                        "consecutiveFailures": 5,
                        "outageDays": -4,
                        "observationAgeDays": -1,
                        "lastErrorSample": "y" * 900,
                    },
                    {
                        "sourceId": "y",
                        "observationAgeDays": -9999,
                    },
                ],
            }
        },
        selected_source_count=1,
    )["registryHygieneAudit"]
    assert normalized["repairCandidateCount"] == 3
    assert normalized["repairCandidateMinFailures"] == 3
    assert normalized["repairCandidateMinOutageDays"] == 7
    assert normalized["repairCandidateMaxObservationAgeDays"] == 30
    row = normalized["sources"][0]
    assert row["flags"] == ["repair_candidate"]
    assert row["consecutiveFailures"] == 5
    assert row["outageDays"] == 0
    assert len(row["lastErrorSample"]) == REGISTRY_HYGIENE_ERROR_SAMPLE_LIMIT
    # Only the exact -1 sentinel survives; other negatives clamp to 0.
    assert row["observationAgeDays"] == -1
    assert normalized["sources"][1]["observationAgeDays"] == 0


def test_audit_is_zero_for_clean_rows() -> None:
    audit = registry_hygiene_audit(
        [
            _row(
                "static:listing_url:https://clean.example/careers",
                ["https://clean.example/careers"],
            )
        ]
    )

    assert audit == {
        "sourceCount": 0,
        "assetPageCount": 0,
        "duplicateGroupCount": 0,
        "duplicateRowCount": 0,
        "knownCollisionGroupCount": 0,
        "uncoveredDuplicateGroupCount": 0,
        "uncoveredDuplicateRowCount": 0,
        "unreachablePageCount": 0,
        "repairCandidateCount": 0,
        "repairCandidateMinFailures": REGISTRY_REACHABILITY_MIN_FAILURES,
        "repairCandidateMinOutageDays": REGISTRY_REACHABILITY_MIN_OUTAGE_DAYS,
        "repairCandidateMaxObservationAgeDays": REGISTRY_REACHABILITY_MAX_OBSERVATION_AGE_DAYS,
        "hostDriftCount": 0,
        "sources": [],
    }


def test_audit_bounds_sources_and_samples_but_keeps_counts_exact() -> None:
    rows = [
        _row(
            f"static:listing_url:https://board{index}.example/careers",
            [f"https://board{index}.example/js/{n}.js" for n in range(5)],
        )
        for index in range(REGISTRY_HYGIENE_SOURCE_LIMIT + 5)
    ]

    audit = registry_hygiene_audit(rows)

    assert audit["sourceCount"] == REGISTRY_HYGIENE_SOURCE_LIMIT + 5
    assert audit["assetPageCount"] == (REGISTRY_HYGIENE_SOURCE_LIMIT + 5) * 5
    assert len(audit["sources"]) == REGISTRY_HYGIENE_SOURCE_LIMIT
    assert len(audit["sources"][0]["sampleAssetPages"]) == REGISTRY_HYGIENE_SAMPLE_LIMIT


def test_audit_tolerates_junk_input() -> None:
    assert registry_hygiene_audit(None)["sourceCount"] == 0
    assert registry_hygiene_audit(42)["sources"] == []
    assert registry_hygiene_audit([None, 7, "x", {}])["sources"] == []


def test_contracts_normalizer_round_trips_and_clamps() -> None:
    audit = registry_hygiene_audit(
        [
            _row(
                "static:listing_url:https://board.example/careers",
                ["https://board.example/careers", "https://board.example/app.js"],
            )
        ]
    )
    normalized = normalize_runtime_payload({"registryHygieneAudit": audit}, selected_source_count=1)
    assert normalized["registryHygieneAudit"] == audit

    clamped = normalize_runtime_payload(
        {
            "registryHygieneAudit": {
                "sourceCount": "many",
                "assetPageCount": None,
                "extra": "dropped",
                "sources": [
                    {
                        "sourceId": 5,
                        "flags": ["asset_pages", "unknown", "host_drift_candidate"],
                        "sampleAssetPages": [1, "ok.js"],
                    },
                    "junk-row",
                ],
            }
        },
        selected_source_count=1,
    )["registryHygieneAudit"]
    assert clamped["sourceCount"] == 0
    assert clamped["sources"][0]["sourceId"] == "5"
    assert clamped["sources"][0]["flags"] == ["asset_pages", "host_drift_candidate"]
    assert clamped["sources"][0]["sampleAssetPages"] == ["ok.js"]


def test_runtime_setup_stamps_hygiene_audit() -> None:
    import src.jobs.pipeline_run_setup as run_setup

    assert run_setup.__dict__.get("registry_hygiene_audit") is registry_hygiene_audit
    assert callable(run_setup.__dict__.get("known_twin_career_urls"))


def test_live_seed_audit_returns_stable_shape() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    seed_path = repo_root / "data" / "defaults" / "source-registry-active.seed.json"
    if not seed_path.exists():
        return
    rows = json.loads(seed_path.read_text(encoding="utf-8"))
    audit = registry_hygiene_audit(rows)
    assert set(audit) == {
        "sourceCount",
        "assetPageCount",
        "duplicateGroupCount",
        "duplicateRowCount",
        "knownCollisionGroupCount",
        "uncoveredDuplicateGroupCount",
        "uncoveredDuplicateRowCount",
        "unreachablePageCount",
        "repairCandidateCount",
        "repairCandidateMinFailures",
        "repairCandidateMinOutageDays",
        "repairCandidateMaxObservationAgeDays",
        "hostDriftCount",
        "sources",
    }
    assert len(audit["sources"]) <= REGISTRY_HYGIENE_SOURCE_LIMIT
