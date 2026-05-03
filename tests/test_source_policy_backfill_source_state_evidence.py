import json
from pathlib import Path

from scripts import source_policy_soak_report as soak


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _cdpr_provider() -> dict[str, str]:
    return {
        "id": "smartrecruiters:company_id:cdprojektred",
        "name": "CDPR Provider",
        "adapter": "smartrecruiters",
        "company_id": "CDPROJEKTRED",
        "providerBucket": "provider",
    }


def test_blocked_provider_coverage_links_expose_disambiguation_blockers() -> None:
    blocked_rows = [
        {"disambiguationBlockers": ["no_source_state_history"]},
        {"disambiguationBlockers": ["no_source_state_history"]},
        {"disambiguationBlockers": ["source_state_not_ok"]},
    ]
    report = {
        "sections": {
            "providerCoverageLinkBackfill": {
                "disambiguationBlockerCounts": soak._disambiguation_blocker_counts(blocked_rows),
                "disambiguationBlockedExamples": [
                    {
                        "providerSourceName": "CDPR Provider",
                        "selectedStaticSourceId": "static:cdpr-one",
                        "selectedStaticSourceName": "CDPR Static One",
                        "disambiguationBlockers": ["no_source_state_history"],
                    },
                    {
                        "providerSourceName": "CDPR Provider",
                        "selectedStaticSourceId": "static:cdpr-two",
                        "selectedStaticSourceName": "CDPR Static Two",
                        "disambiguationBlockers": ["source_state_not_ok"],
                    },
                ],
            }
        }
    }

    assert soak._disambiguation_blocker_counts(blocked_rows) == {
        "no_source_state_history": 2,
        "source_state_not_ok": 1,
    }
    summary = soak._migration_link_disambiguation_blocker_summary(report)
    assert summary == (
        "no source state history 2, source state not ok 1. "
        "Examples: CDPR Provider / CDPR Static One / no source state history | "
        "CDPR Provider / CDPR Static Two / source state not ok."
    )


def test_static_evidence_uses_source_state_aliases_and_provider_coverage_history(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    _write_json(data_dir / "jobs-fetch-report.json", {})
    _write_json(
        data_dir / "jobs-source-state.json",
        {
            "sources": {
                "static:cdpr-active": {
                    "lastStatus": "ok",
                    "lastKeptCount": 1,
                    "lastSuccessAt": "2026-01-01T00:00:00Z",
                    "lastRunAt": "2026-01-02T00:00:00Z",
                    "providerCoverageStatus": "validated_provider",
                    "providerCoverageConsecutiveSuccesses": 1,
                    "providerCoverageLatestKeptCount": 1,
                }
            }
        },
    )
    _write_json(
        data_dir / "source-registry-active.json",
        [
            _cdpr_provider(),
            {
                "id": "static:cdpr-active",
                "name": "CDPR Active Static",
                "adapter": "static",
                "listing_url": "https://cdprojektred.com/jobs",
            },
        ],
    )
    _write_json(data_dir / "source-registry-pending.json", [])

    report = soak.build_soak_report(data_dir)
    section = report["sections"]["providerCoverageLinkBackfill"]
    candidate = next(
        row for row in section["links"] if row["staticSourceId"] == "static:cdpr-active"
    )

    assert candidate["lastSuccessfulAt"] == "2026-01-01T00:00:00Z"
    assert candidate["lastFetchedAt"] == "2026-01-02T00:00:00Z"
    assert candidate["providerCoverageStatus"] == "validated_provider"
    assert candidate["providerCoverageConsecutiveSuccesses"] == 1
    assert candidate["providerCoverageLatestKeptCount"] == 1
    assert "insufficient_provider_success_history" in candidate["disambiguationBlockers"]


def test_static_only_evidence_stays_explicit_when_no_source_state_history() -> None:
    evidence = soak._static_evidence({}, {})

    assert "no_source_state_history" in evidence["disambiguationBlockers"]
    assert "static_only_evidence_present" in evidence["disambiguationBlockers"]


def test_equal_history_ambiguity_gets_group_blocker(tmp_path: Path) -> None:
    provider = _cdpr_provider()
    row_a = soak._provider_link_row(
        provider,
        {
            "staticSourceId": "static:cdpr-a",
            "staticSourceName": "CDPR Static A",
            "staticUrl": "https://cdprojektred.com/jobs-a",
            "registryState": "active",
            "lastKeptCount": 3,
            "lastStatus": "ok",
            "lastSuccessfulAt": "2026-01-01T00:00:00Z",
            "lastFetchedAt": "2026-01-02T00:00:00Z",
            "providerCoverageStatus": "validated_provider",
            "providerCoverageConsecutiveSuccesses": 3,
            "providerCoverageLatestKeptCount": 3,
            "evidenceScore": 40,
            "evidenceReasons": ["active_registry_row", "source_state_kept_jobs"],
            "disambiguationBlockers": ["ambiguous_static_match"],
        },
        confidence=0.65,
        reasons=["redundant_static_rule_exact_match"],
        blockers=["ambiguous_static_match"],
        recommended_action="ambiguous_static_match",
        provider_id_field="company_id",
        provider_id_value="CDPROJEKTRED",
    )
    row_b = soak._provider_link_row(
        provider,
        {
            "staticSourceId": "static:cdpr-b",
            "staticSourceName": "CDPR Static B",
            "staticUrl": "https://cdprojektred.com/jobs-b",
            "registryState": "active",
            "lastKeptCount": 3,
            "lastStatus": "ok",
            "lastSuccessfulAt": "2026-01-01T00:00:00Z",
            "lastFetchedAt": "2026-01-02T00:00:00Z",
            "providerCoverageStatus": "validated_provider",
            "providerCoverageConsecutiveSuccesses": 3,
            "providerCoverageLatestKeptCount": 3,
            "evidenceScore": 40,
            "evidenceReasons": ["active_registry_row", "source_state_kept_jobs"],
            "disambiguationBlockers": ["ambiguous_static_match"],
        },
        confidence=0.65,
        reasons=["redundant_static_rule_exact_match"],
        blockers=["ambiguous_static_match"],
        recommended_action="ambiguous_static_match",
        provider_id_field="company_id",
        provider_id_value="CDPROJEKTRED",
    )

    resolved_rows, resolution = soak._resolve_provider_link_rows([row_a, row_b])

    assert resolution is None
    assert len(resolved_rows) == 2
    assert all(
        "multiple_static_candidates_with_equal_history" in row["disambiguationBlockers"]
        for row in resolved_rows
    )
