#!/usr/bin/env python3
"""Markdown row rendering for the soak report.

Leaf of ``scripts/source_policy_soak_report.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.source_policy_soak_report_spec import (
    PROVIDER_COVERAGE_GAP_BUCKETS,
    PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT,
    _int_value,
    as_json_list,
    as_json_object,
    clean_text,
    json_object_rows,
)

__all__ = [
    "Any",
    "PROVIDER_COVERAGE_GAP_BUCKETS",
    "PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT",
    "_blocked_candidates_markdown_rows",
    "_conservative_cleanup_blocked_markdown_rows",
    "_conservative_cleanup_markdown_rows",
    "_int_value",
    "_markdown_cell",
    "_markdown_joined_values",
    "_markdown_table",
    "_migration_link_disambiguation_blocker_summary",
    "_provider_coverage_gap_markdown_rows",
    "_provider_coverage_next_action_markdown_rows",
    "_review_candidates_markdown_rows",
    "_static_scope_conflict_markdown_rows",
    "_static_scope_patch_proposal_markdown_rows",
    "_suppression_eligibility_markdown_rows",
    "as_json_list",
    "as_json_object",
    "clean_text",
    "json_object_rows",
]


def _markdown_table(rows: list[tuple[str, Any]]) -> list[str]:
    lines = ["| Metric | Value |", "|--------|-------|"]
    for key, value in rows:
        lines.append(f"| `{key}` | `{value}` |")
    return lines


def _markdown_cell(value: Any) -> str:
    return clean_text(value).replace("|", "\\|")


def _provider_coverage_gap_markdown_rows(report: dict[str, Any]) -> list[str]:
    gaps = as_json_object(as_json_object(report.get("sections")).get("providerCoverageGaps"))
    rows = ["| Bucket | Source | Provider | Reason | Status | Registry |"]
    rows.append("|--------|--------|----------|--------|--------|----------|")
    for bucket in PROVIDER_COVERAGE_GAP_BUCKETS:
        bucket_payload = as_json_object(gaps.get(bucket))
        for example in json_object_rows(bucket_payload.get("examples")):
            source = clean_text(example.get("sourceName")) or clean_text(
                example.get("sourceIdentity")
            )
            provider = (
                clean_text(example.get("providerSourceName"))
                or clean_text(example.get("providerSourceIdentity"))
                or clean_text(example.get("detectedProviderFamily"))
            )
            status = clean_text(example.get("providerCoverageStatus")) or clean_text(
                example.get("latestFetchStatus")
            )
            registry = clean_text(example.get("registryBucket")) or clean_text(
                example.get("registryState")
            )
            rows.append(
                "| "
                f"`{bucket}` | "
                f"{_markdown_cell(source)} | "
                f"{_markdown_cell(provider)} | "
                f"`{_markdown_cell(example.get('blockerReason'))}` | "
                f"`{_markdown_cell(status)}` | "
                f"`{_markdown_cell(registry)}` |"
            )
    if len(rows) == 2:
        rows.append("| none |  |  |  |  |  |")
    return rows


def _markdown_joined_values(values: Any) -> str:
    items = [clean_text(item) for item in as_json_list(values) if clean_text(item)]
    return ", ".join(items) if items else "none"


def _provider_coverage_next_action_markdown_rows(report: dict[str, Any]) -> list[str]:
    next_action = as_json_object(
        as_json_object(report.get("sections")).get("providerCoverageNextAction")
    )
    return _markdown_table(
        [
            ("action", clean_text(next_action.get("action")) or "none"),
            ("priority", int(next_action.get("priority") or 0)),
            ("requiresHumanApproval", bool(next_action.get("requiresHumanApproval"))),
            ("blockedBy", _markdown_joined_values(next_action.get("blockedBy"))),
            ("safeLocalCommands", _markdown_joined_values(next_action.get("safeLocalCommands"))),
            ("rationale", clean_text(next_action.get("rationale"))),
        ]
    )


def _review_candidates_markdown_rows(report: dict[str, Any]) -> list[str]:
    candidates = json_object_rows(
        as_json_object(
            as_json_object(report.get("sections")).get("providerCoverageLinkBackfill")
        ).get("reviewCandidates")
    )
    lines = [
        "| Provider | Selected static | Confidence | API eligible | Reason | Last kept | Last status | Why not high confidence |",
        "|----------|-----------------|------------|--------------|--------|-----------|-------------|-------------------------|",
    ]
    if not candidates:
        lines.append("| none | none | `0` | `false` | none | `0` | none | none |")
        return lines
    for candidate in candidates[:10]:
        evidence = as_json_object(candidate.get("sourceStateEvidence"))
        lines.append(
            "| "
            f"{clean_text(candidate.get('providerSourceName')) or clean_text(candidate.get('providerSourceId'))} | "
            f"{clean_text(candidate.get('selectedStaticSourceName')) or clean_text(candidate.get('selectedStaticSourceId'))} | "
            f"`{candidate.get('confidence')}` | "
            f"`{bool(candidate.get('apiEligible'))}` | "
            f"`{clean_text(candidate.get('resolutionReason'))}` | "
            f"`{_int_value(evidence.get('lastKeptCount'))}` | "
            f"`{clean_text(evidence.get('lastStatus'))}` | "
            f"{clean_text(candidate.get('whyNotHighConfidence')) or 'none'} |"
        )
    return lines


def _blocked_candidates_markdown_rows(report: dict[str, Any]) -> list[str]:
    candidates = json_object_rows(
        as_json_object(
            as_json_object(report.get("sections")).get("providerCoverageLinkBackfill")
        ).get("blockedCandidates")
    )
    lines = [
        "| Provider | Selected static | Confidence | Blockers | Evidence | Last kept | Last status | Last successful | Last fetched | Coverage status | Successes | Latest kept |",
        "|----------|-----------------|------------|----------|----------|-----------|-------------|----------------|--------------|-----------------|-----------|-------------|",
    ]
    if not candidates:
        lines.append(
            "| none | none | `0` | none | none | `0` | none | none | none | none | `0` | `0` |"
        )
        return lines
    for candidate in candidates[:10]:
        evidence = as_json_object(candidate.get("sourceStateEvidence"))
        blockers = ", ".join(
            clean_text(blocker)
            for blocker in as_json_list(candidate.get("blockers"))
            if clean_text(blocker)
        )
        evidence_reasons = ", ".join(
            clean_text(reason)
            for reason in as_json_list(candidate.get("evidenceReasons"))
            if clean_text(reason)
        )
        lines.append(
            "| "
            f"{clean_text(candidate.get('providerSourceName')) or clean_text(candidate.get('providerSourceId'))} | "
            f"{clean_text(candidate.get('selectedStaticSourceName')) or clean_text(candidate.get('selectedStaticSourceId'))} | "
            f"`{candidate.get('confidence')}` | "
            f"`{blockers or 'none'}` | "
            f"`{evidence_reasons or 'none'}` | "
            f"`{_int_value(evidence.get('lastKeptCount'))}` | "
            f"`{clean_text(evidence.get('lastStatus'))}` |"
            f" {clean_text(evidence.get('lastSuccessfulAt')) or 'none'} |"
            f" {clean_text(evidence.get('lastFetchedAt')) or 'none'} |"
            f" {clean_text(evidence.get('providerCoverageStatus')) or 'none'} |"
            f" `{_int_value(evidence.get('providerCoverageConsecutiveSuccesses'))}` |"
            f" `{_int_value(evidence.get('providerCoverageLatestKeptCount'))}` |"
        )
    return lines


def _migration_link_disambiguation_blocker_summary(report: dict[str, Any]) -> str:
    section = as_json_object(
        as_json_object(report.get("sections")).get("providerCoverageLinkBackfill")
    )
    counts = as_json_object(section.get("disambiguationBlockerCounts"))
    examples = json_object_rows(section.get("disambiguationBlockedExamples"))
    if not counts and not examples:
        return "none"
    count_summary = ", ".join(
        f"{clean_text(key).replace('_', ' ')} {int(value or 0):,}"
        for key, value in sorted(counts.items(), key=lambda item: (-int(item[1] or 0), item[0]))
        if clean_text(key)
    )
    if not count_summary:
        count_summary = "none"
    example_summary = " | ".join(
        f"{clean_text(example.get('providerSourceName')) or clean_text(example.get('providerSourceId')) or 'Unknown provider'} / "
        f"{clean_text(example.get('selectedStaticSourceName')) or clean_text(example.get('staticSourceName')) or clean_text(example.get('selectedStaticSourceId')) or clean_text(example.get('staticSourceId')) or 'Unknown static source'} / "
        f"{', '.join(clean_text(blocker).replace('_', ' ') for blocker in as_json_list(example.get('disambiguationBlockers')) if clean_text(blocker)) or 'none'}"
        for example in examples[:PROVIDER_COVERAGE_LINK_BACKFILL_EXAMPLE_LIMIT]
        if isinstance(example, dict)
    )
    if not example_summary:
        example_summary = "none"
    return f"{count_summary}. Examples: {example_summary}."


def _suppression_eligibility_markdown_rows(report: dict[str, Any]) -> list[str]:
    rows = json_object_rows(
        as_json_object(as_json_object(report.get("sections")).get("suppressionEligibility")).get(
            "missingLinkedStaticRows"
        )
    )
    lines = [
        "| Provider | Linked static | Readiness | Successes | Latest kept | Selection reason | Bucket | Loader match | Loader reason | Generated loader |",
        "|----------|---------------|-----------|-----------|-------------|------------------|--------|--------------|---------------|------------------|",
    ]
    if not rows:
        lines.append("| none | none | none | `0` | `0` | none | none | none | none | none |")
        return lines
    for row in rows[:10]:
        lines.append(
            "| "
            f"{clean_text(row.get('providerSourceName')) or clean_text(row.get('providerSourceId'))} | "
            f"{clean_text(row.get('migrationSourceName')) or clean_text(row.get('migrationSourceIdentity'))} | "
            f"`{clean_text(row.get('providerReplacementReadiness')) or 'unknown'}` | "
            f"`{_int_value(row.get('providerCoverageConsecutiveSuccesses'))}` | "
            f"`{_int_value(row.get('providerCoverageLatestKeptCount'))}` | "
            f"`{clean_text(row.get('selectionReason')) or clean_text(row.get('reason'))}` | "
            f"`{clean_text(row.get('registryBucket')) or 'unknown'}` | "
            f"`{clean_text(row.get('loaderNameMatchStatus')) or 'unknown'}` | "
            f"`{clean_text(row.get('loaderNotGeneratedReason')) or 'none'}` | "
            f"`{clean_text(row.get('generatedStaticLoaderName')) or 'unknown'}` |"
        )
    return lines


def _conservative_cleanup_markdown_rows(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Static | Provider | Action | Readiness | Freshness | Clean runs | Suppression evidence | Explicit action | Destructive |",
        "|--------|----------|--------|-----------|-----------|------------|----------------------|-----------------|-------------|",
    ]
    if not rows:
        lines.append("| none | none | none | none | none | `0` | none | `false` | `false` |")
        return lines
    for row in rows[:10]:
        lines.append(
            "| "
            f"{clean_text(row.get('staticSourceName')) or clean_text(row.get('staticSourceId'))} | "
            f"{clean_text(row.get('providerSourceName')) or clean_text(row.get('providerSourceId'))} | "
            f"`{clean_text(row.get('recommendedAction'))}` | "
            f"`{clean_text(row.get('proposalReadiness')) or 'actionable'}` | "
            f"`{clean_text(row.get('proposalFreshnessStatus')) or 'fresh'}` | "
            f"`{_int_value(row.get('cleanRunEvidenceCount'))}` | "
            f"`{clean_text(row.get('suppressionEvidenceStatus'))}:"
            f"{clean_text(row.get('suppressionEvidenceReason'))}` | "
            f"`{bool(row.get('requiresExplicitAdminAction'))}` | "
            f"`{bool(row.get('destructiveActionAllowed'))}` |"
        )
    return lines


def _conservative_cleanup_blocked_markdown_rows(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Static | Provider | Readiness | Freshness | Blockers | Clean runs | Static-only runs | Suppression evidence |",
        "|--------|----------|-----------|-----------|----------|------------|------------------|----------------------|",
    ]
    if not rows:
        lines.append("| none | none | none | none | none | `0` | `0` | none |")
        return lines
    for row in rows[:10]:
        blockers = ", ".join(
            clean_text(item) for item in row.get("blockers", []) if clean_text(item)
        )
        lines.append(
            "| "
            f"{clean_text(row.get('staticSourceName')) or clean_text(row.get('staticSourceId'))} | "
            f"{clean_text(row.get('providerSourceName')) or clean_text(row.get('providerSourceId'))} | "
            f"`{clean_text(row.get('proposalReadiness')) or 'blocked'}` | "
            f"`{clean_text(row.get('proposalFreshnessStatus')) or 'fresh'}` | "
            f"`{blockers or 'none'}` | "
            f"`{_int_value(row.get('cleanRunEvidenceCount'))}` | "
            f"`{_int_value(row.get('staticOnlyDetectedRunCount'))}` | "
            f"`{clean_text(row.get('suppressionEvidenceStatus'))}:"
            f"{clean_text(row.get('suppressionEvidenceReason'))}` |"
        )
    return lines


def _static_scope_conflict_markdown_rows(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Source | Classification | Action | Listing host | Off-listing hosts | Covered | Uncovered | Explicit action | Behavior change |",
        "|--------|----------------|--------|--------------|-------------------|---------|-----------|-----------------|-----------------|",
    ]
    if not rows:
        lines.append("| none | none | none | none | none | none | none | `false` | `false` |")
        return lines
    for row in rows[:10]:
        lines.append(
            "| "
            f"{clean_text(row.get('sourceName')) or clean_text(row.get('sourceId'))} | "
            f"`{clean_text(row.get('classification'))}` | "
            f"`{clean_text(row.get('recommendedAction'))}` | "
            f"`{clean_text(row.get('listingHost'))}` | "
            f"{', '.join(clean_text(host) for host in as_json_list(row.get('offListingHosts')) if clean_text(host)) or 'none'} | "
            f"{', '.join(clean_text(host) for host in as_json_list(row.get('coveredOffListingHosts')) if clean_text(host)) or 'none'} | "
            f"{', '.join(clean_text(host) for host in as_json_list(row.get('uncoveredOffListingHosts')) if clean_text(host)) or 'none'} | "
            f"`{bool(row.get('requiresExplicitAdminAction'))}` | "
            f"`{bool(row.get('behaviorChangeAllowed'))}` |"
        )
    return lines


def _static_scope_patch_proposal_markdown_rows(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Source | Proposed action | Remove pages | Keep pages | Apply allowed | Explicit action |",
        "|--------|-----------------|--------------|------------|---------------|-----------------|",
    ]
    if not rows:
        lines.append("| none | none | none | none | `false` | `false` |")
        return lines
    for row in rows[:10]:
        remove_pages = ", ".join(
            clean_text(page) for page in as_json_list(row.get("removePages")) if clean_text(page)
        )
        keep_pages = ", ".join(
            clean_text(page) for page in as_json_list(row.get("keepPages")) if clean_text(page)
        )
        lines.append(
            "| "
            f"{clean_text(row.get('sourceName')) or clean_text(row.get('sourceId'))} | "
            f"`{clean_text(row.get('proposedAction'))}` | "
            f"{remove_pages or 'none'} | "
            f"{keep_pages or 'none'} | "
            f"`{bool(row.get('applyAllowed'))}` | "
            f"`{bool(row.get('requiresExplicitAdminAction'))}` |"
        )
    return lines
