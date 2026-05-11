#!/usr/bin/env python3
"""Summarize dedup pressure from a jobs fetch report.

The report is intentionally read-only. It helps choose the next dedup slice from
fresh pipeline evidence without changing registry, source-policy, or dedup state.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FETCH_REPORT_NAME = "jobs-fetch-report.json"
PARSER_DIRECTORY_CAUSES = {
    "parser_or_directory_text_pollution",
    "category_or_department_bucket",
    "listing_page_bundle",
}
NON_PRIMARY_MERGE_COUNT_KEYS = {
    "secondaryKey",
    "sparseIdentity",
    "socialKey",
    "unknown",
    "knownMirrorPair",
}
MONITOR_NON_PRIMARY_MERGE_COUNT_KEYS = {
    "monitorSecondaryKey": "secondaryKey",
    "monitorSparseIdentity": "sparseIdentity",
    "monitorSocialKey": "socialKey",
    "monitorUnknown": "unknown",
}


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise FileNotFoundError(f"Missing report file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in report file: {path}") from exc


def _candidate_roots(repo_root: Path) -> list[Path]:
    roots: list[Path] = []
    for candidate in (repo_root / "data", repo_root / "_out" / "latest"):
        if candidate.is_dir():
            roots.append(candidate)
    runs_root = repo_root / "_out" / "runs"
    if runs_root.is_dir():
        roots.extend(path for path in runs_root.iterdir() if path.is_dir())
    return roots


def _resolve_latest_fetch_report(repo_root: Path) -> Path:
    candidates: list[Path] = []
    for root in _candidate_roots(repo_root):
        candidate = root / FETCH_REPORT_NAME
        if candidate.is_file():
            candidates.append(candidate)
    if not candidates:
        roots = ", ".join(str(path) for path in _candidate_roots(repo_root))
        raise FileNotFoundError(
            f"Could not find {FETCH_REPORT_NAME} under report roots: {roots or repo_root}"
        )
    return max(candidates, key=lambda path: (int(path.stat().st_mtime_ns), str(path)))


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _positive_counts(mapping: dict[str, Any]) -> dict[str, int]:
    return {str(key): count for key, value in mapping.items() if (count := _int(value)) > 0}


def _top_counts(mapping: dict[str, Any], *, limit: int) -> list[dict[str, Any]]:
    rows = [
        {"cause": str(key), "count": count}
        for key, value in mapping.items()
        if (count := _int(value)) > 0
    ]
    rows.sort(key=lambda row: (-int(row["count"]), str(row["cause"])))
    return rows[: max(0, int(limit or 0))]


def _sample_review_queue(rows: list[Any], *, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in rows[: max(0, int(limit or 0))]:
        row = _as_dict(raw)
        if not row:
            continue
        out.append(
            {
                "title": str(row.get("title") or ""),
                "company": str(row.get("company") or ""),
                "suspectedCause": str(row.get("suspectedCause") or ""),
                "recommendedReviewAction": str(row.get("recommendedReviewAction") or ""),
                "sourceBundleCount": _int(row.get("sourceBundleCount")),
                "bundleEvidenceOrigin": str(row.get("bundleEvidenceOrigin") or ""),
                "jobLink": str(row.get("jobLink") or ""),
            }
        )
    return out


def _sample_family(row: dict[str, Any]) -> tuple[str, str]:
    title = str(row.get("title") or "").strip().lower()
    source = str(row.get("incomingSource") or "").strip().lower()
    link = str(row.get("incomingJobLink") or "").strip().lower()
    static_source = source.startswith("static_source::")
    parser_noise_fragments = (
        "browser does not support",
        "dev insights",
        "welcome to talentnetwork",
        "find a thrilling career",
        "recruitment scam advisory",
        "join the community",
        "create a proxy",
    )
    parser_noise_links = (
        "footer",
        "/careers/ioi-",
        "/search?",
        "/career/",
        "/careers/",
    )
    if static_source and (
        any(fragment in title for fragment in parser_noise_fragments)
        or any(fragment in link for fragment in parser_noise_links)
    ):
        return "parser_static_noise", "fix_static_extraction_filter"
    if not static_source and ("/rd/" in link or "gracklehq.com" in link):
        return "provider_or_job_detail_candidate", "review_provider_identity_or_sparse_key"
    return "needs_manual_review", "manual_dedup_review"


def _count_sample_families(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        family = str(row.get("sampleFamily") or "")
        if not family:
            continue
        counts[family] = counts.get(family, 0) + 1
    return counts


def _sample_merge_examples(rows: list[Any], *, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in rows[: max(0, int(limit or 0))]:
        row = _as_dict(raw)
        if not row:
            continue
        sample = {
            "mergeReason": str(row.get("mergeReason") or ""),
            "title": str(row.get("title") or ""),
            "company": str(row.get("company") or ""),
            "incomingSource": str(row.get("incomingSource") or ""),
            "incomingJobLink": str(row.get("incomingJobLink") or ""),
            "existingDedupKey": str(row.get("existingDedupKey") or ""),
            "bundleEvidenceOrigin": str(row.get("bundleEvidenceOrigin") or ""),
            "blocksLifecycle": bool(row.get("blocksLifecycle")),
            "recommendedReviewAction": str(row.get("recommendedReviewAction") or ""),
            "suspectedCause": str(row.get("suspectedCause") or ""),
        }
        sample_family, recommended_next_action = _sample_family(sample)
        sample["sampleFamily"] = sample_family
        sample["recommendedNextAction"] = recommended_next_action
        out.append(sample)
    return out


def _non_primary_merge_breakdown(
    dedup: dict[str, Any], gate: dict[str, Any], *, limit: int
) -> dict[str, Any]:
    gate_counts = _positive_counts(_as_dict(gate.get("currentRunNonPrimaryMergeCounts")))
    reason_counts = {
        key: count for key, count in gate_counts.items() if key in NON_PRIMARY_MERGE_COUNT_KEYS
    }
    monitor_reason_counts = {
        label: _int(gate_counts.get(key))
        for key, label in MONITOR_NON_PRIMARY_MERGE_COUNT_KEYS.items()
        if _int(gate_counts.get(key)) > 0
    }
    examples_by_reason = _as_dict(dedup.get("currentRunMergeExamplesByReason"))
    blocking_examples_by_reason = _as_dict(dedup.get("currentRunBlockingMergeExamplesByReason"))
    reason_rows: list[dict[str, Any]] = []
    for reason, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0])):
        raw_examples = _as_list(blocking_examples_by_reason.get(reason))
        if not raw_examples:
            raw_examples = [
                row
                for row in _as_list(examples_by_reason.get(reason))
                if _as_dict(row).get("blocksLifecycle") is True
            ]
        samples = _sample_merge_examples(raw_examples, limit=limit)
        reason_rows.append(
            {
                "reason": reason,
                "count": count,
                "sampleCount": len(raw_examples),
                "sampleFamilyCounts": _count_sample_families(samples),
                "samples": samples,
            }
        )
    overall_sample_family_counts: dict[str, int] = {}
    for row in reason_rows:
        for family, count in _as_dict(row.get("sampleFamilyCounts")).items():
            overall_sample_family_counts[family] = overall_sample_family_counts.get(
                family, 0
            ) + _int(count)
    return {
        "totalBlocking": _int(gate_counts.get("blocking")),
        "totalMonitor": _int(gate_counts.get("monitor")),
        "reasonCounts": reason_counts,
        "monitorReasonCounts": monitor_reason_counts,
        "reasonRows": reason_rows,
        "sampleFamilyCounts": overall_sample_family_counts,
        "currentRunBlockingReviewQueueCauseCounts": _positive_counts(
            _as_dict(dedup.get("currentRunBlockingReviewQueueCauseCounts"))
        ),
        "currentRunMonitorReviewQueueCauseCounts": _positive_counts(
            _as_dict(dedup.get("currentRunMonitorReviewQueueCauseCounts"))
        ),
        "mergeReasonCounts": _positive_counts(_as_dict(dedup.get("mergeReasonCounts"))),
        "mergeGateTierCounts": _positive_counts(
            _as_dict(dedup.get("currentRunMergeGateTierCounts"))
        ),
        "sourceBundleComposition": _positive_counts(_as_dict(dedup.get("sourceBundleComposition"))),
        "identityShapeCounts": _positive_counts(_as_dict(dedup.get("identityShapeCounts"))),
        "identityQualityCounts": _positive_counts(_as_dict(dedup.get("identityQualityCounts"))),
        "googleSheetsRoleBucketAuditCounts": _positive_counts(
            _as_dict(dedup.get("googleSheetsRoleBucketAuditCounts"))
        ),
    }


def build_dedup_pressure_summary(
    *,
    fetch_report_path: Path | None = None,
    repo_root: Path | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    root = Path(repo_root or REPO_ROOT).resolve()
    report_path = (
        Path(fetch_report_path).resolve()
        if fetch_report_path
        else _resolve_latest_fetch_report(root)
    )
    payload = _read_json(report_path)
    if not isinstance(payload, dict):
        raise TypeError(f"Fetch report must be a JSON object: {report_path}")
    dedup = _as_dict(payload.get("dedupEvidence"))
    gate = _as_dict(dedup.get("dedupAuditGate"))
    review_queue_causes = _positive_counts(_as_dict(dedup.get("reviewQueueCauseCounts")))
    provider_static_gate = _positive_counts(
        _as_dict(dedup.get("providerStaticDisagreementGateCounts"))
    )
    provider_static_classes = _positive_counts(
        _as_dict(dedup.get("providerStaticDisagreementClassificationCounts"))
    )
    google_sheets_audit = _as_dict(dedup.get("googleSheetsRoleBucketAudit"))
    google_sheets_classes = _positive_counts(
        _as_dict(google_sheets_audit.get("classificationCounts"))
    )
    parser_directory_total = sum(
        count for cause, count in review_queue_causes.items() if cause in PARSER_DIRECTORY_CAUSES
    )
    static_url_variant_count = _int(provider_static_classes.get("static_parser_url_variant"))
    gate_blockers = [str(item) for item in _as_list(gate.get("blockers")) if str(item).strip()]
    gate_warnings = [str(item) for item in _as_list(gate.get("warnings")) if str(item).strip()]
    return {
        "repoRoot": str(root),
        "fetchReport": str(report_path),
        "fetchStartedAt": str(payload.get("startedAt") or ""),
        "fetchFinishedAt": str(payload.get("finishedAt") or ""),
        "dedupAuditGate": {
            "status": str(gate.get("status") or ""),
            "lifecycleUxReady": bool(gate.get("lifecycleUxReady")),
            "blockers": gate_blockers,
            "warnings": gate_warnings,
            "blockerCount": len(gate_blockers),
            "warningCount": len(gate_warnings),
            "googleSheetsRoleBucketUnresolvedCount": _int(
                gate.get("googleSheetsRoleBucketUnresolvedCount")
            ),
            "providerStaticDisagreementBlockedCount": _int(
                gate.get("providerStaticDisagreementBlockedCount")
            ),
            "currentRunBlockingReviewQueueCount": _int(
                gate.get("currentRunBlockingReviewQueueCount")
            ),
            "currentRunMonitorReviewQueueCount": _int(
                gate.get("currentRunMonitorReviewQueueCount")
            ),
            "currentRunNonPrimaryMergeCounts": _as_dict(
                gate.get("currentRunNonPrimaryMergeCounts")
            ),
        },
        "reviewQueueCauseCounts": review_queue_causes,
        "topSuspectedCauses": _top_counts(review_queue_causes, limit=limit),
        "providerStatic": {
            "gateCounts": provider_static_gate,
            "classificationCounts": provider_static_classes,
            "staticUrlVariantCount": static_url_variant_count,
        },
        "googleSheetsRoleBuckets": {
            "unresolvedCount": _int(gate.get("googleSheetsRoleBucketUnresolvedCount")),
            "guardBlockedCount": _int(gate.get("googleSheetsRoleBucketGuardBlockedCount")),
            "historicalCount": _int(gate.get("googleSheetsRoleBucketHistoricalCount")),
            "classificationCounts": google_sheets_classes,
        },
        "staticAndParserPressure": {
            "parserDirectoryCauseCount": parser_directory_total,
            "staticUrlVariantCount": static_url_variant_count,
            "nonProviderUrlIdentityNeedsReviewCount": _int(
                review_queue_causes.get("non_provider_url_identity_needs_review")
            ),
            "unknownCauseCount": _int(review_queue_causes.get("unknown")),
        },
        "currentRunNonPrimaryMergeBreakdown": _non_primary_merge_breakdown(
            dedup, gate, limit=limit
        ),
        "reviewQueuePreview": _sample_review_queue(_as_list(dedup.get("reviewQueue")), limit=limit),
    }


def render_text_summary(summary: dict[str, Any]) -> str:
    gate = _as_dict(summary.get("dedupAuditGate"))
    provider_static = _as_dict(summary.get("providerStatic"))
    google_sheets = _as_dict(summary.get("googleSheetsRoleBuckets"))
    static_pressure = _as_dict(summary.get("staticAndParserPressure"))
    non_primary = _as_dict(summary.get("currentRunNonPrimaryMergeBreakdown"))
    lines = [
        "# Dedup Pressure Report",
        "",
        f"- Fetch report: {str(summary.get('fetchReport') or '')}",
        f"- Fetch started: {str(summary.get('fetchStartedAt') or '')}",
        f"- Fetch finished: {str(summary.get('fetchFinishedAt') or '')}",
        "",
        "## Gate",
        f"- Status: {str(gate.get('status') or '')}",
        f"- Lifecycle UX ready: {str(bool(gate.get('lifecycleUxReady'))).lower()}",
        f"- Blockers: {int(gate.get('blockerCount') or 0)}",
        f"- Warnings: {int(gate.get('warningCount') or 0)}",
        f"- Google Sheets unresolved role buckets: {int(gate.get('googleSheetsRoleBucketUnresolvedCount') or 0)}",
        f"- Provider/static blocked disagreements: {int(gate.get('providerStaticDisagreementBlockedCount') or 0)}",
        f"- Current-run blocking review queue: {int(gate.get('currentRunBlockingReviewQueueCount') or 0)}",
        f"- Current-run monitor review queue: {int(gate.get('currentRunMonitorReviewQueueCount') or 0)}",
        "",
        "## Top Suspected Causes",
    ]
    top_causes = [
        row for row in _as_list(summary.get("topSuspectedCauses")) if isinstance(row, dict)
    ]
    if top_causes:
        for row in top_causes:
            lines.append(f"- {str(row.get('cause') or '')}: {int(row.get('count') or 0)}")
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Provider/Static",
            f"- Gate counts: {json.dumps(_as_dict(provider_static.get('gateCounts')), sort_keys=True)}",
            f"- Classification counts: {json.dumps(_as_dict(provider_static.get('classificationCounts')), sort_keys=True)}",
            f"- Static URL variants: {int(provider_static.get('staticUrlVariantCount') or 0)}",
            "",
            "## Google Sheets Role Buckets",
            f"- Unresolved: {int(google_sheets.get('unresolvedCount') or 0)}",
            f"- Guard blocked: {int(google_sheets.get('guardBlockedCount') or 0)}",
            f"- Historical: {int(google_sheets.get('historicalCount') or 0)}",
            f"- Classification counts: {json.dumps(_as_dict(google_sheets.get('classificationCounts')), sort_keys=True)}",
            "",
            "## Static And Parser Pressure",
            f"- Parser/directory causes: {int(static_pressure.get('parserDirectoryCauseCount') or 0)}",
            f"- Static URL variants: {int(static_pressure.get('staticUrlVariantCount') or 0)}",
            f"- Non-provider URL identity needs review: {int(static_pressure.get('nonProviderUrlIdentityNeedsReviewCount') or 0)}",
            f"- Unknown causes: {int(static_pressure.get('unknownCauseCount') or 0)}",
            "",
            "## Current-Run Non-Primary Merge Breakdown",
            f"- Total blocking: {int(non_primary.get('totalBlocking') or 0)}",
            f"- Total monitor: {int(non_primary.get('totalMonitor') or 0)}",
            f"- Reason counts: {json.dumps(_as_dict(non_primary.get('reasonCounts')), sort_keys=True)}",
            f"- Monitor reason counts: {json.dumps(_as_dict(non_primary.get('monitorReasonCounts')), sort_keys=True)}",
            f"- Current-run review causes: {json.dumps(_as_dict(non_primary.get('currentRunBlockingReviewQueueCauseCounts')), sort_keys=True)}",
            f"- Current-run monitor causes: {json.dumps(_as_dict(non_primary.get('currentRunMonitorReviewQueueCauseCounts')), sort_keys=True)}",
            f"- Gate tier counts: {json.dumps(_as_dict(non_primary.get('mergeGateTierCounts')), sort_keys=True)}",
            f"- Source bundle composition: {json.dumps(_as_dict(non_primary.get('sourceBundleComposition')), sort_keys=True)}",
            f"- Identity shapes: {json.dumps(_as_dict(non_primary.get('identityShapeCounts')), sort_keys=True)}",
            f"- Identity quality: {json.dumps(_as_dict(non_primary.get('identityQualityCounts')), sort_keys=True)}",
            f"- Google Sheets audit: {json.dumps(_as_dict(non_primary.get('googleSheetsRoleBucketAuditCounts')), sort_keys=True)}",
            f"- Sample families: {json.dumps(_as_dict(non_primary.get('sampleFamilyCounts')), sort_keys=True)}",
            "",
            "### Non-Primary Merge Samples",
        ]
    )
    reason_rows = [row for row in _as_list(non_primary.get("reasonRows")) if isinstance(row, dict)]
    if reason_rows:
        for row in reason_rows:
            lines.append(
                f"- {str(row.get('reason') or '')}: count={int(row.get('count') or 0)}, "
                f"samples={int(row.get('sampleCount') or 0)}"
            )
            for sample in _as_list(row.get("samples"))[:3]:
                sample_row = _as_dict(sample)
                if not sample_row:
                    continue
                lines.append(
                    "  - "
                    + " | ".join(
                        [
                            f"{str(sample_row.get('company') or '')} - {str(sample_row.get('title') or '')}",
                            f"source={str(sample_row.get('incomingSource') or '')}",
                            f"cause={str(sample_row.get('suspectedCause') or '')}",
                            f"family={str(sample_row.get('sampleFamily') or '')}",
                            f"next={str(sample_row.get('recommendedNextAction') or '')}",
                        ]
                    )
                )
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Review Queue Preview",
        ]
    )
    preview = [row for row in _as_list(summary.get("reviewQueuePreview")) if isinstance(row, dict)]
    if preview:
        for row in preview:
            lines.append(
                "- "
                + " | ".join(
                    [
                        f"{str(row.get('company') or '')} - {str(row.get('title') or '')}",
                        f"cause={str(row.get('suspectedCause') or '')}",
                        f"action={str(row.get('recommendedReviewAction') or '')}",
                        f"sources={int(row.get('sourceBundleCount') or 0)}",
                    ]
                )
            )
    else:
        lines.append("- None")
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize dedup pressure from a fetch report.")
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root containing data/ and _out/ run locations.",
    )
    parser.add_argument(
        "--fetch-report",
        default="",
        help="Specific jobs-fetch-report.json path. Defaults to the newest local fetch report.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of cause and review preview rows to show.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args(argv)
    try:
        summary = build_dedup_pressure_summary(
            fetch_report_path=Path(str(args.fetch_report)) if args.fetch_report else None,
            repo_root=Path(str(args.repo_root)),
            limit=int(args.limit),
        )
    except Exception as exc:  # noqa: BLE001
        print(str(exc), file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(render_text_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
