"""Breakdown helpers for static-source reporting summaries.

AI boundary owns: static-source reporting breakdowns and grouped summary payloads.
AI boundary implement in: this file for reporting breakdown assembly; source execution and contracts stay in their own leaves.
AI boundary search before contracts: pipeline finalization, reporting summary helpers, and jobs reporting tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs reporting tests.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from src.jobs.text_utils import clean_text, norm_text

UNKNOWN_STATIC_BREAKDOWN_SHAPES = (
    "no_jobs_extracted",
    "transport_network",
    "anti_bot_challenge",
    "other_static",
)
NEEDS_REVIEW_BREAKDOWN_SHAPES = (
    "blank_residue",
    "no_jobs_extracted",
    "transport_network",
    "anti_bot_challenge",
    "site_changed",
    "no_openings",
    "ambiguous_review",
    "other_static",
)


def _classify_unknown_static_shape(report: dict[str, Any]) -> str:
    error_lower = clean_text(report.get("error")).lower()
    failure_bucket = norm_text(report.get("failureBucket"))
    zero_kept = norm_text(report.get("zeroKeptClassification"))
    if failure_bucket == "no_openings" or zero_kept == "legit_empty":
        return "no_jobs_extracted"
    if failure_bucket == "js_required" or zero_kept == "broken_extraction":
        return "no_jobs_extracted"
    if "no jobs extracted from source pages" in error_lower:
        return "no_jobs_extracted"
    if any(
        marker in error_lower
        for marker in (
            "timeout",
            "timed out",
            "time_budget_exceeded",
            "network error",
            "fetch failed",
            "connection reset",
            "connection aborted",
            "name resolution",
            "temporary failure",
            "dns",
        )
    ):
        return "transport_network"
    if failure_bucket == "timeout":
        return "transport_network"
    if any(
        marker in error_lower
        for marker in (
            "429",
            "403",
            "blocked",
            "captcha",
            "challenge",
            "too many requests",
            "rate limit",
        )
    ):
        return "anti_bot_challenge"
    return "other_static"


def _classify_needs_review_shape(report: dict[str, Any]) -> str:
    error_lower = clean_text(report.get("error")).lower()
    failure_bucket = norm_text(report.get("failureBucket"))
    zero_kept = norm_text(report.get("zeroKeptClassification"))

    if not failure_bucket or not zero_kept:
        return "blank_residue"
    if failure_bucket == "no_openings" or zero_kept == "legit_empty":
        return "no_openings"
    if "no jobs extracted from source pages" in error_lower:
        return "no_jobs_extracted"
    if failure_bucket == "js_required" or zero_kept == "broken_extraction":
        return "no_jobs_extracted"
    if failure_bucket == "site_changed" or "site changed" in error_lower:
        return "site_changed"
    if failure_bucket == "timeout" or any(
        marker in error_lower
        for marker in (
            "timeout",
            "timed out",
            "time_budget_exceeded",
            "network error",
            "fetch failed",
            "connection reset",
            "connection aborted",
            "name resolution",
            "temporary failure",
            "dns",
        )
    ):
        return "transport_network"
    if failure_bucket == "anti_bot_or_challenge" or any(
        marker in error_lower
        for marker in (
            "429",
            "403",
            "blocked",
            "captcha",
            "challenge",
            "too many requests",
            "rate limit",
        )
    ):
        return "anti_bot_challenge"
    if failure_bucket == "needs_review" or zero_kept == "needs_review":
        return "ambiguous_review"
    return "other_static"


def _classify_blank_residue_shape(report: dict[str, Any]) -> str:
    failure_bucket = norm_text(report.get("failureBucket"))
    zero_kept = norm_text(report.get("zeroKeptClassification"))
    if not failure_bucket and not zero_kept:
        return "blank_residue"
    return _classify_needs_review_shape(report)


def _has_needs_review_marker(report: dict[str, Any]) -> bool:
    return (
        norm_text(report.get("classification")) == "needs_review"
        or norm_text(report.get("failureBucket")) == "needs_review"
        or norm_text(report.get("zeroKeptClassification")) == "needs_review"
    )


def _include_needs_review_report(report: dict[str, Any]) -> bool:
    return int(report.get("keptCount") or 0) == 0 and (
        norm_text(report.get("failureBucket")) in {"needs_review", ""}
        or norm_text(report.get("zeroKeptClassification")) in {"needs_review", ""}
    )


def _build_breakdown(
    source_reports: Sequence[dict[str, Any]],
    *,
    shapes: tuple[str, ...],
    include_report: Callable[[dict[str, Any]], bool],
    classify_shape: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    by_shape: dict[str, dict[str, Any]] = {
        shape: {"count": 0, "totalDurationMs": 0, "examples": []} for shape in shapes
    }
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        if clean_text(report.get("adapter")) != "static":
            continue
        if not include_report(report):
            continue
        shape = classify_shape(report)
        duration_ms = max(0, int(report.get("durationMs") or 0))
        entry = {
            "name": clean_text(report.get("name")) or "unknown",
            "studio": clean_text(report.get("studio")) or "",
            "adapter": clean_text(report.get("adapter")) or "unknown",
            "durationMs": duration_ms,
            "shape": shape,
            "error": clean_text(report.get("error")),
            "keptCount": int(report.get("keptCount") or 0),
            "fetchedCount": int(report.get("fetchedCount") or 0),
            "failureBucket": clean_text(report.get("failureBucket")) or "",
            "zeroKeptClassification": clean_text(report.get("zeroKeptClassification")) or "",
        }
        rows.append(entry)
        bucket = by_shape[shape]
        bucket["count"] += 1
        bucket["totalDurationMs"] += duration_ms
        if len(bucket["examples"]) < 3:
            bucket["examples"].append(entry["name"])

    top_by_wall_time = sorted(
        rows,
        key=lambda row: (
            -int(row.get("durationMs") or 0),
            clean_text(row.get("studio")),
            clean_text(row.get("name")),
        ),
    )[:20]
    by_frequency = sorted(
        (
            {
                "shape": shape,
                "count": values["count"],
                "totalDurationMs": values["totalDurationMs"],
                "examples": list(values["examples"]),
                "share": (float(values["count"]) / float(len(rows)) if rows else 0.0),
            }
            for shape, values in by_shape.items()
        ),
        key=lambda row: (
            -int(row.get("count") or 0),
            -int(row.get("totalDurationMs") or 0),
            row["shape"],
        ),
    )
    return {
        "byShape": by_shape,
        "topByWallTime": top_by_wall_time,
        "topByFrequency": by_frequency,
    }


def build_unknown_static_breakdown(
    source_reports: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    return _build_breakdown(
        source_reports,
        shapes=UNKNOWN_STATIC_BREAKDOWN_SHAPES,
        include_report=lambda report: (
            not (
                int(report.get("keptCount") or 0) > 0
                and norm_text(report.get("failureBucket")) not in {"unknown", ""}
            )
        ),
        classify_shape=_classify_unknown_static_shape,
    )


def build_needs_review_breakdown(
    source_reports: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    breakdown = _build_breakdown(
        source_reports,
        shapes=NEEDS_REVIEW_BREAKDOWN_SHAPES,
        include_report=_include_needs_review_report,
        classify_shape=_classify_needs_review_shape,
    )
    breakdown["rawMarkerCount"] = sum(
        1
        for report in source_reports
        if isinstance(report, dict) and _has_needs_review_marker(report)
    )
    breakdown["includedCount"] = sum(
        int(values.get("count") or 0)
        for values in (breakdown.get("byShape") or {}).values()
        if isinstance(values, dict)
    )
    return breakdown


def build_blank_residue_breakdown(
    source_reports: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    return _build_breakdown(
        source_reports,
        shapes=NEEDS_REVIEW_BREAKDOWN_SHAPES,
        include_report=lambda report: (
            clean_text(report.get("status")) != "excluded"
            and int(report.get("keptCount") or 0) == 0
            and not (
                norm_text(report.get("failureBucket"))
                and norm_text(report.get("zeroKeptClassification"))
            )
        ),
        classify_shape=_classify_blank_residue_shape,
    )
