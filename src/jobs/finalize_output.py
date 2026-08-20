"""Output shaping, timing payload, and source policy export helpers.

AI boundary owns: final source row shaping, runtime timing payload updates, task progress
completion, and the source policy recommendation export.
AI boundary implement in: this file for output shaping; report writing and the finalization
conductor stay in sibling finalize_* leaves and ``pipeline_finalize.py``.
AI boundary search before contracts: fetch-report contracts, source policy recommendation
artifacts, and pipeline finalization tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline finalization tests.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from src.jobs.common.contracts_source_policy_recommendations import (
    read_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    read_source_policy_review_state_artifact,
)
from src.jobs.finalize_lifecycle import _runtime_timing_summary
from src.jobs.pipeline_runtime_summary import build_detailed_source_rows
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import json_object_rows

_EXPECTED_SOURCE_POLICY_EXPORT_EXCEPTIONS = (OSError, TypeError, ValueError)


def _update_runtime_timing_payload(
    *,
    runtime_payload: dict[str, Any],
    task_runtime: Any,
    source_reports: list[dict[str, Any]],
    run_started_mono: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    detailed_source_rows = build_detailed_source_rows(task_runtime.task_rows, source_reports)
    timing_summary = _runtime_timing_summary(
        detailed_source_rows,
        wall_clock_duration_ms=int((time.perf_counter() - run_started_mono) * 1000),
    )
    runtime_payload["slowestSources"] = list(timing_summary.get("slowestSources") or [])
    runtime_payload["staticDomainGateWaitMs"] = int(
        timing_summary.get("staticDomainGateWaitMs") or 0
    )
    runtime_payload["staticDetailBatchCount"] = int(
        timing_summary.get("staticDetailBatchCount") or 0
    )
    runtime_payload["staticAdaptiveStops"] = int(timing_summary.get("staticAdaptiveStops") or 0)
    runtime_payload["staticListingTimeoutStops"] = int(
        timing_summary.get("staticListingTimeoutStops") or 0
    )
    runtime_payload["staticListingBrowserFallbacks"] = int(
        timing_summary.get("staticListingBrowserFallbacks") or 0
    )
    runtime_payload["timingSummary"] = {
        "totalDurationMs": int(timing_summary.get("totalDurationMs") or 0),
        "wallClockDurationMs": int(timing_summary.get("wallClockDurationMs") or 0),
        "medianSourceDurationMs": int(timing_summary.get("medianSourceDurationMs") or 0),
        "p95SourceDurationMs": int(timing_summary.get("p95SourceDurationMs") or 0),
        "stageTotalsMs": dict(timing_summary.get("stageTotalsMs") or {}),
        "stageTop": list(timing_summary.get("stageTop") or []),
        "adapterTimings": list(timing_summary.get("adapterTimings") or []),
        "slowestAdapters": list(timing_summary.get("slowestAdapters") or []),
        "highCostLowYieldSources": list(timing_summary.get("highCostLowYieldSources") or []),
        "detailHeavySources": list(timing_summary.get("detailHeavySources") or []),
    }
    return detailed_source_rows, timing_summary


def _output_sizes(paths) -> tuple[int, int]:
    return (
        paths.json_path.stat().st_size if paths.json_path.exists() else 0,
        paths.light_json_path.stat().st_size if paths.light_json_path.exists() else 0,
    )


def _is_operational_excluded_row(row: dict[str, Any]) -> bool:
    if norm_text(row.get("status")) != "excluded":
        return False
    reason = clean_text(row.get("exclusionReason"))
    return reason != "only_sources_filter" and not reason.startswith("disabled_by_default:")


def _final_source_rows(
    detailed_source_rows: list[dict[str, Any]],
    source_reports: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in [item for item in detailed_source_rows if isinstance(item, dict)]:
        name = clean_text(row.get("name"))
        if not name:
            continue
        rows.append(row)
        seen.add(name)
    for row in [item for item in source_reports if isinstance(item, dict)]:
        name = clean_text(row.get("name"))
        if not name or name in seen:
            continue
        if not _is_operational_excluded_row(row):
            continue
        rows.append(dict(row))
        seen.add(name)
    return rows


def _completed_task_progress(summary: dict[str, Any]) -> dict[str, Any]:
    source_count = max(0, int(summary.get("sourceCount") or 0))
    failed_sources = max(0, int(summary.get("failedSources") or 0))
    excluded_sources = max(0, int(summary.get("excludedSources") or 0))
    successful_sources = max(0, int(summary.get("successfulSources") or 0))
    resolved_sources = successful_sources + failed_sources + excluded_sources
    output_count = max(0, int(summary.get("outputCount") or 0))
    return {
        "active": False,
        "phaseKey": "completed",
        "phaseLabel": "Completed",
        "mode": "determinate",
        "ratio": 1.0,
        "counts": {
            "sourceCount": source_count,
            "totalTasks": source_count,
            "queuedTasks": 0,
            "runningTasks": 0,
            "completedTasks": resolved_sources,
            "resolvedSources": resolved_sources,
            "outputCount": output_count,
            "failedSources": failed_sources,
            "excludedSources": excluded_sources,
        },
    }


def _export_source_policy_recommendations(
    *,
    report_payload: dict[str, Any],
    source_policy_recommendations_path: Path,
    source_policy_review_state_path: Path,
    finished_at: str,
) -> None:
    from src.jobs import pipeline_finalize as _pf

    source_policy_recommendation_warning = ""
    source_policy_review_state_warning = ""
    updated_recommendation_pair_count = len(
        json_object_rows(report_payload["redundantStaticProposals"].get("proposals"))
    )
    try:
        prior_recommendations, source_policy_recommendation_warning = (
            read_source_policy_recommendations_artifact(source_policy_recommendations_path)
        )
        source_policy_review_state, source_policy_review_state_warning = (
            read_source_policy_review_state_artifact(source_policy_review_state_path)
        )
        source_policy_recommendations = _pf.build_source_policy_recommendations_artifact(
            prior_artifact=prior_recommendations,
            redundant_static_proposals=report_payload["redundantStaticProposals"],
            observed_at=finished_at,
            review_state=source_policy_review_state,
        )
        _pf.write_atomic_if_changed(
            source_policy_recommendations_path,
            json.dumps(source_policy_recommendations, indent=2, ensure_ascii=False),
        )
        review_summary = source_policy_review_state.get("summary", {})
        report_payload["sourcePolicyRecommendationExport"] = {
            "status": "ok",
            "artifactPath": str(source_policy_recommendations_path),
            "reviewStatePath": str(source_policy_review_state_path),
            "updatedPairCount": updated_recommendation_pair_count,
            "reviewStatePairCount": int(review_summary.get("totalPairs") or 0),
            "manualForcePausedCount": int(review_summary.get("forcePausedCount") or 0),
            **(
                {"warning": source_policy_recommendation_warning}
                if source_policy_recommendation_warning
                else {}
            ),
            **(
                {"reviewStateWarning": source_policy_review_state_warning}
                if source_policy_review_state_warning
                else {}
            ),
        }
    except _EXPECTED_SOURCE_POLICY_EXPORT_EXCEPTIONS as exc:
        report_payload["sourcePolicyRecommendationExport"] = {
            "status": "warning",
            "artifactPath": str(source_policy_recommendations_path),
            "reviewStatePath": str(source_policy_review_state_path),
            "updatedPairCount": 0,
            "reviewStatePairCount": 0,
            "manualForcePausedCount": 0,
            "warning": f"source_policy_recommendation_export_failed:{type(exc).__name__}",
        }
