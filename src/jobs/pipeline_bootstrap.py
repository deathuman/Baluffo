from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelinePaths:
    output_dir: Path
    json_path: Path
    csv_path: Path
    light_json_path: Path
    report_path: Path
    success_cache_path: Path
    source_state_path: Path
    lifecycle_state_path: Path
    browser_fallback_queue_path: Path
    parser_regression_queue_path: Path
    source_policy_recommendations_path: Path
    source_policy_review_state_path: Path
    task_state_path: Path
    pending_registry_path: Path
    approval_state_path: Path


def build_pipeline_paths(output_dir: Path) -> PipelinePaths:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    return PipelinePaths(
        output_dir=root,
        json_path=root / "jobs-unified.json",
        csv_path=root / "jobs-unified.csv",
        light_json_path=root / "jobs-unified-light.json",
        report_path=root / "jobs-fetch-report.json",
        success_cache_path=root / "jobs-success-cache.json",
        source_state_path=root / "jobs-source-state.json",
        lifecycle_state_path=root / "jobs-lifecycle-state.json",
        browser_fallback_queue_path=root / "jobs-browser-fallback-queue.json",
        parser_regression_queue_path=root / "jobs-parser-regression-queue.json",
        source_policy_recommendations_path=root / "source-policy-recommendations.json",
        source_policy_review_state_path=root / "source-policy-review-state.json",
        task_state_path=root / "jobs-fetch-tasks.json",
        pending_registry_path=root / "source-registry-pending.json",
        approval_state_path=root / "source-approval-state.json",
    )
