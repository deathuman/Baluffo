from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src import source_registry as source_registry_module

from .core import init_stage_counter
from .runtime_metrics import DISCOVERY_TIMING_STAGE_KEYS
from .url_patches import summarize_url_patch_runtime


@dataclass(frozen=True)
class DiscoveryRunDeps:
    timeout_s: int
    top_n: int
    preset_name: str
    mode: str
    include_web_search: bool
    effective_config: dict[str, Any]
    thresholds: dict[str, Any]
    run_id: str
    started_at: str
    run_started_mono: float
    fetcher: Any
    top_cap_bypassed: bool
    sheet_static_probe_cap_bypassed: bool
    queue_domain_cap: int
    queue_adapter_caps: dict[str, int]
    url_patch_manifest_path: Path
    url_patch_manifest_enabled: bool


@dataclass
class DiscoveryRunState:
    stage_timings_ms: dict[str, int] = field(
        default_factory=lambda: {key: 0 for key in DISCOVERY_TIMING_STAGE_KEYS}
    )
    adapter_runtime: dict[str, dict[str, int | str]] = field(default_factory=dict)
    web_failures: list[dict[str, Any]] = field(default_factory=list)
    streams: list[tuple[str, list[dict[str, Any]]]] = field(default_factory=list)
    generated_count_by_stage: dict[str, int] = field(default_factory=init_stage_counter)
    survived_dedupe_count_by_stage: dict[str, int] = field(default_factory=init_stage_counter)
    probed_count_by_stage: dict[str, int] = field(default_factory=init_stage_counter)
    queued_count_by_stage: dict[str, int] = field(default_factory=init_stage_counter)
    duplicate_reasons: Counter[str] = field(default_factory=Counter)
    dedupe_drop_rows: list[dict[str, Any]] = field(default_factory=list)
    found_endpoint_count: int = 0
    skipped_duplicate_count: int = 0
    filtered: list[dict[str, Any]] = field(default_factory=list)
    queueable_candidates: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    healthy: int = 0
    probed: int = 0
    adapter_counter: Counter[str] = field(default_factory=Counter)
    method_counter: Counter[str] = field(default_factory=Counter)
    skipped_invalid: int = 0
    skipped_low_evidence_probe_count: int = 0
    suppressed_static_count: int = 0
    suppressed_static_by_reason: Counter[str] = field(default_factory=Counter)
    suppressed_static_by_stage: Counter[str] = field(default_factory=Counter)
    validation_skipped_count: int = 0
    queue_filtered_count: int = 0
    probe_failed_count: int = 0
    url_patches: dict[str, Any] = field(default_factory=dict)
    url_patch_stats: dict[str, int] = field(
        default_factory=lambda: summarize_url_patch_runtime(
            loaded=0,
            added=0,
            updated=0,
            reprobed=0,
        )
    )
    patch_added: int = 0
    patch_updated: int = 0
    recovered_count: int = 0
    active: list[dict[str, Any]] = field(default_factory=list)
    pending_existing: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)
    tombstones: Any = field(default_factory=list)
    prior_review_candidates_by_id: dict[str, dict[str, Any]] = field(default_factory=dict)
    ranking_registry_rows: list[dict[str, Any]] = field(default_factory=list)
    source_state_rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    probe_inputs: list[dict[str, Any]] = field(default_factory=list)
    prevalidated_probe_inputs: list[dict[str, Any]] = field(default_factory=list)
    failed_probe_records: list[dict[str, Any]] = field(default_factory=list)
    gamedevmap_audit_summary: dict[str, Any] = field(default_factory=dict)
    directory_audit_summaries: dict[str, dict[str, Any]] = field(default_factory=dict)

    def total_duration_ms(self, deps: DiscoveryRunDeps) -> int:
        import time

        return max(0, int((time.perf_counter() - deps.run_started_mono) * 1000))

    def write_progress_report(
        self,
        current_candidates: list[dict[str, Any]],
        *,
        phase: str,
        phase_label: str,
        deps: DiscoveryRunDeps,
        root: Any,
    ) -> None:
        report_write_path = root._discovery_report_write_path()
        root.write_discovery_progress_report(
            current_candidates=current_candidates,
            phase=phase,
            phase_label=phase_label,
            total_duration_ms=self.total_duration_ms(deps),
            stage_timings_ms=self.stage_timings_ms,
            adapter_runtime=self.adapter_runtime,
            preset_name=deps.preset_name,
            top_cap_bypassed=deps.top_cap_bypassed,
            sheet_static_probe_cap_bypassed=deps.sheet_static_probe_cap_bypassed,
            url_patch_stats=dict(self.url_patch_stats),
            found_endpoint_count=self.found_endpoint_count,
            generated_count_by_stage=self.generated_count_by_stage,
            survived_dedupe_count_by_stage=self.survived_dedupe_count_by_stage,
            probed_count_by_stage=self.probed_count_by_stage,
            queued_count_by_stage=self.queued_count_by_stage,
            probed=self.probed,
            healthy=self.healthy,
            failures=self.failures,
            skipped_duplicate_count=self.skipped_duplicate_count,
            skipped_invalid=self.skipped_invalid,
            skipped_low_evidence_probe_count=self.skipped_low_evidence_probe_count,
            validation_skipped_count=self.validation_skipped_count,
            probe_failed_count=self.probe_failed_count,
            queue_filtered_count=self.queue_filtered_count,
            adapter_counter=self.adapter_counter,
            method_counter=self.method_counter,
            duplicate_reasons=self.duplicate_reasons,
            suppressed_static_count=self.suppressed_static_count,
            suppressed_static_by_reason=dict(self.suppressed_static_by_reason),
            suppressed_static_by_stage=dict(self.suppressed_static_by_stage),
            thresholds=deps.thresholds,
            run_id=deps.run_id,
            mode=deps.mode,
            started_at=deps.started_at,
            report_write_path=report_write_path,
            outputs={
                "report": str(report_write_path),
                "candidates": str(source_registry_module.DISCOVERY_CANDIDATES_PATH),
                "pending": str(source_registry_module.PENDING_PATH),
                "urlPatches": str(source_registry_module.URL_PATCH_MANIFEST_PATH),
            },
            save_json_atomic_fn=root.save_json_atomic,
        )
