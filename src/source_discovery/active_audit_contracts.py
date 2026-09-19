"""Active audit runtime result and strategy dataclasses.

AI boundary owns: the active-audit dataclass contracts (batch/loop results, prepared rows, strategy callables).
AI boundary implement in: this file for active-audit result shapes; batch and loop execution stay in their runtime leaves.
AI boundary search before contracts: active audit runtime callers, GameDevMap active batch plumbing, and active audit tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime.py tests/source_discovery/test_active_audit_runtime_batch.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class HomepagePageOutcome:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    found_candidates: bool = False


@dataclass
class NoCandidateOutcome:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    primary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    secondary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    browser_recovery_candidates: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ActiveHomepageBatchResult:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    primary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    secondary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    browser_recovery_candidates: list[dict[str, Any]] = field(default_factory=list)
    homepages_fetched: int = 0


@dataclass
class ActiveAuditArtifactCounts:
    rejected_rows: list[dict[str, Any]]
    active_rows: list[dict[str, Any]]
    all_candidates: list[dict[str, Any]]
    recovered_candidates: list[dict[str, Any]]
    recovered_active: list[dict[str, Any]]
    technical_failures: list[dict[str, Any]]
    coverage_misses: list[dict[str, Any]]
    reason_counts: dict[str, int]
    detail_counts: dict[str, int]
    active_adapter_counts: dict[str, int]
    zero_job_count: int
    failure_count: int
    failure_sample_count: int
    browser_recovery_candidate_count: int
    browser_recovery_processed_count: int
    browser_recovered_active_count: int
    lost_recovered_active_count: int


@dataclass
class ActiveAuditPreparedRows:
    direct_provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    homepage_rows: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ActiveAuditRecoveryFetchResult:
    results: list[dict[str, Any]] = field(default_factory=list)
    unique_jobs: int = 0
    network_jobs: int = 0


@dataclass
class ActiveAuditRecoveryApplicationResult:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    pages_fetched: int = 0
    grouped_state: dict[str, Any] = field(default_factory=dict)
    recovered_homepages: set[str] = field(default_factory=set)


@dataclass
class ActiveAuditCandidateMergeResult:
    candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ActiveAuditBatchResult:
    timing: dict[str, Any]
    candidates: list[dict[str, Any]]
    recovery_jobs: list[dict[str, Any]]
    recovered_homepages: set[str]


@dataclass
class ActiveAuditBatchStrategy:
    prepare_rows: Callable[[list[dict[str, Any]]], ActiveAuditPreparedRows]
    fetch_homepages: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    analyze_homepages: Callable[[list[dict[str, Any]]], ActiveHomepageBatchResult]
    fetch_recovery: Callable[[list[dict[str, Any]], str], ActiveAuditRecoveryFetchResult]
    apply_recovery: Callable[
        ...,
        ActiveAuditRecoveryApplicationResult,
    ]
    recovery_homepage_key: Callable[[dict[str, Any]], str]
    merge_candidates: Callable[
        [
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ],
        ActiveAuditCandidateMergeResult,
    ]
    merge_artifact_updates: Callable[
        [
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ],
        None,
    ]
    update_summary: Callable[[dict[str, Any]], None]
    probe_candidates: Callable[[list[dict[str, Any]]], Any]
    apply_probe_results: Callable[[Any], None]
    row_identity: Callable[[dict[str, Any]], str]
    append_timing: Callable[[dict[str, Any]], None]
    progress_callback: Callable[[dict[str, Any]], None] | None = None


@dataclass
class ActiveAuditLoopResult:
    batches_run: int
    completed_identities: set[str]
    complete: bool


@dataclass
class ActiveAuditLoopStrategy:
    row_identity: Callable[[dict[str, Any]], str]
    emit_batch_log: Callable[[int, int, int], None]
    run_batch: Callable[[list[dict[str, Any]], int, int], None]
    before_write: Callable[[], None]
    write_artifact: Callable[[bool], None]
    progress_callback: Callable[[dict[str, Any]], None] | None = None
