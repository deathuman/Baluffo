"""Active audit runtime helpers for source discovery.

Thin coordinator composing the active-audit runtime leaves; every leaf unit is re-exported below so the historical module surface is unchanged.

AI boundary owns: active-audit runtime composition, the seam-observable loop-strategy builder, and the compatibility re-export surface.
AI boundary implement in: the active_audit_* leaves for behavior; this file for composition and re-exports only. GameDevMap-specific dry-run behavior stays in gamedevmap_active_dry_run.
AI boundary search before contracts: gamedevmap active dry run, audit reports, and active audit tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused active audit tests.
"""

from __future__ import annotations

import time as time
from collections import Counter as Counter
from collections.abc import Callable
from contextlib import suppress as suppress
from dataclasses import dataclass as dataclass
from dataclasses import field as field
from inspect import Parameter as Parameter
from inspect import signature as signature
from pathlib import Path as Path
from typing import Any

from src.shared.json_shapes import (
    as_json_object as _as_dict,
)
from src.shared.utils import int_or_default as _safe_int
from src.shared.utils import now_iso as now_iso

from . import audit_ledger as audit_ledger
from .active_audit_artifact_runtime import append_artifact_rows as append_artifact_rows
from .active_audit_artifact_runtime import append_batch_timing as append_batch_timing
from .active_audit_artifact_runtime import (
    apply_active_audit_probe_results as apply_active_audit_probe_results,
)
from .active_audit_artifact_runtime import (
    create_active_audit_artifact as create_active_audit_artifact,
)
from .active_audit_artifact_runtime import (
    finalize_active_audit_artifact as finalize_active_audit_artifact,
)
from .active_audit_artifact_runtime import (
    increment_active_audit_summary as increment_active_audit_summary,
)
from .active_audit_artifact_runtime import (
    load_or_initialize_active_audit_artifact as load_or_initialize_active_audit_artifact,
)
from .active_audit_artifact_runtime import (
    merge_active_audit_batch_artifact_updates as merge_active_audit_batch_artifact_updates,
)
from .active_audit_artifact_runtime import record_failure_rows as record_failure_rows
from .active_audit_artifact_runtime import run_active_audit_cache as run_active_audit_cache
from .active_audit_artifact_runtime import (
    save_updated_active_audit_artifact as save_updated_active_audit_artifact,
)
from .active_audit_batch_runtime import run_active_audit_batch as run_active_audit_batch
from .active_audit_batch_strategy import (
    _apply_recovery_with_progress as _apply_recovery_with_progress,
)
from .active_audit_batch_strategy import _duration_ms as _duration_ms
from .active_audit_batch_strategy import _emit_progress as _emit_progress
from .active_audit_batch_strategy import (
    build_active_audit_batch_strategy as build_active_audit_batch_strategy,
)
from .active_audit_candidate_rows import _dict_rows as _dict_rows
from .active_audit_candidate_rows import (
    active_audit_artifact_counts as active_audit_artifact_counts,
)
from .active_audit_candidate_rows import (
    compare_recovered_active_maps as compare_recovered_active_maps,
)
from .active_audit_candidate_rows import failure_count as failure_count
from .active_audit_candidate_rows import (
    index_rejections_by_identity as index_rejections_by_identity,
)
from .active_audit_candidate_rows import merge_rows_by_identity as merge_rows_by_identity
from .active_audit_candidate_rows import merge_unique_candidate_rows as merge_unique_candidate_rows
from .active_audit_candidate_rows import prune_rerun_rejections as prune_rerun_rejections
from .active_audit_candidate_rows import (
    recovered_active_by_identity as recovered_active_by_identity,
)
from .active_audit_candidate_rows import rejection_lookup_keys as rejection_lookup_keys
from .active_audit_candidate_rows import rejection_rerun_key as rejection_rerun_key
from .active_audit_candidate_rows import row_identity_keys as row_identity_keys
from .active_audit_candidate_rows import select_rerun_rows as select_rerun_rows
from .active_audit_candidate_rows import (
    validated_active_candidates_from_artifact as validated_active_candidates_from_artifact,
)
from .active_audit_contracts import ActiveAuditArtifactCounts as ActiveAuditArtifactCounts
from .active_audit_contracts import ActiveAuditBatchResult as ActiveAuditBatchResult
from .active_audit_contracts import ActiveAuditBatchStrategy as ActiveAuditBatchStrategy
from .active_audit_contracts import (
    ActiveAuditCandidateMergeResult as ActiveAuditCandidateMergeResult,
)
from .active_audit_contracts import ActiveAuditLoopResult as ActiveAuditLoopResult
from .active_audit_contracts import ActiveAuditLoopStrategy as ActiveAuditLoopStrategy
from .active_audit_contracts import ActiveAuditPreparedRows as ActiveAuditPreparedRows
from .active_audit_contracts import (
    ActiveAuditRecoveryApplicationResult as ActiveAuditRecoveryApplicationResult,
)
from .active_audit_contracts import ActiveAuditRecoveryFetchResult as ActiveAuditRecoveryFetchResult
from .active_audit_contracts import ActiveHomepageBatchResult as ActiveHomepageBatchResult
from .active_audit_contracts import HomepagePageOutcome as HomepagePageOutcome
from .active_audit_contracts import NoCandidateOutcome as NoCandidateOutcome
from .active_audit_homepage_batch import run_active_homepage_batch as run_active_homepage_batch
from .active_audit_loop_runtime import _next_unprocessed_batch as _next_unprocessed_batch
from .active_audit_loop_runtime import run_active_audit_loop as run_active_audit_loop
from .active_audit_recovery_apply import (
    apply_active_audit_recovery_fetch_results as apply_active_audit_recovery_fetch_results,
)


def build_active_audit_loop_strategy(
    *,
    artifact: dict[str, Any],
    row_identity: Callable[[dict[str, Any]], str],
    batch_strategy: ActiveAuditBatchStrategy,
    completed_identities: set[str],
    emit_batch_log: Callable[[int, int, int], None],
    before_write: Callable[[], None],
    write_artifact: Callable[[bool], None],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ActiveAuditLoopStrategy:
    def _run_batch(
        batch_rows: list[dict[str, Any]],
        cursor: int,
        _batch_number: int,
    ) -> None:
        progress = _as_dict(artifact.get("progress"))
        run_active_audit_batch(
            artifact=artifact,
            batch_rows=batch_rows,
            cursor=cursor,
            batch_number=_safe_int(progress.get("batchesCompleted")) + 1,
            strategy=batch_strategy,
            completed_identities=completed_identities,
        )

    return ActiveAuditLoopStrategy(
        row_identity=row_identity,
        emit_batch_log=emit_batch_log,
        run_batch=_run_batch,
        before_write=before_write,
        write_artifact=write_artifact,
        progress_callback=progress_callback,
    )
