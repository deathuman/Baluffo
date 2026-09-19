"""Active audit batch strategy construction and progress emission.

AI boundary owns: batch strategy assembly, progress event emission, and recovery applier arity adaptation.
AI boundary implement in: this file for strategy construction; batch execution stays in active_audit_batch_runtime.
AI boundary search before contracts: batch strategy callables and active audit batch tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime_batch.py -q`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import suppress
from inspect import Parameter, signature
from typing import Any

from .active_audit_contracts import ActiveAuditBatchStrategy as ActiveAuditBatchStrategy
from .active_audit_contracts import (
    ActiveAuditCandidateMergeResult as ActiveAuditCandidateMergeResult,
)
from .active_audit_contracts import ActiveAuditPreparedRows as ActiveAuditPreparedRows
from .active_audit_contracts import (
    ActiveAuditRecoveryApplicationResult as ActiveAuditRecoveryApplicationResult,
)
from .active_audit_contracts import ActiveAuditRecoveryFetchResult as ActiveAuditRecoveryFetchResult
from .active_audit_contracts import ActiveHomepageBatchResult as ActiveHomepageBatchResult


def _emit_progress(
    callback: Callable[[dict[str, Any]], None] | None,
    event: dict[str, Any],
    *,
    force: bool = False,
) -> None:
    if callback is None:
        return
    callback({**event, "force": bool(force)})


def _apply_recovery_with_progress(
    strategy: ActiveAuditBatchStrategy,
    results: list[dict[str, Any]],
    grouped: dict[str, Any] | None,
    finalize: bool,
    progress_label: str,
) -> ActiveAuditRecoveryApplicationResult:
    use_progress_label = False
    with suppress(TypeError, ValueError):
        params = list(signature(strategy.apply_recovery).parameters.values())
        accepts_varargs = any(param.kind == Parameter.VAR_POSITIONAL for param in params)
        positional_params = [
            param
            for param in params
            if param.kind in {Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD}
        ]
        use_progress_label = accepts_varargs or len(positional_params) >= 4
    if use_progress_label:
        return strategy.apply_recovery(results, grouped, finalize, progress_label)
    return strategy.apply_recovery(results, grouped, finalize)


def _duration_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def build_active_audit_batch_strategy(
    *,
    prepare_rows: Callable[[list[dict[str, Any]]], ActiveAuditPreparedRows],
    fetch_homepages: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    analyze_homepages: Callable[[list[dict[str, Any]]], ActiveHomepageBatchResult],
    fetch_recovery: Callable[[list[dict[str, Any]], str], ActiveAuditRecoveryFetchResult],
    apply_recovery: Callable[
        ...,
        ActiveAuditRecoveryApplicationResult,
    ],
    recovery_homepage_key: Callable[[dict[str, Any]], str],
    merge_candidates: Callable[
        [
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ],
        ActiveAuditCandidateMergeResult,
    ],
    merge_artifact_updates: Callable[
        [
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ],
        None,
    ],
    update_summary: Callable[[dict[str, Any]], None],
    probe_candidates: Callable[[list[dict[str, Any]]], Any],
    apply_probe_results: Callable[[Any], None],
    row_identity: Callable[[dict[str, Any]], str],
    append_timing: Callable[[dict[str, Any]], None],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ActiveAuditBatchStrategy:
    return ActiveAuditBatchStrategy(
        prepare_rows=prepare_rows,
        fetch_homepages=fetch_homepages,
        analyze_homepages=analyze_homepages,
        fetch_recovery=fetch_recovery,
        apply_recovery=apply_recovery,
        recovery_homepage_key=recovery_homepage_key,
        merge_candidates=merge_candidates,
        merge_artifact_updates=merge_artifact_updates,
        update_summary=update_summary,
        probe_candidates=probe_candidates,
        apply_probe_results=apply_probe_results,
        row_identity=row_identity,
        append_timing=append_timing,
        progress_callback=progress_callback,
    )
