"""GameDevMap active-audit batch row preparation, fetching, and merge helpers.

AI boundary owns: active batch row preparation, homepage/recovery fetching, batch candidate merging, and loop strategy plumbing.
AI boundary implement in: this file for active batch plumbing; batch strategy stays in gamedevmap_active_dry_run.
AI boundary search before contracts: active_audit_runtime batch contract and GameDevMap active dry-run tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_gamedevmap_active_dry_run.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.source_registry import unique_sources

from . import active_audit_runtime
from .gamedevmap import _apply_gamedevmap_provenance
from .gamedevmap_artifact_store import (
    _as_dict as _as_dict,
)
from .gamedevmap_artifact_store import (
    _as_list as _as_list,
)
from .gamedevmap_artifact_store import (
    _safe_int as _safe_int,
)
from .gamedevmap_artifact_store import (
    _summarize_artifact as _summarize_artifact,
)
from .gamedevmap_recovery_comparison import (
    apply_gamedevmap_lost_recovery_audit as apply_gamedevmap_lost_recovery_audit,
)
from .gamedevmap_recovery_queue import (
    _filter_bad_provider_inferences as _filter_bad_provider_inferences,
)
from .gamedevmap_rejection import (
    _rejection,
    _row_url,
)
from .reporting import emit_log
from .web_search import infer_web_candidate


def _prepare_gamedevmap_active_batch_rows(
    rows: list[dict[str, Any]],
    *,
    index_url: str,
) -> active_audit_runtime.ActiveAuditPreparedRows:
    direct_provider_rows: list[dict[str, Any]] = []
    homepage_rows: list[dict[str, Any]] = []
    rejected_missing: list[dict[str, Any]] = []
    for row in rows:
        studio = str(row.get("studio") or "").strip()
        target_url = _row_url(row)
        if not studio or not target_url:
            rejected_missing.append(
                _rejection(
                    reason="missing_studio_or_url",
                    row=row,
                    reason_detail="missing_studio_or_url",
                )
            )
            continue
        inferred = infer_web_candidate(
            target_url,
            studio,
            nl_priority=False,
            discovery_method="gamedevmap",
        )
        if inferred:
            inferred["careersUrl"] = target_url
            direct_provider_rows.append(
                _apply_gamedevmap_provenance(
                    inferred,
                    row,
                    index_url=index_url,
                    include_direct_url=True,
                )
            )
        else:
            homepage_rows.append(row)
    return active_audit_runtime.ActiveAuditPreparedRows(
        direct_provider_candidates=direct_provider_rows,
        homepage_rows=homepage_rows,
        rejected_rows=rejected_missing,
    )


def _merge_gamedevmap_active_batch_candidates(
    direct_provider_rows: list[dict[str, Any]],
    provider_rows: list[dict[str, Any]],
    static_rows: list[dict[str, Any]],
    recovery_provider_rows: list[dict[str, Any]],
    recovery_static_rows: list[dict[str, Any]],
) -> active_audit_runtime.ActiveAuditCandidateMergeResult:
    all_candidates = unique_sources(
        [
            *direct_provider_rows,
            *provider_rows,
            *static_rows,
            *recovery_provider_rows,
            *recovery_static_rows,
        ]
    )
    all_candidates, bad_provider_rejections = _filter_bad_provider_inferences(all_candidates)
    return active_audit_runtime.ActiveAuditCandidateMergeResult(
        candidates=all_candidates,
        rejected_rows=bad_provider_rejections,
    )


def _build_gamedevmap_active_loop_strategy(
    *,
    artifact: dict[str, Any],
    output_path: Path,
    parsed_rows: list[dict[str, Any]],
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
    compare_artifact_path: Path | str | None,
    batch_strategy: active_audit_runtime.ActiveAuditBatchStrategy,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> active_audit_runtime.ActiveAuditLoopStrategy:
    return active_audit_runtime.build_active_audit_loop_strategy(
        artifact=artifact,
        row_identity=_row_url,
        batch_strategy=batch_strategy,
        completed_identities=completed_urls,
        emit_batch_log=lambda batch_number, row_count, cursor: emit_log(
            "GameDevMap active-source dry run: "
            f"batch={batch_number}, rows={row_count}, cursor={cursor}."
        ),
        before_write=lambda: apply_gamedevmap_lost_recovery_audit(
            artifact,
            compare_artifact_path=compare_artifact_path,
        ),
        write_artifact=lambda complete: active_audit_runtime.finalize_active_audit_artifact(
            artifact,
            output_path,
            completed_identities=completed_urls,
            complete=complete,
            completed_cursor_position=len(representative_rows),
            completed_key="completedUrls",
            summarize=lambda current, identities: _summarize_artifact(
                current,
                parsed_rows=parsed_rows,
                representative_rows=representative_rows,
                completed_urls=identities,
            ),
        ),
        progress_callback=progress_callback,
    )


def _build_gamedevmap_subtask_progress_callback(
    *,
    artifact: dict[str, Any],
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
    batch_size: int,
    progress_callback: Callable[[dict[str, Any]], None] | None,
) -> Callable[[dict[str, Any]], None] | None:
    if progress_callback is None:
        return None

    total_urls = len(representative_rows)

    def _callback(event: dict[str, Any]) -> None:
        progress = _as_dict(artifact.get("progress"))
        summary = _as_dict(artifact.get("summary"))
        phase = str(event.get("phase") or "audit").strip()
        phase_label = str(event.get("phaseLabel") or "GameDevMap active audit").strip()
        completed = _safe_int(event.get("completed"), len(completed_urls))
        if completed <= 0:
            completed = _safe_int(progress.get("completedUrlsCount"), len(completed_urls))
        total = _safe_int(event.get("total"), total_urls) or total_urls
        phase_completed = _safe_int(event.get("phaseCompleted"), 0)
        phase_total = _safe_int(event.get("phaseTotal"), 0)
        counts = {
            "subtaskKey": "gamedevmap_active_audit",
            "subtaskLabel": "GameDevMap active audit",
            "activeAuditPhase": phase,
            "activeAuditCompletedUrls": completed,
            "activeAuditTotalUrls": total,
            "activeAuditBatch": _safe_int(event.get("batch"), 0),
            "activeAuditBatchSize": int(batch_size),
            "activeAuditBatchRows": _safe_int(event.get("batchRows"), 0),
            "activeAuditCursor": _safe_int(event.get("cursor"), 0),
            "activeAuditPhaseCompleted": phase_completed,
            "activeAuditPhaseTotal": phase_total,
            "activeAuditHomepageFetched": _safe_int(summary.get("homepagesFetched"), 0),
            "activeAuditRecoveryFetched": _safe_int(summary.get("recoveryNetworkFetchAttempts"), 0),
            "activeAuditRecoveryAnalyzed": _safe_int(summary.get("recoveryPagesFetched"), 0),
            "activeAuditCandidates": len(_as_list(artifact.get("allCandidates"))),
            "activeAuditFailures": len(_as_list(artifact.get("failures"))),
        }
        if "recoveryPayloads" in event:
            counts["activeAuditRecoveryPayloads"] = _safe_int(event.get("recoveryPayloads"), 0)
        progress_callback(
            {
                "phaseKey": "scanning_sources",
                "phaseLabel": "Scanning GameDevMap directory",
                "targetLabel": phase_label,
                "counts": counts,
                "force": bool(event.get("force")),
            }
        )

    return _callback
