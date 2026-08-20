"""Ops API live — live task payload, hot snapshot selection, fetcher metrics, and bounded fetch summaries.

AI boundary owns: live task payload, hot snapshot selection, fetcher metrics, and bounded fetch summaries.
AI boundary implement in: this leaf for the OpsApi mixin group; the coordinator
composes `OpsApi` from the mixin leaves and keeps the public construction surface.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops API tests.
"""

from __future__ import annotations

from typing import Any, cast

from src import fetcher_metrics as fetcher_metrics_module
from src.bridge import active_task_snapshot as _active_task_snapshot
from src.bridge import ops_task_live as _ops_task_live
from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.bridge.fetch_report_summary import (
    load_fetch_report_summary_artifact,
    load_fetch_task_summary_artifact,
)
from src.bridge.ops_api_core import OpsApiState, OpsPaths
from src.shared.json_shapes import as_json_object
from src.shared.live_task import (
    LiveTaskPayload,
)


def _bounded_fetch_summary_for_run(paths: OpsPaths, run_id: str) -> dict[str, Any]:
    clean_run_id = str(run_id or "").strip()
    if not clean_run_id:
        return {}
    for payload in (
        load_fetch_report_summary_artifact(paths.jobs_fetch_report),
        load_fetch_task_summary_artifact(paths.jobs_fetch_report),
    ):
        if not isinstance(payload, dict):
            continue
        if str(payload.get("runId") or "").strip() == clean_run_id:
            return payload
    return {}


def _merge_bounded_fetch_summary_for_task_state_row(
    route_row: dict[str, Any],
    *,
    paths: OpsPaths,
    task_type: str,
    run_id: str,
    lifecycle_row: dict[str, Any],
) -> dict[str, Any]:
    if task_type != "fetch":
        return route_row
    fetch_summary = _bounded_fetch_summary_for_run(paths, run_id)
    if not fetch_summary:
        return route_row
    return {
        **route_row,
        **fetch_summary,
        "id": run_id,
        "runId": run_id,
        "type": task_type,
        "taskType": task_type,
        "active": True,
        "finishedAt": "",
        "lifecycleStatus": str(
            lifecycle_row.get("lifecycleStatus") or lifecycle_row.get("status") or ""
        ).strip(),
        "parentRunId": str(lifecycle_row.get("parentRunId") or "").strip(),
        "parentTaskType": str(lifecycle_row.get("parentTaskType") or "").strip().lower(),
        "ownerKind": str(lifecycle_row.get("ownerKind") or "").strip().lower(),
        "ownerPid": lifecycle_row.get("ownerPid"),
        "stage": str(lifecycle_row.get("stage") or "").strip(),
    }


class OpsApiLiveMixin(OpsApiState):
    def _task_live_context(self) -> _ops_task_live.OpsTaskLiveContext:
        return _ops_task_live.OpsTaskLiveContext(paths=self._paths, deps=self._deps)

    def _fresh_active_task_snapshot(self) -> dict[str, Any] | None:
        return _active_task_snapshot.load_fresh_snapshot(
            self._paths.active_task_snapshot,
            now=self._deps.now_utc(),
        )

    @staticmethod
    def _should_use_hot_snapshot(
        snapshot: dict[str, Any] | None,
        pipeline_status: dict[str, Any],
    ) -> bool:
        return bool(
            (snapshot and _active_task_snapshot.snapshot_has_active_task(snapshot))
            or _active_task_snapshot.pipeline_is_active(pipeline_status)
        )

    def _load_fetch_report_with_dedup_review_state(self) -> dict[str, Any]:
        payload, warning = load_fetch_report_with_dedup_review_state(
            normalize_fetch_report_contract=self._deps.normalize_fetch_report_contract,
            jobs_fetch_report_path=self._paths.jobs_fetch_report,
            dedup_review_state_path=self._paths.dedup_review_state,
        )
        if warning:
            payload["dedupReviewStateReadWarning"] = warning
        dedup_evidence = as_json_object(payload.get("dedupEvidence"))
        gate_counts = as_json_object(dedup_evidence.get("providerStaticDisagreementGateCounts"))
        export = as_json_object(payload.get("dedupReviewStateExport"))
        reviewed_safe = int(gate_counts.get("reviewedSafeWarning") or 0)
        confirmed_blocking = int(gate_counts.get("confirmedBlocking") or 0)
        payload["dedupReviewStateSummary"] = {
            "artifactPath": str(export.get("artifactPath") or self._paths.dedup_review_state),
            "status": "warning" if warning else "ok",
            "readWarning": warning,
            "reviewedPairCount": reviewed_safe + confirmed_blocking,
            "reviewedSafeCount": reviewed_safe,
            "confirmedBlockingCount": confirmed_blocking,
            "unresolvedBlockingCount": int(gate_counts.get("blocked") or 0),
        }
        return payload

    def get_task_live_payload(
        self,
        task_type: str,
        *,
        summary: bool = False,
    ) -> LiveTaskPayload:
        if summary:
            pipeline_status = self._deps.get_jobs_pipeline_status_payload()
            snapshot = self._fresh_active_task_snapshot()
            if self._should_use_hot_snapshot(snapshot, pipeline_status):
                hot_payload = _active_task_snapshot.live_summary_from_snapshot(
                    snapshot,
                    task_type,
                    pipeline_status=pipeline_status,
                )
                if hot_payload is not None:
                    return cast(LiveTaskPayload, hot_payload)
        projection = self.get_projected_run_history()
        return cast(
            LiveTaskPayload,
            _ops_task_live.get_task_live_payload(
                self._task_live_context(),
                task_type,
                projection=projection,
                summary=summary,
            ),
        )

    def compute_fetcher_metrics(self, *, window_runs: int = 20) -> dict[str, Any]:
        latest_fetch_report = self._load_fetch_report_with_dedup_review_state()
        history = self.get_projected_run_history().rows
        return fetcher_metrics_module.build_metrics(
            latest_fetch_report,
            history,
            window=max(1, int(window_runs or 1)),
        )
