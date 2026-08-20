"""Discovery service discovery service lifecycle.

AI boundary owns: bridge-owned discovery task launch, config persistence, and auto-sync watch behavior.
AI boundary implement in: this discovery_service_lifecycle.py leaf.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

from typing import Any

from src.bridge.active_task_snapshot import upsert_snapshot_rows
from src.bridge.discovery_service_core import DiscoveryServiceState
from src.bridge.task_abort_evidence import (
    ABORT_TERMINAL_REASON,
    repair_discovery_canceled_evidence,
)
from src.shared.json_shapes import as_json_object


class DiscoveryServiceLifecycleMixin(DiscoveryServiceState):
    def _finalize_discovery_run(
        self,
        *,
        run_id: str,
        started_at: str,
        finished_at: str,
        summary: dict[str, Any],
    ) -> None:
        self._deps.finish_lifecycle_run(
            run_id,
            "discovery",
            finished_at=finished_at,
            summary=dict(summary or {}),
            terminal_reason="completed",
        )
        if self._paths.active_task_snapshot is not None:
            upsert_snapshot_rows(
                self._paths.active_task_snapshot,
                [
                    {
                        "taskType": "discovery",
                        "type": "discovery",
                        "runId": run_id,
                        "id": run_id,
                        "active": False,
                        "status": "ok",
                        "startedAt": started_at,
                        "heartbeatAt": finished_at,
                        "finishedAt": finished_at,
                        "taskProgress": {
                            "active": False,
                            "phaseKey": "completed",
                            "phaseLabel": "Discovery completed",
                            "mode": "determinate",
                            "ratio": 1,
                            "updatedAt": finished_at,
                            "counts": {},
                        },
                        "summary": dict(summary or {}),
                        "outputs": {"report": str(self._paths.report)},
                    }
                ],
                snapshot_at=finished_at,
            )

    def _cancel_discovery_run(
        self,
        *,
        run_id: str,
        finished_at: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        canceled_at = str(finished_at or self._deps.now_iso() or "")
        report = repair_discovery_canceled_evidence(
            report_path=self._paths.report,
            run_id=run_id,
            finished_at=canceled_at,
            load_json_object=self._deps.load_json_object,
            save_json_atomic=self._deps.save_json_atomic,
            normalize_report=self._deps.normalize_discovery_report_contract,
            reason=reason,
            overwrite_finished=True,
        )
        self._deps.cancel_lifecycle_run(
            run_id,
            "discovery",
            finished_at=str(report.get("finishedAt") or canceled_at),
            terminal_reason=ABORT_TERMINAL_REASON,
            summary=dict(report.get("summary") or {}),
            progress=dict(report.get("taskProgress") or {}),
        )
        if self._paths.active_task_snapshot is not None:
            upsert_snapshot_rows(
                self._paths.active_task_snapshot,
                [
                    {
                        "taskType": "discovery",
                        "type": "discovery",
                        "runId": run_id,
                        "id": run_id,
                        "active": False,
                        "status": "canceled",
                        "startedAt": str(report.get("startedAt") or ""),
                        "heartbeatAt": str(report.get("finishedAt") or canceled_at),
                        "finishedAt": str(report.get("finishedAt") or canceled_at),
                        "terminalReason": ABORT_TERMINAL_REASON,
                        "taskProgress": dict(report.get("taskProgress") or {}),
                        "summary": dict(report.get("summary") or {}),
                        "outputs": {"report": str(self._paths.report)},
                    }
                ],
                snapshot_at=str(report.get("finishedAt") or canceled_at),
            )
        return report

    def _read_discovery_report(self) -> dict[str, Any]:
        reader = (
            self._deps.load_runtime_evidence
            if callable(self._deps.load_runtime_evidence)
            else self._deps.load_json_object
        )
        return dict(reader(self._paths.report, {}) or {})

    @staticmethod
    def _lifecycle_status_token(row: dict[str, Any]) -> str:
        return str(row.get("lifecycleStatus") or row.get("status") or "").strip().lower()

    @classmethod
    def _lifecycle_row_is_terminal(cls, row: dict[str, Any] | None) -> bool:
        if not isinstance(row, dict):
            return False
        if str(row.get("finishedAt") or "").strip():
            return True
        return cls._lifecycle_status_token(row) in {
            "succeeded",
            "failed",
            "canceled",
            "cancelled",
            "orphaned",
            "ok",
            "error",
            "completed",
        }

    @classmethod
    def _report_status_from_lifecycle_row(cls, row: dict[str, Any]) -> str:
        status = cls._lifecycle_status_token(row)
        if status in {"canceled", "cancelled"}:
            return "canceled"
        if status in {"succeeded", "ok", "completed"}:
            return "ok"
        return "error"

    @staticmethod
    def _terminal_phase_label(status: str) -> str:
        if status == "canceled":
            return "Discovery canceled"
        if status == "ok":
            return "Discovery completed"
        return "Discovery failed"

    @staticmethod
    def _terminal_phase_key(status: str) -> str:
        if status == "canceled":
            return "canceled"
        if status == "ok":
            return "completed"
        return "failed"

    def _repair_terminal_discovery_report_from_row(
        self,
        row: dict[str, Any],
        report: dict[str, Any],
        *,
        finished_at: str = "",
    ) -> dict[str, Any] | None:
        if not isinstance(report, dict):
            return None
        run_id = str(report.get("runId") or row.get("runId") or row.get("id") or "").strip()
        if not run_id:
            return None
        row_run_id = str(row.get("runId") or row.get("id") or "").strip()
        if row_run_id and row_run_id != run_id:
            return None
        if str(report.get("finishedAt") or "").strip():
            return dict(report)

        terminal_at = str(finished_at or row.get("finishedAt") or self._deps.now_iso() or "")
        terminal_reason = str(row.get("terminalReason") or "").strip()
        status = self._report_status_from_lifecycle_row(row)
        phase_key = self._terminal_phase_key(status)
        phase_label = self._terminal_phase_label(status)

        row_summary = as_json_object(row.get("summary"))
        summary = {**as_json_object(report.get("summary")), **row_summary}
        if terminal_reason:
            summary["terminalReason"] = terminal_reason
        summary["status"] = status
        if status != "ok" and not str(summary.get("error") or "").strip():
            summary["error"] = terminal_reason or "discovery_task_terminal_without_report"

        row_progress = as_json_object(row.get("taskProgress") or row.get("progress"))
        report_progress = as_json_object(report.get("taskProgress"))
        progress = {**row_progress, **report_progress}
        row_counts = as_json_object(row_progress.get("counts"))
        report_counts = as_json_object(report_progress.get("counts"))
        counts = {**row_counts, **report_counts}
        progress.update(
            {
                "active": False,
                "phaseKey": phase_key,
                "phaseLabel": phase_label,
                "mode": progress.get("mode") or "indeterminate",
                "updatedAt": terminal_at,
            }
        )
        if counts:
            progress["counts"] = counts

        runtime = as_json_object(report.get("runtime"))
        lifecycle = as_json_object(runtime.get("lifecycle"))
        runtime["lifecycle"] = {
            **lifecycle,
            "owner": lifecycle.get("owner") or "discovery_report",
            "heartbeatAt": terminal_at,
            "terminalReason": terminal_reason,
        }

        repaired = {
            **dict(report),
            "runId": run_id,
            "finishedAt": terminal_at,
            "status": status,
            "terminalReason": terminal_reason,
            "summary": summary,
            "taskProgress": progress,
            "runtime": runtime,
        }
        normalized = self._deps.normalize_discovery_report_contract(repaired)
        self._deps.save_json_atomic(self._paths.report, normalized)
        self._deps.bridge_log(
            "warn",
            "discovery_report_repaired_from_terminal_lifecycle",
            runId=run_id,
            status=status,
            terminalReason=terminal_reason,
            finishedAt=terminal_at,
        )
        return dict(normalized)

    def _reconcile_terminal_discovery_report_from_state(self) -> dict[str, Any] | None:
        raw = self._read_discovery_report()
        if not isinstance(raw, dict):
            return None
        report = self._deps.normalize_discovery_report_contract(raw)
        run_id = str(report.get("runId") or "").strip()
        if not run_id:
            return None
        finished_at = str(report.get("finishedAt") or "").strip()
        if finished_at:
            self._reconcile_terminal_discovery_registry_state(
                run_id=run_id,
                finished_at=finished_at,
                report=report,
            )
            return None
        row = self._deps.get_lifecycle_row(run_id, "discovery")
        if not self._lifecycle_row_is_terminal(row):
            return None
        repaired = self._repair_terminal_discovery_report_from_row(row or {}, report)
        if not isinstance(repaired, dict):
            return None
        repaired_finished_at = str(repaired.get("finishedAt") or "").strip()
        if repaired_finished_at:
            self._reconcile_terminal_discovery_registry_state(
                run_id=run_id,
                finished_at=repaired_finished_at,
                report=repaired,
            )
        return repaired

    def reconcile_terminal_discovery_report_from_state(self) -> dict[str, Any] | None:
        return self._reconcile_terminal_discovery_report_from_state()
