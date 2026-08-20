"""Discovery service discovery service registry.

AI boundary owns: bridge-owned discovery task launch, config persistence, and auto-sync watch behavior.
AI boundary implement in: this discovery_service_registry.py leaf.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

from typing import Any

from src.bridge.discovery_service_core import DiscoveryServiceState
from src.shared.json_shapes import as_json_list, as_json_object
from src.source_registry import (
    REGISTRY_REASON_DISCOVERY_AUTO_APPROVE,
    apply_discovery_auto_approval,
    source_identity,
    transition_registry_to_active,
    unique_sources,
)
from src.source_registry import (
    _pending_row_is_auto_approvable as registry_pending_row_is_auto_approvable,
)


class DiscoveryServiceRegistryMixin(DiscoveryServiceState):
    @classmethod
    def _pending_row_is_auto_approvable(cls, row: dict[str, Any]) -> bool:
        return registry_pending_row_is_auto_approvable(row)

    @staticmethod
    def _discovery_report_finalization_settled(report: dict[str, Any]) -> bool:
        runtime = as_json_object(report.get("runtime"))
        registry_finalization = as_json_object(runtime.get("registryFinalization"))
        registry_status = str(registry_finalization.get("status") or "").strip().lower()
        if registry_status == "running":
            return False
        auto_approval = as_json_object(runtime.get("autoApproval"))
        auto_status = str(auto_approval.get("status") or "").strip().lower()
        if bool(auto_approval.get("enabled")) and auto_status == "running":
            return False
        return True

    @staticmethod
    def _state_bucket_counts(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        return {
            bucket: len([row for row in list(state.get(bucket) or []) if isinstance(row, dict)])
            for bucket in ("active", "pending", "rejected")
        }

    @staticmethod
    def _state_bucket_identity_signature(
        state: dict[str, list[dict[str, Any]]],
    ) -> tuple[tuple[str, tuple[str, ...]], ...]:
        signature: list[tuple[str, tuple[str, ...]]] = []
        for bucket in ("active", "pending", "rejected"):
            identities: list[str] = []
            for index, row in enumerate(list(state.get(bucket) or [])):
                if not isinstance(row, dict):
                    continue
                try:
                    identity = str(source_identity(row) or "").strip()
                except (TypeError, ValueError):
                    identity = ""
                if not identity:
                    identity = str(row.get("id") or row.get("sourceId") or index).strip()
                identities.append(identity)
            signature.append((bucket, tuple(sorted(identities))))
        return tuple(signature)

    @staticmethod
    def _registry_finalization_counts(report: dict[str, Any]) -> dict[str, int] | None:
        runtime = as_json_object(report.get("runtime"))
        finalization = as_json_object(runtime.get("registryFinalization"))
        status = str(finalization.get("status") or "").strip().lower()
        if status and status not in {"completed", "ok", "success"}:
            return None
        keys = {
            "active": "activeCount",
            "pending": "pendingCount",
            "rejected": "rejectedCount",
        }
        counts: dict[str, int] = {}
        for bucket, key in keys.items():
            if key not in finalization:
                return None
            try:
                counts[bucket] = int(finalization.get(key) or 0)
            except (TypeError, ValueError):
                return None
        return counts

    @classmethod
    def _state_matches_registry_finalization(
        cls,
        state: dict[str, list[dict[str, Any]]],
        finalization_counts: dict[str, int] | None,
    ) -> bool:
        if not finalization_counts:
            return True
        counts = cls._state_bucket_counts(state)
        return all(counts.get(bucket) == finalization_counts.get(bucket) for bucket in counts)

    @staticmethod
    def _report_declares_auto_approval(report: dict[str, Any]) -> bool:
        runtime = as_json_object(report.get("runtime"))
        auto_approval = as_json_object(runtime.get("autoApproval"))
        return any(key in auto_approval for key in ("enabled", "status", "approvedCount"))

    @staticmethod
    def _report_declared_auto_approved_candidates(
        report: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        approved: dict[str, dict[str, Any]] = {}
        for row in as_json_list(report.get("candidates")):
            if not isinstance(row, dict):
                continue
            registry_state = str(row.get("registryState") or "").strip().lower()
            changed_by = str(row.get("stateChangedBy") or row.get("approvedBy") or "").strip()
            if registry_state != "active" or changed_by != "discovery_auto_approve":
                continue
            identity = str(source_identity(row) or "").strip()
            if identity:
                approved[identity] = dict(row)
        return approved

    def _apply_report_declared_auto_approval_state(
        self,
        state: dict[str, list[dict[str, Any]]],
        report: dict[str, Any],
        *,
        finished_at: str,
    ) -> tuple[dict[str, list[dict[str, Any]]], int]:
        approved_candidates = self._report_declared_auto_approved_candidates(report)
        if not approved_candidates:
            return state, 0
        moved: list[dict[str, Any]] = []
        remaining_pending: list[dict[str, Any]] = []
        for pending_row in list(state.get("pending") or []):
            if not isinstance(pending_row, dict):
                continue
            identity = str(source_identity(pending_row) or "").strip()
            candidate = approved_candidates.get(identity)
            if not candidate:
                remaining_pending.append(dict(pending_row))
                continue
            approved_at = str(
                candidate.get("stateChangedAt")
                or candidate.get("lastPromotedAt")
                or candidate.get("approvedAt")
                or finished_at
                or self._deps.now_iso()
                or ""
            )
            promotion_reason = str(
                candidate.get("promotionReason")
                or candidate.get("pendingReason")
                or REGISTRY_REASON_DISCOVERY_AUTO_APPROVE
            ).strip()
            merged = {**dict(pending_row), **candidate}
            moved.append(
                dict(
                    transition_registry_to_active(
                        merged,
                        reason=promotion_reason or REGISTRY_REASON_DISCOVERY_AUTO_APPROVE,
                        actor="discovery_auto_approve",
                        at=approved_at,
                    )
                )
            )
        if not moved:
            return state, 0
        next_state = {
            "active": unique_sources([*list(state.get("active") or []), *moved]),
            "pending": unique_sources(remaining_pending),
            "rejected": unique_sources(list(state.get("rejected") or [])),
        }
        return next_state, len(moved)

    def _terminal_report_auto_approval_enabled(
        self,
        report: dict[str, Any],
        *,
        saved_config_enabled: bool,
    ) -> bool:
        runtime = as_json_object(report.get("runtime"))
        auto_approval = as_json_object(runtime.get("autoApproval"))
        if "enabled" in auto_approval:
            return bool(auto_approval.get("enabled"))
        return bool(saved_config_enabled)

    def _reconcile_terminal_discovery_registry_state(
        self,
        *,
        run_id: str,
        finished_at: str,
        report: dict[str, Any],
        saved_config_enabled: bool | None = None,
    ) -> tuple[dict[str, Any], int, bool]:
        if not str(finished_at or "").strip() or not self._discovery_report_finalization_settled(
            report
        ):
            return report, 0, False

        if saved_config_enabled is None:
            saved_config = self.get_saved_discovery_config_payload()
            saved_config_enabled = bool(saved_config.get("autoApproveHealthyPendingOnComplete"))

        auto_approve_enabled = self._terminal_report_auto_approval_enabled(
            report,
            saved_config_enabled=bool(saved_config_enabled),
        )
        current_state = self._deps.load_state()
        before_signature = self._state_bucket_identity_signature(current_state)
        finalization_counts = self._registry_finalization_counts(report)
        if finalization_counts is not None and self._state_matches_registry_finalization(
            current_state, finalization_counts
        ):
            return report, 0, False

        report_declares_auto_approval = self._report_declares_auto_approval(report)
        next_state, auto_approved = apply_discovery_auto_approval(
            current_state,
            report,
            auto_approve_enabled=auto_approve_enabled,
            approval_state_path=self._paths.approval_state,
            record_approval_state=not report_declares_auto_approval,
            now_iso_fn=self._deps.now_iso,
        )
        if (
            report_declares_auto_approval
            and finalization_counts is not None
            and not self._state_matches_registry_finalization(next_state, finalization_counts)
        ):
            declared_state, declared_approved = self._apply_report_declared_auto_approval_state(
                current_state,
                report,
                finished_at=finished_at,
            )
            if self._state_bucket_identity_signature(declared_state) != before_signature:
                next_state = declared_state
                auto_approved = max(int(auto_approved), int(declared_approved))
        after_signature = self._state_bucket_identity_signature(next_state)
        state_changed = before_signature != after_signature
        if not state_changed:
            if finalization_counts is not None:
                self._deps.bridge_log(
                    "warn",
                    "discovery_registry_reconciliation_unresolved",
                    runId=run_id,
                    finishedAt=finished_at,
                    autoApprovalEnabled=auto_approve_enabled,
                    approved=int(auto_approved),
                    expectedCounts=finalization_counts,
                    actualCounts=self._state_bucket_counts(current_state),
                )
            return report, 0, False

        if finalization_counts and not self._state_matches_registry_finalization(
            next_state, finalization_counts
        ):
            self._deps.bridge_log(
                "warn",
                "discovery_registry_reconciliation_skipped",
                runId=run_id,
                finishedAt=finished_at,
                approved=int(auto_approved),
                expectedCounts=finalization_counts,
                reconciledCounts=self._state_bucket_counts(next_state),
            )
            return report, 0, False

        persisted_state = self._deps.persist_state_and_auto_sync(
            next_state,
            reason="discovery_auto_approve",
        )
        self._deps.save_json_atomic(self._paths.report, report)
        self._deps.bridge_log(
            "info",
            "discovery_registry_reconciled_from_terminal_report",
            runId=run_id,
            finishedAt=finished_at,
            approved=int(auto_approved),
            expectedCounts=finalization_counts or {},
            persistedCounts=self._state_bucket_counts(persisted_state),
        )
        return report, int(auto_approved), True
