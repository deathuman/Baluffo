"""Discovery service discovery service registry.

AI boundary owns: bridge-owned discovery task launch, config persistence, and auto-sync watch behavior.
AI boundary implement in: this discovery_service_registry.py leaf.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.bridge.discovery_registry_retry_queue import (
    RECONCILE_RETRY_MAX_BYTES,
    RECONCILE_RETRY_MAX_ENTRIES,
    evict_oldest_retries,
    evict_retries_over_budget,
    retry_entry_bytes,
    retry_queue_bytes,
    retry_report_for_drain,
    retry_report_snapshot,
)
from src.bridge.discovery_service_core import DiscoveryServiceState
from src.bridge.registry_tombstones import load_tombstones as load_registry_tombstones
from src.shared.json_shapes import as_json_list, as_json_object
from src.shared.utils import parse_iso
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
from src.source_registry_pending_merge import merge_run_pending_rows
from src.source_registry_write_lock import (
    SourceRegistryWriteLockTimeout,
    source_registry_write_lock,
)

_REGISTRY_RECONCILE_LOCK_TIMEOUT_S = 30.0
_REGISTRY_RECONCILE_RETRY_FILENAME = "source-registry-reconcile-retry.json"
# Names the one entry a drain is reconciling. A bridge that dies mid-reconcile
# charges that entry on the next drain instead of spending an attempt on every
# queued report.
_REGISTRY_RECONCILE_INFLIGHT_FILENAME = "source-registry-reconcile-retry-inflight.json"
_REGISTRY_RECONCILE_RETRY_LOCK = threading.Lock()
_RECONCILE_RETRY_ERRORS = (OSError, RuntimeError, sqlite3.Error, TypeError, ValueError)
# A retry older than a week is stale: the authority has moved on and the report
# snapshot is no longer a current repair. The attempt cap bounds repeated
# failures long before that; both checks keep a stuck entry from retrying forever.
_RECONCILE_RETRY_MAX_AGE_S = 7 * 24 * 60 * 60
_RECONCILE_RETRY_MAX_ATTEMPTS = 10
# The pre-publish report is written immediately before the short authority
# publish. Five minutes without a terminal report means that publish died.
_RUNNING_REPORT_STALE_S = 5 * 60


def _same_queued_retry(stored: dict[str, Any], drained: dict[str, Any] | None) -> bool:
    """Whether a stored entry is still the snapshot the drain read."""
    if drained is None:
        return False
    return str(stored.get("finishedAt") or "") == str(drained.get("finishedAt") or "")


@dataclass(frozen=True)
class _PendingMergeOutcome:
    """What merging a failed publish's staged rows into the authority produced."""

    restaged: int = 0
    updated: int = 0
    audit: dict[str, int] = field(default_factory=dict)
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    active_conflicts: list[dict[str, Any]] = field(default_factory=list)


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

    def _running_report_is_stale(self, report: dict[str, Any]) -> bool:
        runtime = as_json_object(report.get("runtime"))
        finalization = as_json_object(runtime.get("registryFinalization"))
        if str(finalization.get("status") or "").strip().lower() != "running":
            return False
        if not as_json_list(finalization.get("pendingRows")):
            return False
        lifecycle = as_json_object(runtime.get("lifecycle"))
        stamp = str(
            report.get("finishedAt")
            or lifecycle.get("heartbeatAt")
            or report.get("startedAt")
            or ""
        ).strip()
        try:
            finished = parse_iso(stamp)
            now = parse_iso(self._deps.now_iso())
        except (TypeError, ValueError):
            return False
        if finished is None or now is None:
            return False
        try:
            return (now - finished).total_seconds() >= _RUNNING_REPORT_STALE_S
        except (TypeError, ValueError):
            return False

    def _discovery_report_finalization_recoverable(self, report: dict[str, Any]) -> bool:
        return self._discovery_report_finalization_settled(report) or self._running_report_is_stale(
            report
        )

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

    def _load_registry_tombstones(self) -> dict[str, dict[str, Any]]:
        """Read the configured tombstone ledger, not a sibling of the pending export.

        Deriving the ledger from the pending path silently disabled resurrection
        protection whenever a custom data layout put the pending export elsewhere.
        """
        return load_registry_tombstones(Path(self._paths.tombstones))

    def _staged_pending_rows(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        runtime = as_json_object(report.get("runtime"))
        finalization = as_json_object(runtime.get("registryFinalization"))
        status = str(finalization.get("status") or "").strip().lower()
        if status != "failed" and not self._running_report_is_stale(report):
            return []
        return [
            dict(row)
            for row in as_json_list(finalization.get("pendingRows"))
            if isinstance(row, dict)
        ]

    def _stage_failed_publish_pending(
        self,
        state: dict[str, list[dict[str, Any]]],
        report: dict[str, Any],
    ) -> tuple[dict[str, list[dict[str, Any]]], _PendingMergeOutcome, set[str]]:
        """Restage pending rows the worker could not publish to the authority.

        The worker records the exact rows it would have staged in the terminal
        report. A failed authority publish leaves them out of the registry, so
        merge them through the same function the authority publish uses: current
        rows keep identity, routing, display and lifecycle, the run's newer
        observations are carried onto them, a repeated-zero-job hide the run
        applied is carried as a lifecycle transition, and rows the authority has
        since placed or tombstoned are not re-added.
        """
        run_pending = self._staged_pending_rows(report)
        if not run_pending:
            return state, _PendingMergeOutcome(), set()
        audit: dict[str, int] = {}
        conflicts: list[dict[str, Any]] = []
        active_conflicts: list[dict[str, Any]] = []
        routing_conflict_ids: set[str] = set()
        placed_rows = {
            source_identity(row): row
            for bucket in ("active", "rejected")
            for row in list(state.get(bucket) or [])
            if isinstance(row, dict) and source_identity(row)
        }
        merged_pending, restaged_count, updated_count = merge_run_pending_rows(
            state.get("pending") or [],
            run_pending,
            placed_ids=placed_rows,
            placed_rows=placed_rows,
            tombstones=self._load_registry_tombstones(),
            audit=audit,
            conflicts=conflicts,
            active_conflicts=active_conflicts,
            routing_conflict_ids=routing_conflict_ids,
        )
        return (
            {
                "active": list(state.get("active") or []),
                "pending": merged_pending,
                "rejected": list(state.get("rejected") or []),
            },
            _PendingMergeOutcome(
                restaged=restaged_count,
                updated=updated_count,
                audit=audit,
                conflicts=conflicts,
                active_conflicts=active_conflicts,
            ),
            routing_conflict_ids,
        )

    def _mark_pending_merge(self, report: dict[str, Any], merge: _PendingMergeOutcome) -> None:
        if not merge.restaged and not merge.updated and not merge.audit:
            return
        now = self._deps.now_iso()
        runtime = as_json_object(report.get("runtime"))
        finalization = as_json_object(runtime.get("registryFinalization"))
        marker = dict(finalization)
        if merge.restaged:
            marker["pendingRestagedCount"] = merge.restaged
            marker["pendingRestagedAt"] = now
        if merge.updated:
            marker["pendingUpdatedCount"] = merge.updated
            marker["pendingUpdatedAt"] = now
        if merge.audit:
            marker["pendingMergeAudit"] = dict(merge.audit)
            if merge.conflicts:
                marker["pendingMergeConflicts"] = [dict(conflict) for conflict in merge.conflicts]
            else:
                marker.pop("pendingMergeConflicts", None)
            if merge.active_conflicts:
                marker["activeMergeConflicts"] = [
                    dict(conflict) for conflict in merge.active_conflicts
                ]
            else:
                marker.pop("activeMergeConflicts", None)
        report["runtime"] = {**runtime, "registryFinalization": marker}

    def _log_pending_merge(
        self,
        report: dict[str, Any],
        *,
        run_id: str,
        finished_at: str,
        merge: _PendingMergeOutcome,
        pending_count: int,
    ) -> None:
        if not merge.restaged and not merge.updated:
            return
        finalization = as_json_object(
            as_json_object(report.get("runtime")).get("registryFinalization")
        )
        common = {
            "runId": run_id,
            "finishedAt": finished_at,
            "pendingCount": pending_count,
            "publishError": str(finalization.get("error") or ""),
        }
        if merge.restaged:
            self._deps.bridge_log(
                "info",
                "discovery_registry_pending_restaged",
                restagedCount=merge.restaged,
                **common,
            )
        if merge.updated:
            self._deps.bridge_log(
                "info",
                "discovery_registry_pending_updated",
                updatedCount=merge.updated,
                **common,
            )

    def _log_pending_merge_audit(
        self,
        *,
        run_id: str,
        finished_at: str,
        merge: _PendingMergeOutcome,
    ) -> None:
        if not merge.audit and not merge.conflicts and not merge.active_conflicts:
            return
        fields: dict[str, Any] = {
            "runId": run_id,
            "finishedAt": finished_at,
            **merge.audit,
        }
        if merge.conflicts:
            fields["truncated"] = int(merge.audit.get("fieldConflicts") or 0) > len(merge.conflicts)
            fields["conflicts"] = merge.conflicts
        if merge.active_conflicts:
            fields["activeTruncated"] = int(merge.audit.get("activeFieldConflicts") or 0) > len(
                merge.active_conflicts
            )
            fields["activeConflicts"] = merge.active_conflicts
        self._deps.bridge_log(
            "info",
            "discovery_registry_pending_merge_audit",
            **fields,
        )

    def _mark_recovered_running_report(self, report: dict[str, Any]) -> None:
        runtime = as_json_object(report.get("runtime"))
        finalization = as_json_object(runtime.get("registryFinalization"))
        finalization["status"] = "failed"
        finalization["error"] = "stale_running_report_recovered"
        finalization["recoveredAt"] = self._deps.now_iso()
        auto_approval = as_json_object(runtime.get("autoApproval"))
        if str(auto_approval.get("status") or "").strip().lower() == "running":
            auto_approval["status"] = "failed"
            auto_approval["error"] = "stale_running_report_recovered"
            runtime["autoApproval"] = auto_approval
        report["runtime"] = {**runtime, "registryFinalization": finalization}

    @staticmethod
    def _pending_merge_reason(merge: _PendingMergeOutcome) -> str:
        """Name the merge that actually happened: a field-only carry is not a restage."""
        if merge.restaged:
            return "discovery_pending_restage"
        if merge.updated:
            return "discovery_pending_update"
        return "discovery_auto_approve"

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

    def _reconciliation_retry_path(self) -> Path:
        return Path(self._paths.pending).parent / _REGISTRY_RECONCILE_RETRY_FILENAME

    def _load_reconciliation_retries(self) -> dict[str, dict[str, Any]]:
        payload = as_json_object(self._deps.load_json_object(self._reconciliation_retry_path(), {}))
        retries = as_json_object(payload.get("retries"))
        return {
            str(run_id): dict(entry) for run_id, entry in retries.items() if isinstance(entry, dict)
        }

    def _save_reconciliation_retries(self, retries: dict[str, dict[str, Any]]) -> None:
        self._deps.save_json_atomic(self._reconciliation_retry_path(), {"retries": retries})

    def _reconciliation_inflight_path(self) -> Path:
        return Path(self._paths.pending).parent / _REGISTRY_RECONCILE_INFLIGHT_FILENAME

    def _load_reconciliation_inflight(self) -> str:
        payload = as_json_object(
            self._deps.load_json_object(self._reconciliation_inflight_path(), {})
        )
        return str(payload.get("runId") or "").strip()

    def _mark_reconciliation_inflight(self, run_id: str) -> None:
        """Record the entry being reconciled so a crash charges it, not the queue."""
        self._deps.save_json_atomic(self._reconciliation_inflight_path(), {"runId": run_id})

    def _clear_reconciliation_inflight(self) -> None:
        self._deps.save_json_atomic(self._reconciliation_inflight_path(), {})

    def _record_reconciliation_retry(
        self,
        *,
        run_id: str,
        finished_at: str,
        report: dict[str, Any],
        reason: str,
    ) -> None:
        """Keep a minimal run snapshot so a later launch, completion or startup can retry it."""
        with _REGISTRY_RECONCILE_RETRY_LOCK:
            retries = self._load_reconciliation_retries()
            prior = retries.get(run_id) or {}
            attempts = self._retry_attempt_count(prior)
            entry: dict[str, Any] = {
                "finishedAt": finished_at,
                "report": retry_report_snapshot(report),
            }
            if attempts:
                entry["attempts"] = attempts
            retries[run_id] = entry
            evicted = evict_oldest_retries(
                retries,
                keep_run_id=run_id,
                max_entries=RECONCILE_RETRY_MAX_ENTRIES,
            )
            evicted_over_budget = evict_retries_over_budget(
                retries,
                keep_run_id=run_id,
                max_bytes=RECONCILE_RETRY_MAX_BYTES,
            )
            queue_bytes = retry_queue_bytes(retries)
            entry_bytes = retry_entry_bytes(run_id, entry)
            self._save_reconciliation_retries(retries)
        if evicted:
            self._deps.bridge_log(
                "warn",
                "discovery_registry_reconciliation_retry_evicted",
                reason="queue_limit",
                cap=RECONCILE_RETRY_MAX_ENTRIES,
                queueSize=len(retries),
                evictedRunIds=evicted,
                keptRunId=run_id,
            )
        if evicted_over_budget:
            self._deps.bridge_log(
                "warn",
                "discovery_registry_reconciliation_retry_evicted",
                reason="byte_budget",
                budgetBytes=RECONCILE_RETRY_MAX_BYTES,
                queueBytes=queue_bytes,
                queueSize=len(retries),
                evictedRunIds=evicted_over_budget,
                keptRunId=run_id,
            )
        if entry_bytes > RECONCILE_RETRY_MAX_BYTES:
            self._deps.bridge_log(
                "warn",
                "discovery_registry_reconciliation_retry_oversized",
                runId=run_id,
                entryBytes=entry_bytes,
                budgetBytes=RECONCILE_RETRY_MAX_BYTES,
            )
        self._deps.bridge_log(
            "warn",
            "discovery_registry_reconciliation_retry_recorded",
            runId=run_id,
            finishedAt=finished_at,
            reason=reason,
        )

    def _finalize_reconciliation_retries(
        self,
        *,
        attempts_by_run: dict[str, int],
        remove_run_ids: list[str],
        drained: dict[str, dict[str, Any]],
    ) -> None:
        """Persist a drain's attempt counters and resolved removals in one rewrite.

        Only entries the drain read are written: a retry re-recorded while the
        pass ran keeps its newer snapshot and its own attempt count.
        """
        if not attempts_by_run and not remove_run_ids:
            return
        with _REGISTRY_RECONCILE_RETRY_LOCK:
            retries = self._load_reconciliation_retries()
            changed = False
            for run_id, attempts in attempts_by_run.items():
                entry = retries.get(run_id)
                if entry is None or not _same_queued_retry(entry, drained.get(run_id)):
                    continue
                entry["attempts"] = attempts
                retries[run_id] = entry
                changed = True
            for run_id in remove_run_ids:
                entry = retries.get(run_id)
                if entry is None or not _same_queued_retry(entry, drained.get(run_id)):
                    continue
                retries.pop(run_id)
                changed = True
            if changed:
                self._save_reconciliation_retries(retries)

    @staticmethod
    def _retry_attempt_count(entry: dict[str, Any]) -> int:
        try:
            return max(0, int(entry.get("attempts") or 0))
        except (TypeError, ValueError):
            return 0

    def _reconciliation_retry_is_expired(self, finished_at: str) -> bool:
        try:
            finished = parse_iso(finished_at)
            now = parse_iso(self._deps.now_iso())
        except (TypeError, ValueError):
            return True
        if finished is None:
            return True
        if now is None:
            return False
        try:
            return (now - finished).total_seconds() > _RECONCILE_RETRY_MAX_AGE_S
        except (TypeError, ValueError):
            return True

    def _charge_interrupted_reconciliation_retry(
        self, queued: dict[str, dict[str, Any]]
    ) -> set[str]:
        """Charge the entry a previous drain died while reconciling.

        Charging the whole queue before the drain reaches it spends an attempt on
        every queued report, so ten restarts abandon reports the drain never
        touched. Only the in-flight entry is charged here; every other entry
        keeps the count the last completed drain left it.
        """
        run_id = self._load_reconciliation_inflight()
        if not run_id:
            return set()
        self._clear_reconciliation_inflight()
        entry = queued.get(run_id)
        if entry is None:
            return set()
        attempts = self._retry_attempt_count(entry) + 1
        entry["attempts"] = attempts
        self._finalize_reconciliation_retries(
            attempts_by_run={run_id: attempts},
            remove_run_ids=[],
            drained={run_id: entry},
        )
        self._deps.bridge_log(
            "warn",
            "discovery_registry_reconciliation_retry_interrupted",
            runId=run_id,
            attempts=attempts,
        )
        return {run_id}

    def _log_reconciliation_retry_abandoned(
        self, *, run_id: str, entry: dict[str, Any], reason: str
    ) -> None:
        self._deps.bridge_log(
            "warn",
            "discovery_registry_reconciliation_retry_abandoned",
            runId=run_id,
            finishedAt=str(entry.get("finishedAt") or ""),
            attempts=self._retry_attempt_count(entry),
            reason=reason,
        )

    def _reconciliation_report_is_applied(self, report: dict[str, Any]) -> bool:
        """Whether re-running the report's merge would change the authority state."""
        state = self._deps.load_state()
        pending_rows = self._staged_pending_rows(report)
        if not pending_rows:
            counts = self._registry_finalization_counts(report)
            return counts is None or self._state_matches_registry_finalization(state, counts)
        placed = {
            source_identity(row)
            for bucket in ("active", "rejected")
            for row in list(state.get(bucket) or [])
            if isinstance(row, dict)
        }
        _merged, restaged, updated = merge_run_pending_rows(
            state.get("pending") or [],
            pending_rows,
            placed_ids=placed,
            tombstones=self._load_registry_tombstones(),
        )
        return restaged == 0 and updated == 0

    def drain_pending_registry_reconciliation(self) -> int:
        """Retry reports left by a lock timeout; the bridge startup calls this.

        Returns the number of queued reports the drain attempted.
        """
        return self._drain_reconciliation_retries()

    def _plan_reconciliation_retry_attempts(
        self,
        entries: list[tuple[str, dict[str, Any]]],
        interrupted: set[str],
    ) -> tuple[dict[str, int], list[str]]:
        """Split queued entries into the ones this drain attempts and the ones it abandons."""
        attempts_by_run: dict[str, int] = {}
        dropped: list[str] = []
        for run_id, entry in entries:
            report = as_json_object(entry.get("report"))
            finished_at = str(entry.get("finishedAt") or "").strip()
            if not report or not finished_at:
                dropped.append(run_id)
                continue
            attempts = self._retry_attempt_count(entry)
            if attempts >= _RECONCILE_RETRY_MAX_ATTEMPTS:
                self._log_reconciliation_retry_abandoned(
                    run_id=run_id, entry=entry, reason="attempt_limit"
                )
                dropped.append(run_id)
                continue
            if self._reconciliation_retry_is_expired(finished_at):
                self._log_reconciliation_retry_abandoned(
                    run_id=run_id, entry=entry, reason="age_limit"
                )
                dropped.append(run_id)
                continue
            # Charge only the entries the drain reaches. The count is written
            # with the removals at the end, so an unreached entry keeps its own.
            if run_id not in interrupted:
                attempts_by_run[run_id] = attempts + 1
        return attempts_by_run, dropped

    def _reconcile_drained_retry(self, run_id: str, entry: dict[str, Any]) -> bool:
        """Reconcile one queued report; True once the authority reflects it."""
        try:
            # Name the entry before touching it: a bridge that dies in the
            # reconcile charges this entry on the next drain, not the queue.
            self._mark_reconciliation_inflight(run_id)
            report = retry_report_for_drain(as_json_object(entry.get("report")))
            finished_at = str(entry.get("finishedAt") or "").strip()
            _report, _approved, persisted = self._reconcile_terminal_discovery_registry_state(
                run_id=run_id,
                finished_at=finished_at,
                report=report,
                write_report=False,
            )
            return bool(persisted or self._reconciliation_report_is_applied(report))
        except _RECONCILE_RETRY_ERRORS as exc:
            self._deps.bridge_log(
                "warn",
                "discovery_registry_reconciliation_retry_failed",
                runId=run_id,
                errorType=type(exc).__name__,
                error=str(exc),
            )
            return False

    def _drain_reconciliation_retries(self) -> int:
        queued = self._load_reconciliation_retries()
        entries = list(queued.items())
        interrupted = self._charge_interrupted_reconciliation_retry(queued)
        attempts_by_run, dropped = self._plan_reconciliation_retry_attempts(entries, interrupted)
        attempted = (set(attempts_by_run) | interrupted) - set(dropped)
        cleared: list[str] = []
        for run_id, entry in entries:
            if run_id not in attempted:
                continue
            if self._reconcile_drained_retry(run_id, entry):
                cleared.append(run_id)
        # A queue entry can hold a large report snapshot. Persist the whole
        # drain's attempt counters and removals in one rewrite, after the pass.
        self._finalize_reconciliation_retries(
            attempts_by_run=attempts_by_run,
            remove_run_ids=[*dropped, *cleared],
            drained=queued,
        )
        if attempted:
            self._clear_reconciliation_inflight()
        for run_id in cleared:
            self._deps.bridge_log(
                "info",
                "discovery_registry_reconciliation_retry_succeeded",
                runId=run_id,
            )
        return len(entries)

    def _reconcile_terminal_discovery_registry_state(
        self,
        *,
        run_id: str,
        finished_at: str,
        report: dict[str, Any],
        saved_config_enabled: bool | None = None,
        write_report: bool = True,
    ) -> tuple[dict[str, Any], int, bool]:
        """Reconcile the registry under the registry write lock.

        The reconciliation loads the authority, merges the run's rows, and
        persists a whole state. Without the lock an admin mutation that lands in
        between is overwritten by this write. A retry drain passes
        ``write_report=False``: its snapshot must not replace a newer terminal
        report, but its state merge still persists.
        """
        try:
            with source_registry_write_lock(
                Path(self._paths.pending).parent,
                timeout_s=_REGISTRY_RECONCILE_LOCK_TIMEOUT_S,
            ):
                return self._reconcile_terminal_discovery_registry_state_locked(
                    run_id=run_id,
                    finished_at=finished_at,
                    report=report,
                    saved_config_enabled=saved_config_enabled,
                    write_report=write_report,
                )
        except SourceRegistryWriteLockTimeout:
            self._record_reconciliation_retry(
                run_id=run_id,
                finished_at=finished_at,
                report=report,
                reason="lock_timeout",
            )
            self._deps.bridge_log(
                "warn",
                "discovery_registry_reconciliation_lock_timeout",
                runId=run_id,
                finishedAt=finished_at,
                retryRecorded=True,
            )
            return report, 0, False

    def _reconcile_terminal_discovery_registry_state_locked(
        self,
        *,
        run_id: str,
        finished_at: str,
        report: dict[str, Any],
        saved_config_enabled: bool | None = None,
        write_report: bool = True,
    ) -> tuple[dict[str, Any], int, bool]:
        if not str(
            finished_at or ""
        ).strip() or not self._discovery_report_finalization_recoverable(report):
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
        recovered_running = self._running_report_is_stale(report)

        staged_state, merge, routing_conflict_ids = self._stage_failed_publish_pending(
            current_state, report
        )
        self._log_pending_merge_audit(run_id=run_id, finished_at=finished_at, merge=merge)
        report_declares_auto_approval = self._report_declares_auto_approval(report)
        next_state, auto_approved = apply_discovery_auto_approval(
            staged_state,
            report,
            auto_approve_enabled=auto_approve_enabled,
            approval_state_path=self._paths.approval_state,
            record_approval_state=not report_declares_auto_approval,
            blocked_ids=routing_conflict_ids,
            now_iso_fn=self._deps.now_iso,
        )
        next_state, auto_approved = self._apply_declared_auto_approval_fallback(
            next_state=next_state,
            auto_approved=auto_approved,
            current_state=current_state,
            report=report,
            finished_at=finished_at,
            before_signature=before_signature,
            finalization_counts=finalization_counts,
            report_declares_auto_approval=report_declares_auto_approval,
        )
        after_signature = self._state_bucket_identity_signature(next_state)
        state_changed = before_signature != after_signature or merge.updated > 0
        if not state_changed:
            return self._finish_unchanged_registry_reconciliation(
                run_id=run_id,
                finished_at=finished_at,
                report=report,
                current_state=current_state,
                finalization_counts=finalization_counts,
                recovered_running=recovered_running,
                merge=merge,
                auto_approve_enabled=auto_approve_enabled,
                auto_approved=auto_approved,
                write_report=write_report,
            )

        if not self._state_matches_registry_finalization(next_state, finalization_counts):
            # The report's counts are a start-of-run snapshot: a concurrent admin
            # change can legitimately make them disagree with the merged state.
            # Persist the merge anyway; dropping it here loses this run's rows.
            self._deps.bridge_log(
                "warn",
                "discovery_registry_reconciliation_count_mismatch",
                runId=run_id,
                finishedAt=finished_at,
                approved=int(auto_approved),
                expectedCounts=finalization_counts,
                reconciledCounts=self._state_bucket_counts(next_state),
            )

        if recovered_running:
            self._mark_recovered_running_report(report)
        self._mark_pending_merge(report, merge)
        persisted_state = self._deps.persist_state_and_auto_sync(
            next_state,
            reason=self._pending_merge_reason(merge),
        )
        if write_report:
            self._deps.save_json_atomic(self._paths.report, report)
        self._log_pending_merge(
            report,
            run_id=run_id,
            finished_at=finished_at,
            merge=merge,
            pending_count=len(next_state.get("pending") or []),
        )
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

    def _apply_declared_auto_approval_fallback(
        self,
        *,
        next_state: dict[str, list[dict[str, Any]]],
        auto_approved: int,
        current_state: dict[str, list[dict[str, Any]]],
        report: dict[str, Any],
        finished_at: str,
        before_signature: tuple[tuple[str, tuple[str, ...]], ...],
        finalization_counts: dict[str, int] | None,
        report_declares_auto_approval: bool,
    ) -> tuple[dict[str, list[dict[str, Any]]], int]:
        """Adopt the report's declared auto-approval rows when the merge disagrees.

        A retried terminal report declares the candidates it already approved;
        the saved config may have moved on since that run, so re-running the
        approval pass can reproduce a different bucket. The fallback only fires
        when the merged state cannot reproduce the report's counts, and it is
        dropped when it changes nothing, so it never masks a real merge.
        """
        if not report_declares_auto_approval or finalization_counts is None:
            return next_state, auto_approved
        if self._state_matches_registry_finalization(next_state, finalization_counts):
            return next_state, auto_approved
        declared_state, declared_approved = self._apply_report_declared_auto_approval_state(
            current_state,
            report,
            finished_at=finished_at,
        )
        if self._state_bucket_identity_signature(declared_state) == before_signature:
            return next_state, auto_approved
        return declared_state, max(int(auto_approved), int(declared_approved))

    def _finish_unchanged_registry_reconciliation(
        self,
        *,
        run_id: str,
        finished_at: str,
        report: dict[str, Any],
        current_state: dict[str, list[dict[str, Any]]],
        finalization_counts: dict[str, int] | None,
        recovered_running: bool,
        merge: _PendingMergeOutcome,
        auto_approve_enabled: bool,
        auto_approved: int,
        write_report: bool,
    ) -> tuple[dict[str, Any], int, bool]:
        """Record recovered-report evidence for a merge that changed no rows."""
        if recovered_running or merge.active_conflicts:
            if recovered_running:
                self._mark_recovered_running_report(report)
            if merge.active_conflicts:
                self._mark_pending_merge(report, merge)
            if write_report:
                self._deps.save_json_atomic(self._paths.report, report)
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
