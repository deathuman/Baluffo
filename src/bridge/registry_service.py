"""Registry service for source registry operations.

This module provides RegistryService for managing active/pending/rejected
source registry state.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge.registry_conflicts import apply_registry_conflict_safe_demotions
from src.bridge.registry_tombstones import filter_tombstoned_rows
from src.source_registry import (
    canonicalize_registry_row,
    demote_duplicate_active_variants,
    ensure_source_id,
    hide_repeated_zero_job_pending,
    load_json_array,
    load_json_object,
    normalize_source_url,
    save_json_atomic,
    save_registry_state_atomic,
    source_identity,
    source_url_fingerprint,
    unique_sources,
)

NormalizeManualStaticFunc = Callable[[dict[str, Any]], dict[str, Any]]

_DUPLICATE_STATE_FIELDS = {
    "id",
    "sourceId",
    "registryState",
    "candidateState",
    "transitionReason",
    "pendingReason",
    "quarantineReason",
    "quarantinedAt",
    "quarantinedBy",
    "reason",
    "stateChangedAt",
    "stateChangedBy",
    "lastPromotedAt",
    "lastDemotedAt",
    "approvedAt",
    "approvedBy",
    "liveAt",
}


@dataclass(frozen=True)
class RegistryPaths:
    active: Any
    pending: Any
    rejected: Any


class RegistryService:
    def __init__(
        self,
        *,
        paths: RegistryPaths,
        default_active: list[dict[str, Any]],
        normalize_manual_static: NormalizeManualStaticFunc,
    ) -> None:
        self._paths = paths
        self._default_active = [
            dict(row) for row in (default_active or []) if isinstance(row, dict)
        ]
        self._normalize_manual_static = normalize_manual_static
        self._last_auto_heal_report: dict[str, Any] = {
            "autoHealed": False,
            "duplicateSourceIdCount": 0,
            "duplicates": [],
            "safeAutomation": self._empty_safe_automation_report(),
        }

    @staticmethod
    def _value_is_missing(value: Any) -> bool:
        return value in ("", None) or value == [] or value == {}

    @staticmethod
    def _empty_auto_heal_report() -> dict[str, Any]:
        return {
            "autoHealed": False,
            "duplicateSourceIdCount": 0,
            "duplicates": [],
            "safeAutomation": RegistryService._empty_safe_automation_report(),
        }

    @staticmethod
    def _empty_safe_automation_report() -> dict[str, Any]:
        return {
            "autoDemoted": False,
            "demoted": 0,
            "skipped": 0,
            "applied": [],
            "skippedRows": [],
        }

    def _record_duplicate_auto_heal(
        self,
        report: dict[str, Any],
        *,
        source_id: str,
        kept_bucket: str,
        skipped_bucket: str,
        kept_row: dict[str, Any],
        skipped_row: dict[str, Any],
    ) -> None:
        merged_fields: list[str] = []
        for key, value in skipped_row.items():
            if key in _DUPLICATE_STATE_FIELDS:
                continue
            if self._value_is_missing(kept_row.get(key)) and not self._value_is_missing(value):
                kept_row[key] = value
                merged_fields.append(key)
        report["autoHealed"] = True
        duplicates = report.setdefault("duplicates", [])
        duplicates.append(
            {
                "sourceId": source_id,
                "keptBucket": kept_bucket,
                "removedBucket": skipped_bucket,
                "keptName": str(kept_row.get("name") or "").strip(),
                "removedName": str(skipped_row.get("name") or "").strip(),
                "mergedFields": sorted(merged_fields),
            }
        )
        report["duplicateSourceIdCount"] = len(
            {str(row.get("sourceId") or "") for row in duplicates if isinstance(row, dict)}
        )

    def get_auto_heal_report(self) -> dict[str, Any]:
        safe_automation = self._last_auto_heal_report.get("safeAutomation")
        if not isinstance(safe_automation, dict):
            safe_automation = self._empty_safe_automation_report()
        return {
            "autoHealed": bool(self._last_auto_heal_report.get("autoHealed")),
            "duplicateSourceIdCount": int(
                self._last_auto_heal_report.get("duplicateSourceIdCount") or 0
            ),
            "duplicates": [
                dict(row)
                for row in list(self._last_auto_heal_report.get("duplicates") or [])
                if isinstance(row, dict)
            ],
            "safeAutomation": {
                "autoDemoted": bool(safe_automation.get("autoDemoted")),
                "demoted": int(safe_automation.get("demoted") or 0),
                "skipped": int(safe_automation.get("skipped") or 0),
                "applied": [
                    dict(row)
                    for row in list(safe_automation.get("applied") or [])
                    if isinstance(row, dict)
                ],
                "skippedRows": [
                    dict(row)
                    for row in list(safe_automation.get("skippedRows") or [])
                    if isinstance(row, dict)
                ],
            },
        }

    def ensure_active_registry(self) -> list[dict[str, Any]]:
        active = load_json_array(self._paths.active, [])
        if active:
            return filter_tombstoned_rows(active)
        active = [
            canonicalize_registry_row(dict(row), bucket="active") for row in self._default_active
        ]
        save_json_atomic(self._paths.active, active)
        return active

    def normalize_state(
        self, state: dict[str, list[dict[str, Any]]]
    ) -> dict[str, list[dict[str, Any]]]:
        # Precedence is explicit: active > pending > rejected.
        seen: set[str] = set()
        seen_buckets: dict[str, str] = {}
        seen_rows: dict[str, dict[str, Any]] = {}
        auto_heal_report = self._empty_auto_heal_report()
        normalized: dict[str, list[dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
        for bucket in ("active", "pending", "rejected"):
            bucket_rows = filter_tombstoned_rows(
                [dict(row) for row in state.get(bucket, []) if isinstance(row, dict)]
            )
            for row in bucket_rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("adapter") or "").strip().lower() == "static":
                    row = self._normalize_manual_static(row)
                row = canonicalize_registry_row(row, bucket=bucket)
                if bucket == "pending":
                    row = hide_repeated_zero_job_pending(row)
                key = source_identity(row)
                if key in seen:
                    self._record_duplicate_auto_heal(
                        auto_heal_report,
                        source_id=key,
                        kept_bucket=seen_buckets.get(key, ""),
                        skipped_bucket=bucket,
                        kept_row=seen_rows[key],
                        skipped_row=row,
                    )
                    continue
                seen.add(key)
                seen_buckets[key] = bucket
                kept_row = ensure_source_id(row)
                seen_rows[key] = kept_row
                normalized[bucket].append(kept_row)
        self._last_auto_heal_report = auto_heal_report
        return normalized

    def _load_source_state_payload(self) -> dict[str, Any]:
        source_state_path = Path(self._paths.active).with_name("jobs-source-state.json")
        return load_json_object(source_state_path, {})

    def _apply_safe_conflict_demotions(
        self, state: dict[str, list[dict[str, Any]]]
    ) -> dict[str, list[dict[str, Any]]]:
        result = apply_registry_conflict_safe_demotions(
            state,
            self._load_source_state_payload(),
        )
        safe_automation_report = {
            "autoDemoted": bool(result.get("demoted")),
            "demoted": int(result.get("demoted") or 0),
            "skipped": int(result.get("skipped") or 0),
            "applied": [
                dict(row) for row in list(result.get("applied") or []) if isinstance(row, dict)
            ],
            "skippedRows": [
                dict(row) for row in list(result.get("skippedRows") or []) if isinstance(row, dict)
            ],
        }
        self._last_auto_heal_report["safeAutomation"] = safe_automation_report
        if safe_automation_report["autoDemoted"]:
            self._last_auto_heal_report["autoHealed"] = True
        return result["state"]

    def load_state(self) -> dict[str, list[dict[str, Any]]]:
        state = {
            "active": self.ensure_active_registry(),
            "pending": load_json_array(self._paths.pending, []),
            "rejected": load_json_array(self._paths.rejected, []),
        }
        normalized = self.normalize_state(state)
        normalized = self._apply_safe_conflict_demotions(normalized)
        if normalized != state:
            self._save_state(normalized)
        return normalized

    @staticmethod
    def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        return {
            "activeCount": len(state["active"]),
            "pendingCount": len(state["pending"]),
            "rejectedCount": len(state["rejected"]),
        }

    def persist_state(
        self, state: dict[str, list[dict[str, Any]]]
    ) -> dict[str, list[dict[str, Any]]]:
        normalized = self.normalize_state(state)
        self._save_state(normalized)
        return normalized

    def _save_state(self, state: dict[str, list[dict[str, Any]]]) -> None:
        save_registry_state_atomic(
            self._paths.active,
            self._paths.pending,
            self._paths.rejected,
            state,
        )

    @staticmethod
    def move_entries(
        pending: list[dict[str, Any]], selected_ids: list[str]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        selected = set(str(item) for item in selected_ids)
        moved: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        for row in pending:
            if source_identity(row) in selected:
                moved.append(row)
            else:
                remaining.append(row)
        return moved, remaining

    @staticmethod
    def unique_sources(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return unique_sources(rows)

    @staticmethod
    def source_identity(row: dict[str, Any]) -> str:
        return source_identity(row)

    @staticmethod
    def source_url_fingerprint(row: dict[str, Any]) -> str:
        return source_url_fingerprint(row)

    @staticmethod
    def normalize_source_url(url: str) -> str:
        return normalize_source_url(url)

    @staticmethod
    def demote_duplicate_active_variants(
        active_rows: list[dict[str, Any]],
        *,
        target_families: list[str] | None = None,
        source_state: Any = None,
        actor: str = "registry_noise_cleanup",
        at: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return demote_duplicate_active_variants(
            active_rows,
            target_families=target_families,
            source_state=source_state,
            actor=actor,
            at=at,
        )


__all__ = ["RegistryPaths", "RegistryService"]
