"""Registry service for source registry operations.

This module provides RegistryService for managing active/pending/rejected
source registry state.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge.registry_conflicts import apply_registry_conflict_safe_demotions
from src.bridge.registry_tombstones import filter_tombstoned_rows
from src.bridge.registry_tombstones import load_tombstones as load_registry_tombstones
from src.bridge.storage_health import (
    get_storage_store,
)
from src.bridge.storage_health import (
    record_storage_diagnostic as default_record_storage_diagnostic,
)
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
from src.storage.baluffo_store import BaluffoStoreError
from src.storage.source_registry_runtime import (
    SourceRegistryRuntimeStore,
    source_registry_state_hash,
    source_registry_tombstone_hash,
)

NormalizeManualStaticFunc = Callable[[dict[str, Any]], dict[str, Any]]
RuntimeStoreFactory = Callable[[], SourceRegistryRuntimeStore]
StorageDiagnosticRecorder = Callable[..., None]

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

_STORAGE_OPERATION_ERRORS = (
    BaluffoStoreError,
    OSError,
    RuntimeError,
    sqlite3.Error,
    TypeError,
    ValueError,
)


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
        runtime_store_factory: RuntimeStoreFactory | None = None,
        record_storage_diagnostic: StorageDiagnosticRecorder | None = None,
    ) -> None:
        self._paths = paths
        self._data_dir = Path(paths.active).expanduser().resolve().parent
        self._default_active = [
            dict(row) for row in (default_active or []) if isinstance(row, dict)
        ]
        self._normalize_manual_static = normalize_manual_static
        self._runtime_store_factory = runtime_store_factory
        self._record_storage_diagnostic = (
            record_storage_diagnostic or default_record_storage_diagnostic
        )
        self._last_auto_heal_report: dict[str, Any] = {
            "autoHealed": False,
            "duplicateSourceIdCount": 0,
            "duplicates": [],
            "safeAutomation": self._empty_safe_automation_report(),
        }

    def _runtime_store(self) -> SourceRegistryRuntimeStore:
        if self._runtime_store_factory is not None:
            return self._runtime_store_factory()
        return SourceRegistryRuntimeStore(get_storage_store(self._data_dir))

    def _record_registry_diagnostic(
        self,
        code: str,
        *,
        ok: bool,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        self._record_storage_diagnostic(
            self._data_dir,
            surface="sourceRegistry",
            code=code,
            ok=ok,
            message=message,
            details=details or {},
        )

    def _authority_mode(self) -> str:
        try:
            mode = self._runtime_store().store.get_authority_modes().get("sourceRegistry")
        except _STORAGE_OPERATION_ERRORS as exc:
            self._record_registry_diagnostic(
                "source_registry_authority_read_failed",
                ok=False,
                message=str(exc),
            )
            return "json"
        return str(mode or "json").strip().lower()

    def _force_json_authority(self, reason: str) -> None:
        try:
            self._runtime_store().store.set_authority_mode(
                "sourceRegistry",
                "json",
                reason=reason,
            )
        except _STORAGE_OPERATION_ERRORS:
            return

    def _tombstones_path(self) -> Path:
        return Path(self._paths.active).with_name("source-registry-tombstones.json")

    def _load_tombstones_json(self) -> dict[str, dict[str, Any]]:
        return load_registry_tombstones(self._tombstones_path())

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
        mode = self._authority_mode()
        if mode in {"shadow", "sqlite"}:
            self._mirror_state_to_sqlite(state, reason=f"source_registry_{mode}_mirror")

    def _mirror_state_to_sqlite(
        self,
        state: dict[str, list[dict[str, Any]]],
        *,
        reason: str,
    ) -> None:
        tombstones = self._load_tombstones_json()
        try:
            runtime_store = self._runtime_store()
            summary = runtime_store.replace_state(
                state=state,
                tombstones=tombstones,
                reason=reason,
            )
            sqlite_hashes = runtime_store.parity_hash()
            json_state_hash = source_registry_state_hash(state)
            json_tombstone_hash = source_registry_tombstone_hash(tombstones)
            if (
                sqlite_hashes["stateHash"] != json_state_hash
                or sqlite_hashes["tombstoneHash"] != json_tombstone_hash
            ):
                runtime_store.store.set_authority_mode(
                    "sourceRegistry",
                    "json",
                    reason="source_registry_projection_mismatch",
                )
                self._record_registry_diagnostic(
                    "source_registry_projection_mismatch",
                    ok=False,
                    message="SQLite source-registry projection did not match JSON authority",
                    details={
                        "generation": summary.generation,
                        "jsonStateHash": json_state_hash,
                        "sqliteStateHash": sqlite_hashes["stateHash"],
                        "jsonTombstoneHash": json_tombstone_hash,
                        "sqliteTombstoneHash": sqlite_hashes["tombstoneHash"],
                    },
                )
                return
            deleted_generations = runtime_store.cleanup_old_generations()
            self._record_registry_diagnostic(
                "source_registry_projection_match",
                ok=True,
                message="SQLite source-registry projection matched JSON authority",
                details={
                    **summary.to_dict(),
                    "deletedOldGenerations": deleted_generations,
                },
            )
        except _STORAGE_OPERATION_ERRORS as exc:
            self._force_json_authority("source_registry_shadow_write_failed")
            self._record_registry_diagnostic(
                "source_registry_shadow_write_failed",
                ok=False,
                message=str(exc),
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
