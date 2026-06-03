"""Registry service for source registry operations.

This module provides RegistryService for managing active/pending/rejected
source registry state.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

from src.bridge import registry_tombstones as registry_tombstones_module
from src.bridge.registry_conflicts import apply_registry_conflict_safe_demotions
from src.bridge.registry_tombstones import filter_tombstoned_rows
from src.bridge.registry_tombstones import load_tombstones as load_registry_tombstones
from src.bridge.registry_tombstones import normalize_tombstones as normalize_registry_tombstones
from src.bridge.registry_tombstones import save_tombstones as save_registry_tombstones
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
    summarize_json_array_storage,
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
_DEFAULT_TOMBSTONES_PATH = Path(registry_tombstones_module.TOMBSTONES_PATH)


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
        service_path = Path(self._paths.active).with_name("source-registry-tombstones.json")
        module_path = Path(registry_tombstones_module.TOMBSTONES_PATH)
        if (
            module_path != _DEFAULT_TOMBSTONES_PATH
            and module_path != service_path
            and module_path.exists()
            and not service_path.exists()
        ):
            return module_path
        return service_path

    def _load_tombstones_json(self) -> dict[str, dict[str, Any]]:
        return load_registry_tombstones(self._tombstones_path())

    def _save_tombstones_json(
        self, tombstones: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        return save_registry_tombstones(tombstones, path=self._tombstones_path())

    def _save_state_json(self, state: dict[str, list[dict[str, Any]]]) -> None:
        save_registry_state_atomic(
            self._paths.active,
            self._paths.pending,
            self._paths.rejected,
            state,
        )

    def _json_file_exists(self, path: Any) -> bool:
        raw_path = Path(path)
        return raw_path.exists() or raw_path.with_name(raw_path.name + ".gz").exists()

    def _has_registry_json_exports(self) -> bool:
        return any(
            self._json_file_exists(path)
            for path in (self._paths.active, self._paths.pending, self._paths.rejected)
        )

    def _rollback_to_json(
        self, code: str, message: str, details: dict[str, Any] | None = None
    ) -> None:
        self._force_json_authority(code)
        self._record_registry_diagnostic(
            code,
            ok=False,
            message=message,
            details=details or {},
        )

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
            return filter_tombstoned_rows(active, self.load_tombstones())
        active = [
            canonicalize_registry_row(dict(row), bucket="active") for row in self._default_active
        ]
        save_json_atomic(self._paths.active, active)
        return active

    def load_tombstones(self) -> dict[str, dict[str, Any]]:
        if self._authority_mode() == "sqlite":
            try:
                runtime_store = self._runtime_store()
                if runtime_store.current_generation():
                    tombstones = runtime_store.current_tombstones()
                    if self._json_file_exists(self._tombstones_path()):
                        json_tombstones = self._load_tombstones_json()
                        sqlite_hash = source_registry_tombstone_hash(tombstones)
                        json_hash = source_registry_tombstone_hash(json_tombstones)
                        if sqlite_hash != json_hash:
                            self._rollback_to_json(
                                "source_registry_tombstone_json_sqlite_mismatch",
                                "JSON source-registry tombstones diverged from SQLite authority",
                                details={
                                    "sqliteTombstoneHash": sqlite_hash,
                                    "jsonTombstoneHash": json_hash,
                                },
                            )
                            return json_tombstones
                    return tombstones
            except _STORAGE_OPERATION_ERRORS as exc:
                self._rollback_to_json(
                    "source_registry_tombstone_read_failed",
                    str(exc),
                )
        return self._load_tombstones_json()

    def save_tombstones(self, tombstones: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        normalized = normalize_registry_tombstones(tombstones)
        mode = self._authority_mode()
        if mode == "sqlite":
            state = self.load_state()
            try:
                self._publish_state_to_sqlite(
                    state,
                    tombstones=normalized,
                    reason="source_registry_tombstones_sqlite",
                )
                self._save_state_json(state)
                return self._save_tombstones_json(normalized)
            except _STORAGE_OPERATION_ERRORS as exc:
                self._rollback_to_json(
                    "source_registry_tombstone_write_failed",
                    str(exc),
                )
                return self._save_tombstones_json(normalized)
        saved = self._save_tombstones_json(normalized)
        if mode == "shadow":
            self._mirror_state_to_sqlite(
                self._load_json_state_normalized(save_on_change=False),
                reason="source_registry_tombstones_shadow_mirror",
            )
        return saved

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
            tombstones = self.load_tombstones()
            bucket_rows = filter_tombstoned_rows(
                [dict(row) for row in state.get(bucket, []) if isinstance(row, dict)],
                tombstones,
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
        if self._authority_mode() == "sqlite":
            return self._load_state_from_sqlite()
        return self._load_json_state_normalized(save_on_change=True)

    def _load_json_state_normalized(
        self, *, save_on_change: bool
    ) -> dict[str, list[dict[str, Any]]]:
        state = {
            "active": self.ensure_active_registry(),
            "pending": load_json_array(self._paths.pending, []),
            "rejected": load_json_array(self._paths.rejected, []),
        }
        normalized = self.normalize_state(state)
        normalized = self._apply_safe_conflict_demotions(normalized)
        if save_on_change and normalized != state:
            self._save_state(normalized)
        return normalized

    def _load_state_from_sqlite(self) -> dict[str, list[dict[str, Any]]]:
        try:
            runtime_store = self._runtime_store()
            generation = runtime_store.current_generation()
            if not generation:
                state = self._load_json_state_normalized(save_on_change=False)
                self._publish_state_to_sqlite(
                    state,
                    tombstones=self._load_tombstones_json(),
                    reason="source_registry_seed_from_json",
                )
                self._save_state_json(state)
                self._record_registry_diagnostic(
                    "source_registry_seeded_from_json",
                    ok=True,
                    message="Seeded SQLite source-registry authority from JSON exports",
                    details=runtime_store.current_summary(),
                )
                return state
            state = runtime_store.current_state()
            normalized = self.normalize_state(state)
            normalized = self._apply_safe_conflict_demotions(normalized)
            if self._has_registry_json_exports():
                json_state = self._load_json_state_normalized(save_on_change=False)
                sqlite_hash = source_registry_state_hash(normalized)
                json_hash = source_registry_state_hash(json_state)
                if sqlite_hash != json_hash:
                    self._rollback_to_json(
                        "source_registry_json_sqlite_mismatch",
                        "JSON source-registry exports diverged from SQLite authority",
                        details={
                            "generation": generation,
                            "sqliteStateHash": sqlite_hash,
                            "jsonStateHash": json_hash,
                        },
                    )
                    return self._load_json_state_normalized(save_on_change=True)
            if normalized != state:
                self._save_state(normalized)
            return normalized
        except _STORAGE_OPERATION_ERRORS as exc:
            self._rollback_to_json("source_registry_sqlite_read_failed", str(exc))
            return self._load_json_state_normalized(save_on_change=True)

    @staticmethod
    def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        return {
            "activeCount": len(state["active"]),
            "pendingCount": len(state["pending"]),
            "rejectedCount": len(state["rejected"]),
        }

    def _cheap_json_summary_payload(self, *, reason: str) -> dict[str, Any]:
        active_summary = summarize_json_array_storage(self._paths.active, self._default_active)
        pending_summary = summarize_json_array_storage(self._paths.pending, [])
        rejected_summary = summarize_json_array_storage(self._paths.rejected, [])
        tombstones = self._load_tombstones_json()
        evidence = {
            "active": active_summary,
            "pending": pending_summary,
            "rejected": rejected_summary,
            "tombstoneCount": len(tombstones),
            "tombstoneHash": source_registry_tombstone_hash(tombstones),
        }
        fingerprint = sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        return {
            "activeCount": int(active_summary.get("count") or 0),
            "pendingCount": int(pending_summary.get("count") or 0),
            "rejectedCount": int(rejected_summary.get("count") or 0),
            "tombstoneCount": len(tombstones),
            "hiddenPendingCount": int(pending_summary.get("hiddenCount") or 0),
            "deferredPendingCount": int(pending_summary.get("deferredCount") or 0),
            "duplicatePendingCount": int(pending_summary.get("duplicateCount") or 0),
            "invalidRowsCount": int(active_summary.get("invalidCount") or 0)
            + int(pending_summary.get("invalidCount") or 0)
            + int(rejected_summary.get("invalidCount") or 0),
            "stateHash": "",
            "tombstoneHash": source_registry_tombstone_hash(tombstones),
            "stateFingerprint": fingerprint,
            "generation": "",
            "reason": reason,
            "publishedAt": "",
            "updatedAt": "",
            "summaryStatus": "ready",
            "summaryExact": False,
            "storage": {
                "active": active_summary,
                "pending": pending_summary,
                "rejected": rejected_summary,
            },
        }

    def get_summary_payload(self) -> dict[str, Any]:
        mode = self._authority_mode()
        if mode == "sqlite":
            try:
                summary = self._runtime_store().current_summary()
                if str(summary.get("generation") or "").strip():
                    if self._has_registry_json_exports():
                        json_summary = self._cheap_json_summary_payload(
                            reason="sqlite_export_summary"
                        )
                        return {
                            **json_summary,
                            "authorityMode": mode,
                            "generation": str(summary.get("generation") or ""),
                            "publishedAt": str(summary.get("publishedAt") or ""),
                            "updatedAt": str(summary.get("updatedAt") or ""),
                            "sqliteStateHash": str(summary.get("stateHash") or ""),
                            "sqliteTombstoneHash": str(summary.get("tombstoneHash") or ""),
                        }
                    return {**summary, "authorityMode": mode}
            except _STORAGE_OPERATION_ERRORS:
                pass
        return {
            **self._cheap_json_summary_payload(reason=f"{mode}_summary"),
            "authorityMode": mode,
        }

    def persist_state(
        self, state: dict[str, list[dict[str, Any]]]
    ) -> dict[str, list[dict[str, Any]]]:
        normalized = self.normalize_state(state)
        self._save_state(normalized)
        return normalized

    def _save_state(self, state: dict[str, list[dict[str, Any]]]) -> None:
        mode = self._authority_mode()
        if mode == "sqlite":
            tombstones = self.load_tombstones()
            try:
                self._publish_state_to_sqlite(
                    state,
                    tombstones=tombstones,
                    reason="source_registry_sqlite_publish",
                )
                self._save_state_json(state)
                self._save_tombstones_json(tombstones)
                return
            except _STORAGE_OPERATION_ERRORS as exc:
                self._rollback_to_json("source_registry_sqlite_write_failed", str(exc))
        self._save_state_json(state)
        if mode == "shadow":
            self._mirror_state_to_sqlite(state, reason="source_registry_shadow_mirror")

    def _mirror_state_to_sqlite(
        self,
        state: dict[str, list[dict[str, Any]]],
        *,
        reason: str,
    ) -> None:
        tombstones = self._load_tombstones_json()
        try:
            summary, deleted_generations = self._publish_state_to_sqlite(
                state,
                tombstones=tombstones,
                reason=reason,
            )
            self._record_registry_diagnostic(
                "source_registry_projection_match",
                ok=True,
                message="SQLite source-registry projection matched JSON authority",
                details={
                    **summary.to_dict(),
                    "deletedOldGenerations": deleted_generations,
                },
            )
        except ValueError as exc:
            if "source registry projection mismatch" in str(exc):
                return
            self._force_json_authority("source_registry_shadow_write_failed")
            self._record_registry_diagnostic(
                "source_registry_shadow_write_failed",
                ok=False,
                message=str(exc),
            )
        except _STORAGE_OPERATION_ERRORS as exc:
            self._force_json_authority("source_registry_shadow_write_failed")
            self._record_registry_diagnostic(
                "source_registry_shadow_write_failed",
                ok=False,
                message=str(exc),
            )

    def _publish_state_to_sqlite(
        self,
        state: dict[str, list[dict[str, Any]]],
        *,
        tombstones: dict[str, dict[str, Any]],
        reason: str,
    ) -> tuple[Any, int]:
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
            raise ValueError("source registry projection mismatch")
        deleted_generations = runtime_store.cleanup_old_generations()
        return summary, deleted_generations

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
