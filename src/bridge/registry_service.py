from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple

from src.source_registry import (
    ensure_source_id,
    load_json_array,
    normalize_source_url,
    save_json_atomic,
    source_identity,
    source_url_fingerprint,
    unique_sources,
)


NormalizeManualStaticFunc = Callable[[Dict[str, Any]], Dict[str, Any]]


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
        default_active: List[Dict[str, Any]],
        normalize_manual_static: NormalizeManualStaticFunc,
    ) -> None:
        self._paths = paths
        self._default_active = [dict(row) for row in (default_active or []) if isinstance(row, dict)]
        self._normalize_manual_static = normalize_manual_static

    def ensure_active_registry(self) -> List[Dict[str, Any]]:
        active = load_json_array(self._paths.active, [])
        if active:
            return active
        active = [dict(row) for row in self._default_active]
        save_json_atomic(self._paths.active, active)
        return active

    def normalize_state(self, state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        # Precedence is explicit: active > pending > rejected.
        seen = set()
        normalized: Dict[str, List[Dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
        for bucket in ("active", "pending", "rejected"):
            for row in state.get(bucket, []):
                if not isinstance(row, dict):
                    continue
                if str(row.get("adapter") or "").strip().lower() == "static":
                    row = self._normalize_manual_static(row)
                key = source_identity(row)
                if key in seen:
                    continue
                seen.add(key)
                normalized[bucket].append(ensure_source_id(row))
        return normalized

    def load_state(self) -> Dict[str, List[Dict[str, Any]]]:
        return self.normalize_state(
            {
                "active": self.ensure_active_registry(),
                "pending": load_json_array(self._paths.pending, []),
                "rejected": load_json_array(self._paths.rejected, []),
            }
        )

    @staticmethod
    def summarize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
        return {
            "activeCount": len(state["active"]),
            "pendingCount": len(state["pending"]),
            "rejectedCount": len(state["rejected"]),
        }

    def persist_state(self, state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        normalized = self.normalize_state(state)
        save_json_atomic(self._paths.active, normalized["active"])
        save_json_atomic(self._paths.pending, normalized["pending"])
        save_json_atomic(self._paths.rejected, normalized["rejected"])
        return normalized

    @staticmethod
    def move_entries(
        pending: List[Dict[str, Any]], selected_ids: List[str]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        selected = set(str(item) for item in selected_ids)
        moved: List[Dict[str, Any]] = []
        remaining: List[Dict[str, Any]] = []
        for row in pending:
            if source_identity(row) in selected:
                moved.append(row)
            else:
                remaining.append(row)
        return moved, remaining

    @staticmethod
    def unique_sources(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return unique_sources(rows)

    @staticmethod
    def source_identity(row: Dict[str, Any]) -> str:
        return source_identity(row)

    @staticmethod
    def source_url_fingerprint(row: Dict[str, Any]) -> str:
        return source_url_fingerprint(row)

    @staticmethod
    def normalize_source_url(url: str) -> str:
        return normalize_source_url(url)


__all__ = ["RegistryPaths", "RegistryService"]

