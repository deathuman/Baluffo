"""Registry-conflict GET route handlers."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Protocol

from src.bridge.registry_conflict_adjudication import overlay_adjudication
from src.bridge.registry_conflicts import (
    build_registry_conflicts_summary_cache_key,
    load_registry_conflicts_payload,
    load_registry_conflicts_summary_payload,
    summarize_registry_conflicts_payload,
    write_registry_conflicts_summary_cache,
)
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_payload_helpers import (
    as_dict as _as_dict,
)
from src.bridge.routes.route_payload_helpers import (
    clean_text as _clean_text,
)

logger = logging.getLogger(__name__)


class _RegistryConflictsRouteApi(Protocol):
    JOBS_FETCH_REPORT_PATH: Path

    def get_registry_auto_heal_report(self) -> dict[str, Any]: ...

    def get_registry_summary_payload(self) -> dict[str, Any]: ...

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]: ...

    def load_registry_conflict_adjudication(self) -> dict[str, Any]: ...

    def load_state(self) -> dict[str, Any]: ...

    def summarize_state(self, state: dict[str, Any]) -> dict[str, Any]: ...


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return int(default)


def registry_conflicts_badge_from_exact_summary(
    api: _RegistryConflictsRouteApi,
) -> dict[str, Any]:
    registry_summary = api.get_registry_summary_payload()
    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    adjudication = api.load_registry_conflict_adjudication()
    registry_auto_heal = api.get_registry_auto_heal_report()
    conflicts_payload = load_registry_conflicts_summary_payload(
        registry_summary=registry_summary,
        source_state_path=source_state_path,
        adjudication_payload=adjudication,
        registry_auto_heal=registry_auto_heal,
    )
    if _clean_text(conflicts_payload.get("summaryStatus")).lower() != "ready":
        full_payload = load_registry_conflicts_payload(
            load_state=api.load_state,
            load_json_object=api.load_json_object,
            source_state_path=source_state_path,
            adjudication_payload=adjudication,
        )
        full_payload = overlay_adjudication(full_payload, adjudication)
        full_payload["registrySummary"] = registry_summary
        full_payload["registryAutoHeal"] = registry_auto_heal
        full_payload["ok"] = True
        conflicts_payload = summarize_registry_conflicts_payload(full_payload)
        try:
            cache_key = build_registry_conflicts_summary_cache_key(
                registry_summary=registry_summary,
                source_state_path=source_state_path,
                adjudication_payload=adjudication,
            )
            write_registry_conflicts_summary_cache(
                source_state_path=source_state_path,
                cache_key=cache_key,
                payload=conflicts_payload,
            )
        except OSError:
            logger.debug("Could not write registry conflicts summary cache", exc_info=True)
    summary = _as_dict(conflicts_payload.get("summary"))
    count = _safe_int(summary.get("conflictCount"), 0)
    return {
        "count": max(0, count),
        "tone": "warning" if count > 0 else "neutral",
        "title": (
            f"{count} registry conflict{'' if count == 1 else 's'}"
            if count > 0
            else "No registry conflicts"
        ),
        "loaded": True,
        "error": "",
    }


def handle_registry_conflict_routes(
    handler: BridgeResponseWriter,
    *,
    api: _RegistryConflictsRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/registry/conflicts":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
        adjudication = api.load_registry_conflict_adjudication()
        registry_summary = api.get_registry_summary_payload()
        registry_auto_heal = api.get_registry_auto_heal_report()
        if view == "summary":
            handler.send_json(
                load_registry_conflicts_summary_payload(
                    registry_summary=registry_summary,
                    source_state_path=source_state_path,
                    adjudication_payload=adjudication,
                    registry_auto_heal=registry_auto_heal,
                )
            )
            return True
        state = api.load_state()
        registry_summary = api.get_registry_summary_payload()
        registry_auto_heal = api.get_registry_auto_heal_report()
        payload = load_registry_conflicts_payload(
            load_state=lambda: state,
            load_json_object=api.load_json_object,
            source_state_path=source_state_path,
            adjudication_payload=adjudication,
        )
        payload = overlay_adjudication(payload, adjudication)
        payload["registrySummary"] = api.summarize_state(state)
        payload["registryAutoHeal"] = registry_auto_heal
        payload["ok"] = True
        try:
            cache_key = build_registry_conflicts_summary_cache_key(
                registry_summary=registry_summary,
                source_state_path=source_state_path,
                adjudication_payload=adjudication,
            )
            write_registry_conflicts_summary_cache(
                source_state_path=source_state_path,
                cache_key=cache_key,
                payload=summarize_registry_conflicts_payload(payload),
            )
        except OSError:
            logger.debug("Could not write registry conflicts summary cache", exc_info=True)
        handler.send_json(payload)
        return True

    return False
