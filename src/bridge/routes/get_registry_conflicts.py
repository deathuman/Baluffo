"""Registry-conflict GET route handlers.

AI boundary owns: `/registry/conflicts` GET route response wiring only.
AI boundary implement in: registry conflict summaries, adjudication, and source-state helpers.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from src.bridge.registry_conflicts import (
    build_registry_conflicts_summary_cache_key,
    load_registry_conflicts_summary_payload,
    summarize_registry_conflicts_payload,
)
from src.bridge.registry_conflicts_refresh import (
    is_registry_conflicts_refresh_in_flight,
    schedule_registry_conflicts_refresh,
)
from src.bridge.registry_conflicts_summary import (
    load_any_cached_registry_conflicts_full,
    load_any_cached_registry_conflicts_summary,
    load_registry_conflicts_full_cache,
    registry_conflicts_full_cache_age_seconds,
)
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_payload_helpers import (
    as_dict as _as_dict,
)
from src.bridge.routes.route_payload_helpers import (
    clean_text as _clean_text,
)
from src.shared.utils import int_or_default as _safe_int


class _RegistryConflictsRouteApi(Protocol):
    JOBS_FETCH_REPORT_PATH: Path

    def get_registry_auto_heal_report(self) -> dict[str, Any]: ...

    def get_registry_summary_payload(self) -> dict[str, Any]: ...

    def load_json_object(self, path: Path, default: Any) -> dict[str, Any]: ...

    def load_registry_conflict_adjudication(self) -> dict[str, Any]: ...

    def load_state(self) -> dict[str, Any]: ...

    def summarize_state(self, state: dict[str, Any]) -> dict[str, Any]: ...


def _conflict_page_sort_key(card: dict[str, Any]) -> tuple[int, str, str]:
    try:
        priority = int(card.get("reviewPriority", 3))
    except (TypeError, ValueError):
        priority = 3
    return (
        priority,
        _clean_text(card.get("reviewQueue")),
        _clean_text(card.get("familyKey")),
    )


def slice_registry_conflicts_for_query(
    payload: dict[str, Any],
    *,
    limit: int,
    offset: int,
    queue: str,
) -> dict[str, Any]:
    """Page the conflict cards after the full payload (and its summary cache) exist.

    Deterministic ordering mirrors the frontend grouping key
    (reviewPriority, reviewQueue, familyKey) so offset pages stay stable.
    """
    conflicts = payload.get("conflicts")
    rows = (
        [card for card in conflicts if isinstance(card, dict)]
        if isinstance(conflicts, list)
        else []
    )
    if limit <= 0 and offset <= 0 and not queue:
        return payload
    if queue:
        rows = [card for card in rows if _clean_text(card.get("reviewQueue")).lower() == queue]
    rows = sorted(rows, key=_conflict_page_sort_key)
    end = offset + limit if limit > 0 else None
    paged = rows[offset:end]
    payload["conflicts"] = paged
    payload["returnedCount"] = len(paged)
    return payload


def _registry_conflicts_badge(
    *,
    count: int,
    loaded: bool = True,
    error: str = "",
) -> dict[str, Any]:
    safe_count = max(0, int(count or 0))
    if not loaded:
        title = "Registry conflict count unavailable"
    elif safe_count > 0:
        title = f"{safe_count} registry conflict{'' if safe_count == 1 else 's'}"
    else:
        title = "No registry conflicts"
    return {
        "count": safe_count,
        "tone": "warning" if safe_count > 0 else "neutral",
        "title": title,
        "loaded": bool(loaded),
        "error": str(error or ""),
    }


def _registry_summary_row_count(registry_summary: dict[str, Any]) -> int | None:
    counts = ("activeCount", "pendingCount", "rejectedCount")
    if not any(key in registry_summary for key in counts):
        return None
    return sum(max(0, _safe_int(registry_summary.get(key), 0)) for key in counts)


def _last_known_conflicts_summary(source_state_path: Path) -> dict[str, Any]:
    cached = load_any_cached_registry_conflicts_summary(source_state_path=source_state_path)
    if cached:
        return cached
    full_payload = load_any_cached_registry_conflicts_full(source_state_path)
    if full_payload:
        # Reduce the cached full payload instead of rebuilding it: summarize is a
        # bounded slice of already-derived cards, the derivation is the 54s part.
        return summarize_registry_conflicts_payload(full_payload)
    return {}


def registry_conflicts_badge_from_exact_summary(
    api: _RegistryConflictsRouteApi,
) -> dict[str, Any]:
    """Registry conflict badge from caches only.

    This badge is polled with the Admin shell, so it must stay cheap even while a
    run rewrites the source-state signature and invalidates the exact cache key.
    It serves last-known counts in that case and never builds the full conflict
    queue: that derivation measured 54s+ on a Pi and is the conflicts tab's job.
    """
    registry_summary = _as_dict(api.get_registry_summary_payload())
    if _registry_summary_row_count(registry_summary) == 0:
        return _registry_conflicts_badge(count=0)
    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    conflicts_payload = load_registry_conflicts_summary_payload(
        registry_summary=registry_summary,
        source_state_path=source_state_path,
        adjudication_payload=api.load_registry_conflict_adjudication(),
        registry_auto_heal=api.get_registry_auto_heal_report(),
    )
    if _clean_text(conflicts_payload.get("summaryStatus")).lower() != "ready":
        conflicts_payload = _last_known_conflicts_summary(source_state_path)
    if not conflicts_payload:
        return _registry_conflicts_badge(
            count=0,
            loaded=False,
            error="conflict_summary_not_ready",
        )
    summary = _as_dict(conflicts_payload.get("summary"))
    return _registry_conflicts_badge(count=_safe_int(summary.get("conflictCount"), 0))


def handle_registry_conflict_routes(
    handler: BridgeResponseWriter,
    *,
    api: _RegistryConflictsRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/registry/conflicts":
        from src.bridge.registry_conflict_adjudication import overlay_adjudication

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
        cache_key = build_registry_conflicts_summary_cache_key(
            registry_summary=registry_summary,
            source_state_path=source_state_path,
            adjudication_payload=adjudication,
        )
        payload = load_registry_conflicts_full_cache(source_state_path, cache_key)
        full_cache_status = "fresh"
        refresh_in_flight = is_registry_conflicts_refresh_in_flight(cache_key)
        if payload is None:
            # Never derive the queue inside the request: a cold build measured
            # 54s+ on a Pi while the Admin client aborts at 18s. Serve the last
            # payload and rebuild once in the background instead.
            refresh_in_flight = schedule_registry_conflicts_refresh(
                api=api,
                source_state_path=source_state_path,
                cache_key=cache_key,
                adjudication_payload=adjudication,
            )
            payload = load_any_cached_registry_conflicts_full(source_state_path)
            if payload is None:
                building_payload = load_registry_conflicts_summary_payload(
                    registry_summary=registry_summary,
                    source_state_path=source_state_path,
                    adjudication_payload=adjudication,
                    registry_auto_heal=registry_auto_heal,
                )
                building_payload["detailStatus"] = "building"
                building_payload["fullCacheStatus"] = "building"
                building_payload["fullCacheAgeSeconds"] = None
                building_payload["refreshInFlight"] = refresh_in_flight
                handler.send_json(building_payload)
                return True
            full_cache_status = "stale"

        limit = max(0, _safe_int((query.get("limit") or ["0"])[0]))
        offset = max(0, _safe_int((query.get("offset") or ["0"])[0]))
        queue = _clean_text((query.get("queue") or [""])[0]).lower()
        if limit > 0 or offset > 0 or queue:
            payload = slice_registry_conflicts_for_query(
                payload, limit=limit, offset=offset, queue=queue
            )
        # Overlay after slicing: the envelope's adjudication object is shared,
        # but the per-card family overlay is only needed for the returned page.
        payload = overlay_adjudication(payload, adjudication)
        payload["registrySummary"] = api.summarize_state(state)
        payload["registryAutoHeal"] = registry_auto_heal
        payload["ok"] = True
        payload["fullCacheStatus"] = full_cache_status
        payload["fullCacheAgeSeconds"] = registry_conflicts_full_cache_age_seconds(
            source_state_path
        )
        payload["refreshInFlight"] = refresh_in_flight
        handler.send_json(payload)
        return True

    return False
