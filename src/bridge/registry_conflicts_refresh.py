"""Single-flight background refresh for the registry-conflicts full cache.

The detail route must never derive the full conflict queue inside a request:
the derivation measured 54s+ on Pi-class hardware while the Admin client aborts
at 18s. On a cache miss the route serves the last-known payload (or a
``detailStatus: "building"`` marker) and calls this module to rebuild once.

AI boundary owns: refresh scheduling and the background cache writes.
AI boundary implement in: this module; derivation stays in ``registry_conflicts``
and cache payload assembly in ``registry_conflicts_summary``.
AI boundary search before contracts: the conflict detail route and cache tests.
AI boundary verify: focused registry conflict route/refresh tests.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any, Protocol

from src.bridge.registry_conflict_adjudication_run import overlay_adjudication
from src.bridge.registry_conflicts import load_registry_conflicts_payload
from src.bridge.registry_conflicts_summary import (
    summarize_registry_conflicts_payload,
    write_registry_conflicts_full_cache,
    write_registry_conflicts_summary_cache,
)

logger = logging.getLogger(__name__)

_REFRESH_LOCK = threading.Lock()
_REFRESH_IN_FLIGHT: set[str] = set()


class RegistryConflictsRefreshApi(Protocol):
    def get_registry_auto_heal_report(self) -> dict[str, Any]: ...

    def load_json_object(self, path: Path, default: Any) -> dict[str, Any]: ...

    def load_state(self) -> dict[str, Any]: ...

    def summarize_state(self, state: dict[str, Any]) -> dict[str, Any]: ...


def is_registry_conflicts_refresh_in_flight(cache_key: str) -> bool:
    with _REFRESH_LOCK:
        return str(cache_key or "") in _REFRESH_IN_FLIGHT


def schedule_registry_conflicts_refresh(
    *,
    api: RegistryConflictsRefreshApi,
    source_state_path: Path,
    cache_key: str,
    adjudication_payload: Any,
) -> bool:
    """Rebuild and persist the full conflict cache once per cache key.

    Returns ``True`` when a build is running (started here or already in
    flight) so the route can report ``refreshInFlight`` without waiting.
    """
    key = str(cache_key or "")
    if not key:
        return False
    with _REFRESH_LOCK:
        if key in _REFRESH_IN_FLIGHT:
            return True
        _REFRESH_IN_FLIGHT.add(key)

    def _worker() -> None:
        try:
            state = api.load_state()
            payload = load_registry_conflicts_payload(
                load_state=lambda: state,
                load_json_object=api.load_json_object,
                source_state_path=source_state_path,
                adjudication_payload=adjudication_payload,
            )
            try:
                write_registry_conflicts_full_cache(source_state_path, key, payload)
            except OSError:
                logger.debug("Could not write registry conflicts full cache", exc_info=True)
            try:
                enriched = overlay_adjudication(payload, adjudication_payload or {})
                enriched["registrySummary"] = api.summarize_state(state)
                enriched["registryAutoHeal"] = api.get_registry_auto_heal_report()
                enriched["ok"] = True
                write_registry_conflicts_summary_cache(
                    source_state_path=source_state_path,
                    cache_key=key,
                    payload=summarize_registry_conflicts_payload(enriched),
                )
            except OSError:
                logger.debug("Could not write registry conflicts summary cache", exc_info=True)
        except (KeyError, OSError, RuntimeError, TypeError, ValueError):
            logger.warning("registry conflicts background refresh failed", exc_info=True)
        finally:
            with _REFRESH_LOCK:
                _REFRESH_IN_FLIGHT.discard(key)

    try:
        threading.Thread(
            target=_worker,
            name=f"registry-conflicts-refresh-{key[:12]}",
            daemon=True,
        ).start()
    except RuntimeError:
        with _REFRESH_LOCK:
            _REFRESH_IN_FLIGHT.discard(key)
        logger.warning("Could not start registry conflicts refresh thread", exc_info=True)
        return False
    return True


__all__ = [
    "is_registry_conflicts_refresh_in_flight",
    "schedule_registry_conflicts_refresh",
]
