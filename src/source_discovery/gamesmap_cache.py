from __future__ import annotations

from pathlib import Path
from typing import Any

from .directory_cache import load_directory_cache, write_directory_cache
from .directory_page_recovery import RECOVERY_LOGIC_VERSION

GAMESMAP_PARSER_CACHE_VERSION = 2


def _gamesmap_source_config(config: dict[str, Any] | None) -> dict[str, Any]:
    source = config if isinstance(config, dict) else {}
    if isinstance(source.get("gamesmap"), dict):
        source = source.get("gamesmap") or {}
    return source


def gamesmap_config_value(config: dict[str, Any] | None, key: str, default: Any) -> Any:
    source = config if isinstance(config, dict) else {}
    return source.get(key, default)


def gamesmap_cache_path(config: dict[str, Any] | None) -> Path | None:
    source = _gamesmap_source_config(config)
    raw = str(source.get("cachePath") or "").strip()
    if not raw:
        return Path(__file__).resolve().parents[2] / "data" / "gamesmap-discovery-cache.json"
    return Path(raw)


def gamesmap_cache_ttl_minutes(config: dict[str, Any] | None) -> int:
    raw = _gamesmap_source_config(config).get("cacheTtlMinutes", "")
    if raw in {"", None}:
        raw = 360
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 360


def gamesmap_cache_signature(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "parserVersion": GAMESMAP_PARSER_CACHE_VERSION,
        "baseUrl": str(cfg.get("baseUrl") or "").strip(),
        "indexUrls": [
            str(item).strip() for item in (cfg.get("indexUrls") or []) if str(item).strip()
        ],
        "preferEnglish": bool(cfg.get("preferEnglish", True)),
        "websiteOnlyFallback": bool(cfg.get("websiteOnlyFallback", True)),
        "websiteOnlyManualOnly": bool(cfg.get("websiteOnlyManualOnly", False)),
        "maxDetailPages": max(0, int(cfg.get("maxDetailPages") or 0)),
        "allowedCategoryTokens": list(cfg.get("allowedCategoryTokens") or []),
        "blockedCategoryTokens": list(cfg.get("blockedCategoryTokens") or []),
        "activeAuditRecoveryEnabled": bool(cfg.get("activeAuditRecoveryEnabled", True)),
        "recoveryLogicVersion": RECOVERY_LOGIC_VERSION,
    }


def load_gamesmap_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    fetcher: Any,
    default_fetcher: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]] | None:
    cache_path = gamesmap_cache_path(config)
    ttl_minutes = gamesmap_cache_ttl_minutes(config)
    source = _gamesmap_source_config(config)
    return load_directory_cache(
        cache_path,
        ttl_minutes=ttl_minutes,
        expected_signature=gamesmap_cache_signature(cfg),
        use_cache=fetcher is default_fetcher or bool(str(source.get("cachePath") or "").strip()),
    )


def write_gamesmap_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    write_directory_cache(
        gamesmap_cache_path(config),
        signature=gamesmap_cache_signature(cfg),
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
    )
