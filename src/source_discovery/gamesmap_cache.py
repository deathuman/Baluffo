from __future__ import annotations

from typing import Any

from .directory_cache import (
    directory_cache_ttl_minutes,
    load_adapter_directory_cache,
    write_adapter_directory_cache,
)
from .directory_page_recovery import RECOVERY_LOGIC_VERSION, resolve_recovery_url_limit

GAMESMAP_PARSER_CACHE_VERSION = 2


def gamesmap_config_value(config: dict[str, Any] | None, key: str, default: Any) -> Any:
    source = config if isinstance(config, dict) else {}
    return source.get(key, default)


def gamesmap_cache_ttl_minutes(config: dict[str, Any] | None) -> int:
    return directory_cache_ttl_minutes(config, "gamesmap")


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
        "activeAuditRecoveryUrlLimit": resolve_recovery_url_limit(cfg),
        "recoveryLogicVersion": RECOVERY_LOGIC_VERSION,
    }


def load_gamesmap_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    fetcher: Any,
    default_fetcher: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]] | None:
    return load_adapter_directory_cache(
        config,
        section_name="gamesmap",
        default_filename="gamesmap-discovery-cache.json",
        expected_signature=gamesmap_cache_signature(cfg),
        fetcher=fetcher,
        default_fetcher=default_fetcher,
    )


def write_gamesmap_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    write_adapter_directory_cache(
        config,
        section_name="gamesmap",
        default_filename="gamesmap-discovery-cache.json",
        signature=gamesmap_cache_signature(cfg),
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
    )
