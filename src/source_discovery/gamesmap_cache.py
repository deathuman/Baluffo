from __future__ import annotations

from typing import Any

from .directory_page_recovery import RECOVERY_LOGIC_VERSION, resolve_recovery_url_limit

GAMESMAP_PARSER_CACHE_VERSION = 2


def gamesmap_config_value(config: dict[str, Any] | None, key: str, default: Any) -> Any:
    source = config if isinstance(config, dict) else {}
    return source.get(key, default)


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
