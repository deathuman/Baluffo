from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.source_registry import unique_sources

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
    if ttl_minutes <= 0 or cache_path is None:
        return None
    source = _gamesmap_source_config(config)
    if fetcher is not default_fetcher and not str(source.get("cachePath") or "").strip():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    updated_at_raw = str(payload.get("updatedAt") or "").strip()
    if not updated_at_raw:
        return None
    try:
        updated_at = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if datetime.now(UTC) - updated_at > timedelta(minutes=ttl_minutes):
        return None
    if payload.get("configSignature") != gamesmap_cache_signature(cfg):
        return None
    provider_rows = payload.get("providerCandidates")
    static_rows = payload.get("staticCandidates")
    failures = payload.get("failures")
    if (
        not isinstance(provider_rows, list)
        or not isinstance(static_rows, list)
        or not isinstance(failures, list)
    ):
        return None
    return unique_sources(provider_rows), unique_sources(static_rows), failures


def write_gamesmap_cache(
    config: dict[str, Any] | None,
    cfg: dict[str, Any],
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    cache_path = gamesmap_cache_path(config)
    if cache_path is None:
        return
    payload = {
        "updatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "configSignature": gamesmap_cache_signature(cfg),
        "providerCandidates": unique_sources(provider_candidates),
        "staticCandidates": unique_sources(static_candidates),
        "failures": failures,
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return
