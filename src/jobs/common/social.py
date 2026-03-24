"""Social-source defaults and config loading."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.jobs.common.config import (
    DEFAULT_SOCIAL_CONFIG_PATH,
    DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    DEFAULT_SOCIAL_MIN_CONFIDENCE,
)

SOCIAL_SOURCE_NAMES = {"social_reddit", "social_x", "social_mastodon"}

DEFAULT_SOCIAL_CONFIG: dict[str, Any] = {
    "enabled": False,
    "minConfidence": DEFAULT_SOCIAL_MIN_CONFIDENCE,
    "rejectForHirePosts": True,
    "reddit": {
        "enabled": False,
        "subreddits": [],
        "maxPostsPerSubreddit": 25,
        "rssFallback": True,
        "htmlFallback": False,
        "rateLimitDelay": 0.5,
        "timeoutSeconds": 10,
        "retries": 1,
    },
    "x": {
        "enabled": False,
        "minConfidence": 35,
        "queries": [
            "#gamedevjobs",
            "#gamejobs",
            "\"game designer\" \"we're hiring\"",
            "\"gamedev\" \"hiring\"",
        ],
        "maxPostsPerQuery": 10,
        "timeoutSeconds": 8,
        "retries": 1,
        "api": {
            "enabled": True,
            "endpoint": "https://api.x.com/2/tweets/search/recent",
            "bearerTokenEnv": "BALUFFO_X_BEARER_TOKEN",
        },
        "scraperFallback": {
            "enabled": False,
            "endpoint": "",
        },
        "rssFallback": {
            "enabled": True,
            "instances": [
                "https://xcancel.com",
                "https://nitter.net",
                "https://nitter.poast.org",
            ],
        },
    },
    "mastodon": {
        "enabled": True,
        "instances": ["https://mastodon.gamedev.place"],
        "hashtags": ["gamedevjobs", "indiegamejobs"],
        "maxPostsPerTag": 40,
    },
}


def _deep_merge_dicts(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in (overrides or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dicts(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def load_social_config(
    *,
    config_path: Path | None = DEFAULT_SOCIAL_CONFIG_PATH,
    enabled: bool = False,
    lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
) -> dict[str, Any]:
    resolved_path = Path(config_path) if config_path else DEFAULT_SOCIAL_CONFIG_PATH
    try:
        payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    merged = _deep_merge_dicts(DEFAULT_SOCIAL_CONFIG, payload if isinstance(payload, dict) else {})
    merged["enabled"] = bool(enabled)
    merged["lookbackMinutes"] = max(1, int(lookback_minutes or DEFAULT_SOCIAL_LOOKBACK_MINUTES))
    return merged

