from __future__ import annotations

"""Shared cache helpers for directory-style source discovery adapters."""

from typing import Any

from .audit_config import config_section, positive_int


def directory_cache_ttl_minutes(
    config: dict[str, Any] | None,
    section_name: str,
    *,
    default: int = 360,
    flat_fallback: bool = True,
) -> int:
    cfg = config_section(config, section_name, flat_fallback=flat_fallback)
    return positive_int(cfg.get("cacheTtlMinutes", default), default)
