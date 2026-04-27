from __future__ import annotations

"""Shared cache helpers for directory-style source discovery adapters."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.source_registry import unique_sources

DirectoryCacheRows = tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]


def _parse_updated_at(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def load_directory_cache(
    cache_path: Path | None,
    *,
    ttl_minutes: int,
    expected_signature: dict[str, Any],
    use_cache: bool = True,
) -> DirectoryCacheRows | None:
    if not use_cache or ttl_minutes <= 0 or cache_path is None:
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    updated_at = _parse_updated_at(payload.get("updatedAt"))
    if updated_at is None:
        return None
    if datetime.now(UTC) - updated_at > timedelta(minutes=ttl_minutes):
        return None
    if payload.get("configSignature") != expected_signature:
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


def write_directory_cache(
    cache_path: Path | None,
    *,
    signature: dict[str, Any],
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    if cache_path is None:
        return
    payload = {
        "updatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "configSignature": signature,
        "providerCandidates": unique_sources(provider_candidates),
        "staticCandidates": unique_sources(static_candidates),
        "failures": failures,
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return
