from __future__ import annotations

"""Shared cache helpers for directory-style source discovery adapters."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from . import candidate_collections
from .audit_config import config_section, positive_int

DirectoryCacheRows = tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]


def directory_cache_path(
    config: dict[str, Any] | None,
    section_name: str,
    *,
    default_filename: str,
    flat_fallback: bool = True,
) -> Path | None:
    cfg = config_section(config, section_name, flat_fallback=flat_fallback)
    raw = str(cfg.get("cachePath") or "").strip()
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "data" / default_filename


def directory_cache_ttl_minutes(
    config: dict[str, Any] | None,
    section_name: str,
    *,
    default: int = 360,
    flat_fallback: bool = True,
) -> int:
    cfg = config_section(config, section_name, flat_fallback=flat_fallback)
    return positive_int(cfg.get("cacheTtlMinutes", default), default)


def explicit_directory_cache_path_configured(
    config: dict[str, Any] | None,
    section_name: str,
    *,
    flat_fallback: bool = True,
) -> bool:
    cfg = config_section(config, section_name, flat_fallback=flat_fallback)
    return bool(str(cfg.get("cachePath") or "").strip())


def directory_cache_use_allowed(
    config: dict[str, Any] | None,
    section_name: str,
    *,
    fetcher: Any,
    default_fetcher: Any,
    flat_fallback: bool = True,
) -> bool:
    return fetcher is default_fetcher or explicit_directory_cache_path_configured(
        config,
        section_name,
        flat_fallback=flat_fallback,
    )


def load_adapter_directory_cache(
    config: dict[str, Any] | None,
    *,
    section_name: str,
    default_filename: str,
    expected_signature: dict[str, Any],
    fetcher: Any,
    default_fetcher: Any,
    default_ttl_minutes: int = 360,
    flat_fallback: bool = True,
) -> DirectoryCacheRows | None:
    return load_directory_cache(
        directory_cache_path(
            config,
            section_name,
            default_filename=default_filename,
            flat_fallback=flat_fallback,
        ),
        ttl_minutes=directory_cache_ttl_minutes(
            config,
            section_name,
            default=default_ttl_minutes,
            flat_fallback=flat_fallback,
        ),
        expected_signature=expected_signature,
        use_cache=directory_cache_use_allowed(
            config,
            section_name,
            fetcher=fetcher,
            default_fetcher=default_fetcher,
            flat_fallback=flat_fallback,
        ),
    )


def write_adapter_directory_cache(
    config: dict[str, Any] | None,
    *,
    section_name: str,
    default_filename: str,
    signature: dict[str, Any],
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    flat_fallback: bool = True,
) -> None:
    write_directory_cache(
        directory_cache_path(
            config,
            section_name,
            default_filename=default_filename,
            flat_fallback=flat_fallback,
        ),
        signature=signature,
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
    )


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
    provider_candidates, static_candidates = (
        candidate_collections.provider_static_rows_from_payload(payload)
    )
    return provider_candidates, static_candidates, failures


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
        "providerCandidates": candidate_collections.unique_candidate_rows(provider_candidates),
        "staticCandidates": candidate_collections.unique_candidate_rows(static_candidates),
        "failures": failures,
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError:
        return
