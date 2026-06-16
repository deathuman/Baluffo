"""Registry and default configuration access for jobs sources."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from src.jobs.adapters import community
from src.jobs.common.numbers import _clamped_int
from src.jobs.common.registry import registry_entries as common_registry_entries
from src.jobs.common.registry_defaults import (
    DEFAULT_STUDIO_SOURCE_REGISTRY,
    REDUNDANT_STATIC_IF_PROVIDER,
)
from src.jobs.models import SourceConfig
from src.source_registry_identity import source_identity, source_url_fingerprint

from .common import config as common_config
from .common import social as common_social
from .common import sources as common_sources

DEFAULT_SOCIAL_CONFIG = common_social.DEFAULT_SOCIAL_CONFIG
DEFAULT_SOCIAL_CONFIG_PATH = common_config.DEFAULT_SOCIAL_CONFIG_PATH
DEFAULT_OUTPUT_DIR = common_config.DEFAULT_OUTPUT_DIR
DEFAULT_SOCIAL_LOOKBACK_MINUTES = common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES
DEFAULT_SOCIAL_MIN_CONFIDENCE = common_config.DEFAULT_SOCIAL_MIN_CONFIDENCE
SOCIAL_SOURCE_NAMES = common_social.SOCIAL_SOURCE_NAMES
GOOGLE_SHEETS_SOURCES = community.GOOGLE_SHEETS_SOURCES
SOURCE_REGISTRY_ACTIVE_PATH = common_config.SOURCE_REGISTRY_ACTIVE_PATH
SOURCE_REGISTRY_PENDING_PATH = common_config.SOURCE_REGISTRY_PENDING_PATH
SOURCE_APPROVAL_STATE_PATH = common_config.SOURCE_APPROVAL_STATE_PATH
STUDIO_SOURCE_REGISTRY = common_sources.load_studio_source_registry(DEFAULT_STUDIO_SOURCE_REGISTRY)
PENDING_PROVIDER_MIGRATION_REASON = "provider_migration_candidate"
PROVIDER_REGISTRY_ADAPTERS = frozenset(
    {
        "ashby",
        "bamboohr",
        "breezy",
        "greenhouse",
        "jazzhr",
        "lever",
        "oracle_hcm",
        "personio",
        "pinpoint",
        "recruitee",
        "smartrecruiters",
        "teamtailor",
        "workable",
        "workday",
    }
)
PROVIDER_ID_FIELDS = (
    "slug",
    "account",
    "company_id",
    "subdomain",
    "api_url",
    "feed_url",
    "board_url",
    "site_path",
    "listing_url",
    "base_url",
)
INCLUDE_PENDING_PROVIDER_MIGRATION = False


def load_registry_from_file(path: Path, fallback: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return common_sources.load_registry_from_file(path, fallback)


def load_studio_source_registry() -> list[dict[str, Any]]:
    return common_sources.load_studio_source_registry(DEFAULT_STUDIO_SOURCE_REGISTRY)


def load_runtime_studio_source_registry(active_path: Path) -> list[dict[str, Any]]:
    return common_sources.load_runtime_studio_source_registry(active_path)


def replace_studio_source_registry(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    previous = [dict(row) for row in STUDIO_SOURCE_REGISTRY if isinstance(row, dict)]
    STUDIO_SOURCE_REGISTRY[:] = [dict(row) for row in rows if isinstance(row, dict)]
    return previous


def activate_runtime_studio_source_registry(active_path: Path) -> list[dict[str, Any]] | None:
    rows = load_runtime_studio_source_registry(active_path)
    if not rows:
        return None
    return replace_studio_source_registry(rows)


def restore_studio_source_registry(previous: Sequence[dict[str, Any]] | None) -> None:
    if previous is not None:
        replace_studio_source_registry(previous)


def read_approved_since_last_run(path: Path) -> int:
    return common_sources.read_approved_since_last_run(path)


def set_include_pending_provider_migration(enabled: bool) -> bool:
    global INCLUDE_PENDING_PROVIDER_MIGRATION

    previous = INCLUDE_PENDING_PROVIDER_MIGRATION
    INCLUDE_PENDING_PROVIDER_MIGRATION = bool(enabled)
    return previous


def _provider_identity_tokens(row: dict[str, Any]) -> set[str]:
    adapter = str(row.get("adapter") or "").strip().lower()
    tokens = {source_identity(row)}
    fingerprint = source_url_fingerprint(row)
    if fingerprint:
        tokens.add(fingerprint)
    for field in PROVIDER_ID_FIELDS:
        value = str(row.get(field) or "").strip().lower()
        if value:
            tokens.add(f"{adapter}:{field}:{value}")
    return {token for token in tokens if token}


def _active_provider_tokens(adapter: str) -> set[str]:
    tokens: set[str] = set()
    for row in STUDIO_SOURCE_REGISTRY:
        if not isinstance(row, dict):
            continue
        if str(row.get("adapter") or "").strip().lower() != adapter:
            continue
        if not bool(row.get("enabledByDefault", True)):
            continue
        tokens.update(_provider_identity_tokens(row))
    return tokens


def _is_fetchable_pending_provider_migration_row(
    row: object,
    *,
    adapter: str,
) -> bool:
    if not isinstance(row, dict):
        return False
    row_adapter = str(row.get("adapter") or "").strip().lower()
    if row_adapter != adapter or row_adapter not in PROVIDER_REGISTRY_ADAPTERS:
        return False
    registry_state = str(row.get("registryState") or "").strip().lower()
    if registry_state and registry_state != "pending":
        return False
    if str(row.get("pendingReason") or "").strip() != PENDING_PROVIDER_MIGRATION_REASON:
        return False
    if not str(row.get("migrationSourceIdentity") or "").strip():
        return False
    if bool(row.get("hiddenFromDefault")):
        return False
    return str(row.get("candidateState") or "").strip().lower() != "hidden"


def _normalized_pending_provider_migration_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["enabledByDefault"] = True
    normalized["fetchOnlyPendingProviderMigration"] = True
    return normalized


def _pending_provider_migration_rows(adapter: str) -> list[dict[str, Any]]:
    clean_adapter = str(adapter or "").strip().lower()
    if clean_adapter not in PROVIDER_REGISTRY_ADAPTERS:
        return []
    active_tokens = _active_provider_tokens(clean_adapter)
    rows: list[dict[str, Any]] = []
    seen_tokens: set[str] = set()
    for row in load_registry_from_file(SOURCE_REGISTRY_PENDING_PATH, []):
        if not _is_fetchable_pending_provider_migration_row(row, adapter=clean_adapter):
            continue
        tokens = _provider_identity_tokens(row)
        if tokens & active_tokens:
            continue
        if tokens & seen_tokens:
            continue
        seen_tokens.update(tokens)
        rows.append(_normalized_pending_provider_migration_row(row))
    return rows


def registry_entries(
    adapter: str,
    *,
    enabled_only: bool = True,
    registry_rows: Sequence[SourceConfig] | None = None,
    include_pending_provider_migration: bool | None = None,
) -> list[dict[str, Any]]:
    if registry_rows is None:
        include_pending = (
            INCLUDE_PENDING_PROVIDER_MIGRATION
            if include_pending_provider_migration is None
            else bool(include_pending_provider_migration)
        )
        studio_source_registry = list(STUDIO_SOURCE_REGISTRY)
        if include_pending and str(adapter or "").strip().lower() in PROVIDER_REGISTRY_ADAPTERS:
            studio_source_registry = [
                *_pending_provider_migration_rows(str(adapter or "")),
                *studio_source_registry,
            ]
        return common_registry_entries(
            adapter,
            enabled_only=enabled_only,
            studio_source_registry=studio_source_registry,
            redundant_static_rules=REDUNDANT_STATIC_IF_PROVIDER,
        )
    rows: list[dict[str, Any]] = []
    for row in registry_rows:
        from src.jobs.text_utils import clean_text

        if clean_text(row.get("adapter")) != adapter:
            continue
        if enabled_only and not bool(row.get("enabledByDefault", True)):
            continue
        normalized = dict(row)
        normalized["fetchStrategy"] = clean_text(row.get("fetchStrategy")) or "auto"
        normalized["cadenceMinutes"] = _clamped_int(row.get("cadenceMinutes"), 0, 0)  # noqa: SLF001
        rows.append(normalized)
    return rows


def load_social_config(
    *,
    config_path: Path = DEFAULT_SOCIAL_CONFIG_PATH,
    enabled: bool = False,
    lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
) -> dict[str, Any]:
    return common_social.load_social_config(
        config_path=config_path,
        enabled=enabled,
        lookback_minutes=lookback_minutes,
    )
