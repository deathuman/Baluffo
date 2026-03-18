"""Registry and default configuration access for jobs sources."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from src.jobs.adapters import community
from src.jobs.common import config as common_config
from src.jobs.common import social as common_social
from src.jobs.common import sources as common_sources
from src.jobs.common import DEFAULT_STUDIO_SOURCE_REGISTRY
from src.jobs.models import SourceConfig

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


def load_registry_from_file(path: Path, fallback: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return common_sources.load_registry_from_file(path, fallback)


def load_studio_source_registry() -> List[Dict[str, Any]]:
    return common_sources.load_studio_source_registry(DEFAULT_STUDIO_SOURCE_REGISTRY)


def read_approved_since_last_run(path: Path) -> int:
    return common_sources.read_approved_since_last_run(path)


def registry_entries(
    adapter: str,
    *,
    enabled_only: bool = True,
    registry_rows: Optional[Sequence[SourceConfig]] = None,
) -> List[Dict[str, Any]]:
    if registry_rows is None:
        from src.jobs.common import registry_entries as common_registry_entries

        return common_registry_entries(adapter, enabled_only=enabled_only)
    rows: List[Dict[str, Any]] = []
    for row in registry_rows:
        from src.jobs.text_utils import clean_text
        from src.jobs.common import _clamped_int  # noqa: SLF001

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
) -> Dict[str, Any]:
    return common_social.load_social_config(
        config_path=config_path,
        enabled=enabled,
        lookback_minutes=lookback_minutes,
    )

