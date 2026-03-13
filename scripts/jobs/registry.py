"""Registry and default configuration access for jobs sources."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from scripts.jobs import common
from scripts.jobs.models import SourceConfig

DEFAULT_STUDIO_SOURCE_REGISTRY = common.DEFAULT_STUDIO_SOURCE_REGISTRY
DEFAULT_SOCIAL_CONFIG = common.DEFAULT_SOCIAL_CONFIG
DEFAULT_SOCIAL_CONFIG_PATH = common.DEFAULT_SOCIAL_CONFIG_PATH
DEFAULT_OUTPUT_DIR = common.DEFAULT_OUTPUT_DIR
DEFAULT_SOCIAL_LOOKBACK_MINUTES = common.DEFAULT_SOCIAL_LOOKBACK_MINUTES
DEFAULT_SOCIAL_MIN_CONFIDENCE = common.DEFAULT_SOCIAL_MIN_CONFIDENCE
SOCIAL_SOURCE_NAMES = common.SOCIAL_SOURCE_NAMES
GOOGLE_SHEETS_SOURCES = common.GOOGLE_SHEETS_SOURCES
SOURCE_REGISTRY_ACTIVE_PATH = common.SOURCE_REGISTRY_ACTIVE_PATH
SOURCE_REGISTRY_PENDING_PATH = common.SOURCE_REGISTRY_PENDING_PATH
SOURCE_APPROVAL_STATE_PATH = common.SOURCE_APPROVAL_STATE_PATH


def load_registry_from_file(path: Path, fallback: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return common.load_registry_from_file(path, fallback)


def load_studio_source_registry() -> List[Dict[str, Any]]:
    return common.load_studio_source_registry()


def read_approved_since_last_run(path: Path) -> int:
    return common.read_approved_since_last_run(path)


def registry_entries(
    adapter: str,
    *,
    enabled_only: bool = True,
    registry_rows: Optional[Sequence[SourceConfig]] = None,
) -> List[Dict[str, Any]]:
    if registry_rows is None:
        return common.registry_entries(adapter, enabled_only=enabled_only)
    rows: List[Dict[str, Any]] = []
    for row in registry_rows:
        if common.clean_text(row.get("adapter")) != adapter:
            continue
        if enabled_only and not bool(row.get("enabledByDefault", True)):
            continue
        normalized = dict(row)
        normalized["fetchStrategy"] = common.clean_text(row.get("fetchStrategy")) or "auto"
        normalized["cadenceMinutes"] = common._clamped_int(row.get("cadenceMinutes"), 0, 0)  # noqa: SLF001
        rows.append(normalized)
    return rows


def load_social_config(
    *,
    config_path: Path = DEFAULT_SOCIAL_CONFIG_PATH,
    enabled: bool = False,
    lookback_minutes: int = DEFAULT_SOCIAL_LOOKBACK_MINUTES,
) -> Dict[str, Any]:
    return common.load_social_config(
        config_path=config_path,
        enabled=enabled,
        lookback_minutes=lookback_minutes,
    )
