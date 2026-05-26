# Config reader helpers extracted from web_search_candidates.py.
# All pure — no coordinator import.

from pathlib import Path
from typing import Any

from .audit_config import audit_artifact_path, audit_ttl_minutes, config_section, int_config_value
from .config import DEFAULT_DISCOVERY_CONFIG
from .directory_page_recovery import resolve_recovery_url_limit


# pure — reads config key
def _web_search_config_section(config: dict[str, Any] | None) -> dict[str, Any]:
    return config_section(
        config,
        "webSearch",
        defaults=dict(DEFAULT_DISCOVERY_CONFIG.get("webSearch") or {}),
    )


# pure — reads config key
def _web_search_audit_path(config: dict[str, Any] | None) -> Path:
    cfg = _web_search_config_section(config)
    return audit_artifact_path(
        cfg,
        default_filename="web-search-discovery-audit.json",
    )


# pure — reads config key
def _web_search_audit_ttl_minutes(config: dict[str, Any] | None) -> int:
    return audit_ttl_minutes(_web_search_config_section(config))


# pure — reads config key
def _web_search_recovery_enabled(config: dict[str, Any] | None) -> bool:
    cfg = _web_search_config_section(config)
    return bool(cfg.get("activeAuditRecoveryEnabled", True))


# pure — reads config key
def _web_search_recovery_url_limit(config: dict[str, Any] | None) -> int:
    return resolve_recovery_url_limit(_web_search_config_section(config))


# pure — reads config key
def _web_search_max_queries(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "maxQueries",
        default=24,
    )


# pure — reads config key
def _web_search_max_links_per_query(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "maxLinksPerQuery",
        default=8,
    )


# strategy factory — browser recovery batch wrapper
def _web_search_browser_recovery_batch_size(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "browserRecoveryBatchSize",
        default=50,
    )


# pure — reads config key
def _web_search_browser_recovery_max_batches(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "browserRecoveryMaxBatchesPerRun",
        default=1,
    )


# pure — reads config key
def _web_search_browser_recovery_concurrency(config: dict[str, Any] | None) -> int:
    return int_config_value(
        _web_search_config_section(config),
        "browserRecoveryConcurrency",
        default=2,
        minimum=1,
    )


# pure — reads config key
def _web_search_browser_recovery_timeout_s(
    config: dict[str, Any] | None,
    timeout_s: int,
) -> int:
    configured = int_config_value(
        _web_search_config_section(config),
        "browserRecoveryTimeoutSeconds",
        default=15,
        minimum=1,
    )
    return max(1, min(max(1, int(timeout_s)), configured))
