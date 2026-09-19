"""Web-search audit schema version, sample limits, and recovery summary shape.

AI boundary owns: audit artifact schema version, bounded sample/queue caps, and recovery summary field projection.
AI boundary implement in: this file for audit contracts; artifact writing stays in the audit runners.
AI boundary search before contracts: audit artifact readers, prevalidated queue policy, and web search tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_web_search_directory_audit.py -q`.
"""

from __future__ import annotations

from typing import Any

from .config import DEFAULT_DISCOVERY_CONFIG as DEFAULT_DISCOVERY_CONFIG

WEB_SEARCH_AUDIT_SCHEMA_VERSION = 2


WEB_SEARCH_AUDIT_FAILURE_SAMPLE_LIMIT = 10_000


WEB_SEARCH_AUDIT_SAMPLE_LIMIT = 25


WEB_SEARCH_RECOVERY_SUMMARY_KEYS = (
    "recoveryFetchAttempts",
    "recoveryPagesFetched",
    "recoveredProviderCandidates",
    "recoveredStaticCandidates",
    "recoveryFailures",
)


_PREVALIDATED_BROWSER_QUEUE_CAP = int(
    DEFAULT_DISCOVERY_CONFIG["gamedevmap"]["validatedStaticQueueCap"]
)


_PREVALIDATED_BROWSER_DOMAIN_CAP = int(
    DEFAULT_DISCOVERY_CONFIG["gamedevmap"]["validatedStaticDomainCap"]
)


# pure inference helper


def _append_bounded_sample(samples: list[dict[str, Any]], sample: dict[str, Any]) -> None:
    if len(samples) < WEB_SEARCH_AUDIT_SAMPLE_LIMIT:
        samples.append(sample)


# mutation — modifies in-place state


def _recovery_summary_fields(summary: dict[str, Any]) -> dict[str, int]:
    return {
        key: int(summary.get(key) or 0)
        for key in WEB_SEARCH_RECOVERY_SUMMARY_KEYS
        if key in summary
    }


# orchestration — network + mutation
