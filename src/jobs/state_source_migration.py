from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from src.jobs.common.registry import _migration_adapter_for_host
from src.jobs.text_utils import clean_text, normalize_url

from .common import url as common_url


def structured_duplicate_rate(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


def normalized_google_sheets_redirect_cache(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        return {}
    out: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        key = clean_text(raw_key)
        resolved = normalize_url(raw_value)
        if not key or not resolved or not common_url.is_supported_redirect_url(key):
            continue
        out[key] = resolved
    return out


def structured_source_host(source_row: dict[str, Any]) -> str:
    pages = source_row.get("pages") if isinstance(source_row.get("pages"), list) else []
    url = clean_text(source_row.get("listing_url")) or (clean_text(pages[0]) if pages else "")
    if not url:
        return ""
    try:
        host = (urlparse(url).netloc or "").strip().lower()
    except Exception:  # noqa: BLE001
        return ""
    return host[4:] if host.startswith("www.") else host


def structured_migration_target(source_row: dict[str, Any]) -> str:
    return _migration_adapter_for_host(structured_source_host(source_row))


def should_skip_static_source_for_structured_migration(
    source_name: str,
    source_row: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]] | None,
) -> bool:
    target = structured_migration_target(source_row)
    if target not in {"bamboohr", "workday"}:
        return False
    entry = (
        (source_state_rows or {}).get(clean_text(source_name))
        if isinstance(source_state_rows, dict)
        else {}
    )
    if not isinstance(entry, dict):
        return False
    if clean_text(entry.get("structuredMigrationPromotedAt")):
        return True
    return int(entry.get("structuredMigrationHealthyRunCount") or 0) >= 3


def apply_structured_migration_state(
    entry: dict[str, Any],
    *,
    report: dict[str, Any],
    finished_at: str,
    prior_state: dict[str, Any],
) -> None:
    adapter = clean_text(report.get("adapter"))
    if adapter not in {"bamboohr", "workday"} or entry["lastStatus"] == "excluded":
        return
    entry["structuredMigrationTargetAdapter"] = adapter
    if not clean_text(entry.get("structuredMigrationBaselineCapturedAt")):
        entry["structuredMigrationBaselineCapturedAt"] = finished_at
        entry["structuredMigrationBaselineDurationMs"] = prior_state["lastDurationMs"]
        entry["structuredMigrationBaselineStatus"] = prior_state["lastStatus"]
        entry["structuredMigrationBaselineError"] = prior_state["lastError"]
        entry["structuredMigrationBaselineFailureBucket"] = prior_state["lastFailureBucket"]
        entry["structuredMigrationBaselineKeptCount"] = prior_state["lastKeptCount"]
    entry["structuredMigrationShadowRunCount"] = (
        int(entry.get("structuredMigrationShadowRunCount") or 0) + 1
    )
    current_duplicate_rate = structured_duplicate_rate(report.get("duplicateRate"))
    previous_duplicate_rate = structured_duplicate_rate(
        entry.get("structuredMigrationLastDuplicateRate")
    )
    entry["structuredMigrationLastDuplicateRate"] = current_duplicate_rate
    entry["structuredMigrationLastKeptCount"] = entry["lastKeptCount"]
    healthy_run = (
        entry["lastStatus"] == "ok"
        and entry["lastKeptCount"] > 0
        and current_duplicate_rate <= (previous_duplicate_rate + 0.01)
    )
    if healthy_run:
        healthy_count = int(entry.get("structuredMigrationHealthyRunCount") or 0) + 1
        entry["structuredMigrationHealthyRunCount"] = healthy_count
        entry.pop("structuredMigrationDemotedAt", None)
        if healthy_count >= 3 and not clean_text(entry.get("structuredMigrationPromotedAt")):
            entry["structuredMigrationPromotedAt"] = finished_at
        return
    if clean_text(entry.get("structuredMigrationPromotedAt")):
        entry["structuredMigrationDemotedAt"] = finished_at
    entry["structuredMigrationHealthyRunCount"] = 0
    entry.pop("structuredMigrationPromotedAt", None)
