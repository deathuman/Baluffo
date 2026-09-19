"""Shared provider-board lifecycle helpers.

AI boundary owns: provider-board lifecycle metadata, source diagnostics, and run result helpers.
AI boundary implement in: this file for provider lifecycle shape; concrete fetch/parsing stays in runner/parser leaves.
AI boundary search before contracts: provider API runners, source error helpers, and provider lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused provider lifecycle tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.exceptions import AdapterValidationError
from src.jobs.state_incremental import get_incremental_cache_decision
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url


def build_provider_entry_report(
    *,
    adapter_name: str,
    studio: str,
    source_name: str,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "adapter": adapter_name,
        "studio": studio,
        "name": source_name,
        "status": "ok",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "",
    }
    if extra:
        row.update(extra)
    return row


def apply_provider_cache_decision(
    *,
    entry_report: dict[str, object],
    source_name: str,
    adapter_name: str,
    source_state_rows: dict[str, dict[str, object]] | None,
    force_refresh_all: bool,
) -> None:
    cache_decision = get_incremental_cache_decision(
        source_name,
        source_state_rows or {},
        adapter=adapter_name,
        force_refresh_all=force_refresh_all,
    )
    entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
    entry_report["cacheDecisionReason"] = (
        clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
    )


def run_registry_entries_source(
    *,
    registry_entries_fn: Callable[[str], list[dict[str, Any]]],
    registry_key: str,
    source_name: str,
    adapter_name: str,
    run_entry: Callable[..., tuple[list[Any], dict[str, object], str | None]],
    set_diagnostics: Callable[..., None],
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None = None,
    force_refresh_all: bool = False,
) -> list[Any]:
    """Iterate ``registry_key`` entries, aggregating jobs, errors, and details.

    Every per-entry runner shares this shape: run one registry source, collect
    its report, accumulate rows and error text, remember the first provider URL
    for diagnostics, then either return the jobs or raise the collected errors.
    """
    jobs: list[Any] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    provider_url = ""
    for source in registry_entries_fn(registry_key):
        source_jobs, entry_report, error_text = run_entry(
            source,
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        details.append(entry_report)
        jobs.extend(source_jobs)
        if error_text:
            errors.append(error_text)
            provider_url = provider_url or clean_text(entry_report.get("providerUrl"))

    set_diagnostics(
        source_name,
        adapter=adapter_name,
        studio="multiple",
        provider_url=provider_url,
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def skip_provider_for_cache(entry_report: dict[str, object]) -> bool:
    if entry_report["cacheDecision"] not in {"skip_fresh", "cooldown_skip"}:
        return False
    entry_report["status"] = "excluded"
    entry_report["error"] = entry_report["cacheDecisionReason"]
    entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecisionReason']}"
    return True


def provider_revalidate_not_modified(
    *,
    entry_report: dict[str, object],
    url: str,
    timeout_s: int,
    source_name: str,
    source_state_rows: dict[str, dict[str, object]] | None,
    revalidate_url: Callable[..., dict[str, Any]] = conditional_revalidate_url,
) -> bool:
    if entry_report["cacheDecision"] != "revalidate_only":
        return False
    state_entry = (
        (source_state_rows or {}).get(source_name) if isinstance(source_state_rows, dict) else {}
    )
    revalidate = revalidate_url(
        url,
        timeout_s,
        etag=clean_text((state_entry or {}).get("lastHttpEtag")),
        last_modified=clean_text((state_entry or {}).get("lastHttpLastModified")),
    )
    entry_report["httpStatus"] = int(revalidate.get("statusCode") or 0)
    if clean_text(revalidate.get("etag")):
        entry_report["httpEtag"] = clean_text(revalidate.get("etag"))
    if clean_text(revalidate.get("lastModified")):
        entry_report["httpLastModified"] = clean_text(revalidate.get("lastModified"))
    if not bool(revalidate.get("notModified")):
        return False
    entry_report["status"] = "excluded"
    entry_report["error"] = "not_modified_304"
    entry_report["exclusionReason"] = "cache_not_modified_304"
    entry_report["cacheDecisionReason"] = "not_modified_304"
    return True
