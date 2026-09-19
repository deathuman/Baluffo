"""Web-search browser recovery artifact handling and fetch analysis.

AI boundary owns: browser recovery artifact lifecycle, per-fetch analysis, and validated recovery row projection.
AI boundary implement in: this file for browser recovery; the recovery entry point stays in the coordinator.
AI boundary search before contracts: browser_recovery helpers, prevalidated queue policy, and browser recovery tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_browser_recovery.py -q`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.source_registry import unique_sources
from src.source_registry_io import load_json_object

from . import browser_recovery as browser_recovery_helpers
from . import candidate_collections as candidate_collections
from .prevalidated_queue_policy import apply_prevalidated_queue_overrides
from .probe_runtime import (
    candidate_with_probe_evidence as probe_candidate_with_probe_evidence,
)
from .probe_runtime import (
    rendered_static_probe_result,
)
from .web_page_outcomes import _append_page_analysis_outcome as _append_page_analysis_outcome
from .web_search_audit_contracts import (
    _PREVALIDATED_BROWSER_DOMAIN_CAP as _PREVALIDATED_BROWSER_DOMAIN_CAP,
)
from .web_search_audit_contracts import (
    _PREVALIDATED_BROWSER_QUEUE_CAP as _PREVALIDATED_BROWSER_QUEUE_CAP,
)
from .web_search_audit_contracts import (
    WEB_SEARCH_AUDIT_SCHEMA_VERSION as WEB_SEARCH_AUDIT_SCHEMA_VERSION,
)


def _load_web_search_browser_recovery_artifact(output_path: Path) -> dict[str, Any]:
    payload = load_json_object(output_path, {})
    return payload if isinstance(payload, dict) else {}


# pure — returns empty artifact skeleton


def _initial_web_search_browser_recovery_artifact() -> dict[str, Any]:
    return {
        "schemaVersion": WEB_SEARCH_AUDIT_SCHEMA_VERSION,
        "adapter": "web_search",
        "summary": {},
        "providerCandidates": [],
        "staticCandidates": [],
        "browserRecoveryCandidates": [],
        "browserRecovery": {},
    }


# mutation — records browser fetch failure


def _record_web_browser_recovery_fetch_failure(
    _row: dict[str, Any],
    source_url: str,
    error: str,
    browser_recovery: dict[str, Any],
) -> list[dict[str, Any]]:
    browser_recovery_helpers.append_failure_sample(
        browser_recovery,
        {
            "url": source_url,
            "stage": "browser_fetch",
            "error": error,
        },
    )
    return []


# pure — page analysis for browser-rendered page


def _analyze_web_browser_recovery_success(
    row: dict[str, Any],
    source_url: str,
    html: str,
) -> browser_recovery_helpers.BrowserRecoveryPageAnalysis:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    _append_page_analysis_outcome(
        page_url=source_url,
        page_html=html,
        studio=str(row.get("studio") or ""),
        nl_priority=bool(row.get("nlPriority")),
        discovery_method=str(row.get("discoveryMethod") or "web_search"),
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
    )
    for candidate in [*provider_candidates, *static_candidates]:
        candidate["webSearchBrowserRecovery"] = True
    return browser_recovery_helpers.BrowserRecoveryPageAnalysis(
        all_candidates=[*provider_candidates, *static_candidates],
        rendered_static_candidates=static_candidates,
    )


# pure — deduplication


def _finalize_web_browser_recovery_candidates(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return unique_sources(candidates), []


# browser recovery callback chain: web_search analyze_fetches


def _analyze_web_browser_recovery_fetches(
    fetch_results: list[tuple[dict[str, Any], str, str, int]],
    *,
    browser_recovery: dict[str, Any],
    processed: set[str],
) -> tuple[
    list[dict[str, Any]],
    list[tuple[dict[str, Any], bool, int, str, int]],
    int,
]:
    analysis = browser_recovery_helpers.analyze_browser_recovery_fetch_results(
        fetch_results=fetch_results,
        browser_recovery=browser_recovery,
        processed=processed,
        analyze_success=_analyze_web_browser_recovery_success,
        handle_fetch_failure=_record_web_browser_recovery_fetch_failure,
        rendered_static_probe_result=lambda candidate, rendered_url, rendered_html: (
            _browser_static_probe_result_from_rendered_html(
                candidate,
                rendered_url=rendered_url,
                rendered_html=rendered_html,
            )
        ),
        finalize_candidates=_finalize_web_browser_recovery_candidates,
    )
    return (
        analysis.all_candidates,
        analysis.rendered_probe_results,
        analysis.fetch_failures,
    )


# strategy factory — browser recovery batch wrapper


def _analyze_web_browser_recovery_batch(
    fetch_results: list[tuple[dict[str, Any], str, str, int]],
    browser_recovery: dict[str, Any],
    processed: set[str],
) -> browser_recovery_helpers.BrowserRecoveryAnalysis:
    all_candidates, rendered_probe_results, fetch_failures = _analyze_web_browser_recovery_fetches(
        fetch_results,
        browser_recovery=browser_recovery,
        processed=processed,
    )
    return browser_recovery_helpers.BrowserRecoveryAnalysis(
        all_candidates=all_candidates,
        rendered_probe_results=rendered_probe_results,
        fetch_failures=fetch_failures,
    )


# pure — applies queue/domain caps to probe results


def _candidate_with_probe_evidence(candidate: dict[str, Any], jobs_found: int) -> dict[str, Any]:
    return probe_candidate_with_probe_evidence(
        candidate,
        jobs_found,
        prevalidated_discovery=True,
    )


# pure — thin delegation to rendered_static_probe_result


def _browser_static_probe_result_from_rendered_html(
    candidate: dict[str, Any],
    *,
    rendered_url: str,
    rendered_html: str,
) -> tuple[dict[str, Any], bool, int, str, int] | None:
    return rendered_static_probe_result(
        candidate,
        rendered_url=rendered_url,
        rendered_html=rendered_html,
    )


# mutation — artifact read (load from disk)


def _validated_web_browser_recovery_rows(
    combined_probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
) -> list[dict[str, Any]]:
    return [
        apply_prevalidated_queue_overrides(
            row,
            adapter_cap=_PREVALIDATED_BROWSER_QUEUE_CAP,
            domain_cap=_PREVALIDATED_BROWSER_DOMAIN_CAP,
        )
        for row in browser_recovery_helpers.positive_probe_candidates(
            combined_probe_results,
            normalize_candidate=_candidate_with_probe_evidence,
        )
    ]


# mutation — modifies in-place state


def _apply_web_browser_recovery_probe_results(
    artifact: dict[str, Any],
    combined_probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
) -> None:
    validated_rows = _validated_web_browser_recovery_rows(combined_probe_results)
    provider_validated, static_validated = candidate_collections.split_provider_static_rows(
        validated_rows
    )
    candidate_collections.append_provider_static_rows(
        artifact,
        provider_rows=provider_validated,
        static_rows=static_validated,
    )


# orchestration — coordinates network + mutation
