"""Thin coordinator composing the web-search candidate leaves.

AI boundary owns: the two web-search discovery entry points and re-export of every leaf unit.
AI boundary implement in: the web_search_*/web_page_* leaves for behavior; this file for composition and re-exports only.
AI boundary search before contracts: orchestrator and web_search entry points, directory audit specs, and web search tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_web_search_directory_audit.py tests/source_discovery/test_web_search_candidates.py -q`.
"""

from __future__ import annotations

import hashlib as hashlib
import json as json
import time as time
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus as quote_plus
from urllib.parse import urlparse as urlparse

from src.shared.regex import find_urls_in_text as find_urls_in_text
from src.shared.utils import now_iso
from src.source_registry import unique_sources as unique_sources
from src.source_registry_io import load_json_object as load_json_object

from . import audit_ledger as audit_ledger
from . import browser_recovery as browser_recovery_helpers
from . import candidate_collections as candidate_collections
from .config import DEFAULT_DISCOVERY_CONFIG as DEFAULT_DISCOVERY_CONFIG
from .config import DUCKDUCKGO_HTML_SEARCH as DUCKDUCKGO_HTML_SEARCH
from .config import MAX_SEARCH_LINKS_PER_QUERY as MAX_SEARCH_LINKS_PER_QUERY
from .config import WEB_SEARCH_QUERY_SUFFIX as WEB_SEARCH_QUERY_SUFFIX
from .directory_audit import (
    DirectoryAuditRunSpec,
    run_directory_audit_spec,
)
from .directory_fetch_jobs import build_directory_fetch_job as build_directory_fetch_job
from .directory_page_recovery import DEFAULT_RECOVERY_URL_LIMIT as DEFAULT_RECOVERY_URL_LIMIT
from .directory_page_recovery import RECOVERY_LOGIC_VERSION as RECOVERY_LOGIC_VERSION
from .directory_page_recovery import DirectoryRecoveryRequest as DirectoryRecoveryRequest
from .directory_page_recovery import apply_recovery_to_scan_result as apply_recovery_to_scan_result
from .directory_page_recovery import (
    http_recovery_request_from_context as http_recovery_request_from_context,
)
from .directory_page_recovery import merge_scan_result_payloads as merge_scan_result_payloads
from .directory_page_recovery import (
    recovery_result_candidates_from_strategy as recovery_result_candidates_from_strategy,
)
from .directory_page_recovery import run_recovery_for_requests as run_recovery_for_requests
from .page_diagnostics import browser_recoverable_error as browser_recoverable_error
from .page_diagnostics import looks_like_js_shell as looks_like_js_shell
from .page_outcomes import FetchedPageContext as FetchedPageContext
from .page_outcomes import PageOutcome as PageOutcome
from .page_outcomes import PageOutcomeStrategy as PageOutcomeStrategy
from .page_outcomes import (
    classify_fetched_page_with_strategy as classify_fetched_page_with_strategy,
)
from .page_outcomes import static_page_outcome_builders as static_page_outcome_builders
from .prevalidated_queue_policy import (
    apply_prevalidated_queue_overrides as apply_prevalidated_queue_overrides,
)
from .probe_runtime import rendered_static_probe_result as rendered_static_probe_result
from .provider_inference import infer_provider_adapter as infer_provider_adapter
from .provider_inference import provider_candidate as provider_candidate
from .scoring import careers_keyword_count as careers_keyword_count
from .scoring import studio_domain_match as studio_domain_match
from .scoring import unique_string_list as unique_string_list
from .web_browser_recovery import (
    _analyze_web_browser_recovery_batch as _analyze_web_browser_recovery_batch,
)
from .web_browser_recovery import (
    _analyze_web_browser_recovery_fetches as _analyze_web_browser_recovery_fetches,
)
from .web_browser_recovery import (
    _analyze_web_browser_recovery_success as _analyze_web_browser_recovery_success,
)
from .web_browser_recovery import (
    _apply_web_browser_recovery_probe_results as _apply_web_browser_recovery_probe_results,
)
from .web_browser_recovery import (
    _browser_static_probe_result_from_rendered_html as _browser_static_probe_result_from_rendered_html,
)
from .web_browser_recovery import _candidate_with_probe_evidence as _candidate_with_probe_evidence
from .web_browser_recovery import (
    _finalize_web_browser_recovery_candidates as _finalize_web_browser_recovery_candidates,
)
from .web_browser_recovery import (
    _initial_web_search_browser_recovery_artifact as _initial_web_search_browser_recovery_artifact,
)
from .web_browser_recovery import (
    _load_web_search_browser_recovery_artifact as _load_web_search_browser_recovery_artifact,
)
from .web_browser_recovery import (
    _record_web_browser_recovery_fetch_failure as _record_web_browser_recovery_fetch_failure,
)
from .web_browser_recovery import (
    _validated_web_browser_recovery_rows as _validated_web_browser_recovery_rows,
)
from .web_candidate_inference import _ATS_HTML_SIGNATURES as _ATS_HTML_SIGNATURES
from .web_candidate_inference import (
    infer_provider_candidates_from_html as infer_provider_candidates_from_html,
)
from .web_candidate_inference import infer_web_candidate as infer_web_candidate
from .web_page_job_stage import (
    _append_browser_recovery_candidate as _append_browser_recovery_candidate,
)
from .web_page_job_stage import _page_job as _page_job
from .web_page_job_stage import _record_web_page_result as _record_web_page_result
from .web_page_job_stage import _run_web_http_recovery as _run_web_http_recovery
from .web_page_job_stage import _run_web_page_job_stage as _run_web_page_job_stage
from .web_page_job_stage import _web_recovery_result_candidates as _web_recovery_result_candidates
from .web_page_outcomes import _append_page_analysis_outcome as _append_page_analysis_outcome
from .web_page_outcomes import _web_page_analysis_outcome as _web_page_analysis_outcome
from .web_search_audit_contracts import (
    _PREVALIDATED_BROWSER_DOMAIN_CAP as _PREVALIDATED_BROWSER_DOMAIN_CAP,
)
from .web_search_audit_contracts import (
    _PREVALIDATED_BROWSER_QUEUE_CAP as _PREVALIDATED_BROWSER_QUEUE_CAP,
)
from .web_search_audit_contracts import (
    WEB_SEARCH_AUDIT_FAILURE_SAMPLE_LIMIT as WEB_SEARCH_AUDIT_FAILURE_SAMPLE_LIMIT,
)
from .web_search_audit_contracts import (
    WEB_SEARCH_AUDIT_SAMPLE_LIMIT as WEB_SEARCH_AUDIT_SAMPLE_LIMIT,
)
from .web_search_audit_contracts import (
    WEB_SEARCH_AUDIT_SCHEMA_VERSION as WEB_SEARCH_AUDIT_SCHEMA_VERSION,
)
from .web_search_audit_contracts import (
    WEB_SEARCH_RECOVERY_SUMMARY_KEYS as WEB_SEARCH_RECOVERY_SUMMARY_KEYS,
)
from .web_search_audit_contracts import _append_bounded_sample as _append_bounded_sample
from .web_search_audit_contracts import _recovery_summary_fields as _recovery_summary_fields
from .web_search_audit_signature import _seed_catalog_signature as _seed_catalog_signature
from .web_search_audit_signature import _web_search_audit_signature as _web_search_audit_signature
from .web_search_config import (
    _web_search_audit_path,
    _web_search_audit_ttl_minutes,
    _web_search_browser_recovery_batch_size,
    _web_search_browser_recovery_concurrency,
    _web_search_browser_recovery_max_batches,
    _web_search_browser_recovery_timeout_s,
    _web_search_max_links_per_query,
    _web_search_max_queries,
    _web_search_recovery_enabled,
    _web_search_recovery_url_limit,
)
from .web_search_extract import extract_links_from_html as extract_links_from_html
from .web_search_fetch import fetch_text as fetch_text
from .web_search_fetch import (
    is_expected_web_search_fetch_failure as is_expected_web_search_fetch_failure,
)
from .web_search_scan import _merge_web_scan_results as _merge_web_scan_results
from .web_search_scan import _queue_web_search_link as _queue_web_search_link
from .web_search_scan import _sample_web_search_query as _sample_web_search_query
from .web_search_scan import (
    _scan_seed_careers_page_candidates as _scan_seed_careers_page_candidates,
)
from .web_search_scan import _scan_web_search_candidates as _scan_web_search_candidates
from .web_search_scan import build_web_search_queries as build_web_search_queries


def run_web_search_directory_audit(
    timeout_s: int,
    *,
    studio_seeds: list[dict[str, Any]],
    include_seed_careers: bool,
    include_web_search: bool,
    config: dict[str, Any] | None = None,
    fetcher=None,
    max_queries: int = 18,
) -> tuple[dict[str, Any], bool]:
    from .reporting import emit_log

    fetcher = fetcher or fetch_text
    configured_max_queries = _web_search_max_queries(config)
    if max_queries != 18:
        configured_max_queries = max(0, int(max_queries))
    max_links_per_query = _web_search_max_links_per_query(config)
    recovery_enabled = _web_search_recovery_enabled(config)
    recovery_url_limit = _web_search_recovery_url_limit(config)

    # pure helper
    def _scan(scan_timeout_s: int) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        if include_seed_careers:
            results.append(
                _scan_seed_careers_page_candidates(
                    scan_timeout_s,
                    studio_seeds=studio_seeds,
                    fetcher=fetcher,
                    enable_recovery=recovery_enabled,
                    recovery_url_limit=recovery_url_limit,
                )
            )
        if include_web_search:
            results.append(
                _scan_web_search_candidates(
                    scan_timeout_s,
                    studio_seeds=studio_seeds,
                    fetcher=fetcher,
                    max_queries=configured_max_queries,
                    max_links_per_query=max_links_per_query,
                    enable_recovery=recovery_enabled,
                    recovery_url_limit=recovery_url_limit,
                )
            )
        merged = _merge_web_scan_results(results)
        summary = dict(merged.get("summary") or {})
        summary.update(
            {
                "seedCareersEnabled": bool(include_seed_careers),
                "webSearchEnabled": bool(include_web_search),
                "seedRows": len(studio_seeds),
                "maxQueries": configured_max_queries,
                "maxLinksPerQuery": max_links_per_query,
            }
        )
        return {
            "providerCandidates": list(merged.get("providerCandidates") or []),
            "staticCandidates": list(merged.get("staticCandidates") or []),
            "browserRecoveryCandidates": list(merged.get("browserRecoveryCandidates") or []),
            "failures": list(merged.get("failures") or []),
            "summary": summary,
            "batchTiming": dict(merged.get("batchTiming") or {}),
            "progress": {
                "complete": True,
                "cursor": len(studio_seeds),
                "completedUrlIdentities": list(merged.get("completedUrlIdentities") or []),
            },
        }

    return run_directory_audit_spec(
        DirectoryAuditRunSpec(
            adapter="web_search",
            schema_version=WEB_SEARCH_AUDIT_SCHEMA_VERSION,
            output_path=_web_search_audit_path(config),
            ttl_minutes=_web_search_audit_ttl_minutes(config),
            signature=_web_search_audit_signature(
                studio_seeds=studio_seeds,
                include_seed_careers=include_seed_careers,
                include_web_search=include_web_search,
                max_queries=configured_max_queries,
                max_links_per_query=max_links_per_query,
                recovery_enabled=recovery_enabled,
                recovery_url_limit=recovery_url_limit,
            ),
            timeout_s=timeout_s,
            scan=_scan,
            runtime={
                "includeSeedCareers": bool(include_seed_careers),
                "includeWebSearch": bool(include_web_search),
                "maxQueries": configured_max_queries,
                "maxLinksPerQuery": max_links_per_query,
                "activeAuditRecoveryEnabled": recovery_enabled,
            },
            summary={
                "seedCareersEnabled": bool(include_seed_careers),
                "webSearchEnabled": bool(include_web_search),
                "seedRows": len(studio_seeds),
                "maxQueries": configured_max_queries,
                "maxLinksPerQuery": max_links_per_query,
            },
            sample_limit=WEB_SEARCH_AUDIT_FAILURE_SAMPLE_LIMIT,
            emit_log=emit_log,
        )
    )


# orchestration — coordinates network + mutation


def run_web_search_browser_recovery(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=None,
    browser_fetcher=None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    from .reporting import emit_log

    fetcher = fetcher or fetch_text
    browser_fetcher = browser_fetcher or browser_recovery_helpers.default_browser_fetcher()
    output_path = output_path or _web_search_audit_path(config)
    artifact = _load_web_search_browser_recovery_artifact(output_path)
    if not artifact:
        artifact = _initial_web_search_browser_recovery_artifact()
    browser_recovery = dict(artifact.get("browserRecovery") or {})
    all_recovery_rows = [
        dict(row)
        for row in list(artifact.get("browserRecoveryCandidates") or [])
        if isinstance(row, dict)
    ]
    batch_size = _web_search_browser_recovery_batch_size(config)
    max_batches = _web_search_browser_recovery_max_batches(config)
    limit = batch_size * max_batches if batch_size and max_batches else batch_size
    concurrency = _web_search_browser_recovery_concurrency(config)
    browser_timeout_s = _web_search_browser_recovery_timeout_s(config, timeout_s)
    assembly_result = browser_recovery_helpers.run_browser_recovery_assembly(
        rows=all_recovery_rows,
        browser_recovery=browser_recovery,
        timeout_s=browser_timeout_s,
        fetcher=fetcher,
        browser_fetcher=browser_fetcher,
        concurrency=concurrency,
        analyze_fetches=_analyze_web_browser_recovery_batch,
        merge_artifact_updates=lambda _batch, combined_probe_results: (
            _apply_web_browser_recovery_probe_results(artifact, combined_probe_results)
        ),
        recovered_rows=lambda: [
            *list(artifact.get("providerCandidates") or []),
            *list(artifact.get("staticCandidates") or []),
        ],
        recovered_predicate=lambda row: bool(row.get("webSearchBrowserRecovery")),
        limit=limit,
        probe_timeout_s=timeout_s,
        emit_log=emit_log,
        log_label="Web-search browser recovery",
        include_fetch_counts=True,
        include_candidate_analysis_count=True,
    )
    artifact["browserRecovery"] = browser_recovery
    summary = dict(artifact.get("summary") or {})
    summary["providerCandidates"] = len(list(artifact.get("providerCandidates") or []))
    summary["staticCandidates"] = len(list(artifact.get("staticCandidates") or []))
    summary["browserRecoveredActiveCandidates"] = assembly_result.active_count
    artifact["summary"] = summary
    artifact["updatedAt"] = now_iso()
    audit_ledger.save_artifact_atomic(artifact, output_path)
    return artifact
