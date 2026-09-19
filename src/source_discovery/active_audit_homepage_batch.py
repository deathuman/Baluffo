"""Active audit homepage batch execution.

AI boundary owns: per-batch homepage result fan-out into provider, static, rejection, failure, and recovery buckets.
AI boundary implement in: this file for homepage batch fan-out; GameDevMap page outcomes stay in gamedevmap_active_dry_run.
AI boundary search before contracts: homepage page outcomes, no-candidate outcomes, and active audit runtime tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .active_audit_contracts import ActiveHomepageBatchResult as ActiveHomepageBatchResult
from .active_audit_contracts import HomepagePageOutcome as HomepagePageOutcome
from .active_audit_contracts import NoCandidateOutcome as NoCandidateOutcome


def run_active_homepage_batch(
    *,
    batch_rows: list[dict[str, Any]],
    homepage_fetch_results: list[dict[str, Any]],
    row_url: Callable[[dict[str, Any]], str],
    infer_direct_provider: Callable[[dict[str, Any]], dict[str, Any] | None],
    fetch_failure_rejection: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    analyze_homepage: Callable[[dict[str, Any], str, str], HomepagePageOutcome],
    handle_no_candidate: Callable[[dict[str, Any], str, str], NoCandidateOutcome],
) -> ActiveHomepageBatchResult:
    result = ActiveHomepageBatchResult()
    fetched_urls = {str(row.get("url") or "").strip() for row in homepage_fetch_results}
    direct_rows = [row for row in batch_rows if row_url(row) not in fetched_urls]

    for row in direct_rows:
        inferred = infer_direct_provider(row)
        if inferred:
            result.provider_candidates.append(inferred)

    for fetch_result in homepage_fetch_results:
        row = dict(fetch_result.get("payload") or {})
        target_url = str(fetch_result.get("url") or row_url(row)).strip()
        if not bool(fetch_result.get("ok")):
            failure = fetch_result.get("failure")
            if isinstance(failure, dict):
                result.failures.append(dict(failure))
            result.rejected_rows.append(fetch_failure_rejection(row, fetch_result))
            continue

        result.homepages_fetched += 1
        html = str(fetch_result.get("text") or "")
        page_outcome = analyze_homepage(row, target_url, html)
        if page_outcome.found_candidates:
            result.provider_candidates.extend(page_outcome.provider_candidates)
            result.static_candidates.extend(page_outcome.static_candidates)
            continue

        no_candidate = handle_no_candidate(row, target_url, html)
        result.provider_candidates.extend(no_candidate.provider_candidates)
        result.static_candidates.extend(no_candidate.static_candidates)
        result.primary_recovery_jobs.extend(no_candidate.primary_recovery_jobs)
        result.secondary_recovery_jobs.extend(no_candidate.secondary_recovery_jobs)
        result.browser_recovery_candidates.extend(no_candidate.browser_recovery_candidates)
        result.rejected_rows.extend(no_candidate.rejected_rows)

    return result
