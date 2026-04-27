from __future__ import annotations

"""Shared runtime mechanics for active-source audit batches."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class HomepagePageOutcome:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    found_candidates: bool = False


@dataclass
class NoCandidateOutcome:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    primary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    secondary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    browser_recovery_candidates: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ActiveHomepageBatchResult:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    rejected_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    primary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    secondary_recovery_jobs: list[dict[str, Any]] = field(default_factory=list)
    browser_recovery_candidates: list[dict[str, Any]] = field(default_factory=list)
    homepages_fetched: int = 0


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
