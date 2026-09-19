"""GameDevMap no-careers recovery queueing and provider inference helpers.

AI boundary owns: recovery path planning, no-careers reason detail, provider inference filtering, and probe helpers.
AI boundary implement in: this file for recovery queueing and provider inference; page outcomes stay in gamedevmap_active_dry_run.
AI boundary search before contracts: recovery URL planner, provider inference filters, and GameDevMap page outcome tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_gamedevmap_page_outcomes.py tests/source_discovery/test_gamedevmap_followup.py -q`.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from . import (
    browser_recovery as browser_recovery_helpers,
)
from . import (
    directory_page_recovery as directory_recovery_helpers,
)
from . import (
    recovery_url_planner,
)
from .gamedevmap import _apply_gamedevmap_provenance
from .gamedevmap_rejection import _rejection
from .io_runtime import endpoint_url
from .page_diagnostics import no_candidate_reason_detail as shared_no_candidate_reason_detail
from .probe_runtime import probe_candidates_async as shared_probe_candidates_async
from .provider_inference_filters import split_bad_provider_inferences
from .web_search import (
    extract_jobish_links,
    infer_provider_candidates_from_html,
)

PRIMARY_RECOVERY_PATHS = ("/careers", "/jobs")


SECONDARY_RECOVERY_PATHS = (
    "/join-us",
    "/work-with-us",
    "/company/careers",
    "/about/careers",
)


SOCIAL_PROFILE_HOSTS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}


THIRD_PARTY_PROFILE_HOSTS = {
    "impress.games",
    "itch.io",
    "linktr.ee",
    "sites.google.com",
}


def _candidate_url_key(candidate: dict[str, Any]) -> str:
    raw = str(
        candidate.get("listing_url")
        or candidate.get("careersUrl")
        or candidate.get("api_url")
        or candidate.get("url")
        or endpoint_url(candidate)
        or ""
    ).strip()
    return f"url:{raw}" if raw else ""


def _host(url: str) -> str:
    return recovery_url_planner.host(url)


def _host_in(host: str, blocked_hosts: set[str]) -> bool:
    return recovery_url_planner.host_in(host, blocked_hosts)


def _no_careers_reason_detail(page_url: str, html: str) -> str:
    return shared_no_candidate_reason_detail(
        page_url,
        html,
        social_profile_hosts=SOCIAL_PROFILE_HOSTS,
        third_party_profile_hosts=THIRD_PARTY_PROFILE_HOSTS,
        jobish_url_fn=lambda url, body: extract_jobish_links(body, url),
        include_noscript_script_shell=True,
    )


def _recovery_job_label(studio: str, recovery_url: str) -> str:
    try:
        parsed = urlparse(str(recovery_url or ""))
    except ValueError:
        path = ""
    else:
        path = parsed.path or "/"
    return f"{studio} recovery {path or 'unknown'}"


def _provider_candidates_from_html_text(
    *,
    row: dict[str, Any],
    page_url: str,
    html: str,
    index_url: str,
) -> list[dict[str, Any]]:
    studio = str(row.get("studio") or "").strip()
    candidates: list[dict[str, Any]] = []
    for inferred_row in infer_provider_candidates_from_html(
        page_url=page_url,
        html=html,
        studio=studio,
        nl_priority=False,
        discovery_method="gamedevmap",
    ):
        inferred = dict(inferred_row)
        inferred["careersUrl"] = page_url
        inferred["gamedevmapRecovery"] = True
        inferred["gamedevmapRecoverySource"] = "homepage_html_provider_url"
        inferred["evidenceTypes"] = list(
            dict.fromkeys(
                [
                    *(inferred.get("evidenceTypes") or []),
                    "gamedevmap_recovery_provider_url",
                ]
            )
        )
        candidates.append(
            _apply_gamedevmap_provenance(
                inferred,
                row,
                index_url=index_url,
                include_homepage_fetch=True,
            )
        )
    return candidates


def _rendered_page_has_static_job_evidence(page_url: str, html: str) -> bool:
    try:
        from .probe import static_probe_evidence

        return int(static_probe_evidence(html, page_url).count or 0) > 0
    except (TypeError, ValueError):
        return False


def _queue_no_careers_recovery(
    *,
    row: dict[str, Any],
    target_url: str,
    html: str,
    index_url: str,
    provider_candidates: list[dict[str, Any]],
    primary_recovery_jobs: list[dict[str, Any]],
    secondary_recovery_jobs: list[dict[str, Any]],
    browser_recovery_candidates: list[dict[str, Any]],
) -> bool:
    studio = str(row.get("studio") or "").strip()
    detail = _no_careers_reason_detail(target_url, html)
    if detail == "js_shell":
        browser_recovery_candidates.append(
            browser_recovery_helpers.browser_recovery_candidate_row(
                adapter="gamedevmap",
                name=f"{studio} browser recovery",
                studio=studio,
                url=target_url,
                source_directory_entry_url=str(row.get("sourceDirectoryEntryUrl") or "").strip(),
                reason_detail=detail,
            )
        )
    row_provider_candidates = _provider_candidates_from_html_text(
        row=row,
        page_url=target_url,
        html=html,
        index_url=index_url,
    )
    provider_candidates.extend(row_provider_candidates)
    primary_jobs, secondary_jobs = directory_recovery_helpers.plan_recovery_fetch_job_waves(
        page_url=target_url,
        html=html,
        primary_paths=PRIMARY_RECOVERY_PATHS,
        secondary_paths=SECONDARY_RECOVERY_PATHS,
        payload_factory=lambda _url, wave: {
            "row": row,
            "homepageUrl": target_url,
            "homepageReasonDetail": detail,
            "recoverySource": "same_party_recovery_url",
            "recoveryWave": int(wave),
        },
        name_factory=lambda recovery_url, _wave: _recovery_job_label(studio, recovery_url),
        adapter="gamedevmap",
        failure_stage="gamedevmap_recovery_fetch",
        blocked_hosts=SOCIAL_PROFILE_HOSTS | THIRD_PARTY_PROFILE_HOSTS,
        html_url_candidate_fn=recovery_url_planner.html_url_candidates,
    )
    primary_recovery_jobs.extend(primary_jobs)
    secondary_recovery_jobs.extend(secondary_jobs)
    return bool(primary_jobs or secondary_jobs or row_provider_candidates)


def _filter_bad_provider_inferences(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    good, bad = split_bad_provider_inferences(candidates)
    return (
        good,
        [
            _rejection(
                reason="bad_provider_inference",
                candidate=candidate,
                reason_detail=str(candidate.get("reasonDetail") or ""),
            )
            for candidate in bad
        ],
    )


async def _probe_candidates_async(
    candidates: list[dict[str, Any]],
    *,
    timeout_s: int,
    fetcher,
) -> list[tuple[dict[str, Any], bool, int, str, int]]:
    return await shared_probe_candidates_async(candidates, timeout_s=timeout_s, fetcher=fetcher)
