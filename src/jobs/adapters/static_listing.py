from __future__ import annotations

import asyncio
import hashlib
import re
import sys
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.html_parsers import (
    maybe_fetch_kojima_job_listing_html,
    parse_jobpostings_from_html,
    strip_html_text,
)
from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.errors import NoPluginFoundError
from src.jobs.adapters.plugins.static._heuristics import detect_js_shell
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_detail_heuristics import (
    _is_one_man_studio_noise_city,
    add_detail_link,
    choose_detail_traversal_mode,
    is_probable_job_detail_url,
    process_detail_html,
    process_detail_link,
    source_detail_concurrency_for,
    source_detail_limit_for,
    source_detail_retries_for,
)
from src.jobs.adapters.static_runtime_support import (
    _as_dict,
    effective_timeout_for_remaining_budget,
    remaining_static_source_budget_s,
    static_source_budget_exhausted,
    update_source_detail_taxonomy,
)
from src.jobs.common.exact_category_titles import has_static_container_artifact_evidence
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.http import HttpStatusError
from src.jobs.page_gating import classify_job_page, looks_like_job_title_candidate
from src.jobs.state_source_state import should_skip_static_source_for_structured_migration
from src.jobs.text_utils import clean_text, normalize_url, sanitize_location_text
from src.jobs.transport import conditional_revalidate_url
from src.scrapers.domain_profiles import domain_profile_for_url
from src.shared.http_batch import fetch_pages_batched
from src.shared.regex import find_urls_in_text
from src.shared.utils import now_iso

from ..common import config as common_config
from .static_runtime import StaticSourceContext

_EXTERNAL_DETAIL_FANOUT_HOST_THRESHOLD = 2
_EXTERNAL_DETAIL_FANOUT_LINK_CAP = 8

# (adapter_name, html_substring) pairs for diagnostic warnings
# when a static source's page HTML contains an ATS signature.
_ATS_SIGNATURE_HINTS: list[tuple[str, str]] = [
    ("teamtailor", "teamtailor"),
    ("greenhouse", "greenhouse.io"),
    ("bamboohr", "bamboohr"),
    ("workday", "myworkdayjobs"),
    ("smartrecruiters", "smartrecruiters"),
    ("lever", "lever.co"),
    ("workable", "workable"),
]

_CAREERS_LANDING_TOKENS = (
    "careers",
    "career",
    "jobs",
    "job",
    "join-us",
    "open-positions",
    "vacancies",
    "work-with-us",
    "openings",
    "vacancy",
    "positions",
    "recruitment",
    "karriere",
    "stellenanzeigen",
    "emploi",
    "recrutement",
    "vacantes",
    "lavora",
    "offerte",
    "vagas",
)


def _careers_landing_url(url: str) -> bool:
    """Check if URL host+path suggests a career listing page."""
    try:
        parsed = urlparse(str(url or ""))
    except ValueError:
        return False
    text = f"{(parsed.hostname or '').lower()}{(parsed.path or '').lower()}"
    return any(token in text for token in _CAREERS_LANDING_TOKENS)


# pure — budget arithmetic + TimeoutError gate
def _effective_timeout_or_raise(
    *,
    timeout_s: int,
    remaining_budget_s: float,
    source_budget_s: int,
) -> int:
    effective_timeout_s = effective_timeout_for_remaining_budget(
        timeout_s=timeout_s,
        remaining_budget_s=remaining_budget_s,
    )
    if effective_timeout_s <= 0:
        raise TimeoutError(f"time budget exceeded ({source_budget_s}s)")
    return effective_timeout_s


# pure — URL host normalization
def _normalized_host(url: str) -> str:
    host = (urlparse(clean_text(url) or "").hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


# mutation — modifies in-place state
def _cap_external_detail_fanout(
    ctx: StaticSourceContext,
    *,
    page_url: str,
    detail_links: list[StaticDetailCandidate],
    cap: int = _EXTERNAL_DETAIL_FANOUT_LINK_CAP,
) -> list[StaticDetailCandidate]:
    if len(detail_links) <= cap:
        return detail_links
    page_host = _normalized_host(page_url)
    if not page_host:
        return detail_links
    external_hosts = {
        host
        for candidate in detail_links
        if (host := _normalized_host(candidate.url)) and host != page_host
    }
    if len(external_hosts) <= _EXTERNAL_DETAIL_FANOUT_HOST_THRESHOLD:
        return detail_links
    capped: list[StaticDetailCandidate] = []
    external_kept = 0
    for candidate in detail_links:
        host = _normalized_host(candidate.url)
        if not host or host == page_host:
            capped.append(candidate)
            continue
        if external_kept >= cap:
            continue
        capped.append(candidate)
        external_kept += 1
    pruned = max(0, len(detail_links) - len(capped))
    if pruned:
        ctx.stats["external_detail_links_capped"] = (
            int(ctx.stats.get("external_detail_links_capped") or 0) + pruned
        )
        ctx.link_rejections["non_job_url"] += pruned
    return capped


@dataclass(frozen=True)
class StaticDetailCandidate:
    url: str
    title: str = ""
    depth: int = 0
    parent_url: str = ""


def _is_provisional_static_artifact_row(row: dict[str, Any]) -> bool:
    return has_static_container_artifact_evidence(row.get("title"), row.get("jobLink"))


def _append_detail_candidate(
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
    seen_links: set[str],
    *,
    candidate_url: str,
    anchor_text: str,
    depth: int,
    parent_url: str,
) -> bool:
    absolute = normalize_url(candidate_url)
    if not absolute or absolute in detail_seen or absolute in seen_links:
        return False
    detail_seen.add(absolute)
    detail_links.append(
        StaticDetailCandidate(
            url=absolute,
            title=clean_text(anchor_text),
            depth=max(0, int(depth or 0)),
            parent_url=clean_text(parent_url),
        )
    )
    return True


def _source_detail_key(ctx: StaticSourceContext) -> str:
    if ctx.selected_source_count == 1:
        return ctx.run_deps.diagnostics_name
    return ctx.source_name


def _nested_detail_limit_for(ctx: StaticSourceContext, discovered_links: int) -> int:
    return source_detail_limit_for(
        _source_detail_key(ctx),
        source_state_rows=ctx.run_deps.source_state_rows,
        discovered_links=discovered_links,
        listing_jobs_found=0,
        low_yield_detail_cap=ctx.runtime_config.low_yield_detail_cap,
        very_low_yield_detail_cap=ctx.runtime_config.very_low_yield_detail_cap,
        uncapped_deep_static=ctx.runtime_config.uncapped_deep_static,
    )


@dataclass
class StaticListingStageState:
    browser_fallbacks: int = 0
    terminal_reason: str = ""
    batch_meta: dict[str, dict[str, Any]] = field(default_factory=dict)
    lock: Lock = field(default_factory=Lock)

    # pure helper
    def note_terminal_reason(self, reason: str, ctx: StaticSourceContext) -> None:
        clean_reason = clean_text(reason)
        if not clean_reason:
            return
        with self.lock:
            if not clean_text(self.terminal_reason):
                self.terminal_reason = clean_reason
                ctx.stats["listing_terminal_reason"] = clean_reason

    # pure helper
    def record_batch_meta(self, url: str, **payload: Any) -> None:
        with self.lock:
            self.batch_meta[url] = {
                **(self.batch_meta.get(url) or {}),
                **payload,
            }

    # pure helper
    def clear_batch_meta(self) -> None:
        with self.lock:
            self.batch_meta.clear()

    # pure helper
    def increment_browser_fallbacks(self) -> None:
        with self.lock:
            self.browser_fallbacks = int(self.browser_fallbacks or 0) + 1


# pure — classifier
def _needs_detail_location_resolution(
    row: dict[str, Any], link: str = "", location_hint: str = ""
) -> bool:
    del link
    city = clean_text(row.get("city"))
    if city:
        sanitized_city, city_reason = sanitize_location_text(city, field_name="city")
        if city_reason or classify_city_garbage(city) or not sanitized_city:
            return True
    hint = clean_text(location_hint)
    return not city and bool(hint)


# mutation — modifies in-place state
def _apply_one_man_studio_cleanup(
    ctx: StaticSourceContext, plugin_jobs: list[dict[str, Any]]
) -> None:
    if (
        "theonemanstudio" not in ctx.source_name.lower()
        and "one man studio" not in ctx.company.lower()
    ):
        return
    for row in plugin_jobs:
        if not isinstance(row, dict):
            continue
        row_city, _ = sanitize_location_text(row.get("city"), field_name="city")
        row_country, _ = sanitize_location_text(row.get("country"), field_name="country")
        if row_country == "Remote":
            row_country = ""
        if row_city == "Remote":
            row_city = ""
        if row_country in {"", "Unknown"} and _is_one_man_studio_noise_city(
            row_city,
            source_name=ctx.source_name,
            source=ctx.source,
        ):
            row_city = ""
        row["city"] = row_city
        row["country"] = row_country
        if row_city or row_country not in {"", "Unknown"}:
            row["locations"] = [
                {
                    "city": row_city,
                    "country": row_country if row_country != "Unknown" else "",
                }
            ]
            row["locationSummary"] = ", ".join(
                part for part in [row_city, row_country if row_country != "Unknown" else ""] if part
            )
        else:
            row["locations"] = []
            row["locationSummary"] = ""


# mutation — registry-facing artifact update
def _complete_source_without_generic_flow(ctx: StaticSourceContext) -> None:
    update_source_detail_taxonomy(ctx.entry_report)
    ctx.details.append(ctx.entry_report)


# mutation — finalizes stats on ctx
def _finish_generic_source(ctx: StaticSourceContext, stage_state: StaticListingStageState) -> None:
    current_gate_wait_ms, current_gate_wait_count = ctx.current_domain_gate_wait_stats()
    ctx.stats["listing_browser_fallbacks"] = int(stage_state.browser_fallbacks or 0)
    ctx.stats["listing_terminal_reason"] = clean_text(stage_state.terminal_reason)
    ctx.stats["domain_gate_wait_ms"] = int(current_gate_wait_ms)
    ctx.stats["domain_gate_wait_count"] = int(current_gate_wait_count)
    ctx.entry_report["keptCount"] = ctx.current_source_kept_count()
    ctx.stats["jobs_emitted"] = int(ctx.entry_report["keptCount"])
    if int(ctx.stats.get("detail_pages_visited") or 0) > 0:
        ctx.stats["detail_yield_percent"] = int(
            round((ctx.entry_report["keptCount"] / ctx.stats["detail_pages_visited"]) * 100)
        )
    ctx.entry_report["loss"] = {
        "staticNonJobUrlRejected": int(ctx.link_rejections.get("non_job_url", 0)),
        "staticDuplicateLinkRejected": int(ctx.link_rejections.get("duplicate_link", 0)),
        "staticDetailParseEmpty": int(ctx.link_rejections.get("detail_parse_empty", 0)),
        "staticDeadListingPageRejected": int(ctx.link_rejections.get("dead_listing_page", 0)),
    }
    ctx.entry_report["deadListingPageCount"] = int(ctx.link_rejections.get("dead_listing_page", 0))
    ctx.entry_report["deadListingPageExamples"] = ctx.dead_listing_page_examples
    if (
        ctx.entry_report["keptCount"] == 0
        and ctx.pages
        and not clean_text(ctx.entry_report.get("classification"))
        and int(ctx.link_rejections.get("dead_listing_page", 0)) <= 0
    ):
        ctx.entry_report["status"] = "error"
        ctx.entry_report["error"] = "no jobs extracted from source pages"
        terminal_reason = clean_text(ctx.stats.get("listing_terminal_reason"))
        if terminal_reason in {"listing_timeout", "listing_timeout_after_browser_fallback"}:
            ctx.entry_report["classification"] = "timeout"
        elif terminal_reason in {"blocked_after_browser_fallback", "browser_fallback_empty"}:
            ctx.entry_report["classification"] = (
                "anti_bot_or_challenge"
                if bool(ctx.source.get("antiBotBrowserRetry"))
                else "blocked_or_challenge"
            )
        elif (
            bool(ctx.source.get("antiBotBrowserRetry"))
            and int(ctx.stats.get("listing_browser_fallbacks") or 0) > 0
        ):
            ctx.entry_report["classification"] = "anti_bot_or_challenge"
            ctx.entry_report["error"] = (
                "browser retry exhausted: no jobs extracted from source pages"
            )
        if ctx.selected_source_count == 1:
            ctx.errors.append(f"static:{ctx.source_name}: no jobs extracted from source pages")
    ctx.emit_heartbeat()
    update_source_detail_taxonomy(ctx.entry_report)
    if (
        ctx.entry_report["keptCount"] == 0
        and int(ctx.entry_report.get("deadListingPageCount") or 0) > 0
    ):
        ctx.entry_report["classification"] = "dead_listing_page"
        ctx.entry_report["browserFallbackRecommended"] = False
        ctx.entry_report["browserEscalationEligible"] = False
        ctx.entry_report.pop("browserEscalationEligibilityReason", None)
    ctx.emit_source_progress(
        phase_key="static_completed",
        phase_label="Static source completed",
        counts={
            "keptCount": int(ctx.entry_report.get("keptCount") or 0),
            "fetchedCount": int(ctx.entry_report.get("fetchedCount") or 0),
            "detailCandidates": int(ctx.stats.get("candidate_links_found") or 0),
            "detailPagesVisited": int(ctx.stats.get("detail_pages_visited") or 0),
        },
        target_label=ctx.source_name,
        event_level="success" if int(ctx.entry_report.get("keptCount") or 0) > 0 else "warn",
        message=(
            f"Completed static source {ctx.source_name}: kept "
            f"{int(ctx.entry_report.get('keptCount') or 0)} job"
            f"{'' if int(ctx.entry_report.get('keptCount') or 0) == 1 else 's'}."
        ),
    )
    ctx.details.append(ctx.entry_report)


# mutation — artifact read/write + network (revalidation)
def _handle_skip_and_revalidation(ctx: StaticSourceContext) -> bool:
    ctx.emit_source_progress(
        phase_key="static_prepare",
        phase_label="Preparing static source",
        counts={"cacheDecision": ctx.entry_report["cacheDecision"]},
        target_label=ctx.source_name,
        event_level="info",
        message=f"Preparing static source {ctx.source_name}.",
    )
    if ctx.entry_report["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
        ctx.entry_report["status"] = "excluded"
        ctx.entry_report["error"] = ctx.entry_report["cacheDecisionReason"]
        ctx.entry_report["exclusionReason"] = f"cache_{ctx.entry_report['cacheDecisionReason']}"
        _complete_source_without_generic_flow(ctx)
        return True
    if ctx.entry_report["cacheDecision"] == "revalidate_only" and ctx.pages:
        revalidate = conditional_revalidate_url(
            clean_text(ctx.pages[0]),
            ctx.run_deps.timeout_s,
            etag=clean_text((ctx.state_entry or {}).get("lastHttpEtag")),
            last_modified=clean_text((ctx.state_entry or {}).get("lastHttpLastModified")),
        )
        ctx.entry_report["httpStatus"] = int(revalidate.get("statusCode") or 0)
        if clean_text(revalidate.get("etag")):
            ctx.entry_report["httpEtag"] = clean_text(revalidate.get("etag"))
        if clean_text(revalidate.get("lastModified")):
            ctx.entry_report["httpLastModified"] = clean_text(revalidate.get("lastModified"))
        if bool(revalidate.get("notModified")):
            ctx.entry_report["status"] = "excluded"
            ctx.entry_report["error"] = "not_modified_304"
            ctx.entry_report["exclusionReason"] = "cache_not_modified_304"
            ctx.entry_report["cacheDecisionReason"] = "not_modified_304"
            _complete_source_without_generic_flow(ctx)
            return True
    if should_skip_static_source_for_structured_migration(
        ctx.source_name,
        ctx.source,
        ctx.run_deps.source_state_rows,
    ):
        ctx.entry_report["status"] = "excluded"
        ctx.entry_report["error"] = "structured_migration_promoted"
        ctx.entry_report["exclusionReason"] = "structured_migration_promoted"
        ctx.entry_report["structuredMigrationSkipped"] = True
        _complete_source_without_generic_flow(ctx)
        return True
    return False


# pure — builds AdapterPluginContext from ctx
def _static_plugin_context(ctx: StaticSourceContext) -> AdapterPluginContext | None:
    host = ""
    if ctx.pages:
        try:
            parsed = urlparse(clean_text(ctx.pages[0]) or "")
            host = (parsed.netloc or "").strip().lower()
        except Exception:  # noqa: BLE001
            pass
    plugin_identity = host or ctx.source_name
    if host == "jobs.jobvite.com" and ctx.pages:
        plugin_identity = clean_text(ctx.pages[0]) or plugin_identity
    if plugin_identity == "example.com" and not clean_text(ctx.source.get("id")):
        return None
    return AdapterPluginContext(
        family="static",
        adapter_key="static",
        source_identity=plugin_identity,
    )


# network — calls plugin.run() with HTTP fetch
def _invoke_static_plugin(ctx: StaticSourceContext, plugin: Any) -> list[dict[str, Any]]:
    return list(
        plugin.run(
            fetch_text=ctx.run_deps.fetch_text,
            timeout_s=ctx.run_deps.timeout_s,
            retries=ctx.run_deps.retries,
            backoff_s=ctx.run_deps.backoff_s,
            heartbeat_callback=ctx.run_deps.heartbeat_callback,
            pages=ctx.pages,
            source_row=ctx.source,
            parse_jobpostings_from_html=parse_jobpostings_from_html,
            fetch_html_cached=ctx.html_fetcher.fetch_html_cached,
            maybe_fetch_kojima_job_listing_html=maybe_fetch_kojima_job_listing_html,
            try_playwright=ctx.run_deps.try_playwright,
        )
    )


# mutation — sets ctx.entry_report meta fields
def _apply_static_plugin_meta(ctx: StaticSourceContext, plugin_meta: dict[str, Any]) -> None:
    ctx.entry_report["classification"] = clean_text(plugin_meta.get("classification"))
    ctx.entry_report["browserFallbackRecommended"] = bool(
        plugin_meta.get("browserFallbackRecommended")
    )
    ctx.entry_report["extractorHint"] = clean_text(plugin_meta.get("extractorHint"))
    if plugin_meta.get("emptyConfirmed"):
        ctx.entry_report["emptyConfirmed"] = True
    ats_links = plugin_meta.get("atsLinks")
    if isinstance(ats_links, list):
        ctx.entry_report["atsLinks"] = [clean_text(v) for v in ats_links if clean_text(v)][:5]
    dead_listing_count = int(plugin_meta.get("deadListingPageCount") or 0)
    if dead_listing_count > 0:
        ctx.entry_report["deadListingPageCount"] = dead_listing_count
    dead_listing_examples = plugin_meta.get("deadListingPageExamples")
    if isinstance(dead_listing_examples, list) and dead_listing_examples:
        cleaned_examples = [clean_text(v) for v in dead_listing_examples if clean_text(v)][:5]
        ctx.dead_listing_page_examples[:] = cleaned_examples
        ctx.entry_report["deadListingPageExamples"] = cleaned_examples
    meta_error = clean_text(plugin_meta.get("error"))
    if meta_error and not ctx.entry_report.get("error"):
        ctx.entry_report["error"] = meta_error


# network + mutation — reclassifies empty listings
def _probe_empty_plugin_listing(ctx: StaticSourceContext, classification: str) -> str:
    if classification in {"dead_listing_page", "empty_confirmed"} or not ctx.pages:
        return classification
    probe_page = clean_text(ctx.pages[0])
    if not probe_page:
        return classification
    try:
        probe_html, _ = ctx.html_fetcher.fetch_html_cached(
            probe_page,
            remaining_budget_s=float(ctx.run_deps.timeout_s or 1),
        )
    except Exception:  # noqa: BLE001
        probe_html = ""
    if not probe_html:
        return classification
    job_like, gate_reason = classify_job_page(
        probe_html,
        probe_page,
        profile=ctx.source if isinstance(ctx.source, dict) else None,
    )
    if job_like or gate_reason != "dead_listing_page":
        return classification
    ctx.entry_report.update(
        {
            "classification": "dead_listing_page",
            "browserFallbackRecommended": False,
            "browserEscalationEligible": False,
            "deadListingPageCount": max(1, int(ctx.entry_report.get("deadListingPageCount") or 0)),
        }
    )
    ctx.entry_report.pop("browserEscalationEligibilityReason", None)
    if len(ctx.dead_listing_page_examples) < 5:
        ctx.dead_listing_page_examples.append(f"{probe_page} | {ctx.company}")
    ctx.entry_report["deadListingPageExamples"] = ctx.dead_listing_page_examples
    return "dead_listing_page"


# mutation — sets errors/warnings on ctx.entry_report
def _record_empty_plugin_result(ctx: StaticSourceContext) -> None:
    classification = clean_text(ctx.entry_report.get("classification"))
    empty_confirmed = bool(ctx.entry_report.get("emptyConfirmed")) or (
        classification == "empty_confirmed"
    )
    browser_recommended = bool(ctx.entry_report.get("browserFallbackRecommended"))
    classification = _probe_empty_plugin_listing(ctx, classification)
    if int(ctx.entry_report.get("deadListingPageCount") or 0) > 0:
        classification = "dead_listing_page"
        ctx.entry_report["classification"] = classification
    if classification == "dead_listing_page":
        ctx.entry_report["status"] = "ok"
        ctx.entry_report["error"] = ""
        return
    if empty_confirmed:
        return
    ctx.entry_report["status"] = "error"
    if not ctx.entry_report.get("error"):
        ctx.entry_report["error"] = "no jobs extracted from source pages"
    if browser_recommended:
        warn_page = clean_text(ctx.pages[0]) if ctx.pages else ""
        ctx.warnings.append(
            f"static:{ctx.source_name}:{warn_page}: {ctx.entry_report.get('error')}"
        )
    else:
        ctx.errors.append(f"static:{ctx.source_name}: {ctx.entry_report.get('error')}")


# mutation — finalizes plugin job results on ctx
def _finalize_plugin_fast_path(
    ctx: StaticSourceContext,
    plugin_jobs: list[dict[str, Any]],
    plugin_meta: dict[str, Any] | None,
) -> None:
    _apply_one_man_studio_cleanup(ctx, plugin_jobs)
    ctx.emit_heartbeat()
    ctx.jobs.extend(plugin_jobs)
    ctx.entry_report["fetchedCount"] = len(ctx.pages)
    ctx.entry_report["keptCount"] = len(plugin_jobs)
    if plugin_meta is None:
        ctx.entry_report["status"] = "ok"
        return
    _apply_static_plugin_meta(ctx, plugin_meta)
    if plugin_jobs:
        ctx.entry_report["status"] = "ok"
        ctx.entry_report["error"] = ""
        return
    _record_empty_plugin_result(ctx)


# orchestration — plugin selection + invoke + finalize
def _run_plugin_fast_path(ctx: StaticSourceContext) -> bool:
    plugin_ctx = _static_plugin_context(ctx)
    if plugin_ctx is None:
        return False
    try:
        plugin, _ = default_registry.select(plugin_ctx)
        plugin_jobs = _invoke_static_plugin(ctx, plugin)
        plugin_meta = ctx.source.get("_staticPluginMeta") if isinstance(ctx.source, dict) else None
        _finalize_plugin_fast_path(
            ctx, plugin_jobs, plugin_meta if isinstance(plugin_meta, dict) else None
        )
        _complete_source_without_generic_flow(ctx)
        return True
    except NoPluginFoundError:
        return False


# pure — error text classification
def _should_try_listing_browser_fallback(
    url: str,
    error_text: str,
    *,
    anti_bot_browser_retry: bool = False,
) -> tuple[bool, str]:
    err_str = str(error_text or "")
    err_lower = err_str.lower()
    linked_in_throttle = "linkedin" in f"{url} {err_str}".lower()
    if "403" in err_str:
        return True, "403"
    if anti_bot_browser_retry and ("429" in err_str or "too many requests" in err_lower):
        return True, "429"
    if linked_in_throttle and "429" in err_str:
        return True, "429"
    if "timeout" in err_lower or "timed out" in err_lower:
        return True, "timeout"
    return False, ""


# pure — reads ctx field
def _source_studio(ctx: StaticSourceContext) -> str:
    return clean_text(ctx.source.get("studio")) or ctx.company or ctx.source_name


# pure — reads ctx field
def _source_label(ctx: StaticSourceContext) -> str:
    return clean_text(ctx.source.get("name")) or ctx.company or ctx.source_name


# mutation — modifies in-place state
def _append_parsed_listing_rows(
    ctx: StaticSourceContext,
    listing_html: str,
    page_url: str,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
) -> tuple[int, int]:
    emitted_count = 0
    provisional_count = 0
    parsed = parse_jobpostings_from_html(
        listing_html,
        base_url=page_url,
        fallback_company=ctx.company,
        fallback_source_id_prefix=f"static:{ctx.source_name}",
    )
    for row in parsed:
        link = normalize_url(row.get("jobLink"))
        if _is_provisional_static_artifact_row(row):
            provisional_count += 1
            if link:
                _append_detail_candidate(
                    detail_links,
                    detail_seen,
                    ctx.seen_links,
                    candidate_url=link,
                    anchor_text=clean_text(row.get("title")),
                    depth=0,
                    parent_url=page_url,
                )
            continue
        if not link or link in ctx.seen_links:
            continue
        ctx.seen_links.add(link)
        row["adapter"] = "static"
        row["studio"] = _source_studio(ctx)
        ctx.jobs.append(row)
        emitted_count += 1
    return emitted_count, provisional_count


# mutation — modifies in-place state
def _fetch_rendered_detail_rows(
    ctx: StaticSourceContext, link: str, title: str, source_budget_s: int
) -> list[Any]:
    detail_result = process_detail_link(
        detail=link,
        detail_title=title,
        source_started=ctx.source_started,
        static_source_time_budget_s=source_budget_s,
        fetch_html_cached=ctx.html_fetcher.fetch_html_cached,
        timeout_s=ctx.run_deps.timeout_s,
        detail_retries=ctx.run_deps.retries,
        company=ctx.company,
        source_name=ctx.source_name,
        source=ctx.source,
        ignored_link_titles=ctx.ignored_link_titles,
        default_path_tokens=ctx.runtime_config.default_path_tokens,
        default_query_keys=ctx.runtime_config.default_query_keys,
    )
    ctx.stats["detail_pages_visited"] += 1
    ctx.emit_source_progress(
        phase_key="static_detail_traversal",
        phase_label="Traversing detail pages",
        target_label=title or link or ctx.source_name,
        target_url=link,
    )
    ctx.stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
    return detail_result.get("rows") or []


# mutation — modifies in-place state
def _append_rendered_detail_rows(ctx: StaticSourceContext, rows: list[Any]) -> int:
    emitted_count = 0
    for emitted_row in rows:
        if not isinstance(emitted_row, dict):
            continue
        emitted_row["source"] = _source_label(ctx)
        emitted_row["studio"] = _source_studio(ctx)
        ctx.jobs.append(emitted_row)
        emitted_count += 1
    return emitted_count


# mutation — modifies in-place state
def _append_rendered_row(
    ctx: StaticSourceContext,
    raw_row: dict[str, Any],
    source_budget_s: int,
    page_url: str,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
) -> tuple[int, bool, int]:
    row = dict(raw_row)
    if clean_text(row.pop("_renderedCardMode", "")) == "fallback":
        return 0, False, 0
    row["adapter"] = "static"
    row["studio"] = _source_studio(ctx)
    row["source"] = _source_label(ctx)
    title = clean_text(row.get("title"))
    link = normalize_url(row.get("jobLink"))
    location_hint = clean_text(row.pop("_locationHint", ""))
    if _is_provisional_static_artifact_row(row):
        if link:
            _append_detail_candidate(
                detail_links,
                detail_seen,
                ctx.seen_links,
                candidate_url=link,
                anchor_text=title,
                depth=0,
                parent_url=page_url,
            )
        return 0, False, 1
    if not link or link in ctx.seen_links:
        return 0, False, 0
    job_like = looks_like_job_title_candidate(title)
    needs_lookup = (not job_like) or _needs_detail_location_resolution(row, link, location_hint)
    if not needs_lookup:
        ctx.seen_links.add(link)
        ctx.jobs.append(row)
        return 1, True, 0
    emitted_rows = _fetch_rendered_detail_rows(ctx, link, title, source_budget_s)
    if emitted_rows:
        ctx.seen_links.add(link)
        return _append_rendered_detail_rows(ctx, emitted_rows), job_like, 0
    if row:
        ctx.jobs.append(row)
        return 1, False, 0
    return 0, False, 0


# mutation — modifies in-place state
def _append_rendered_card_rows(
    ctx: StaticSourceContext,
    listing_html: str,
    page_url: str,
    source_budget_s: int,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
) -> tuple[int, bool, int]:
    rendered_rows = extract_rendered_card_jobs(
        listing_html,
        page_url=page_url,
        company=ctx.company,
        source_id=clean_text(ctx.source.get("id")) or ctx.source_name,
        allow_any_anchor=True,
    )
    emitted_count = 0
    has_job_like_title = False
    provisional_count = 0
    for raw_row in rendered_rows:
        count, job_like, provisional = _append_rendered_row(
            ctx,
            raw_row,
            source_budget_s,
            page_url,
            detail_links,
            detail_seen,
        )
        emitted_count += count
        has_job_like_title = has_job_like_title or (job_like and count > 0)
        provisional_count += provisional
    return emitted_count, has_job_like_title, provisional_count


# mutation — modifies in-place state
def _record_listing_only_rendered_meta(ctx: StaticSourceContext) -> None:
    ctx.source["_staticPluginMeta"] = {
        "detailFetchRequired": False,
        "detailTraversalMode": "listing_only",
    }
    ctx.emit_heartbeat()


# mutation — modifies in-place state
def _add_listing_detail_link(
    ctx: StaticSourceContext,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
    candidate_url: str,
    anchor_text: str,
    enforce_heuristics: bool,
    page_url: str,
) -> None:
    dead_listing_count_before = int(ctx.link_rejections.get("dead_listing_page", 0))
    new_links: list[tuple[str, str]] = []
    add_detail_link(
        new_links,
        detail_seen,
        ctx.seen_links,
        ctx.link_rejections,
        candidate_url=candidate_url,
        anchor_text=anchor_text,
        enforce_heuristics=enforce_heuristics,
        page_url=page_url,
        source=ctx.source,
        default_path_tokens=ctx.runtime_config.default_path_tokens,
        default_query_keys=ctx.runtime_config.default_query_keys,
    )
    for detail_url, detail_title in new_links:
        detail_links.append(
            StaticDetailCandidate(
                url=detail_url,
                title=clean_text(detail_title),
                depth=0,
                parent_url=page_url,
            )
        )
    if (
        int(ctx.link_rejections.get("dead_listing_page", 0)) > dead_listing_count_before
        and len(ctx.dead_listing_page_examples) < 5
    ):
        ctx.dead_listing_page_examples.append(f"{candidate_url} | {ctx.company}")


# mutation — modifies in-place state
def _collect_listing_detail_links(
    ctx: StaticSourceContext,
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
    listing_html: str,
    page_url: str,
) -> None:
    pattern = (
        r'(?is)<(?:div|tr)[^>]*class=["\'][^"\']*job-listing-item[^"\']*["\']'
        r"[^>]*>(.*?)</(?:div|tr)>"
    )
    for row_match in re.finditer(pattern, listing_html):
        row_html = row_match.group(1) or ""
        link_match = re.search(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', row_html)
        if not link_match:
            continue
        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", link_match.group(2) or ""))
        _add_listing_detail_link(
            ctx,
            detail_links,
            detail_seen,
            clean_text(link_match.group(1)),
            anchor_text,
            False,
            page_url,
        )
    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', listing_html):
        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", match.group(2) or ""))
        _add_listing_detail_link(
            ctx,
            detail_links,
            detail_seen,
            clean_text(match.group(1)),
            anchor_text,
            True,
            page_url,
        )
    for raw in find_urls_in_text(listing_html):
        _add_listing_detail_link(
            ctx,
            detail_links,
            detail_seen,
            clean_text(raw),
            "",
            True,
            page_url,
        )


# mutation — modifies in-place state
def _extract_listing_candidates(
    ctx: StaticSourceContext,
    *,
    page_url: str,
    source_budget_s: int,
    listing_htmls: list[str],
) -> tuple[int, list[StaticDetailCandidate], int]:
    detail_links: list[StaticDetailCandidate] = []
    detail_seen: set[str] = set()
    listing_jobs_found = 0
    provisional_rows_found = 0
    for listing_html in listing_htmls:
        ctx.emit_heartbeat()
        parsed_count, parsed_provisional = _append_parsed_listing_rows(
            ctx,
            listing_html,
            page_url,
            detail_links,
            detail_seen,
        )
        listing_jobs_found += parsed_count
        provisional_rows_found += parsed_provisional
        if listing_jobs_found == 0:
            rendered_count, has_job_like_title, rendered_provisional = _append_rendered_card_rows(
                ctx,
                listing_html,
                page_url,
                source_budget_s,
                detail_links,
                detail_seen,
            )
            listing_jobs_found += rendered_count
            provisional_rows_found += rendered_provisional
            if listing_jobs_found > 0 and has_job_like_title and provisional_rows_found == 0:
                _record_listing_only_rendered_meta(ctx)
                continue
        _collect_listing_detail_links(
            ctx,
            detail_links,
            detail_seen,
            listing_html,
            page_url,
        )
    return listing_jobs_found, detail_links, provisional_rows_found


@dataclass(frozen=True)
class StaticDetailTraversalPlan:
    page_url: str
    detail_links: list[StaticDetailCandidate]
    detail_concurrency: int
    detail_retries: int
    source_budget_s: int


@dataclass
class _StaticDetailTraversalState:
    index: int = 0
    stop: bool = False
    stop_source: bool = False
    off_domain_failure_count: int = 0
    redirect_loop_count: int = 0
    scheduled_urls: set[str] = field(default_factory=set)


# mutation — modifies in-place state
def _stop_detail_traversal_adaptively(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
) -> None:
    state.stop = True
    remaining_candidates = max(0, len(plan.detail_links) - int(state.index or 0))
    if remaining_candidates > 0:
        ctx.stats["detail_pages_skipped_by_adaptive_stop"] = (
            int(ctx.stats.get("detail_pages_skipped_by_adaptive_stop") or 0) + remaining_candidates
        )


# mutation — modifies in-place state
def _next_detail_batch_size(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    *,
    remaining_budget_s: float,
) -> int:
    batch_size = max(1, int(plan.detail_concurrency))
    current_gate_wait_ms, current_gate_wait_count = ctx.current_domain_gate_wait_stats()
    ctx.stats["domain_gate_wait_ms"] = int(current_gate_wait_ms)
    ctx.stats["domain_gate_wait_count"] = int(current_gate_wait_count)
    if (
        current_gate_wait_ms > 0
        and int(state.index or 0) > 0
        and current_gate_wait_ms >= int(ctx.stats.get("detail_fetch_ms") or 0)
    ):
        batch_size = 1
    if remaining_budget_s < 8.0:
        batch_size = 1
    batch_budget_cap = max(1, int(max(0.0, remaining_budget_s) // 3.0))
    return max(1, min(batch_size, batch_budget_cap))


# pure — builds fetch job dicts
def _build_detail_batch_jobs(detail_batch: list[StaticDetailCandidate]) -> list[dict[str, Any]]:
    return [
        {
            "url": candidate.url,
            "payload": {
                "detailTitle": candidate.title,
                "detailDepth": candidate.depth,
                "parentUrl": candidate.parent_url,
            },
        }
        for candidate in detail_batch
    ]


# network — makes HTTP requests
def _fetch_detail_job(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    detail_batch_meta: dict[str, dict[str, Any]],
    batch_job: dict[str, Any],
    url: str,
    _timeout_s: int,
) -> str:
    del batch_job, _timeout_s
    fetch_started = time.perf_counter()
    ctx.sync_source_deadline(plan.source_budget_s)
    current_remaining_budget_s = remaining_static_source_budget_s(
        deadline_monotonic=float(ctx.source_deadline)
    )
    effective_timeout_s = _effective_timeout_or_raise(
        timeout_s=ctx.run_deps.timeout_s,
        remaining_budget_s=current_remaining_budget_s,
        source_budget_s=plan.source_budget_s,
    )
    html, cache_hit = ctx.html_fetcher.fetch_html_cached(
        url,
        remaining_budget_s=current_remaining_budget_s,
        retries_override=plan.detail_retries,
    )
    detail_batch_meta[url] = {
        "cacheHit": cache_hit,
        "fetchMs": int((time.perf_counter() - fetch_started) * 1000),
        "timeoutS": effective_timeout_s,
    }
    return html


# mutation — modifies in-place state
def _emit_detail_batch_progress(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    detail_batch_start: int,
    completed: int,
    total: int,
) -> None:
    completed_count = max(0, int(completed or 0))
    total_count = max(1, int(total or 0))
    detail_candidate_count = len(plan.detail_links)
    fetched_count = min(detail_candidate_count, detail_batch_start + completed_count)
    ctx.emit_heartbeat()
    ctx.emit_source_progress(
        phase_key="static_detail_traversal",
        phase_label="Traversing detail pages",
        counts={
            "detailCandidates": detail_candidate_count,
            "detailPagesFetched": fetched_count,
        },
        target_label=f"Detail fetch {fetched_count}/{detail_candidate_count}",
        target_url=plan.page_url,
        wait_reason="detail_batch",
        event_level="muted",
        message=(
            f"Fetched {completed_count}/{total_count} detail page"
            f"{'' if total_count == 1 else 's'} for {ctx.source_name}."
        ),
    )


# mutation — modifies in-place state
def _record_detail_fetch_error(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    *,
    detail: str,
    msg: str,
) -> None:
    linked_in_throttle = "linkedin" in f"{plan.page_url} {msg}".lower()
    detail_host = (urlparse(detail).netloc or "").strip().lower()
    page_host = (urlparse(plan.page_url).netloc or "").strip().lower()
    if "Exceeded maximum allowed redirects" in msg:
        state.redirect_loop_count += 1
    if detail_host and page_host and detail_host != page_host:
        state.off_domain_failure_count += 1
    if "HTTP 403" in msg or (
        linked_in_throttle and ("HTTP 429" in msg or "Too Many Requests" in msg)
    ):
        ctx.entry_report["classification"] = "blocked_or_challenge"
        ctx.entry_report["browserFallbackRecommended"] = True
        ctx.entry_report["error"] = msg
        ctx.warnings.append(f"static:{ctx.source_name}:{detail}: {msg}")
    else:
        ctx.errors.append(f"static:{ctx.source_name}:{detail}: {msg}")
    if state.redirect_loop_count >= 2 or state.off_domain_failure_count >= 2:
        _stop_detail_traversal_adaptively(ctx, plan, state)


# mutation — modifies in-place state
def _record_detail_rejection(ctx: StaticSourceContext, detail_result: dict[str, Any]) -> None:
    rejected_classification = clean_text(detail_result.get("rejectedClassification"))
    if rejected_classification == "dead_listing_page":
        ctx.link_rejections["dead_listing_page"] += 1
        ctx.stats["dead_listing_pages_rejected"] += 1
        if len(ctx.dead_listing_page_examples) < 5:
            example = clean_text(detail_result.get("rejectedExample"))
            if example:
                ctx.dead_listing_page_examples.append(example)
    elif detail_result.get("parseEmpty"):
        ctx.link_rejections["detail_parse_empty"] += 1


def _append_detail_result_rows(ctx: StaticSourceContext, rows: list[dict[str, Any]]) -> int:
    appended = 0
    for row in rows:
        if not isinstance(row, dict) or _is_provisional_static_artifact_row(row):
            continue
        link = normalize_url(row.get("jobLink"))
        if not link or link in ctx.seen_links:
            continue
        ctx.seen_links.add(link)
        ctx.jobs.append(row)
        appended += 1
    return appended


def _enqueue_nested_detail_links(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    *,
    parent_url: str,
    parent_depth: int,
    nested_links: list[dict[str, Any]],
) -> int:
    if parent_depth >= 1:
        return 0
    child_candidates: list[StaticDetailCandidate] = []
    for item in nested_links:
        if not isinstance(item, dict):
            continue
        child_url = normalize_url(item.get("url"))
        if not child_url or child_url in ctx.seen_links or child_url in state.scheduled_urls:
            continue
        child_candidates.append(
            StaticDetailCandidate(
                url=child_url,
                title=clean_text(item.get("title")),
                depth=parent_depth + 1,
                parent_url=parent_url,
            )
        )
    if not child_candidates:
        return 0
    nested_profile = domain_profile_for_url(parent_url)
    profile_external_cap = max(0, int(nested_profile.get("max_external_detail_links") or 0))
    child_candidates = _cap_external_detail_fanout(
        ctx,
        page_url=parent_url,
        detail_links=child_candidates,
        cap=profile_external_cap or _EXTERNAL_DETAIL_FANOUT_LINK_CAP,
    )
    nested_limit = _nested_detail_limit_for(ctx, len(child_candidates))
    profile_max_detail_links = max(0, int(nested_profile.get("max_detail_links") or 0))
    if profile_max_detail_links > 0:
        nested_limit = (
            min(nested_limit, profile_max_detail_links)
            if nested_limit
            else profile_max_detail_links
        )
    if nested_limit and nested_limit < len(child_candidates):
        child_candidates = child_candidates[:nested_limit]
    added = 0
    for candidate in child_candidates:
        if candidate.url in ctx.seen_links or candidate.url in state.scheduled_urls:
            continue
        state.scheduled_urls.add(candidate.url)
        plan.detail_links.append(candidate)
        added += 1
    if added:
        ctx.stats["candidate_links_found"] = (
            int(ctx.stats.get("candidate_links_found") or 0) + added
        )
    return added


# mutation — modifies in-place state
def _process_detail_result_row(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
    detail_batch_meta: dict[str, dict[str, Any]],
    detail_result_row: dict[str, Any],
) -> None:
    detail = clean_text(detail_result_row.get("url"))
    if not detail:
        return
    detail_payload = _as_dict(detail_result_row.get("payload"))
    detail_title = clean_text(detail_payload.get("detailTitle"))
    detail_depth = max(0, int(detail_payload.get("detailDepth") or 0))
    ctx.stats["detail_pages_visited"] += 1
    ctx.emit_source_progress(
        phase_key="static_detail_traversal",
        phase_label="Traversing detail pages",
        target_label=detail_title or detail or ctx.source_name,
        target_url=detail,
        wait_reason="parsing",
    )
    ctx.emit_heartbeat()
    if not bool(detail_result_row.get("ok")):
        _record_detail_fetch_error(
            ctx,
            plan,
            state,
            detail=detail,
            msg=str(detail_result_row.get("error") or ""),
        )
        return
    detail_meta = detail_batch_meta.get(detail) or {}
    detail_result = process_detail_html(
        detail=detail,
        detail_title=detail_title,
        detail_html=str(detail_result_row.get("text") or ""),
        fetch_ms=int(detail_meta.get("fetchMs") or 0),
        cache_hit=bool(detail_meta.get("cacheHit")),
        company=ctx.company,
        source_name=ctx.source_name,
        source=ctx.source,
        ignored_link_titles=ctx.ignored_link_titles,
        default_path_tokens=ctx.runtime_config.default_path_tokens,
        default_query_keys=ctx.runtime_config.default_query_keys,
    )
    ctx.stats["fetch_cache_hits"] += 1 if detail_result.get("cacheHit") else 0
    ctx.stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
    appended_rows = _append_detail_result_rows(ctx, detail_result.get("rows") or [])
    nested_scheduled = 0
    if appended_rows == 0:
        nested_scheduled = _enqueue_nested_detail_links(
            ctx,
            plan,
            state,
            parent_url=detail,
            parent_depth=detail_depth,
            nested_links=detail_result.get("nestedDetailLinks") or [],
        )
    if appended_rows == 0 and nested_scheduled == 0:
        _record_detail_rejection(ctx, detail_result)
    ctx.emit_heartbeat()


# pure — budget check with optional source stop
def _detail_budget_exhausted(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    *,
    reserve_s: float,
) -> bool:
    if not static_source_budget_exhausted(
        deadline_monotonic=float(ctx.source_deadline),
        reserve_s=reserve_s,
    ):
        return False
    ctx.stop_for_budget_exhaustion(
        target_url=plan.page_url,
        source_budget_s=plan.source_budget_s,
    )
    return True


# network — makes HTTP requests
def _run_detail_batch(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
    state: _StaticDetailTraversalState,
) -> None:
    remaining_budget_s = remaining_static_source_budget_s(
        deadline_monotonic=float(ctx.source_deadline)
    )
    detail_batch_size = _next_detail_batch_size(
        ctx,
        plan,
        state,
        remaining_budget_s=remaining_budget_s,
    )
    detail_batch_start = int(state.index or 0)
    detail_batch = plan.detail_links[
        detail_batch_start : detail_batch_start + min(detail_batch_size, len(plan.detail_links))
    ]
    detail_batch_meta: dict[str, dict[str, Any]] = {}
    ctx.stats["detail_batch_count"] = int(ctx.stats.get("detail_batch_count") or 0) + 1
    detail_results = fetch_pages_batched(
        ctx.run_deps.timeout_s,
        _build_detail_batch_jobs(detail_batch),
        sync_fetch=lambda batch_job, url, timeout_s: _fetch_detail_job(
            ctx,
            plan,
            detail_batch_meta,
            batch_job,
            url,
            timeout_s,
        ),
        total_concurrency=plan.detail_concurrency,
        per_host_concurrency=plan.detail_concurrency,
        progress_callback=lambda completed, total: _emit_detail_batch_progress(
            ctx,
            plan,
            detail_batch_start,
            completed,
            total,
        ),
    )
    for detail_result_row in detail_results:
        if _detail_budget_exhausted(ctx, plan, reserve_s=0.0):
            state.stop_source = True
            break
        _process_detail_result_row(ctx, plan, state, detail_batch_meta, detail_result_row)
    if not state.stop_source:
        state.index = detail_batch_start + len(detail_batch)


# mutation — modifies in-place state
def _finish_detail_traversal(
    ctx: StaticSourceContext,
    *,
    detail_fetch_started: float,
    detail_fetch_base_ms: int,
) -> None:
    ctx.stats["detail_fetch_ms"] += max(
        0,
        int((time.perf_counter() - detail_fetch_started) * 1000)
        - max(0, int(ctx.stats.get("detail_fetch_ms") or 0) - detail_fetch_base_ms),
    )
    current_gate_wait_ms, current_gate_wait_count = ctx.current_domain_gate_wait_stats()
    ctx.stats["domain_gate_wait_ms"] = int(current_gate_wait_ms)
    ctx.stats["domain_gate_wait_count"] = int(current_gate_wait_count)


# orchestration — coordinates network + mutation
def _run_static_detail_traversal(
    ctx: StaticSourceContext,
    plan: StaticDetailTraversalPlan,
) -> bool:
    detail_fetch_started = time.perf_counter()
    detail_fetch_base_ms = int(ctx.stats.get("detail_fetch_ms") or 0)
    state = _StaticDetailTraversalState()
    state.scheduled_urls = {candidate.url for candidate in plan.detail_links if candidate.url}
    while int(state.index or 0) < len(plan.detail_links):
        if state.stop or state.stop_source:
            break
        if _detail_budget_exhausted(ctx, plan, reserve_s=1.0):
            state.stop_source = True
            break
        _run_detail_batch(ctx, plan, state)
    _finish_detail_traversal(
        ctx,
        detail_fetch_started=detail_fetch_started,
        detail_fetch_base_ms=detail_fetch_base_ms,
    )
    return state.stop_source


class StaticFetchRunner:
    # pure helper
    def __init__(self, ctx: StaticSourceContext) -> None:
        self.ctx = ctx
        self.config = ctx.runtime_config
        self.deps = ctx.run_deps
        self.entry_report = ctx.entry_report
        self.pages = ctx.pages
        self.progress_state = ctx.progress_state
        self.source_name = ctx.source_name
        self.stats = ctx.stats
        self.cleaned_pages = [clean_text(page) for page in ctx.pages if clean_text(page)]
        self.listing_batch_size = max(
            1,
            min(
                self.config.static_detail_concurrency,
                len(self.cleaned_pages) if self.cleaned_pages else 1,
            ),
        )
        self.stage_state = StaticListingStageState()
        self.stop_source = False
        self.anti_bot_browser_retry = bool(ctx.source.get("antiBotBrowserRetry"))

    # pure helper
    def run(self) -> None:
        for batch_start in range(0, len(self.cleaned_pages), self.listing_batch_size):
            if self.stop_source:
                break
            target_url = clean_text(self.cleaned_pages[batch_start]) or self.source_name
            if self._page_budget_exhausted(
                target_url, self.config.static_source_time_budget_s, 1.0
            ):
                break
            listing_batch_jobs = self._build_listing_batch(batch_start)
            if self.stop_source or not listing_batch_jobs:
                break
            self._run_listing_batch(listing_batch_jobs)
        _finish_generic_source(self.ctx, self.stage_state)

    # pure helper
    def _build_listing_batch(self, batch_start: int) -> list[dict[str, Any]]:
        listing_batch_jobs: list[dict[str, Any]] = []
        page_batch = self.cleaned_pages[batch_start : batch_start + self.listing_batch_size]
        for page_url in page_batch:
            domain_profile = domain_profile_for_url(page_url)
            source_budget_s = int(
                domain_profile.get("static_source_time_budget_s")
                or self.config.static_source_time_budget_s
            )
            self.ctx.sync_source_deadline(source_budget_s)
            if self._page_budget_exhausted(page_url, source_budget_s, 1.0):
                self.stop_source = True
                break
            listing_batch_jobs.append(
                {
                    "url": page_url,
                    "payload": {
                        "domainProfile": domain_profile,
                        "sourceBudgetS": source_budget_s,
                    },
                }
            )
        return listing_batch_jobs

    # pure helper
    def _page_budget_exhausted(self, page_url, source_budget_s, reserve_s) -> bool:
        if not static_source_budget_exhausted(
            deadline_monotonic=float(self.ctx.source_deadline),
            reserve_s=reserve_s,
        ):
            return False
        self.ctx.stop_for_budget_exhaustion(target_url=page_url, source_budget_s=source_budget_s)
        return True

    # network — makes HTTP requests
    def _fetch_listing_html_sync(self, url: str, *, effective_timeout_s: int) -> str:
        try:
            return fetch_with_retries(
                url,
                self.deps.fetch_text,
                timeout_s=effective_timeout_s,
                retries=self.deps.retries,
                backoff_s=self.deps.backoff_s,
            )
        except HttpStatusError as exc:
            if int(exc.code) not in {301, 302, 303, 307, 308}:
                raise
            redirect_url = self.ctx.html_fetcher._safe_redirect_url(url, exc.location)
            return fetch_with_retries(
                redirect_url,
                self.deps.fetch_text,
                timeout_s=effective_timeout_s,
                retries=0,
                backoff_s=self.deps.backoff_s,
            )

    # network — makes HTTP requests
    def _listing_fetch_timeout(self, batch_job: dict[str, Any]) -> int:
        payload = _as_dict(batch_job.get("payload") if isinstance(batch_job, dict) else {})
        source_budget_s = int(
            payload.get("sourceBudgetS") or self.config.static_source_time_budget_s
        )
        self.ctx.sync_source_deadline(source_budget_s)
        return _effective_timeout_or_raise(
            timeout_s=self.deps.timeout_s,
            remaining_budget_s=self.ctx.remaining_budget_s(),
            source_budget_s=source_budget_s,
        )

    # mutation — modifies in-place state
    def _record_fetch_meta(self, url, started, timeout_s, fallback_used, fallback_error) -> None:
        self.stage_state.record_batch_meta(
            url,
            durationMs=int((time.perf_counter() - started) * 1000),
            timeoutS=timeout_s,
            cacheHit=False,
            browserFallbackUsed=fallback_used,
            browserFallbackError=fallback_error,
        )

    # mutation — modifies in-place state
    def _note_listing_fetch_failure(self, err_str: str, attempted: bool, reason: str) -> None:
        if not attempted:
            if reason != "timeout":
                return
            self.stage_state.note_terminal_reason("listing_timeout", self.ctx)
            return
        blocked = "403" in err_str or (
            self.anti_bot_browser_retry
            and ("429" in err_str or "too many requests" in err_str.lower())
        )
        terminal_reason = "blocked_after_browser_fallback" if blocked else "browser_fallback_empty"
        self.stage_state.note_terminal_reason(terminal_reason, self.ctx)

    # mutation — modifies in-place state
    def _log_playwright_fallback(self, url: str, reason: str, html: str) -> None:
        print(
            f"[static] playwright_fallback_used url={url!r} reason={reason} got_html={bool(html)}",
            file=sys.stderr,
            flush=True,
        )

    # pure — display helper
    def _listing_position_label(self) -> str:
        return f"Listing {self.progress_state['listingPagesVisited']}/{max(1, len(self.pages))}"

    # network — makes HTTP requests
    def _fetch_listing_job(self, batch_job: dict[str, Any], url: str, _timeout_s: int) -> str:
        del _timeout_s
        fetch_started = time.perf_counter()
        effective_timeout_s = self._listing_fetch_timeout(batch_job)
        html = ""
        browser_fallback_attempted = False
        browser_fallback_error = ""
        try:
            html = self._fetch_listing_html_sync(url, effective_timeout_s=effective_timeout_s)
        except Exception as exc:  # noqa: BLE001
            html, browser_fallback_attempted, browser_fallback_error = self._sync_browser_fallback(
                url, str(exc)
            )
        self._record_fetch_meta(
            url,
            fetch_started,
            effective_timeout_s,
            browser_fallback_attempted,
            browser_fallback_error,
        )
        return html

    # network — makes HTTP requests
    def _sync_browser_fallback(self, url: str, err_str: str) -> tuple[str, bool, str]:
        html = ""
        fallback_error = ""
        should_fallback, reason = _should_try_listing_browser_fallback(
            url,
            err_str,
            anti_bot_browser_retry=self.anti_bot_browser_retry,
        )
        attempted = bool(self.deps.try_playwright and should_fallback)
        if attempted:
            browser_budget_s = effective_timeout_for_remaining_budget(
                timeout_s=self.deps.timeout_s,
                remaining_budget_s=self.ctx.remaining_budget_s(),
            )
            if browser_budget_s > 0:
                self.stage_state.increment_browser_fallbacks()
                html, fallback_error = self.deps.try_playwright(url, browser_budget_s)
            self._log_playwright_fallback(url, reason, html)
        if not html:
            self._note_listing_fetch_failure(err_str, attempted, reason)
            raise RuntimeError(err_str)
        return html, attempted, fallback_error

    # network — makes HTTP requests
    async def _fetch_listing_job_async(self, client, batch_job, url, _timeout_s):
        del _timeout_s
        fetch_started = time.perf_counter()
        effective_timeout_s = self._listing_fetch_timeout(batch_job)
        html = ""
        browser_fallback_attempted = False
        browser_fallback_error = ""
        try:
            if self.deps.listing_async_fetch is None:
                html = await asyncio.to_thread(
                    self._fetch_listing_html_sync,
                    url,
                    effective_timeout_s=effective_timeout_s,
                )
            else:
                try:
                    html = await self.deps.listing_async_fetch(
                        client,
                        batch_job,
                        url,
                        effective_timeout_s,
                    )
                except HttpStatusError as exc:
                    if int(exc.code) not in {301, 302, 303, 307, 308}:
                        raise
                    redirect_url = self.ctx.html_fetcher._safe_redirect_url(url, exc.location)
                    html = await self.deps.listing_async_fetch(
                        client,
                        batch_job,
                        redirect_url,
                        effective_timeout_s,
                    )
        except Exception as exc:  # noqa: BLE001
            html, browser_fallback_attempted, browser_fallback_error = await asyncio.to_thread(
                self._sync_browser_fallback, url, str(exc)
            )
        self._record_fetch_meta(
            url,
            fetch_started,
            effective_timeout_s,
            browser_fallback_attempted,
            browser_fallback_error,
        )
        return html

    # mutation — modifies in-place state
    def _on_listing_batch_progress(self, completed: int, total: int) -> None:
        completed_count = max(0, int(completed or 0))
        total_count = max(1, int(total or 0))
        self.ctx.emit_heartbeat()
        self.ctx.emit_source_progress(
            phase_key="static_listing_fetch",
            phase_label="Fetching listing pages",
            counts={"listingPagesFetched": completed_count},
            target_label=f"Listing fetch {completed_count}/{total_count}",
            wait_reason="listing_batch",
            event_level="muted",
            message=(
                f"Fetched {completed_count}/{total_count} listing page"
                f"{'' if total_count == 1 else 's'} for {self.source_name}."
            ),
        )

    # network — makes HTTP requests
    def _run_listing_batch(self, listing_batch_jobs: list[dict[str, Any]]) -> None:
        self.stage_state.clear_batch_meta()
        self.ctx.emit_source_progress(
            phase_key="static_listing_fetch",
            phase_label="Fetching listing pages",
            counts={"listingPagesFetched": 0},
            target_label=f"Listing fetch 0/{max(1, len(listing_batch_jobs))}",
            target_url=clean_text((listing_batch_jobs[0] or {}).get("url")),
            wait_reason="listing_batch",
            event_level="muted",
            message=(
                f"Fetching {max(1, len(listing_batch_jobs))} listing page"
                f"{'' if len(listing_batch_jobs) == 1 else 's'} for {self.source_name}."
            ),
        )
        listing_results = fetch_pages_batched(
            self.deps.timeout_s,
            listing_batch_jobs,
            sync_fetch=self._fetch_listing_job,
            async_fetch=self._fetch_listing_job_async
            if self.deps.listing_async_fetch is not None
            else None,
            total_concurrency=max(1, len(listing_batch_jobs)),
            per_host_concurrency=common_config.DEFAULT_STATIC_FETCH_MAX_PER_DOMAIN,
            progress_callback=self._on_listing_batch_progress,
        )
        self.stats["listing_batch_count"] = int(self.stats.get("listing_batch_count") or 0) + 1
        for result in listing_results:
            self.ctx.emit_heartbeat()
            if static_source_budget_exhausted(
                deadline_monotonic=float(self.ctx.source_deadline),
                reserve_s=0.0,
            ):
                self.stop_source = True
                break
            self._process_listing_result(result)

    # pure — extracts context from fetch result
    def _listing_result_context(self, result: dict[str, Any]) -> tuple[str, dict[str, Any], int]:
        page_url = clean_text(result.get("url"))
        payload = _as_dict(result.get("payload"))
        payload_domain_profile = _as_dict(payload.get("domainProfile"))
        domain_profile = payload_domain_profile or domain_profile_for_url(page_url)
        source_budget_s = int(
            payload.get("sourceBudgetS") or self.config.static_source_time_budget_s
        )
        self.ctx.sync_source_deadline(source_budget_s)
        return page_url, domain_profile, source_budget_s

    # orchestration — coordinates network + mutation
    def _process_listing_result(self, result: dict[str, Any]) -> None:
        page_url, domain_profile, source_budget_s = self._listing_result_context(result)
        if not page_url:
            return
        self.progress_state["listingPagesVisited"] += 1
        self.ctx.emit_source_progress(
            phase_key="static_listing_fetch",
            phase_label="Fetching listing pages",
            target_label=self._listing_position_label(),
            target_url=page_url,
            wait_reason="listing_batch",
            event_level="muted",
            message=(
                f"Fetching listing page {self.progress_state['listingPagesVisited']}/"
                f"{max(1, len(self.pages))} for {self.source_name}."
            ),
        )
        try:
            if not bool(result.get("ok")):
                self.ctx.record_static_fetch_failure(
                    target_url=page_url,
                    exc=str(result.get("error") or ""),
                )
                self.ctx.emit_heartbeat()
                return
            if self._page_budget_exhausted(page_url, source_budget_s, 0.0):
                self.stop_source = True
                return
            listing_htmls = self._prepare_listing_htmls(page_url, result)
            detail_links, listing_jobs_found, provisional_rows_found = self._extract_listing_page(
                page_url, source_budget_s, listing_htmls
            )
            detail_links = self._apply_listing_fingerprint(
                detail_links,
                listing_htmls,
                provisional_rows_found=provisional_rows_found,
            )
            if not detail_links:
                self.ctx.emit_heartbeat()
                return
            if self._page_budget_exhausted(page_url, source_budget_s, 1.0):
                self.stop_source = True
                return
            detail_plan = self._detail_plan(
                page_url,
                source_budget_s,
                domain_profile,
                detail_links,
                listing_jobs_found,
                provisional_rows_found,
            )
            if detail_plan is None:
                self.ctx.emit_heartbeat()
                return
            self._emit_detail_traversal_start(page_url, detail_plan.detail_links)
            self.stop_source = _run_static_detail_traversal(self.ctx, detail_plan)
        except Exception as exc:  # noqa: BLE001
            self.ctx.record_static_fetch_failure(target_url=page_url, exc=exc)
            if self.ctx.current_source_kept_count() <= 0 and clean_text(
                self.stage_state.terminal_reason
            ) in {
                "blocked_after_browser_fallback",
                "browser_fallback_empty",
                "listing_timeout",
                "listing_timeout_after_browser_fallback",
            }:
                self.stop_source = True
            self.ctx.emit_heartbeat()

    # mutation — modifies in-place state
    def _try_playwright_fallback(
        self,
        html: str,
        page_url: str,
        timeout_s: int,
        label: str,
        condition: bool,
    ) -> str:
        if not (self.deps.try_playwright and condition and html):
            return html
        fallback_timeout_s = effective_timeout_for_remaining_budget(
            timeout_s=max(1, timeout_s),
            remaining_budget_s=self.ctx.remaining_budget_s(),
        )
        parsed_pre = parse_jobpostings_from_html(
            html,
            base_url=page_url,
            fallback_company=self.ctx.company,
            fallback_source_id_prefix=f"static:{self.source_name}",
        )
        if parsed_pre or fallback_timeout_s <= 0:
            return html
        html2, _ = self.deps.try_playwright(page_url, fallback_timeout_s)
        self._log_playwright_fallback(page_url, label, html2)
        if html2:
            self.stage_state.increment_browser_fallbacks()
            return html2
        return html

    def _prepare_listing_htmls(self, page_url: str, result: dict[str, Any]) -> list[str]:
        listing_meta = self.stage_state.batch_meta.get(page_url) or {}
        self.stats["listing_fetch_ms"] += int(listing_meta.get("durationMs") or 0)
        self.stats["listing_browser_fallbacks"] = int(
            self.stats.get("listing_browser_fallbacks") or 0
        ) + int(bool(listing_meta.get("browserFallbackUsed")))
        if bool(listing_meta.get("cacheHit")):
            self.stats["fetch_cache_hits"] += 1
        effective_timeout_s = int(listing_meta.get("timeoutS") or self.deps.timeout_s)
        html = str(result.get("text") or "")
        html = self._try_playwright_fallback(
            html,
            page_url,
            effective_timeout_s,
            "js_shell",
            bool(html and detect_js_shell(html)),
        )
        listing_path = urlparse(page_url).path or ""
        html = self._try_playwright_fallback(
            html,
            page_url,
            effective_timeout_s,
            "jobs_path",
            bool(html and "/jobs" in listing_path),
        )
        if html:
            html_lower = html.lower()
            for _adapter, _sig in _ATS_SIGNATURE_HINTS:
                if _sig in html_lower:
                    self.ctx.warnings.append(
                        f"static:{self.source_name}:{page_url}: "
                        f"HTML contains {_adapter} signature — "
                        f"consider adapter reclassification"
                    )
                    break
        html = self._try_playwright_fallback(
            html,
            page_url,
            effective_timeout_s,
            "empty_page",
            bool(html and _careers_landing_url(page_url)),
        )
        listing_htmls = [html]
        try:
            dynamic_listing_timeout_s = effective_timeout_for_remaining_budget(
                timeout_s=self.deps.timeout_s,
                remaining_budget_s=self.ctx.remaining_budget_s(),
            )
            if dynamic_listing_timeout_s > 0:
                dynamic_listing_html = maybe_fetch_kojima_job_listing_html(
                    page_url=page_url,
                    page_html=html,
                    timeout_s=dynamic_listing_timeout_s,
                    retries=self.deps.retries,
                    backoff_s=self.deps.backoff_s,
                )
                if dynamic_listing_html and dynamic_listing_html not in listing_htmls:
                    listing_htmls.append(dynamic_listing_html)
        except Exception as exc:  # noqa: BLE001
            self.ctx.errors.append(
                f"static:{self.source_name}:{page_url}: dynamic-listing-fetch failed: {exc}"
            )
        return listing_htmls

    # mutation — modifies in-place state
    def _extract_listing_page(self, page_url, source_budget_s, listing_htmls):
        extraction_started = time.perf_counter()
        self.ctx.emit_source_progress(
            phase_key="static_candidate_extraction",
            phase_label="Extracting candidates",
            target_label=self._listing_position_label(),
            target_url=page_url,
            wait_reason="parsing",
            event_level="muted",
            message=f"Extracting listing candidates for {self.source_name}.",
        )
        listing_jobs_found, detail_links, provisional_rows_found = _extract_listing_candidates(
            self.ctx,
            page_url=page_url,
            source_budget_s=source_budget_s,
            listing_htmls=listing_htmls,
        )
        self.stats["candidate_links_found"] += len(detail_links)
        self.stats["candidate_extraction_ms"] += int(
            (time.perf_counter() - extraction_started) * 1000
        )
        self.ctx.emit_source_progress(
            phase_key="static_candidate_extraction",
            phase_label="Candidates extracted",
            counts={"detailCandidates": len(detail_links), "listingJobsFound": listing_jobs_found},
            target_label=self._listing_position_label(),
            target_url=page_url,
            event_level="muted",
            message=(
                f"Found {len(detail_links)} detail candidate"
                f"{'' if len(detail_links) == 1 else 's'} for {self.source_name}."
            ),
        )
        return detail_links, listing_jobs_found, provisional_rows_found

    # mutation — modifies in-place state
    def _apply_listing_fingerprint(
        self,
        detail_links: list[StaticDetailCandidate],
        listing_htmls: list[str],
        *,
        provisional_rows_found: int,
    ) -> list[StaticDetailCandidate]:
        listing_fingerprint = hashlib.sha1("\n".join(listing_htmls).encode("utf-8")).hexdigest()
        previous_listing_fingerprint = clean_text(
            (self.ctx.state_entry or {}).get("lastListingFingerprint")
        )
        self.entry_report["listingFingerprint"] = listing_fingerprint
        self.entry_report["listingCheckedAt"] = now_iso()
        self.entry_report["listingChanged"] = bool(
            listing_fingerprint != previous_listing_fingerprint
        )
        if (
            previous_listing_fingerprint
            and listing_fingerprint == previous_listing_fingerprint
            and not self.deps.force_refresh_all
            and provisional_rows_found <= 0
        ):
            self.entry_report["cacheDecision"] = "listing_only"
            self.entry_report["cacheDecisionReason"] = "listing_fingerprint_unchanged"
            self.entry_report["detailSkippedByListingFingerprint"] = True
            self.stats["detail_skipped_by_listing_fingerprint"] += 1
            return []
        return detail_links

    # strategy factory — builds StaticDetailTraversalPlan
    def _detail_plan(
        self,
        page_url,
        source_budget_s,
        domain_profile,
        detail_links,
        listing_jobs_found,
        provisional_rows_found,
    ):
        source_key = _source_detail_key(self.ctx)
        probable_detail_links = [
            candidate
            for candidate in detail_links
            if is_probable_job_detail_url(
                candidate.url,
                self.ctx.source,
                default_path_tokens=self.config.default_path_tokens,
                default_query_keys=self.config.default_query_keys,
            )
        ]
        plugin_meta = (
            self.ctx.source.get("_staticPluginMeta") if isinstance(self.ctx.source, dict) else None
        )
        mode = choose_detail_traversal_mode(
            page_url,
            runtime_config=self.config,
            profile=domain_profile,
            plugin_meta=plugin_meta,
            listing_jobs_found=listing_jobs_found,
            discovered_links=len(detail_links),
            probable_detail_candidates=len(probable_detail_links),
            source_key=source_key,
            source_state_rows=self.deps.source_state_rows,
        )
        source_has_listing_rows = self.ctx.current_source_kept_count() > 0
        if (
            detail_links
            and probable_detail_links
            and (source_has_listing_rows or self.config.uncapped_deep_static)
        ):
            detail_links = probable_detail_links
        if not detail_links:
            return None
        profile_external_cap = max(0, int(domain_profile.get("max_external_detail_links") or 0))
        detail_links = _cap_external_detail_fanout(
            self.ctx,
            page_url=page_url,
            detail_links=detail_links,
            cap=profile_external_cap or _EXTERNAL_DETAIL_FANOUT_LINK_CAP,
        )
        detail_limit = source_detail_limit_for(
            source_key,
            source_state_rows=self.deps.source_state_rows,
            discovered_links=len(detail_links),
            listing_jobs_found=listing_jobs_found,
            low_yield_detail_cap=self.config.low_yield_detail_cap,
            very_low_yield_detail_cap=self.config.very_low_yield_detail_cap,
            uncapped_deep_static=self.config.uncapped_deep_static,
        )
        profile_max_detail_links = max(0, int(domain_profile.get("max_detail_links") or 0))
        if profile_max_detail_links > 0:
            detail_limit = (
                min(detail_limit, profile_max_detail_links)
                if detail_limit
                else profile_max_detail_links
            )
        if mode == "listing_only" and provisional_rows_found > 0:
            mode = (
                "capped_detail"
                if detail_limit and detail_limit < len(detail_links)
                else "full_detail"
            )
        self.entry_report["detailTraversalMode"] = mode
        if mode == "listing_only":
            return None
        if detail_limit and detail_limit < len(detail_links):
            detail_links = detail_links[:detail_limit]
        return StaticDetailTraversalPlan(
            page_url=page_url,
            detail_links=detail_links,
            detail_concurrency=source_detail_concurrency_for(
                source_key,
                source_state_rows=self.deps.source_state_rows,
                static_detail_concurrency=self.deps.static_detail_concurrency,
            ),
            detail_retries=source_detail_retries_for(
                source_key,
                source_state_rows=self.deps.source_state_rows,
                base_retries=self.deps.retries,
                listing_jobs_found=listing_jobs_found,
                uncapped_deep_static=self.config.uncapped_deep_static,
            ),
            source_budget_s=source_budget_s,
        )

    # mutation — modifies in-place state
    def _emit_detail_traversal_start(self, page_url, detail_links) -> None:
        self.ctx.emit_source_progress(
            phase_key="static_detail_traversal",
            phase_label="Traversing detail pages",
            counts={"detailCandidates": len(detail_links)},
            target_label=f"{len(detail_links)} detail page(s)",
            target_url=page_url,
            wait_reason="detail_batch",
            event_level="muted",
            message=(
                f"Traversing {len(detail_links)} detail page"
                f"{'' if len(detail_links) == 1 else 's'} for {self.source_name}."
            ),
        )
        self.ctx.emit_heartbeat()


# orchestration — coordinates network + mutation
def process_static_source(ctx: StaticSourceContext) -> None:
    if _handle_skip_and_revalidation(ctx):
        return
    if _run_plugin_fast_path(ctx):
        return
    StaticFetchRunner(ctx).run()
