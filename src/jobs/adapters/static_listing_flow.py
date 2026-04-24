from __future__ import annotations

import re
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
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_detail_heuristics import (
    _is_one_man_studio_noise_city,
    add_detail_link,
    process_detail_link,
)
from src.jobs.adapters.static_runtime_support import (
    update_source_detail_taxonomy,
)
from src.jobs.page_gating import classify_job_page, looks_like_job_title_candidate
from src.jobs.state import should_skip_static_source_for_structured_migration
from src.jobs.text_utils import clean_text, normalize_url, sanitize_location_text
from src.jobs.transport import conditional_revalidate_url
from src.shared.regex import find_urls_in_text

from .static_runtime import StaticSourceContext

root: Any | None = None


@dataclass
class StaticListingStageState:
    browser_fallbacks: int = 0
    terminal_reason: str = ""
    batch_meta: dict[str, dict[str, Any]] = field(default_factory=dict)
    lock: Lock = field(default_factory=Lock)

    def note_terminal_reason(self, reason: str, ctx: StaticSourceContext) -> None:
        clean_reason = clean_text(reason)
        if not clean_reason:
            return
        with self.lock:
            if not clean_text(self.terminal_reason):
                self.terminal_reason = clean_reason
                ctx.stats["listing_terminal_reason"] = clean_reason

    def record_batch_meta(self, url: str, **payload: Any) -> None:
        with self.lock:
            self.batch_meta[url] = {
                **(self.batch_meta.get(url) or {}),
                **payload,
            }

    def clear_batch_meta(self) -> None:
        with self.lock:
            self.batch_meta.clear()

    def increment_browser_fallbacks(self) -> None:
        with self.lock:
            self.browser_fallbacks = int(self.browser_fallbacks or 0) + 1


def _root_module():
    if root is not None:
        return root
    from src.jobs.adapters import static as static_root

    return static_root


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


def _complete_source_without_generic_flow(ctx: StaticSourceContext) -> None:
    update_source_detail_taxonomy(ctx.entry_report)
    ctx.details.append(ctx.entry_report)


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
            ctx.entry_report["classification"] = "blocked_or_challenge"
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


def _run_plugin_fast_path(ctx: StaticSourceContext) -> bool:
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
    plugin_ctx = AdapterPluginContext(
        family="static",
        adapter_key="static",
        source_identity=plugin_identity,
    )
    try:
        plugin, _ = default_registry.select(plugin_ctx)
        plugin_jobs = list(
            plugin.run(
                fetch_text=ctx.run_deps.fetch_text,
                timeout_s=ctx.run_deps.timeout_s,
                retries=ctx.run_deps.retries,
                backoff_s=ctx.run_deps.backoff_s,
                heartbeat_callback=ctx.run_deps.heartbeat_callback,
                pages=ctx.pages,
                source_row=ctx.source,
                parse_jobpostings_from_html=parse_jobpostings_from_html,
                maybe_fetch_kojima_job_listing_html=maybe_fetch_kojima_job_listing_html,
                try_playwright=ctx.run_deps.try_playwright,
            )
        )
        _apply_one_man_studio_cleanup(ctx, plugin_jobs)
        ctx.emit_heartbeat()
        ctx.jobs.extend(plugin_jobs)
        ctx.entry_report["fetchedCount"] = len(ctx.pages)
        ctx.entry_report["keptCount"] = len(plugin_jobs)
        plugin_meta = ctx.source.get("_staticPluginMeta") if isinstance(ctx.source, dict) else None
        if isinstance(plugin_meta, dict):
            ctx.entry_report["classification"] = clean_text(plugin_meta.get("classification"))
            ctx.entry_report["browserFallbackRecommended"] = bool(
                plugin_meta.get("browserFallbackRecommended")
            )
            ctx.entry_report["extractorHint"] = clean_text(plugin_meta.get("extractorHint"))
            if plugin_meta.get("emptyConfirmed"):
                ctx.entry_report["emptyConfirmed"] = True
            ats_links = plugin_meta.get("atsLinks")
            if isinstance(ats_links, list):
                ctx.entry_report["atsLinks"] = [clean_text(v) for v in ats_links if clean_text(v)][
                    :5
                ]
            dead_listing_count = int(plugin_meta.get("deadListingPageCount") or 0)
            if dead_listing_count > 0:
                ctx.entry_report["deadListingPageCount"] = dead_listing_count
            dead_listing_examples = plugin_meta.get("deadListingPageExamples")
            if isinstance(dead_listing_examples, list) and dead_listing_examples:
                cleaned_examples = [clean_text(v) for v in dead_listing_examples if clean_text(v)][
                    :5
                ]
                ctx.dead_listing_page_examples[:] = cleaned_examples
                ctx.entry_report["deadListingPageExamples"] = cleaned_examples
            meta_error = clean_text(plugin_meta.get("error"))
            if meta_error and not ctx.entry_report.get("error"):
                ctx.entry_report["error"] = meta_error

            if not plugin_jobs:
                classification = clean_text(ctx.entry_report.get("classification"))
                empty_confirmed = bool(ctx.entry_report.get("emptyConfirmed")) or (
                    classification == "empty_confirmed"
                )
                browser_recommended = bool(ctx.entry_report.get("browserFallbackRecommended"))
                if classification not in {"dead_listing_page", "empty_confirmed"} and ctx.pages:
                    probe_page = clean_text(ctx.pages[0])
                    if probe_page:
                        try:
                            probe_html, _ = ctx.html_fetcher.fetch_html_cached(
                                probe_page,
                                remaining_budget_s=float(ctx.run_deps.timeout_s or 1),
                            )
                        except Exception:  # noqa: BLE001
                            probe_html = ""
                        if probe_html:
                            job_like, gate_reason = classify_job_page(
                                probe_html,
                                probe_page,
                                profile=ctx.source if isinstance(ctx.source, dict) else None,
                            )
                            if not job_like and gate_reason == "dead_listing_page":
                                classification = "dead_listing_page"
                                ctx.entry_report["classification"] = classification
                                ctx.entry_report["browserFallbackRecommended"] = False
                                ctx.entry_report["browserEscalationEligible"] = False
                                ctx.entry_report.pop("browserEscalationEligibilityReason", None)
                                ctx.entry_report["deadListingPageCount"] = max(
                                    1, int(ctx.entry_report.get("deadListingPageCount") or 0)
                                )
                                if len(ctx.dead_listing_page_examples) < 5:
                                    ctx.dead_listing_page_examples.append(
                                        f"{probe_page} | {ctx.company}"
                                    )
                                ctx.entry_report["deadListingPageExamples"] = (
                                    ctx.dead_listing_page_examples
                                )
                                empty_confirmed = True
                if int(ctx.entry_report.get("deadListingPageCount") or 0) > 0:
                    classification = "dead_listing_page"
                    ctx.entry_report["classification"] = classification
                if classification == "dead_listing_page":
                    ctx.entry_report["status"] = "ok"
                    ctx.entry_report["error"] = ""
                elif not empty_confirmed:
                    ctx.entry_report["status"] = "error"
                    if not ctx.entry_report.get("error"):
                        ctx.entry_report["error"] = "no jobs extracted from source pages"
                    if browser_recommended:
                        warn_page = clean_text(ctx.pages[0]) if ctx.pages else ""
                        ctx.warnings.append(
                            f"static:{ctx.source_name}:{warn_page}: {ctx.entry_report.get('error')}"
                        )
                    else:
                        ctx.errors.append(
                            f"static:{ctx.source_name}: {ctx.entry_report.get('error')}"
                        )
            else:
                ctx.entry_report["status"] = "ok"
                ctx.entry_report["error"] = ""
        else:
            ctx.entry_report["status"] = "ok"
        _complete_source_without_generic_flow(ctx)
        return True
    except NoPluginFoundError:
        return False


def _should_try_listing_browser_fallback(url: str, error_text: str) -> tuple[bool, str]:
    err_str = str(error_text or "")
    err_lower = err_str.lower()
    linked_in_throttle = "linkedin" in f"{url} {err_str}".lower()
    if "403" in err_str:
        return True, "403"
    if linked_in_throttle and "429" in err_str:
        return True, "429"
    if "timeout" in err_lower or "timed out" in err_lower:
        return True, "timeout"
    return False, ""


def _extract_listing_candidates(
    ctx: StaticSourceContext,
    *,
    page_url: str,
    source_budget_s: int,
    listing_htmls: list[str],
) -> tuple[int, list[tuple[str, str]]]:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    listing_jobs_found = 0
    for listing_html in listing_htmls:
        ctx.emit_heartbeat()
        parsed = parse_jobpostings_from_html(
            listing_html,
            base_url=page_url,
            fallback_company=ctx.company,
            fallback_source_id_prefix=f"static:{ctx.source_name}",
        )
        for row in parsed:
            link = normalize_url(row.get("jobLink"))
            if not link or link in ctx.seen_links:
                continue
            ctx.seen_links.add(link)
            row["adapter"] = "static"
            row["studio"] = clean_text(ctx.source.get("studio")) or ctx.company or ctx.source_name
            ctx.jobs.append(row)
            listing_jobs_found += 1

        if listing_jobs_found == 0:
            rendered_rows = _root_module().extract_rendered_card_jobs(
                listing_html,
                page_url=page_url,
                company=ctx.company,
                source_id=clean_text(ctx.source.get("id")) or ctx.source_name,
                allow_any_anchor=True,
            )
            if rendered_rows:
                has_job_like_rendered_title = False
                for row in rendered_rows:
                    row = dict(row)
                    mode = clean_text(row.pop("_renderedCardMode", ""))
                    if mode == "fallback":
                        continue
                    row["adapter"] = "static"
                    row["studio"] = (
                        clean_text(ctx.source.get("studio")) or ctx.company or ctx.source_name
                    )
                    row["source"] = (
                        clean_text(ctx.source.get("name")) or ctx.company or ctx.source_name
                    )
                    title = clean_text(row.get("title"))
                    link = normalize_url(row.get("jobLink"))
                    location_hint = clean_text(row.pop("_locationHint", ""))
                    needs_detail_lookup = _needs_detail_location_resolution(
                        row,
                        link,
                        location_hint,
                    )
                    if looks_like_job_title_candidate(title):
                        if not link or link in ctx.seen_links:
                            continue
                        if needs_detail_lookup:
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
                            )
                            ctx.stats["detail_pages_visited"] += 1
                            ctx.emit_source_progress(
                                phase_key="static_detail_traversal",
                                phase_label="Traversing detail pages",
                                target_label=title or link or ctx.source_name,
                                target_url=link,
                            )
                            ctx.stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
                            emitted_detail_rows = detail_result.get("rows") or []
                            if emitted_detail_rows:
                                ctx.seen_links.add(link)
                                for emitted_row in emitted_detail_rows:
                                    if not isinstance(emitted_row, dict):
                                        continue
                                    emitted_row["source"] = (
                                        clean_text(ctx.source.get("name"))
                                        or ctx.company
                                        or ctx.source_name
                                    )
                                    emitted_row["studio"] = (
                                        clean_text(ctx.source.get("studio"))
                                        or ctx.company
                                        or ctx.source_name
                                    )
                                    ctx.jobs.append(emitted_row)
                                    listing_jobs_found += 1
                                has_job_like_rendered_title = True
                                continue
                        ctx.seen_links.add(link)
                        ctx.jobs.append(row)
                        listing_jobs_found += 1
                        has_job_like_rendered_title = True
                        continue
                    if not link or link in ctx.seen_links:
                        continue
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
                    )
                    ctx.stats["detail_pages_visited"] += 1
                    ctx.emit_source_progress(
                        phase_key="static_detail_traversal",
                        phase_label="Traversing detail pages",
                        target_label=title or link or ctx.source_name,
                        target_url=link,
                    )
                    ctx.stats["detail_fetch_ms"] += int(detail_result.get("fetchMs") or 0)
                    emitted_detail_rows = detail_result.get("rows") or []
                    if emitted_detail_rows:
                        ctx.seen_links.add(link)
                        for emitted_row in emitted_detail_rows:
                            if not isinstance(emitted_row, dict):
                                continue
                            emitted_row["source"] = (
                                clean_text(ctx.source.get("name")) or ctx.company or ctx.source_name
                            )
                            emitted_row["studio"] = (
                                clean_text(ctx.source.get("studio"))
                                or ctx.company
                                or ctx.source_name
                            )
                            ctx.jobs.append(emitted_row)
                            listing_jobs_found += 1
                    elif row:
                        ctx.jobs.append(row)
                        listing_jobs_found += 1
                if listing_jobs_found > 0 and has_job_like_rendered_title:
                    ctx.source["_staticPluginMeta"] = {
                        "detailFetchRequired": False,
                        "detailTraversalMode": "listing_only",
                    }
                    ctx.emit_heartbeat()
                    continue

        for row_match in re.finditer(
            r'(?is)<(?:div|tr)[^>]*class=["\'][^"\']*job-listing-item[^"\']*["\'][^>]*>(.*?)</(?:div|tr)>',
            listing_html,
        ):
            row_html = row_match.group(1) or ""
            link_match = re.search(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', row_html)
            if not link_match:
                continue
            href = clean_text(link_match.group(1))
            anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", link_match.group(2) or ""))
            add_detail_link(
                detail_links,
                detail_seen,
                ctx.seen_links,
                ctx.link_rejections,
                candidate_url=href,
                anchor_text=anchor_text,
                enforce_heuristics=False,
                page_url=page_url,
                source=ctx.source,
                default_path_tokens=ctx.runtime_config.default_path_tokens,
                default_query_keys=ctx.runtime_config.default_query_keys,
            )

        for match in re.finditer(
            r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', listing_html
        ):
            href = clean_text(match.group(1))
            anchor_inner = match.group(2) or ""
            anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", anchor_inner))
            add_detail_link(
                detail_links,
                detail_seen,
                ctx.seen_links,
                ctx.link_rejections,
                candidate_url=href,
                anchor_text=anchor_text,
                enforce_heuristics=True,
                page_url=page_url,
                source=ctx.source,
                default_path_tokens=ctx.runtime_config.default_path_tokens,
                default_query_keys=ctx.runtime_config.default_query_keys,
            )
        for raw in find_urls_in_text(listing_html):
            add_detail_link(
                detail_links,
                detail_seen,
                ctx.seen_links,
                ctx.link_rejections,
                candidate_url=clean_text(raw),
                anchor_text="",
                enforce_heuristics=True,
                page_url=page_url,
                source=ctx.source,
                default_path_tokens=ctx.runtime_config.default_path_tokens,
                default_query_keys=ctx.runtime_config.default_query_keys,
            )
    return listing_jobs_found, detail_links
