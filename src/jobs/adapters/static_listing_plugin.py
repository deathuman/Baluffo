"""Static listing plugin fast path and static-artifact repair.

AI boundary owns: plugin context/registry invocation, empty-listing probing, static artifact
detail repair, plugin meta application, and the fast-path decision used by `process_static_source`.
AI boundary implement in: this leaf for plugin handling; state helpers come from
``static_listing_state.py`` and source labels from ``static_listing_rows.py``. Seam: detail/parser
helpers are resolved through the coordinator at call time so tests can patch them.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.errors import NoPluginFoundError
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.static_listing_common import (
    _EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS,
    _PLUGIN_STATIC_ARTIFACT_NESTED_DETAIL_LIMIT,
    _is_expected_static_listing_fetch_fallback,
)
from src.jobs.adapters.static_listing_rows import _source_label, _source_studio
from src.jobs.adapters.static_listing_state import (
    _apply_one_man_studio_cleanup,
    _complete_source_without_generic_flow,
    _is_provisional_static_artifact_row,
)
from src.jobs.page_gating import (
    classify_job_page,
    looks_like_static_parser_noise_title,
)
from src.jobs.text_utils import clean_text, normalize_url

from .static_runtime import StaticSourceContext


def _static_plugin_context(ctx: StaticSourceContext) -> AdapterPluginContext | None:
    host = ""
    if ctx.pages:
        try:
            parsed = urlparse(clean_text(ctx.pages[0]) or "")
            host = (parsed.netloc or "").strip().lower()
        except ValueError:
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
    from src.jobs.adapters import static_listing as _sl

    return list(
        plugin.run(
            fetch_text=ctx.run_deps.fetch_text,
            timeout_s=ctx.run_deps.timeout_s,
            retries=ctx.run_deps.retries,
            backoff_s=ctx.run_deps.backoff_s,
            heartbeat_callback=ctx.run_deps.heartbeat_callback,
            pages=ctx.pages,
            source_row=ctx.source,
            parse_jobpostings_from_html=_sl.parse_jobpostings_from_html,
            fetch_html_cached=ctx.html_fetcher.fetch_html_cached,
            maybe_fetch_kojima_job_listing_html=_sl.maybe_fetch_kojima_job_listing_html,
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
    except _EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS as exc:
        if not _is_expected_static_listing_fetch_fallback(exc):
            raise
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


def _plugin_static_artifact_detail_result(
    ctx: StaticSourceContext, *, detail: str, title: str, source_budget_s: int
) -> dict[str, Any]:
    from src.jobs.adapters import static_listing as _sl

    try:
        result = _sl.process_detail_link(
            detail=detail,
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
    except _EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS as exc:
        if not _is_expected_static_listing_fetch_fallback(exc):
            raise
        ctx.warnings.append(f"static:{ctx.source_name}:{detail}: artifact repair failed: {exc}")
        return {}
    ctx.stats["detail_pages_visited"] += 1
    ctx.stats["detail_fetch_ms"] += int(result.get("fetchMs") or 0)
    return result


def _append_repaired_plugin_rows(
    ctx: StaticSourceContext,
    target: list[dict[str, Any]],
    rows: list[Any],
    seen_links: set[str],
) -> int:
    appended = 0
    for raw_row in rows:
        if not isinstance(raw_row, dict) or _is_provisional_static_artifact_row(raw_row):
            continue
        if looks_like_static_parser_noise_title(clean_text(raw_row.get("title"))):
            continue
        row = dict(raw_row)
        link = normalize_url(row.get("jobLink"))
        if link:
            if link in seen_links:
                continue
            seen_links.add(link)
        row["adapter"] = "static"
        row["source"] = _source_label(ctx)
        row["studio"] = _source_studio(ctx)
        target.append(row)
        appended += 1
    return appended


def _repair_plugin_static_artifact_rows(
    ctx: StaticSourceContext, plugin_jobs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if not plugin_jobs:
        return plugin_jobs
    source_budget_s = int(
        getattr(ctx.runtime_config, "static_source_time_budget_s", 0)
        or max(5, int(ctx.run_deps.timeout_s or 5) * 2)
    )
    repaired: list[dict[str, Any]] = []
    seen_repaired_links: set[str] = set()
    for raw_row in plugin_jobs:
        if not isinstance(raw_row, dict):
            continue
        row = dict(raw_row)
        if not _is_provisional_static_artifact_row(row):
            _append_repaired_plugin_rows(ctx, repaired, [row], seen_repaired_links)
            continue
        link = normalize_url(row.get("jobLink"))
        title = clean_text(row.get("title"))
        if not link:
            continue
        detail_result = _plugin_static_artifact_detail_result(
            ctx, detail=link, title=title, source_budget_s=source_budget_s
        )
        if _append_repaired_plugin_rows(
            ctx, repaired, detail_result.get("rows") or [], seen_repaired_links
        ):
            continue
        for nested in list(detail_result.get("nestedDetailLinks") or [])[
            :_PLUGIN_STATIC_ARTIFACT_NESTED_DETAIL_LIMIT
        ]:
            nested_link = normalize_url((nested or {}).get("url"))
            if not nested_link or nested_link == link:
                continue
            nested_result = _plugin_static_artifact_detail_result(
                ctx,
                detail=nested_link,
                title=clean_text((nested or {}).get("title")),
                source_budget_s=source_budget_s,
            )
            _append_repaired_plugin_rows(
                ctx, repaired, nested_result.get("rows") or [], seen_repaired_links
            )
    return repaired


# mutation — finalizes plugin job results on ctx
def _finalize_plugin_fast_path(
    ctx: StaticSourceContext,
    plugin_jobs: list[dict[str, Any]],
    plugin_meta: dict[str, Any] | None,
) -> None:
    original_plugin_count = len(plugin_jobs)
    plugin_jobs = _repair_plugin_static_artifact_rows(ctx, plugin_jobs)
    _apply_one_man_studio_cleanup(ctx, plugin_jobs)
    ctx.emit_heartbeat()
    ctx.jobs.extend(plugin_jobs)
    ctx.entry_report["fetchedCount"] = len(ctx.pages)
    ctx.entry_report["keptCount"] = len(plugin_jobs)
    rejected_static_artifacts = max(0, original_plugin_count - len(plugin_jobs))
    if rejected_static_artifacts:
        ctx.entry_report["staticArtifactRowsRejected"] = rejected_static_artifacts
    if plugin_meta is None:
        ctx.entry_report["status"] = "ok"
        return
    _apply_static_plugin_meta(ctx, plugin_meta)
    if plugin_jobs:
        ctx.entry_report["status"] = "ok"
        ctx.entry_report["error"] = ""
        return
    if original_plugin_count and rejected_static_artifacts:
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
