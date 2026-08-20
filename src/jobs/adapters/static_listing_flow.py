"""Static listing generic flow: source finish and skip/revalidation.

AI boundary owns: the non-plugin completion path (`_finish_generic_source`) and the
skip/revalidation gate that `process_static_source` consults first.
AI boundary implement in: this leaf for generic flow; state helpers come from
``static_listing_state.py`` and plugin fast path from ``static_listing_plugin.py``.
"""

from __future__ import annotations

from src.jobs.adapters.static_listing_state import (
    StaticListingStageState,
    _complete_source_without_generic_flow,
)
from src.jobs.adapters.static_runtime_support import (
    update_source_detail_taxonomy,
)
from src.jobs.state_source_state import should_skip_static_source_for_structured_migration
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url

from .static_runtime import StaticSourceContext


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
