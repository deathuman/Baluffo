"""Static listing generic flow: source finish and skip/revalidation.

AI boundary owns: the non-plugin completion path (`_finish_generic_source`) and the
skip/revalidation gate that `process_static_source` consults first.
AI boundary implement in: this leaf for generic flow; state helpers come from
``static_listing_state.py`` and plugin fast path from ``static_listing_plugin.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.jobs.adapters.static_listing_plugin import static_board_renders_cards
from src.jobs.adapters.static_listing_state import (
    StaticListingStageState,
    _complete_source_without_generic_flow,
)
from src.jobs.adapters.static_runtime_support import (
    update_source_detail_taxonomy,
)
from src.jobs.adapters.static_zero_kept_guard import promote_clean_zero_kept
from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.page_gating import looks_like_asset_url
from src.jobs.state_source_records import normalized_http_page_validators
from src.jobs.state_source_state import should_skip_static_source_for_structured_migration
from src.jobs.text_utils import clean_text
from src.jobs.transport import (
    apply_conditional_revalidate_result,
    apply_full_read_response_headers,
    conditional_revalidate_url,
)

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
    dead_listing_rejections = int(ctx.link_rejections.get("dead_listing_page", 0))
    gate_entry_classification = clean_text(ctx.entry_report.get("classification"))
    if (
        ctx.entry_report["keptCount"] == 0
        and ctx.pages
        and (not gate_entry_classification or dead_listing_rejections > 0)
    ):
        # A live-200 read that proves its own emptiness (explicit no-openings
        # marker or a prior clean zero read) is an observed-empty board, not an
        # error — promotes to ok/0 so the availability drain can retire the
        # source's rows as observed-empty instead of overdue-forever.
        # S7 (Konami 2026-09-12): a nav-only listing with dead-listing
        # rejections also gets the guard chance. The guard's own refusals
        # (dead-listing without the stale-detail demotion shape, challenge
        # classifications, terminal reasons, browser fallbacks attempted)
        # already encode every case the old pre-filter excluded, so deferring
        # to it keeps the gate fail-closed while letting the trusted-empty +
        # stale-link-404 shape through.
        if promote_clean_zero_kept(ctx):
            pass
        elif gate_entry_classification or dead_listing_rejections > 0:
            # S7 decline on a widened-gate shape: the pre-S7 gate excluded
            # these from the error path entirely, so preserve that outcome
            # byte-for-byte (the taxonomy recompute + re-stamp below still
            # applies its dead-listing classification when evidence demands).
            pass
        else:
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
    promoted_to_empty_confirmed = bool(ctx.entry_report.get("emptyConfirmed"))
    update_source_detail_taxonomy(ctx.entry_report)
    if (
        ctx.entry_report["keptCount"] == 0
        and int(ctx.entry_report.get("deadListingPageCount") or 0) > 0
        and not promoted_to_empty_confirmed
    ):
        # S7: the guard's promoted empty-confirmed stamp must survive the
        # post-taxonomy re-stamp — a nav-only board it promoted on marker
        # evidence is observed-empty, not dead-listing evidence.
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


# A page that publishes no validator cannot answer 304, so asking it is pure latency.
# A read that sees one tagless starts the clock; failing that, the first sweep does.
# Either way the question is re-asked at most weekly, which is how a board that starts
# publishing one is noticed without every tagless multi-page board paying a HEAD per
# page on every run (a four-page board measured ~1.2 s per HEAD, so the ungated
# version taxed exactly the runs it was meant to save).
_PAGE_VALIDATOR_SEED_INTERVAL_S = 7 * 24 * 60 * 60


def _listable_listing_pages(ctx: StaticSourceContext) -> list[str]:
    """The configured listing pages a read would actually fetch."""
    pages = [clean_text(page) for page in ctx.pages if clean_text(page)]
    return [page for page in pages if not looks_like_asset_url(page)]


def _page_validator_seed_due(ctx: StaticSourceContext) -> bool:
    seeded_at = parse_datetime((ctx.state_entry or {}).get("lastHttpPageSeedAt"))
    if not seeded_at:
        return True
    return (datetime.now(UTC) - seeded_at).total_seconds() >= _PAGE_VALIDATOR_SEED_INTERVAL_S


def _probe_listing_pages(
    ctx: StaticSourceContext,
) -> tuple[bool, dict[str, dict[str, str]], dict[str, object] | None, bool]:
    """Ask the configured listing pages whether they still hold the stored copy.

    Returns ``(all_unchanged, validators, first_probe, seeded)``. ``all_unchanged`` is
    true only when every page was asked and every page answered 304. ``validators`` is
    what the board publishes now, merged over what state held: a 2xx without a tag
    retires that page's entry, while an errored page says nothing and keeps the stored
    one. The sweep stops at the first page that answers 200 while holding a validator,
    because the read is certain from then on. ``first_probe`` is page 1's own answer,
    which the singular pair mirrors -- never
    another page's, or the pair would seed page 1 with a tag that describes a different
    document. ``seeded`` reports that a page without a validator was asked, so the
    handler can start the weekly clock.
    """
    stored = (ctx.state_entry or {}).get("lastHttpPageValidators")
    merged: dict[str, dict[str, str]] = {
        clean_text(url): dict(tags)
        for url, tags in (stored.items() if isinstance(stored, dict) else [])
        if clean_text(url) and isinstance(tags, dict)
    }
    listing_pages = _listable_listing_pages(ctx)
    if listing_pages and listing_pages[0] not in merged:
        # A single-page board keeps its pair in the singular fields; a board that
        # only now gained a second page may have entered the paged path with it.
        legacy_etag = clean_text((ctx.state_entry or {}).get("lastHttpEtag"))
        legacy_last_modified = clean_text((ctx.state_entry or {}).get("lastHttpLastModified"))
        if legacy_etag or legacy_last_modified:
            merged[listing_pages[0]] = {
                "etag": legacy_etag,
                "lastModified": legacy_last_modified,
            }
    # A single-page board keeps the long-standing always-probe behaviour (one HEAD
    # per window is also how it discovers a tag it never had); the seed gate only
    # stops a multi-page sweep from repeating that latency on every page.
    seed_due = len(listing_pages) == 1 or _page_validator_seed_due(ctx)
    first_probe: dict[str, object] | None = None
    probed_count = 0
    unchanged_count = 0
    seeded = False
    for page_url in listing_pages:
        tags = merged.get(page_url) or {}
        has_validator = bool(clean_text(tags.get("etag")) or clean_text(tags.get("lastModified")))
        if not has_validator and not seed_due:
            continue
        if not has_validator:
            seeded = True
        probe = conditional_revalidate_url(
            page_url,
            ctx.run_deps.timeout_s,
            etag=clean_text(tags.get("etag")),
            last_modified=clean_text(tags.get("lastModified")),
            probe=True,
        )
        if page_url == listing_pages[0]:
            first_probe = probe
        if not bool(probe.get("supported")):
            # No usable answer: the full read is the only evidence there is.
            return False, merged, first_probe, seeded
        probed_count += 1
        status_code = int(probe.get("statusCode") or 0)
        if status_code == 304:
            unchanged_count += 1
            continue
        if not 200 <= status_code < 300:
            continue
        etag = clean_text(probe.get("etag"))
        last_modified = clean_text(probe.get("lastModified"))
        if etag or last_modified:
            merged[page_url] = {"etag": etag, "lastModified": last_modified}
        else:
            merged.pop(page_url, None)
        if has_validator:
            # This page changed while holding a validator, so the full read is now
            # certain and the pages behind it cannot change that. Their stored tags
            # say nothing about them being stale (a 200 here is not their answer),
            # so they are kept and the next sweep resumes at this page. A page with
            # no validator is a seed, not a change -- seeding continues past it, or
            # a multi-page board would need one window per page to become skippable.
            break
    all_unchanged = probed_count > 0 and probed_count == len(listing_pages) == unchanged_count
    return all_unchanged, merged, first_probe, seeded


def record_listing_read_validators(
    ctx: StaticSourceContext, observed: dict[str, dict[str, str]]
) -> None:
    """Store the validators a full read's own listing responses carried.

    A full read answers the same question a conditional probe asks -- which copy of
    the page the board serves now -- so the tag it returns is worth keeping. Seeding
    it here means a board can revalidate the first time its listing window expires,
    instead of spending that window's one check learning a tag the read already saw.

    ``observed`` maps each configured listing page that answered this run to the
    capture sink its response filled (``status``/``etag``/``last-modified``). Pages
    that did not answer -- a failed fetch, a browser fallback, a truncated run -- are
    absent, and absence says nothing: their stored tag is kept. A page that answered
    without a tag retires its entry, exactly like the 200 probe it matches.
    """
    if not observed:
        return
    listing_pages = _listable_listing_pages(ctx)
    captured = {
        page: observed[page] for page in listing_pages if isinstance(observed.get(page), dict)
    }
    if not captured:
        return
    first_capture = captured.get(listing_pages[0]) if listing_pages else None
    if first_capture is not None:
        # The response that produced the rows is also the one that earns
        # revalidation; a tag it omitted is one the board stopped publishing.
        apply_full_read_response_headers(ctx.entry_report, first_capture)
    if len(listing_pages) <= 1:
        ctx.entry_report["httpPageValidators"] = {}
        return
    # The probe that sent this run into the read may already have learned a page's
    # new tag (the one that changed), so its map is the starting point when present;
    # a plain run_now has none and starts from stored state.
    merged = normalized_http_page_validators(
        ctx.entry_report.get("httpPageValidators")
        if isinstance(ctx.entry_report.get("httpPageValidators"), dict)
        else (ctx.state_entry or {}).get("lastHttpPageValidators")
    )
    if listing_pages[0] not in captured and listing_pages[0] not in merged:
        # The same legacy bridge the probe uses: a board that was single-page when
        # its pair was stored keeps that pair as page 1's validator.
        legacy_etag = clean_text((ctx.state_entry or {}).get("lastHttpEtag"))
        legacy_last_modified = clean_text((ctx.state_entry or {}).get("lastHttpLastModified"))
        if legacy_etag or legacy_last_modified:
            merged[listing_pages[0]] = {
                "etag": legacy_etag,
                "lastModified": legacy_last_modified,
            }
    tagless_observed = False
    for page, sink in captured.items():
        etag = clean_text(sink.get("etag"))
        last_modified = clean_text(sink.get("last-modified"))
        if etag or last_modified:
            merged[page] = {"etag": etag, "lastModified": last_modified}
        else:
            merged.pop(page, None)
            tagless_observed = True
    if tagless_observed:
        # The read just proved the page publishes no validator, so the weekly
        # discovery sweep has nothing to learn from a HEAD on the next window.
        ctx.entry_report["httpPageSeedAt"] = datetime.now(UTC).isoformat()
    ctx.entry_report["httpPageValidators"] = merged


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
    if (
        ctx.entry_report["cacheDecision"] == "revalidate_only"
        and ctx.pages
        and not static_board_renders_cards(ctx)
    ):
        # Validators come from the board's own read responses when it has them, and
        # from the first HEAD when it does not; either way a 304 skips fetch, parse,
        # extraction and fallbacks for an unchanged board -- one that kept jobs past
        # its listing window, or one that kept none past the empty window. Every
        # configured page is asked on its own validator,
        # because page 1's 304 cannot vouch for the pages behind it; only all of them
        # unchanged completes the source. The per-page map is the report's answer for
        # what the board publishes now, and an empty one retires the field (a
        # single-page board keeps that answer in the singular pair).
        listing_pages = _listable_listing_pages(ctx)
        not_modified, page_validators, first_probe, seeded = _probe_listing_pages(ctx)
        paged = len(listing_pages) > 1
        ctx.entry_report["httpPageValidators"] = page_validators if paged else {}
        if seeded and paged:
            ctx.entry_report["httpPageSeedAt"] = datetime.now(UTC).isoformat()
        if first_probe is not None:
            apply_conditional_revalidate_result(ctx.entry_report, first_probe)
        if not_modified:
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
