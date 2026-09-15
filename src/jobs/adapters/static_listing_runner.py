"""Static listing fetch runner.

AI boundary owns: `StaticFetchRunner` — listing batch fetch, browser fallback, HTML preparation,
listing-page extraction, and detail traversal kickoff.
AI boundary implement in: this leaf for the runner; traversal/rows/flow/plugin helpers come from
sibling leaves. Seam: parser/fetch helpers are resolved through the coordinator at call time so
tests can patch them.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
import time
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.plugins.static._heuristics import (
    detect_cookie_challenge_shell,
    detect_js_shell,
)
from src.jobs.adapters.static_cookie_retry import cookie_retry_allowed_for_url
from src.jobs.adapters.static_detail_heuristics import (
    choose_detail_traversal_mode,
    is_probable_job_detail_url,
    source_detail_concurrency_for,
    source_detail_limit_for,
    source_detail_retries_for,
)
from src.jobs.adapters.static_listing_common import (
    _ATS_SIGNATURE_HINTS,
    _EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS,
    _EXTERNAL_DETAIL_FANOUT_LINK_CAP,
    StaticDetailCandidate,
    _cap_external_detail_fanout,
    _careers_landing_url,
    _effective_timeout_or_raise,
    _is_expected_static_listing_fetch_fallback,
)
from src.jobs.adapters.static_listing_flow import _finish_generic_source
from src.jobs.adapters.static_listing_pagination import (
    STATIC_PAGINATION_MAX_FOLLOWED_PAGES,
    card_lane_render_skip_enabled,
    pagination_anchors_for_html,
)
from src.jobs.adapters.static_listing_plugin import _should_try_listing_browser_fallback
from src.jobs.adapters.static_listing_rows import _extract_listing_candidates
from src.jobs.adapters.static_listing_state import StaticListingStageState, _source_detail_key
from src.jobs.adapters.static_listing_traversal import (
    StaticDetailTraversalPlan,
    _run_static_detail_traversal,
)
from src.jobs.adapters.static_runtime_support import (
    _MAX_STATIC_REDIRECT_HOPS,
    effective_timeout_for_remaining_budget,
    static_source_budget_exhausted,
)
from src.jobs.common.config import STATIC_ASSET_URL_FILTER_ENABLED
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.http import HttpStatusError
from src.jobs.page_gating import filter_listable_pages, looks_like_asset_url
from src.jobs.text_utils import clean_text
from src.scrapers.domain_profiles import domain_profile_for_url
from src.shared.http_batch import fetch_pages_batched
from src.shared.json_shapes import as_json_object as _as_dict
from src.shared.utils import now_iso

from ..common import config as common_config
from .static_runtime import StaticSourceContext

# S6 (Big Moxi 2026-09-12): rendered-empty confirmation bounds. A Playwright
# render of a JS-shell listing that surfaced at most this many visible-text
# characters is a provably empty board read (no hidden text surface); the
# stamp feeds the guard's two_rendered_empty_confirmations evidence kind.
_RENDERED_EMPTY_MAX_VISIBLE_CHARS = 240


def _rendered_visible_text_len(html: str) -> int:
    from src.jobs.adapters.html_parsers import strip_html_text

    return len(clean_text(strip_html_text(html or "")))


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
        # Asset-URL filter (2026-09-15): documents must never enter as listing
        # pages. The sms.playstation.com seed row carried five webpack bundle
        # URLs in its pages list — each was fetched every pass, detected as a
        # JS shell, and escalated to the browser fallback (52 dead escalations
        # in the 2026-09-15 full pass). Fail-open: unset the env var to restore
        # fetch-as-registered.
        if STATIC_ASSET_URL_FILTER_ENABLED:
            asset_pages = [page for page in self.cleaned_pages if looks_like_asset_url(page)]
            if asset_pages:
                self.stats["asset_listing_pages_filtered"] = len(asset_pages)
                self.ctx.warnings.append(
                    f"static:{self.source_name}: filtered "
                    f"{len(asset_pages)} static-asset URL(s) from listing pages"
                )
                self.cleaned_pages = filter_listable_pages(self.cleaned_pages)
                self.seen_listing_pages = set(self.cleaned_pages)
        self.anti_bot_browser_retry = bool(ctx.source.get("antiBotBrowserRetry"))
        # hrmos pagination (2026-09-14): listing pages discovered from
        # ?page=N anchors queue behind the registry pages and are followed
        # within the same run (kill-switch + cap live in
        # static_listing_pagination.py).
        self.pending_listing_pages: list[str] = []
        self.seen_listing_pages: set[str] = set(self.cleaned_pages)
        self.pagination_discovered_count = 0

    # pure helper
    def run(self) -> None:
        registry_batch_start = 0
        while not self.stop_source:
            if registry_batch_start < len(self.cleaned_pages):
                batch_start = registry_batch_start
                registry_batch_start += 1
                target_url = clean_text(self.cleaned_pages[batch_start]) or self.source_name
                listing_batch_jobs = self._build_listing_batch(batch_start)
            elif self.pending_listing_pages:
                target_url = self.pending_listing_pages[0]
                listing_batch_jobs = self._build_discovered_listing_batch()
            else:
                break
            if self.stop_source or not listing_batch_jobs:
                break
            if self._page_budget_exhausted(
                target_url, self.config.static_source_time_budget_s, 1.0
            ):
                break
            self._run_listing_batch(listing_batch_jobs)
        _finish_generic_source(self.ctx, self.stage_state)

    # pure helper
    def _build_discovered_listing_batch(self) -> list[dict[str, Any]]:
        page_url = clean_text(self.pending_listing_pages.pop(0))
        if not page_url:
            return []
        domain_profile = domain_profile_for_url(page_url)
        source_budget_s = int(
            domain_profile.get("static_source_time_budget_s")
            or self.config.static_source_time_budget_s
        )
        self.ctx.sync_source_deadline(source_budget_s)
        if self._page_budget_exhausted(page_url, source_budget_s, 1.0):
            self.stop_source = True
            return []
        return [
            {
                "url": page_url,
                "payload": {
                    "domainProfile": domain_profile,
                    "sourceBudgetS": source_budget_s,
                },
            }
        ]

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
        self.ctx.stop_for_budget_exhaustion(target_url=page_url)
        return True

    # network — makes HTTP requests
    def _fetch_listing_html_sync(self, url: str, *, effective_timeout_s: int) -> str:
        current_url = url
        for hop in range(_MAX_STATIC_REDIRECT_HOPS):
            try:
                return fetch_with_retries(
                    current_url,
                    self.deps.fetch_text,
                    timeout_s=effective_timeout_s,
                    retries=self.deps.retries if hop == 0 else 0,
                    backoff_s=self.deps.backoff_s,
                )
            except HttpStatusError as exc:
                if int(exc.code) not in {301, 302, 303, 307, 308}:
                    raise
                if urljoin(current_url, exc.location or "") == current_url and (
                    cookie_retry_allowed_for_url(current_url)
                ):
                    # S4: an allowlisted same-URL bounce is the geo/consent-cookie
                    # shape; the base lane cannot carry cookies, so finish the
                    # chain through the S4 cookie-jar lane (terminal errors keep
                    # the retry-exhausted marker so classification never changes).
                    request = self.ctx.html_fetcher.build_request(url, retries_override=0)
                    if request is not None:
                        retry_text, _visited, _cached = (
                            self.ctx.html_fetcher._cookie_jar_retry_fetch(request)
                        )
                        return retry_text
                    raise
                current_url = self.ctx.html_fetcher._safe_redirect_url(current_url, exc.location)
        raise RuntimeError(f"Static redirect chain exceeded for {url}")

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
        except _EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS as exc:
            if not _is_expected_static_listing_fetch_fallback(exc):
                raise
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
        try_playwright = self.deps.try_playwright
        attempted = bool(try_playwright is not None and should_fallback)
        if attempted:
            browser_budget_s = effective_timeout_for_remaining_budget(
                timeout_s=self.deps.timeout_s,
                remaining_budget_s=self.ctx.remaining_budget_s(),
            )
            if browser_budget_s > 0:
                self.stage_state.increment_browser_fallbacks()
                if try_playwright is not None:
                    html, fallback_error = try_playwright(url, browser_budget_s)
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
                current_url = url
                html = ""
                s4_bounce_continue_used = False
                for _hop in range(_MAX_STATIC_REDIRECT_HOPS):
                    try:
                        html = await self.deps.listing_async_fetch(
                            client,
                            batch_job,
                            current_url,
                            effective_timeout_s,
                        )
                        break
                    except HttpStatusError as exc:
                        if int(exc.code) not in {301, 302, 303, 307, 308}:
                            raise
                        if urljoin(current_url, exc.location or "") == current_url and (
                            cookie_retry_allowed_for_url(current_url)
                        ):
                            # S4: allowlisted same-URL bounce (geo/consent cookie).
                            # The shared batch client stores the bounce's
                            # Set-Cookie, so re-issuing the identical request
                            # carries it — _safe_redirect_url would raise the
                            # loop error on exactly this shape.
                            s4_bounce_continue_used = True
                            continue
                        current_url = self.ctx.html_fetcher._safe_redirect_url(
                            current_url, exc.location
                        )
                else:
                    raise RuntimeError(
                        f"Static redirect chain exceeded for {url}"
                        + (" (cookie-jar retry exhausted)" if s4_bounce_continue_used else "")
                    )
        except _EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS as exc:
            if not _is_expected_static_listing_fetch_fallback(exc):
                raise
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
        # PS 2026-09-15: the detail flow anchors per-detail deadlines at
        # source_started + this budget, so it must receive the effective
        # budget (floor + pagination extension), not the payload's base —
        # with the extension active, a base-budget detail deadline is already
        # in the past and every card-row detail verification raises,
        # aborting the page's card loop after a dozen rows.
        return page_url, domain_profile, self.ctx.effective_source_budget_s()

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
            # hrmos pagination (2026-09-14): queue same-listing ?page=N anchors
            # before the fingerprint skip so a changed board still syncs its
            # full window; the queued pages are consumed by run() after the
            # registry pages and share the source budget.
            self._queue_discovered_pagination_pages(page_url, listing_htmls)

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
        except _EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS as exc:
            if not _is_expected_static_listing_fetch_fallback(exc):
                raise
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
    def _queue_discovered_pagination_pages(self, page_url: str, listing_htmls: list[str]) -> None:
        remaining = STATIC_PAGINATION_MAX_FOLLOWED_PAGES - self.pagination_discovered_count
        discovered_before = self.pagination_discovered_count
        for listing_html in listing_htmls:
            for next_page in pagination_anchors_for_html(
                listing_html, page_url, max_pages=max(0, remaining)
            ):
                if next_page in self.seen_listing_pages:
                    continue
                # Anchors are trusted as pagination, but an asset URL here is
                # a misparse — never fetch it as a document.
                if looks_like_asset_url(next_page):
                    continue
                self.seen_listing_pages.add(next_page)
                self.pending_listing_pages.append(next_page)
                self.pagination_discovered_count += 1
                remaining -= 1
                if remaining <= 0:
                    break
        # Adaptive budget (2026-09-15): every newly discovered continuation
        # page earns bounded extra budget seconds so a multi-page board fits
        # its per-source budget instead of truncating mid-universe.
        if self.pagination_discovered_count > discovered_before:
            self.ctx.extend_source_deadline_for_pagination(
                pages_discovered=self.pagination_discovered_count
            )

    # mutation — modifies in-place state
    def _try_playwright_fallback(
        self,
        html: str,
        page_url: str,
        timeout_s: int,
        label: str,
        condition: bool,
    ) -> str:
        from src.jobs.adapters import static_listing as _sl

        if not (self.deps.try_playwright and condition and html):
            return html
        fallback_timeout_s = effective_timeout_for_remaining_budget(
            timeout_s=max(1, timeout_s),
            remaining_budget_s=self.ctx.remaining_budget_s(),
        )
        parsed_pre = _sl.parse_jobpostings_from_html(
            html,
            base_url=page_url,
            fallback_company=self.ctx.company,
            fallback_source_id_prefix=f"static:{self.source_name}",
        )
        if parsed_pre or fallback_timeout_s <= 0:
            return html
        try:
            card_lane_skip = card_lane_render_skip_enabled() and self._card_lane_parses_static_html(
                html, page_url
            )
        except Exception:  # the oracle must never break a listing pass
            card_lane_skip = False
        if card_lane_skip:
            # Board-variant oracle (PS Nuxt flip, 2026-09-15): a page can be a
            # JS-app shell for the generic parser while the rendered-card lane
            # extracts its rows from the same static document — escalating it
            # to Playwright gambles the whole page on a degraded render. The
            # static HTML is already harvestable by the lane that runs next;
            # keep it. Fail-open by design: any oracle error falls back to the
            # legacy render path.
            return html
        html2, _ = self.deps.try_playwright(page_url, fallback_timeout_s)
        self._log_playwright_fallback(page_url, label, html2)
        if html2:
            self.stage_state.increment_browser_fallbacks()
            # S6 (Big Moxi 2026-09-12): a render that provably mounted the app
            # (JS shell) without a challenge interstitial and surfaced near-
            # zero visible text is a rendered-empty confirmation. Two such
            # reads across passes become guard emptiness evidence (the guard
            # then promotes ok/0 with two_rendered_empty_confirmations).
            # Fail-closed: challenge interstitials are JS shells too — they
            # must never stamp; and a textful render means the board is not
            # provably empty (extraction trouble, not emptiness).
            if (
                detect_js_shell(html2)
                and not detect_cookie_challenge_shell(html2)
                and _rendered_visible_text_len(html2) <= _RENDERED_EMPTY_MAX_VISIBLE_CHARS
            ):
                self.stats["renderedEmptyConfirmedAt"] = now_iso()
            return html2
        return html

    # pure — HTML parsing only, no network
    def _card_lane_parses_static_html(self, html: str, page_url: str) -> bool:
        """True when the rendered-card lane extracts job rows from the static
        document — the same extractor the card-row flow runs next, with the
        same config (allow_any_anchor, real company/source id), so a skip
        here can never starve the downstream lane of rows it would have
        found. Pure parsing; no network. Any failure fails open (render-first
        escalation preserved).
        """
        try:
            from src.jobs.adapters.plugins.static._rendered_cards import (
                extract_rendered_card_jobs,
            )

            return bool(
                extract_rendered_card_jobs(
                    html,
                    page_url=page_url,
                    company=self.ctx.company,
                    source_id=clean_text(self.ctx.source.get("id")) or self.ctx.source_name,
                    allow_any_anchor=True,
                )
            )
        except Exception:  # malformed HTML must never break a listing pass
            return False

    def _prepare_listing_htmls(self, page_url: str, result: dict[str, Any]) -> list[str]:
        from src.jobs.adapters import static_listing as _sl

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
                dynamic_listing_html = _sl.maybe_fetch_kojima_job_listing_html(
                    page_url=page_url,
                    page_html=html,
                    timeout_s=dynamic_listing_timeout_s,
                    retries=self.deps.retries,
                    backoff_s=self.deps.backoff_s,
                )
                if dynamic_listing_html and dynamic_listing_html not in listing_htmls:
                    listing_htmls.append(dynamic_listing_html)
        except _EXPECTED_STATIC_LISTING_FETCH_FALLBACK_EXCEPTIONS as exc:
            if not _is_expected_static_listing_fetch_fallback(exc):
                raise
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
            # Provisional inline verifications (2026-09-14) performed detail
            # fetches on this page — "detail skipped by fingerprint" would lie.
            and not int(self.ctx.stats.get("provisional_inline_verified") or 0)
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
