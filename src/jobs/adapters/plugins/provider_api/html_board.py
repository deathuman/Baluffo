"""HTML board factory and runner for provider APIs (breezy, jazzhr, ashby).

AI boundary owns: HTML-board provider runner factory and board-specific extraction handoff.
AI boundary implement in: this file for generic HTML-board provider runner behavior; parser rules stay in parser leaves.
AI boundary search before contracts: provider HTML parser, lifecycle helpers, and provider API tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused HTML-board provider tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.adapters.plugins.types import AdapterPluginContext, SimpleAdapterPlugin
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text

from .lifecycle import (
    apply_provider_cache_decision,
    build_provider_entry_report,
    provider_revalidate_not_modified,
    skip_provider_for_cache,
)
from .source_errors import (
    EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS,
    reraise_unexpected_provider_api_source_exception,
)

ParseHtml = Callable[[str, str, str], list[RawJob]]
BuildUrl = Callable[[dict[str, object]], str]
TryPlaywright = Callable[[str, int], tuple[str, str]]


def _normalize_ashby_board_url(url: str) -> str:
    text = clean_text(url)
    if not text:
        return ""
    parsed = urlparse(text)
    path = parsed.path.rstrip("/")
    if path.lower().endswith("/jobs"):
        normalized = parsed._replace(path=path[:-5] or "/", query="", fragment="")
        return normalized.geturl().rstrip("/")
    return text


def _iter_ashby_candidate_urls(source: dict[str, object]) -> list[str]:
    candidates = [
        clean_text(source.get("board_url")),
        _normalize_ashby_board_url(clean_text(source.get("board_url"))),
        clean_text(source.get("careersUrl")),
        clean_text(source.get("sourceDirectoryEntryUrl")),
    ]
    rows: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        if not item or item in seen:
            continue
        seen.add(item)
        rows.append(item)
    return rows


def _should_try_browser_retry(error_text: str) -> bool:
    text = clean_text(error_text).lower()
    if not text:
        return False
    return (
        "403" in text
        or "429" in text
        or "too many requests" in text
        or "timeout" in text
        or "timed out" in text
    )


def _fetch_board_html(
    *,
    source: dict[str, object],
    url: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    try_playwright: TryPlaywright | None,
) -> tuple[str, bool, str]:
    try:
        return (
            fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s),
            False,
            "",
        )
    except EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS as exc:
        reraise_unexpected_provider_api_source_exception(exc)
        error_text = str(exc)
        if (
            not bool(source.get("antiBotBrowserRetry"))
            or try_playwright is None
            or not _should_try_browser_retry(error_text)
        ):
            raise
        html, browser_error = try_playwright(url, max(3, min(int(timeout_s or 1), 25)))
        if html:
            return html, True, clean_text(browser_error)
        msg = clean_text(browser_error) or "browser retry returned no html"
        raise RuntimeError(f"{error_text}; browser retry exhausted for {url}: {msg}") from exc


def _build_entry_report(
    *,
    adapter_name: str,
    studio: str,
    source_name: str,
    source_id: str,
    board_url: str,
) -> dict[str, object]:
    return build_provider_entry_report(
        adapter_name=adapter_name,
        studio=studio,
        source_name=source_name,
        extra={
            "sourceId": source_id,
            "pages": [board_url] if board_url else [],
            "browserFallbackRecommended": False,
        },
    )


def _mark_empty_parse(
    entry_report: dict[str, object],
    *,
    adapter_name: str,
    last_text: str,
    browser_retry_used: bool,
    anti_bot_browser_retry: bool,
) -> None:
    entry_report["status"] = "error"
    if adapter_name == "ashby" and "page not found" in clean_text(last_text).lower():
        entry_report["classification"] = "dead_listing_page"
    elif browser_retry_used and anti_bot_browser_retry:
        entry_report["classification"] = "anti_bot_or_challenge"
        entry_report["browserFallbackRecommended"] = True
    entry_report["error"] = (
        f"browser retry exhausted: no jobs extracted from {adapter_name} board html"
        if browser_retry_used
        else f"no jobs extracted from {adapter_name} board html"
    )


def _mark_antibot_retry_error(
    entry_report: dict[str, object],
    source: dict[str, object],
    error_text: str,
) -> None:
    if bool(source.get("antiBotBrowserRetry")) and _should_try_browser_retry(error_text):
        entry_report["classification"] = "anti_bot_or_challenge"
        entry_report["browserFallbackRecommended"] = True


def _fetch_and_parse_board_source(
    *,
    source: dict[str, object],
    adapter_name: str,
    board_url: str,
    studio: str,
    parse_html: ParseHtml,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    try_playwright: TryPlaywright | None,
) -> tuple[list[RawJob], str, bool, str]:
    candidate_urls = _iter_ashby_candidate_urls(source) if adapter_name == "ashby" else [board_url]
    parsed: list[RawJob] = []
    last_text = ""
    browser_retry_used = False
    browser_retry_error = ""
    for candidate_url in candidate_urls:
        last_text, used_browser, browser_error = _fetch_board_html(
            source=source,
            url=candidate_url,
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            try_playwright=try_playwright,
        )
        browser_retry_used = browser_retry_used or used_browser
        if browser_error:
            browser_retry_error = browser_error
        parsed = parse_html(last_text, candidate_url, studio)
        if parsed:
            break
    return parsed, last_text, browser_retry_used, browser_retry_error


def _skip_html_board_before_fetch(
    *,
    entry_report: dict[str, object],
    board_url: str,
    default_error: str,
    timeout_s: int,
    source_name: str,
    source_state_rows: dict[str, dict[str, object]] | None,
) -> bool:
    if not board_url:
        entry_report["status"] = "error"
        entry_report["error"] = default_error
        return True
    if skip_provider_for_cache(entry_report):
        return True
    return provider_revalidate_not_modified(
        entry_report=entry_report,
        url=board_url,
        timeout_s=timeout_s,
        source_name=source_name,
        source_state_rows=source_state_rows,
    )


def _record_html_board_parse_result(
    *,
    entry_report: dict[str, object],
    parsed: list[RawJob],
    adapter_name: str,
    studio: str,
    last_text: str,
    browser_retry_used: bool,
    browser_retry_error: str,
    anti_bot_browser_retry: bool,
) -> None:
    if browser_retry_used:
        entry_report["browserRetryUsed"] = True
        if browser_retry_error:
            entry_report["browserRetryError"] = browser_retry_error
    entry_report["fetchedCount"] = len(parsed)
    entry_report["keptCount"] = len(parsed)
    if not parsed:
        _mark_empty_parse(
            entry_report,
            adapter_name=adapter_name,
            last_text=last_text,
            browser_retry_used=browser_retry_used,
            anti_bot_browser_retry=anti_bot_browser_retry,
        )
    for row in parsed:
        row["adapter"] = adapter_name
        row["studio"] = studio


def _run_html_board_sources(
    *,
    adapter_name: str,
    registry_adapter: str,
    default_error: str,
    parse_html: ParseHtml,
    build_url: BuildUrl,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None = None,
    force_refresh_all: bool = False,
    try_playwright: TryPlaywright | None = None,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    provider_url = ""
    for source in registry_entries(registry_adapter):
        source_name = clean_text(source.get("name")) or f"{registry_adapter}_source"
        studio = clean_text(source.get("studio")) or source_name
        board_url = build_url(source)
        entry_report = _build_entry_report(
            adapter_name=adapter_name,
            studio=studio,
            source_name=source_name,
            source_id=clean_text(source.get("id")),
            board_url=board_url,
        )
        apply_provider_cache_decision(
            entry_report=entry_report,
            source_name=source_name,
            adapter_name=adapter_name,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        if _skip_html_board_before_fetch(
            entry_report=entry_report,
            board_url=board_url,
            default_error=default_error,
            timeout_s=timeout_s,
            source_name=source_name,
            source_state_rows=source_state_rows,
        ):
            details.append(entry_report)
            continue
        try:
            parsed, last_text, browser_retry_used, browser_retry_error = (
                _fetch_and_parse_board_source(
                    source=source,
                    adapter_name=adapter_name,
                    board_url=board_url,
                    studio=studio,
                    parse_html=parse_html,
                    fetch_text=fetch_text,
                    timeout_s=timeout_s,
                    retries=retries,
                    backoff_s=backoff_s,
                    try_playwright=try_playwright,
                )
            )
            _record_html_board_parse_result(
                entry_report=entry_report,
                parsed=parsed,
                adapter_name=adapter_name,
                studio=studio,
                last_text=last_text,
                browser_retry_used=browser_retry_used,
                browser_retry_error=browser_retry_error,
                anti_bot_browser_retry=bool(source.get("antiBotBrowserRetry")),
            )
            jobs.extend(parsed)
        except EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS as exc:
            reraise_unexpected_provider_api_source_exception(exc)
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            _mark_antibot_retry_error(entry_report, source, str(exc))
            if not provider_url:
                provider_url = board_url
            errors.append(f"{registry_adapter}:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        f"{registry_adapter}_sources",
        adapter=adapter_name,
        studio="multiple",
        provider_url=provider_url,
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _html_board_plugin(adapter_name: str) -> SimpleAdapterPlugin:
    registry_adapter = adapter_name
    parse_html: ParseHtml
    build_url: BuildUrl
    if adapter_name == "breezy":
        default_error = "missing board_url"
        parse_html = _provider_parsers.parse_breezy_jobs_html

        def build_url(source: dict[str, object]) -> str:
            return clean_text(source.get("board_url"))

    elif adapter_name == "ashby":
        default_error = "missing board_url"
        parse_html = _provider_parsers.parse_ashby_jobs_from_html

        def build_url(source: dict[str, object]) -> str:
            return _normalize_ashby_board_url(clean_text(source.get("board_url")))

    else:
        default_error = "missing board_url"
        parse_html = _provider_parsers.parse_jazzhr_jobs_html

        def build_url(source: dict[str, object]) -> str:
            return clean_text(source.get("board_url"))

    adapter_key = f"{adapter_name}_sources"

    def can_handle(ctx: AdapterPluginContext) -> bool:
        return ctx.family == "provider_api" and ctx.adapter_key == adapter_key

    def run_plugin(**kwargs: Any) -> list[RawJob]:
        return _run_html_board_sources(
            adapter_name=adapter_name,
            registry_adapter=registry_adapter,
            default_error=default_error,
            parse_html=parse_html,
            build_url=build_url,
            **kwargs,
        )

    return SimpleAdapterPlugin(
        name=adapter_key,
        family="provider_api",
        priority=55,
        can_handle_fn=can_handle,
        run_fn=run_plugin,
    )
