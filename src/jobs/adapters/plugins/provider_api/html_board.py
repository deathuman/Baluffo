"""HTML board factory and runner for provider APIs (breezy, jazzhr, ashby)."""

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
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url

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
    except Exception as exc:  # noqa: BLE001
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
    return {
        "adapter": adapter_name,
        "studio": studio,
        "name": source_name,
        "sourceId": source_id,
        "pages": [board_url] if board_url else [],
        "status": "ok",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "",
        "browserFallbackRecommended": False,
    }


def _set_entry_cache_decision(
    entry_report: dict[str, object],
    source_name: str,
    adapter_name: str,
    source_state_rows: dict[str, dict[str, object]] | None,
    force_refresh_all: bool,
) -> None:
    cache_decision = get_incremental_cache_decision(
        source_name,
        source_state_rows or {},
        adapter=adapter_name,
        force_refresh_all=force_refresh_all,
    )
    entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
    entry_report["cacheDecisionReason"] = (
        clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
    )


def _apply_revalidate_only(
    *,
    entry_report: dict[str, object],
    board_url: str,
    timeout_s: int,
    source_name: str,
    source_state_rows: dict[str, dict[str, object]] | None,
) -> bool:
    state_entry = (
        (source_state_rows or {}).get(source_name) if isinstance(source_state_rows, dict) else {}
    )
    revalidate = conditional_revalidate_url(
        board_url,
        timeout_s,
        etag=clean_text((state_entry or {}).get("lastHttpEtag")),
        last_modified=clean_text((state_entry or {}).get("lastHttpLastModified")),
    )
    entry_report["httpStatus"] = int(revalidate.get("statusCode") or 0)
    if clean_text(revalidate.get("etag")):
        entry_report["httpEtag"] = clean_text(revalidate.get("etag"))
    if clean_text(revalidate.get("lastModified")):
        entry_report["httpLastModified"] = clean_text(revalidate.get("lastModified"))
    if not bool(revalidate.get("notModified")):
        return False
    entry_report["status"] = "excluded"
    entry_report["error"] = "not_modified_304"
    entry_report["exclusionReason"] = "cache_not_modified_304"
    entry_report["cacheDecisionReason"] = "not_modified_304"
    return True


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
        _set_entry_cache_decision(
            entry_report,
            source_name,
            adapter_name,
            source_state_rows,
            force_refresh_all,
        )
        if not board_url:
            entry_report["status"] = "error"
            entry_report["error"] = default_error
            details.append(entry_report)
            continue
        if entry_report["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
            entry_report["status"] = "excluded"
            entry_report["error"] = entry_report["cacheDecisionReason"]
            entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecisionReason']}"
            details.append(entry_report)
            continue
        if entry_report["cacheDecision"] == "revalidate_only":
            if _apply_revalidate_only(
                entry_report=entry_report,
                board_url=board_url,
                timeout_s=timeout_s,
                source_name=source_name,
                source_state_rows=source_state_rows,
            ):
                details.append(entry_report)
                continue
        try:
            candidate_urls = (
                _iter_ashby_candidate_urls(source) if adapter_name == "ashby" else [board_url]
            )
            parsed = []
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
                    anti_bot_browser_retry=bool(source.get("antiBotBrowserRetry")),
                )
            for row in parsed:
                row["adapter"] = adapter_name
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
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
