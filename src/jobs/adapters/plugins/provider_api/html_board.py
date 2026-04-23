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
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    provider_url = ""
    for source in registry_entries(registry_adapter):
        source_name = clean_text(source.get("name")) or f"{registry_adapter}_source"
        studio = clean_text(source.get("studio")) or source_name
        board_url = build_url(source)
        entry_report = {
            "adapter": adapter_name,
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
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
            state_entry = (
                (source_state_rows or {}).get(source_name)
                if isinstance(source_state_rows, dict)
                else {}
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
            if bool(revalidate.get("notModified")):
                entry_report["status"] = "excluded"
                entry_report["error"] = "not_modified_304"
                entry_report["exclusionReason"] = "cache_not_modified_304"
                entry_report["cacheDecisionReason"] = "not_modified_304"
                details.append(entry_report)
                continue
        try:
            candidate_urls = (
                _iter_ashby_candidate_urls(source) if adapter_name == "ashby" else [board_url]
            )
            parsed = []
            last_text = ""
            for candidate_url in candidate_urls:
                last_text = fetch_with_retries(
                    candidate_url, fetch_text, timeout_s, retries, backoff_s
                )
                parsed = parse_html(last_text, candidate_url, studio)
                if parsed:
                    break
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            if not parsed:
                entry_report["status"] = "error"
                if adapter_name == "ashby" and "page not found" in clean_text(last_text).lower():
                    entry_report["classification"] = "dead_listing_page"
                entry_report["error"] = f"no jobs extracted from {adapter_name} board html"
            for row in parsed:
                row["adapter"] = adapter_name
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
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
