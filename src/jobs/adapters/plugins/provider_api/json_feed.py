"""JSON feed factory and runner for provider APIs (lever, workable, smartrecruiters, recruitee, pinpoint).

AI boundary owns: JSON-feed provider runner factory, request handling, and parser handoff.
AI boundary implement in: this file for JSON-feed runner behavior; payload parsing stays in parser leaves.
AI boundary search before contracts: JSON payload parsers, lifecycle helpers, and provider API tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused JSON-feed provider tests.
"""

from __future__ import annotations

import json
import time
import urllib.parse
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from src.ashby_board_urls import ashby_source_api_url
from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.adapters.plugins.types import AdapterPluginContext, SimpleAdapterPlugin
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url

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

ParsePayload = Callable[[dict[str, object], Any, str], list[RawJob]]
BuildUrl = Callable[[dict[str, object]], str]
PayloadCount = Callable[[Any, list[RawJob]], int]
ProviderParser = Callable[[Any, str, str], list[RawJob]]
FetchPayload = Callable[[str], Any]

JSON_FEED_SOURCE_FETCH_CONCURRENCY = 6


@dataclass(frozen=True)
class JsonFeedSpec:
    default_error: str
    source_key: str
    url_template: str
    parser: ProviderParser
    payload_key: str = ""
    dotted_url_template: str = ""
    list_payload: bool = False
    # Offset pagination, for the feeds that cap a response. SmartRecruiters answers with
    # `totalFound` postings and returns at most 100 of them however the request is phrased,
    # so a single GET sees a third of Ubisoft's board and the rest never arrive: measured
    # `totalFound=332` against 100 rows returned, with 175 of the catalogue's missing
    # Ubisoft roles in the pages that were never requested.
    #
    # Pages are merged back into one payload before parsing rather than parsed separately,
    # so a parser sees exactly the shape it always did.
    page_offset_param: str = ""
    page_size_param: str = ""
    page_total_key: str = ""
    page_size: int = 100
    max_pages: int = 1


def _with_query(url: str, **params: object) -> str:
    """Append query parameters, respecting a URL that already carries some."""
    added = "&" if "?" in url else "?"
    return (
        url
        + added
        + urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
    )


def _json_feed_total(payload: Any, spec: JsonFeedSpec) -> int:
    if not spec.page_total_key or not isinstance(payload, dict):
        return 0
    try:
        return int(payload.get(spec.page_total_key) or 0)
    except (TypeError, ValueError):
        return 0


def _merge_json_feed_page(payload: Any, page: Any, spec: JsonFeedSpec) -> Any:
    """Fold one page into the payload, so the parser sees a single listing."""
    if spec.list_payload and isinstance(payload, list) and isinstance(page, list):
        return [*payload, *page]
    if not spec.payload_key or not isinstance(payload, dict) or not isinstance(page, dict):
        return payload
    merged = dict(payload)
    merged[spec.payload_key] = [
        *(payload.get(spec.payload_key) or []),
        *(page.get(spec.payload_key) or []),
    ]
    return merged


def _fetch_json_feed_payload(
    endpoint: str,
    spec: JsonFeedSpec,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> Any:
    """Fetch the listing, following offset pages when the feed caps a response.

    The first request is the bare endpoint so the conditional-revalidate path, which is keyed
    on that exact URL, keeps working unchanged.
    """
    payload = json.loads(fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s))
    if not spec.page_offset_param or spec.max_pages <= 1:
        return payload
    total = _json_feed_total(payload, spec)
    have = len(payload.get(spec.payload_key) or []) if isinstance(payload, dict) else 0
    if not total or have >= total:
        return payload
    page_size = max(1, spec.page_size)
    for page_number in range(2, spec.max_pages + 1):
        offset = (page_number - 1) * page_size
        if offset >= total:
            break
        url = _with_query(
            endpoint,
            **{
                spec.page_size_param: page_size,
                spec.page_offset_param: offset,
            },
        )
        try:
            page = json.loads(fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s))
        except (ValueError, OSError):
            # A later page failing must not discard the pages already fetched.
            break
        merged = _merge_json_feed_page(payload, page, spec)
        if merged is payload or len(merged.get(spec.payload_key) or []) == have:
            break
        payload, have = merged, len(merged.get(spec.payload_key) or [])
    return payload


def _build_json_feed_url(source: dict[str, object], spec: JsonFeedSpec) -> str:
    if spec.source_key == "board_url":
        # Ashby: the slug lives in the board URL's path, so the posting-API URL is
        # derived rather than templated. A row that already carries its API URL
        # keeps it.
        return ashby_source_api_url(source)
    api_url = clean_text(source.get("api_url"))
    if api_url:
        return api_url
    source_value = clean_text(source.get(spec.source_key))
    if not source_value:
        return ""
    if spec.dotted_url_template and "." in source_value:
        return spec.dotted_url_template.format(value=source_value)
    return spec.url_template.format(value=source_value)


def _parse_json_feed_payload(
    source: dict[str, object], payload: Any, studio: str, spec: JsonFeedSpec
) -> list[RawJob]:
    return spec.parser(payload, clean_text(source.get(spec.source_key)), studio)


def _json_feed_payload_count(payload: Any, parsed: list[RawJob], spec: JsonFeedSpec) -> int:
    if spec.list_payload and isinstance(payload, list):
        return len(payload)
    if spec.payload_key and isinstance(payload, dict):
        return len(payload.get(spec.payload_key, []))
    return len(parsed)


JSON_FEED_SPECS: dict[str, JsonFeedSpec] = {
    "smartrecruiters": JsonFeedSpec(
        default_error="missing company_id/api_url",
        source_key="company_id",
        url_template="https://api.smartrecruiters.com/v1/companies/{value}/postings",
        parser=_provider_parsers.parse_smartrecruiters_jobs_payload,
        payload_key="content",
        page_offset_param="offset",
        page_size_param="limit",
        page_total_key="totalFound",
        page_size=100,
        max_pages=20,
    ),
    "workable": JsonFeedSpec(
        default_error="missing account/api_url",
        source_key="account",
        url_template="https://apply.workable.com/api/v1/widget/accounts/{value}?details=true",
        parser=_provider_parsers.parse_workable_jobs_payload,
        payload_key="jobs",
    ),
    "recruitee": JsonFeedSpec(
        default_error="missing subdomain/api_url",
        source_key="subdomain",
        url_template="https://{value}.recruitee.com/api/offers/",
        dotted_url_template="https://{value}/api/offers/",
        parser=_provider_parsers.parse_recruitee_jobs_payload,
        payload_key="offers",
    ),
    "pinpoint": JsonFeedSpec(
        default_error="missing subdomain/api_url",
        source_key="subdomain",
        url_template="https://{value}.pinpointhq.com/postings.json",
        dotted_url_template="https://{value}/postings.json",
        parser=_provider_parsers.parse_pinpoint_jobs_payload,
        payload_key="data",
    ),
    "lever": JsonFeedSpec(
        default_error="missing account/api_url",
        source_key="account",
        url_template="https://api.lever.co/v0/postings/{value}?mode=json",
        parser=_provider_parsers.parse_lever_jobs_payload,
        list_payload=True,
    ),
    # Ashby boards are client-rendered: `jobs.ashbyhq.com/<slug>` serves no `/job/`
    # anchors at all, so the HTML-board path read every Ashby board as empty and the
    # registered ones kept 0. The posting API is the same data the page renders from.
    # `source_key` is `board_url` and the slug is extracted from it, because that is
    # the field Ashby registry rows actually carry.
    "ashby": JsonFeedSpec(
        default_error="missing board_url",
        source_key="board_url",
        url_template="",
        parser=_provider_parsers.parse_ashby_jobs_from_payload,
        payload_key="jobs",
    ),
}


def _json_feed_source_identity(source: dict[str, object], registry_adapter: str) -> tuple[str, str]:
    source_name = clean_text(source.get("name")) or f"{registry_adapter}_source"
    studio = clean_text(source.get("studio")) or source_name
    return source_name, studio


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _resolve_json_feed_fetch(
    endpoint: str,
    fetch_payload: FetchPayload | None,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> Any:
    """The injected paginating fetcher, or a single plain request without one.

    A caller that supplies no fetcher gets the behaviour this had before pagination
    existed. Kept as the default so the seam is additive rather than a signature every
    caller has to adopt.
    """
    if fetch_payload is not None:
        return fetch_payload(endpoint)
    return json.loads(fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s))


def _run_json_feed_sources(
    *,
    adapter_name: str,
    registry_adapter: str,
    default_error: str,
    parse_payload: ParsePayload,
    build_url: BuildUrl,
    payload_count: PayloadCount,
    fetch_text: Callable[[str, int], str],
    fetch_payload: FetchPayload | None = None,
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

    def _process_source(
        source: dict[str, object],
    ) -> tuple[list[RawJob], dict[str, object], str, str]:
        source_started = time.perf_counter()
        source_name, studio = _json_feed_source_identity(source, registry_adapter)
        endpoint = build_url(source)
        source_jobs: list[RawJob] = []
        error = ""
        error_provider_url = ""
        entry_report = build_provider_entry_report(
            adapter_name=adapter_name,
            studio=studio,
            source_name=source_name,
            extra={"providerUrl": endpoint},
        )
        apply_provider_cache_decision(
            entry_report=entry_report,
            source_name=source_name,
            adapter_name=adapter_name,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        if not endpoint:
            entry_report["status"] = "error"
            entry_report["error"] = default_error
            entry_report["durationMs"] = _elapsed_ms(source_started)
            return source_jobs, entry_report, error, error_provider_url
        if skip_provider_for_cache(entry_report):
            entry_report["durationMs"] = _elapsed_ms(source_started)
            return source_jobs, entry_report, error, error_provider_url
        if provider_revalidate_not_modified(
            url=endpoint,
            entry_report=entry_report,
            source_name=source_name,
            source_state_rows=source_state_rows,
            timeout_s=timeout_s,
            revalidate_url=conditional_revalidate_url,
        ):
            entry_report["durationMs"] = _elapsed_ms(source_started)
            return source_jobs, entry_report, error, error_provider_url
        try:
            fetch_started = time.perf_counter()
            payload = _resolve_json_feed_fetch(
                endpoint, fetch_payload, fetch_text, timeout_s, retries, backoff_s
            )
            entry_report["fetchMs"] = _elapsed_ms(fetch_started)
            parse_started = time.perf_counter()
            parsed = parse_payload(source, payload, studio)
            entry_report["parseMs"] = _elapsed_ms(parse_started)
            entry_report["fetchedCount"] = payload_count(payload, parsed)
            entry_report["keptCount"] = len(parsed)
            for row in parsed:
                row["adapter"] = adapter_name
                row["studio"] = studio
            source_jobs.extend(parsed)
        except EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS as exc:
            reraise_unexpected_provider_api_source_exception(exc)
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            error_provider_url = endpoint
            error = f"{registry_adapter}:{source_name}: {exc}"
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return source_jobs, entry_report, error, error_provider_url

    sources = list(registry_entries(registry_adapter))
    concurrency = max(1, min(JSON_FEED_SOURCE_FETCH_CONCURRENCY, len(sources) or 1))
    if concurrency <= 1 or len(sources) <= 1:
        results = [_process_source(source) for source in sources]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            results = list(executor.map(_process_source, sources))
    for source_jobs, entry_report, error, error_provider_url in results:
        entry_report["sourceFetchConcurrency"] = concurrency
        if source_jobs:
            jobs.extend(source_jobs)
        details.append(entry_report)
        if error:
            errors.append(error)
            if error_provider_url and not provider_url:
                provider_url = error_provider_url

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


def _json_feed_plugin(adapter_name: str) -> SimpleAdapterPlugin:
    spec = JSON_FEED_SPECS.get(adapter_name, JSON_FEED_SPECS["lever"])
    registry_adapter = adapter_name

    adapter_key = f"{adapter_name}_sources"

    def can_handle(ctx: AdapterPluginContext) -> bool:
        return ctx.family == "provider_api" and ctx.adapter_key == adapter_key

    def run_plugin(**kwargs: Any) -> list[RawJob]:
        # Pulled out of kwargs because the pagination closure needs them; they are still
        # forwarded below, because _run_json_feed_sources uses fetch_text for its own retries.
        fetch_text = kwargs["fetch_text"]
        timeout_s = kwargs["timeout_s"]
        retries = kwargs["retries"]
        backoff_s = kwargs["backoff_s"]
        return _run_json_feed_sources(
            adapter_name=adapter_name,
            registry_adapter=registry_adapter,
            default_error=spec.default_error,
            parse_payload=lambda source, payload, studio: _parse_json_feed_payload(
                source, payload, studio, spec
            ),
            build_url=lambda source: _build_json_feed_url(source, spec),
            payload_count=lambda payload, parsed: _json_feed_payload_count(payload, parsed, spec),
            fetch_payload=lambda endpoint: _fetch_json_feed_payload(
                endpoint, spec, fetch_text, timeout_s, retries, backoff_s
            ),
            **kwargs,
        )

    return SimpleAdapterPlugin(
        name=adapter_key,
        family="provider_api",
        priority=50,
        can_handle_fn=can_handle,
        run_fn=run_plugin,
    )
