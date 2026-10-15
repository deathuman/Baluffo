"""Dover provider runner.

AI boundary owns: Dover provider execution, the two-step slug -> client_id -> jobs
resolution, and job row extraction.
AI boundary implement in: this file for Dover runner behavior; shared provider
lifecycle stays in provider_api helpers and payload parsing stays in the parser leaf.
AI boundary search before contracts: provider plugin register, Dover board URL leaf,
and focused Dover provider tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused Dover provider tests.

Dover boards are client-rendered: ``https://app.dover.com/jobs/<slug>`` serves a SPA
shell whose only job links come from a JSON API, so an anchor-counting path reads
every Dover board as empty. The public careers-page API is the same data the page
renders from, and it is **two requests**, because the endpoints are keyed
differently and neither key is the other:

1. ``GET /api/v1/careers-page-slug/<slug>`` -> ``{"id": "<client_id>", ...}``
2. ``GET /api/v1/careers-page/<client_id>/jobs`` -> ``{"count": N, "results": [...]}``

The second URL is deliberately written without a trailing slash: ``.../jobs/`` is
routed to the SPA shell and answers HTML, which would parse as zero jobs rather
than as an error. The global ``/api/v1/job-board/jobs/`` endpoint lists every Dover
board and ignores a ``?client=`` filter, so it cannot serve one studio.

On access: Dover's ``robots.txt`` carries ``Disallow: /api/``, which covers both
endpoints above. They are unauthenticated and serve published careers-page content
only, and their use was reviewed and approved on that basis. A change to what this
adapter reads -- another path, an authenticated call -- re-opens that decision.

Both halves of the slug resolution live in ``src.dover_board_urls``, a leaf that
imports nothing from ``src.jobs``, so the fetch adapter and the discovery probe
resolve the same board the same way.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from src.dover_board_urls import dover_jobs_api_url, dover_source_slug_url
from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
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

FetchText = Callable[[str, int], str]
FetchPayload = Callable[[str], Any]

DOVER_SOURCE_FETCH_CONCURRENCY = 6
#: How much of a body to keep in a "that is HTML, not JSON" error. The SPA shell is
#: ~4.4KB and says nothing useful, so the marker is the point, not the payload.
_DOVER_HTML_SNIFF_CHARS = 256


class DoverSourceError(AdapterValidationError):
    """A Dover board that cannot be read, with the reason it cannot."""


def _dover_as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _dover_payload_from_text(text: str) -> Any:
    """Decode a response body, refusing the SPA shell instead of reading it as empty.

    ``.../jobs/`` with a trailing slash, or any misrouted path, answers the board's
    HTML shell. ``json.loads`` on that raises, which is right, but the message has to
    say what happened or the source reads as a mystery failure. A leading ``<`` is
    the reliable tell.
    """
    stripped = clean_text(text)
    if stripped.startswith("<"):
        raise DoverSourceError(
            "dover: response is HTML, not JSON "
            f"(SPA shell, first bytes: {stripped[:_DOVER_HTML_SNIFF_CHARS]!r})"
        )
    try:
        return json.loads(stripped)
    except ValueError as exc:
        raise DoverSourceError(f"dover: response is not valid JSON: {exc}") from exc


def _resolve_dover_fetch(
    endpoint: str,
    fetch_payload: FetchPayload | None,
    fetch_text: FetchText,
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> Any:
    """The injected payload fetcher, or one plain JSON request without one.

    The seam is additive, exactly as in ``json_feed``: a caller that injects a
    fetcher gets it for both requests and no network is touched, and a caller that
    does not gets the shipped transport behaviour.
    """
    if fetch_payload is not None:
        return fetch_payload(endpoint)
    return _dover_payload_from_text(
        fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s)
    )


def _dover_client_id(slug_payload: Any, slug_url: str) -> str:
    """The ``client_id`` the jobs endpoint is keyed on, or an error that says so.

    A slug the API does not know answers with an object that carries no ``id`` --
    and a source that returned zero jobs there would be indistinguishable from a
    board with no openings, so this is deliberately a hard error.
    """
    payload = _dover_as_dict(slug_payload)
    if isinstance(slug_payload, list):
        first = slug_payload[0] if slug_payload else None
        payload = _dover_as_dict(first)
    client_id = clean_text(payload.get("id") or payload.get("client_id"))
    if not client_id:
        raise DoverSourceError(f"dover: slug response carried no client id (url: {slug_url})")
    return client_id


def _dover_payload_count(payload: Any) -> int:
    """Rows the jobs payload announced, before any row filter."""
    if isinstance(payload, dict):
        try:
            count = int(payload.get("count") or 0)
        except (TypeError, ValueError):
            count = 0
        if count:
            return count
        rows = payload.get("results")
    else:
        rows = payload
    return len(rows) if isinstance(rows, list) else 0


def _dover_source_identity(source: dict[str, object]) -> tuple[str, str]:
    source_name = clean_text(source.get("name")) or "dover_source"
    studio = clean_text(source.get("studio")) or source_name
    return source_name, studio


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _run_dover_sources(
    *,
    adapter_name: str = "dover",
    registry_adapter: str = "dover",
    default_error: str = "missing board_url/slug",
    fetch_text: FetchText,
    fetch_payload: FetchPayload | None = None,
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    """Read every registered Dover board, two requests per board."""
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    provider_url = ""

    def _process_source(
        source: dict[str, object],
    ) -> tuple[list[RawJob], dict[str, object], str, str]:
        source_started = time.perf_counter()
        source_name, studio = _dover_source_identity(source)
        slug_url = dover_source_slug_url(source)
        source_jobs: list[RawJob] = []
        error = ""
        error_provider_url = ""
        entry_report = build_provider_entry_report(
            adapter_name=adapter_name,
            studio=studio,
            source_name=source_name,
            extra={"providerUrl": slug_url},
        )
        apply_provider_cache_decision(
            entry_report=entry_report,
            source_name=source_name,
            adapter_name=adapter_name,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        if not slug_url:
            entry_report["status"] = "error"
            entry_report["error"] = default_error
            entry_report["durationMs"] = _elapsed_ms(source_started)
            error = f"{registry_adapter}:{source_name}: {default_error}"
            return source_jobs, entry_report, error, error_provider_url
        if skip_provider_for_cache(entry_report):
            entry_report["durationMs"] = _elapsed_ms(source_started)
            return source_jobs, entry_report, error, error_provider_url
        if provider_revalidate_not_modified(
            url=slug_url,
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
            slug_payload = _resolve_dover_fetch(
                slug_url, fetch_payload, fetch_text, timeout_s, retries, backoff_s
            )
            client_id = _dover_client_id(slug_payload, slug_url)
            jobs_url = dover_jobs_api_url(client_id)
            if not jobs_url:
                raise DoverSourceError(f"dover: no jobs url for client id {client_id!r}")
            # The jobs URL is what the source is really read from, so that is the
            # providerUrl the report and the revalidation identity carry.
            entry_report["providerUrl"] = jobs_url
            entry_report["clientId"] = client_id
            payload = _resolve_dover_fetch(
                jobs_url, fetch_payload, fetch_text, timeout_s, retries, backoff_s
            )
            entry_report["fetchMs"] = _elapsed_ms(fetch_started)
            parse_started = time.perf_counter()
            parsed = _provider_parsers.parse_dover_jobs_payload(payload, source, studio)
            entry_report["parseMs"] = _elapsed_ms(parse_started)
            entry_report["fetchedCount"] = _dover_payload_count(payload)
            entry_report["keptCount"] = len(parsed)
            for row in parsed:
                row["adapter"] = adapter_name
                row["studio"] = studio
            source_jobs.extend(parsed)
        except EXPECTED_PROVIDER_API_SOURCE_EXCEPTIONS as exc:
            reraise_unexpected_provider_api_source_exception(exc)
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            error_provider_url = clean_text(entry_report.get("providerUrl")) or slug_url
            error = f"{registry_adapter}:{source_name}: {exc}"
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return source_jobs, entry_report, error, error_provider_url

    sources = list(registry_entries(registry_adapter))
    concurrency = max(1, min(DOVER_SOURCE_FETCH_CONCURRENCY, len(sources) or 1))
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
    if not provider_url:
        provider_url = next(
            (
                clean_text(detail.get("providerUrl"))
                for detail in details
                if clean_text(detail.get("providerUrl"))
            ),
            "",
        )

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


def make_dover_sources_runner(
    *,
    adapter_name: str = "dover",
    registry_adapter: str = "dover",
) -> Callable[..., list[RawJob]]:
    """Build the keyword-only runner the plugin registry and compat surface bind."""

    def runner(
        *,
        fetch_text: FetchText,
        timeout_s: int,
        retries: int,
        backoff_s: float,
        source_state_rows: dict[str, dict[str, object]] | None = None,
        force_refresh_all: bool = False,
        fetch_payload: FetchPayload | None = None,
    ) -> list[RawJob]:
        return _run_dover_sources(
            adapter_name=adapter_name,
            registry_adapter=registry_adapter,
            fetch_text=fetch_text,
            fetch_payload=fetch_payload,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )

    return runner


run_dover_sources_source = make_dover_sources_runner()

__all__ = [
    "DOVER_SOURCE_FETCH_CONCURRENCY",
    "DoverSourceError",
    "make_dover_sources_runner",
    "run_dover_sources_source",
    "_run_dover_sources",
]
