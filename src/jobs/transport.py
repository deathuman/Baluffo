"""Centralized transport helpers for jobs fetching.

AI boundary owns: jobs HTTP transport, conditional revalidation, redirect resolution, and async fetch helpers.
AI boundary implement in: this file for transport behavior; source-specific retry/report policy stays in adapters/pipeline leaves.
AI boundary search before contracts: adapter runners, static runtime, source execution, and transport tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs transport tests.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters import community
from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.common.fetch import fetch_with_retries as common_fetch_with_retries
from src.jobs.common.http import HttpStatusError
from src.jobs.common.http import default_fetch_text as common_default_fetch_text
from src.jobs.common.http import (
    default_fetch_text_with_response_headers as common_fetch_text_with_response_headers,
)
from src.jobs.common.http import fetch_max_bytes as common_fetch_max_bytes
from src.jobs.models import RequestConfig
from src.jobs.text_utils import clean_text, norm_text
from src.jobs.text_utils import normalize_url as normalize_url_impl

from .common import config as common_config
from .common import url as common_url

DEFAULT_TIMEOUT_S = common_config.DEFAULT_TIMEOUT_S
DEFAULT_RETRIES = common_config.DEFAULT_RETRIES
DEFAULT_BACKOFF_S = common_config.DEFAULT_BACKOFF_S
DEFAULT_FETCH_STRATEGY = common_config.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = common_config.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = community.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_HTTP_HEADERS = dict(common_config.DEFAULT_HTTP_HEADERS)
DEFAULT_REDIRECT_HEADERS = dict(common_config.DEFAULT_REDIRECT_HEADERS)
SUPPORTED_REDIRECT_HOSTS = common_config.SUPPORTED_REDIRECT_HOSTS
_EXPECTED_TRANSPORT_CLOSE_EXCEPTIONS = (OSError, RuntimeError, ValueError)
_EXPECTED_ASYNC_TRANSPORT_CLOSE_EXCEPTIONS = (
    *(_EXPECTED_TRANSPORT_CLOSE_EXCEPTIONS),
    FutureTimeoutError,
)
httpx: Any | None
try:
    import importlib

    httpx = importlib.import_module("httpx")
except ImportError:
    httpx = None


def default_request_config(
    *,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    headers: dict[str, str] | None = None,
    user_agent: str = "",
    proxy_url: str = "",
) -> RequestConfig:
    merged = dict(DEFAULT_HTTP_HEADERS)
    if headers:
        merged.update({str(key): str(value) for key, value in headers.items()})
    if user_agent:
        merged["User-Agent"] = str(user_agent)
    return RequestConfig(
        timeout_s=max(1, int(timeout_s or DEFAULT_TIMEOUT_S)),
        headers=merged,
        user_agent=str(merged.get("User-Agent") or ""),
        proxy_url=str(proxy_url or ""),
    )


def build_headers(request: RequestConfig) -> dict[str, str]:
    headers = dict(DEFAULT_HTTP_HEADERS)
    headers.update(request.headers)
    if request.user_agent:
        headers["User-Agent"] = request.user_agent
    return headers


def build_fetch_headers(url: str, request: RequestConfig) -> dict[str, str]:
    """build_headers plus the S5 browser-profile override for allowlisted hosts.

    See ``src.jobs.common.browser_headers``: under the flag + host allowlist,
    allowlisted URLs get the browser header profile instead of the bot default
    shape (some edges 500 the bot shape — Mundfish) — everyone else keeps the
    exact pre-S5 headers.
    """

    from src.jobs.common.browser_headers import resolve_fetch_headers

    return resolve_fetch_headers(url, build_headers(request))


def normalize_url(url: Any) -> str:
    return normalize_url_impl(url)


def fingerprint_url(url: Any) -> str:
    return common_url.fingerprint_url(url)


def is_supported_redirect_url(url: Any) -> bool:
    return common_url.is_supported_redirect_url(url)


def resolve_supported_redirect_url(url: Any, *, timeout_s: int = DEFAULT_TIMEOUT_S) -> str:
    return common_url.resolve_supported_redirect_url(url, timeout_s=timeout_s)


def conditional_revalidate_url(
    url: str,
    timeout_s: int,
    *,
    etag: str = "",
    last_modified: str = "",
    since: str = "",
    probe: bool = False,
) -> dict[str, Any]:
    """Cheap HEAD check for "has this document changed?".

    Without validators this is a no-op unless ``probe`` is set: the static
    flow probes once to seed ``ETag``/``Last-Modified`` so later runs can ask
    the conditional question, while provider callers keep the old no-request
    behavior.

    ``since`` is the caller's own read clock, as an HTTP date -- the answer to "is
    the copy I already have still current?" for a board that publishes no validator.
    It is a question, never an answer: unlike ``last_modified`` it is not a tag the
    server issued, so nothing may persist it as one. A server that ignores the header
    simply answers 200 and the caller reads the page as it always did.
    """
    clean_etag = str(etag or "").strip()
    clean_last_modified = str(last_modified or "").strip()
    clean_since = str(since or "").strip()
    if not clean_etag and not clean_last_modified and not clean_since and not probe:
        return {
            "supported": False,
            "notModified": False,
            "statusCode": 0,
            "etag": "",
            "lastModified": "",
        }
    headers = dict(DEFAULT_HTTP_HEADERS)
    if clean_etag:
        headers["If-None-Match"] = clean_etag
    # A server-issued Last-Modified is authoritative; the caller's read clock is the
    # fallback question for a page that has never published one.
    if clean_last_modified:
        headers["If-Modified-Since"] = clean_last_modified
    elif clean_since:
        headers["If-Modified-Since"] = clean_since
    if httpx is not None:
        try:
            with httpx.Client(
                follow_redirects=True,
                headers=headers,
                timeout=httpx.Timeout(float(max(1, timeout_s))),
            ) as client:
                response = client.request("HEAD", url)
                return {
                    "supported": True,
                    "notModified": int(response.status_code) == 304,
                    "statusCode": int(response.status_code or 0),
                    "etag": str(response.headers.get("ETag") or ""),
                    "lastModified": str(response.headers.get("Last-Modified") or ""),
                }
        except httpx.HTTPError as exc:
            exc_response = getattr(exc, "response", None)
            if exc_response is not None:
                exc_headers = getattr(exc_response, "headers", {})
                return {
                    "supported": True,
                    "notModified": int(getattr(exc_response, "status_code", 0) or 0) == 304,
                    "statusCode": int(getattr(exc_response, "status_code", 0) or 0),
                    "etag": str(exc_headers.get("ETag") or ""),
                    "lastModified": str(exc_headers.get("Last-Modified") or ""),
                }
    return {
        "supported": False,
        "notModified": False,
        "statusCode": 0,
        "etag": "",
        "lastModified": "",
    }


# How long a board that ignored a conditional request is left alone before the question is
# asked again. Retiring is what stops a feed that never answers 304 from paying a HEAD in
# front of every read; the interval is what keeps it reversible, because a retired board can
# no longer be asked whether it changed. A week of full reads costs a server that starts
# honouring the header seven reads before it is noticed again, and every board that does
# honour it is discovered on the first ask.
PROBE_RETIRE_RECHECK_S = 7 * 24 * 60 * 60


def _weak_etag(value: str) -> str:
    """An ETag without its weakness marker, for If-None-Match's weak comparison."""
    text = clean_text(value)
    return text[2:].strip() if text.startswith("W/") else text


def conditional_probe_ignored(
    revalidate: dict[str, Any], *, etag: str = "", last_modified: str = ""
) -> bool:
    """Whether a response proves the server ignored the validator it was asked about.

    A server that holds the document a tag identifies must answer 304. Answering 200 while
    echoing that same tag is the unambiguous opposite: the conditional header did nothing.
    A different tag is an honest change, and a tagless response is ambiguous (many servers
    omit the header on a HEAD), so neither retires anything.

    The date is only evidence when no tag was asked about. ``If-None-Match`` and
    ``If-Modified-Since`` are usually sent together, and a reply that carries a *different*
    ETag but the same ``Last-Modified`` is an ordinary edit inside one second -- the date
    cannot see it, so a matching date must not overrule the tag's proof of a change.
    """
    status_code = int(revalidate.get("statusCode") or 0)
    if not 200 <= status_code < 300 or bool(revalidate.get("notModified")):
        return False
    sent_etag = _weak_etag(etag)
    if sent_etag:
        return sent_etag == _weak_etag(str(revalidate.get("etag") or ""))
    sent_last_modified = clean_text(last_modified)
    return bool(sent_last_modified) and sent_last_modified == clean_text(
        revalidate.get("lastModified")
    )


def probe_retirement_active(
    state_entry: dict[str, Any] | None, *, now: datetime | None = None
) -> bool:
    """Whether a board's conditional probe is retired, so it is read without one.

    The stamp is written only once a second consecutive ask has echoed its validator --
    one echo can be a cache miss on the HEAD -- and cleared when a later ask is answered
    304, so the state and the board's behaviour stay in step.
    """
    retired_at = parse_datetime((state_entry or {}).get("lastHttpProbeIgnoredAt"))
    if retired_at is None:
        return False
    if retired_at.tzinfo is None:
        retired_at = retired_at.replace(tzinfo=UTC)
    return ((now or datetime.now(UTC)) - retired_at).total_seconds() < PROBE_RETIRE_RECHECK_S


def _header_value(headers: Any, name: str) -> str:
    value = headers.get(name) if hasattr(headers, "get") else None
    if value is None and hasattr(headers, "get"):
        value = headers.get(name.lower())
    if isinstance(value, (list, tuple)):
        return str(value[0]).strip() if value else ""
    return str(value or "").strip()


def _record_response_validators(sink: dict[str, str], status_code: int, headers: Any) -> None:
    sink["status"] = str(int(status_code or 0))
    etag = _header_value(headers, "ETag")
    last_modified = _header_value(headers, "Last-Modified")
    if etag:
        sink["etag"] = etag
    if last_modified:
        sink["last-modified"] = last_modified


def note_probe_validator_stability(entry_report: dict[str, Any], captured: dict[str, str]) -> None:
    """Whether the validator a conditional 200 published survived the read after it.

    A board that recomputes its validator per request answers the conditional ask with
    one tag and then publishes a different one in the full read seconds later, so no
    stored tag can ever match it and the HEAD in front of every read can only answer 200.
    That is recorded as one observation (``httpProbeUnstableAt``); the state update retires
    the probe only after a second consecutive window, because a single publish between the
    two requests looks exactly the same from here.

    The pair must be a *third* value to count. Two busy frontends with different validators
    answer a probe with the other node's tag and the read with the tag that was asked about
    -- the board alternates rather than churns, and it does answer 304 often enough that
    retiring it would cost more reads than the HEAD it saves.

    Only a caller that asked a conditional question can record anything: a full read after
    ``run_now`` has no probe to compare with, and says nothing either way.
    """
    if not bool(entry_report.get("httpProbeAsked")):
        return
    sent_etag = clean_text(entry_report.get("httpProbeSentEtag"))
    sent_last_modified = clean_text(entry_report.get("httpProbeSentLastModified"))
    probe_etag = clean_text(entry_report.get("httpProbeEtag"))
    probe_last_modified = clean_text(entry_report.get("httpProbeLastModified"))
    read_etag = clean_text(captured.get("etag"))
    read_last_modified = clean_text(captured.get("last-modified"))
    if probe_etag and read_etag:
        unstable = probe_etag != read_etag and read_etag != sent_etag
    elif probe_last_modified and read_last_modified and not probe_etag:
        unstable = (
            probe_last_modified != read_last_modified and read_last_modified != sent_last_modified
        )
    else:
        # A read that publishes the tag the HEAD withheld, or none at all, is not churn.
        unstable = False
    if unstable:
        entry_report["httpProbeUnstableAt"] = datetime.now(UTC).isoformat()
    else:
        entry_report["clearHttpProbeUnstable"] = True


def apply_full_read_response_headers(
    entry_report: dict[str, Any], captured: dict[str, str]
) -> None:
    """Record the validators a full read's own response returned.

    A provider feed may publish its ETag on the GET and not on the conditional HEAD,
    so the response that produced the rows is also the one that earns revalidation.
    A capture that never happened -- a custom seam or a test double -- leaves the
    report untouched rather than retiring a tag on absent evidence.

    When a conditional probe ran earlier in this source's flow, the read is also the
    second half of the question that decides whether the board's validator is stable:
    this is the one place that sees both answers, for the static sweep and every
    provider adapter alike.
    """
    status_code = int(clean_text(captured.get("status")) or 0)
    if not 200 <= status_code < 300:
        return
    note_probe_validator_stability(entry_report, captured)
    apply_conditional_revalidate_result(
        entry_report,
        {
            "statusCode": status_code,
            "notModified": False,
            "etag": captured.get("etag", ""),
            "lastModified": captured.get("last-modified", ""),
        },
    )


def apply_conditional_revalidate_result(
    entry_report: dict[str, Any],
    revalidate: dict[str, Any],
    *,
    sent_etag: str = "",
    sent_last_modified: str = "",
) -> None:
    """Record what a conditional probe said about a board's validators.

    Copies the tags the response publishes. A 2xx that is not a 304 answered with
    the board's current copy, so a validator it did not send is one the board no
    longer publishes: the report marks it for removal from stored state. A 304 is
    exempt -- many servers echo no headers on one -- and an errored probe says
    nothing about the board's validators, so neither may retire a tag.

    ``sent_etag`` / ``sent_last_modified`` are what the request conditioned on. A 200
    that echoes one of them is recorded as one observation (``httpProbeEchoAt``), not
    as a retirement: a CDN can answer a HEAD from a cache miss and echo the tag it holds,
    so it takes a second ask to confirm the header is ignored (the state update promotes
    it). A 304 proves the opposite and clears both the streak and any retirement on file.
    Callers that did not ask a conditional question -- a full read recording its own
    response -- leave both empty, and none of these branches can fire.

    A conditional 200 also records the pair the read will be compared against: what was
    asked about and what came back. A board that republishes a third value seconds later
    cannot be revalidated, which is the second way a probe retires.
    """
    status_code = int(revalidate.get("statusCode") or 0)
    etag = clean_text(revalidate.get("etag"))
    last_modified = clean_text(revalidate.get("lastModified"))
    asked_conditional = bool(clean_text(sent_etag) or clean_text(sent_last_modified))
    entry_report["httpStatus"] = status_code
    if etag:
        entry_report["httpEtag"] = etag
    if last_modified:
        entry_report["httpLastModified"] = last_modified
    if 200 <= status_code < 300 and not bool(revalidate.get("notModified")):
        if not etag:
            entry_report["clearHttpEtag"] = True
        if not last_modified:
            entry_report["clearHttpLastModified"] = True
        if asked_conditional:
            entry_report["httpProbeAsked"] = True
            entry_report["httpProbeSentEtag"] = clean_text(sent_etag)
            entry_report["httpProbeSentLastModified"] = clean_text(sent_last_modified)
            entry_report["httpProbeEtag"] = etag
            entry_report["httpProbeLastModified"] = last_modified
    if status_code == 304:
        entry_report["clearHttpProbeIgnored"] = True
        entry_report["clearHttpProbeEcho"] = True
        entry_report["clearHttpProbeUnstable"] = True
    elif conditional_probe_ignored(revalidate, etag=sent_etag, last_modified=sent_last_modified):
        entry_report["httpProbeEchoAt"] = datetime.now(UTC).isoformat()
    elif clean_text(sent_etag) or clean_text(sent_last_modified):
        # An ordinary changed answer. It breaks the streak: the confirmation has to come
        # from consecutive asks, not from a stale echo the board has since moved past.
        entry_report["clearHttpProbeEcho"] = True


class PooledRedirectResolver:
    def __init__(
        self,
        *,
        timeout_s: int = DEFAULT_TIMEOUT_S,
        max_connections: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
        initial_cache: dict[str, str] | None = None,
    ) -> None:
        self._timeout_s = max(1, int(timeout_s or DEFAULT_TIMEOUT_S))
        self._max_connections = max(1, int(max_connections or 1))
        # Preserve transient redirects, but stop paying the full two-attempt
        # timeout cost for every remaining URL after two full waves of
        # response-less failures on the same host. Any HTTP response resets it.
        self._transport_failure_threshold = max(2, self._max_connections * 2)
        self._cache: dict[str, str] = {}
        self._inflight: dict[str, threading.Event] = {}
        self._host_transport_failures: dict[str, int] = {}
        self._unreachable_hosts: set[str] = set()
        self._lock = threading.Lock()
        self._cache_hits = 0
        self._resolved_count = 0
        self._transport_failures = 0
        self._short_circuits = 0
        self._client = None
        if httpx is not None:
            try:
                try:
                    self._client = httpx.Client(
                        follow_redirects=True,
                        headers=DEFAULT_REDIRECT_HEADERS,
                        timeout=httpx.Timeout(float(self._timeout_s)),
                        limits=httpx.Limits(
                            max_keepalive_connections=self._max_connections,
                            max_connections=max(2, self._max_connections * 2),
                        ),
                        http2=True,
                    )
                except (TypeError, ImportError):
                    self._client = httpx.Client(
                        follow_redirects=True,
                        headers=DEFAULT_REDIRECT_HEADERS,
                        timeout=httpx.Timeout(float(self._timeout_s)),
                        limits=httpx.Limits(
                            max_keepalive_connections=self._max_connections,
                            max_connections=max(2, self._max_connections * 2),
                        ),
                    )
            except _EXPECTED_TRANSPORT_CLOSE_EXCEPTIONS:
                self._client = None
            except ImportError:
                self._client = None
        if isinstance(initial_cache, dict) and initial_cache:
            self.seed_cache(initial_cache)

    @staticmethod
    def _redirect_host(url: str) -> str:
        return (urlparse(url).hostname or "").strip().lower()

    def _record_host_result(self, host: str, *, transport_failed: bool) -> None:
        if not host:
            return
        with self._lock:
            if not transport_failed:
                self._host_transport_failures.pop(host, None)
                self._unreachable_hosts.discard(host)
                return
            failures = self._host_transport_failures.get(host, 0) + 1
            self._host_transport_failures[host] = failures
            self._transport_failures += 1
            if failures >= self._transport_failure_threshold:
                self._unreachable_hosts.add(host)

    def _resolve_with_client(self, normalized: str) -> str:
        host = self._redirect_host(normalized)
        if self._client is None:
            try:
                resolved = resolve_supported_redirect_url(normalized, timeout_s=self._timeout_s)
            except (OSError, RuntimeError):
                self._record_host_result(host, transport_failed=True)
                return normalized
            self._record_host_result(host, transport_failed=False)
            return resolved
        httpx_mod = httpx
        if httpx_mod is None:
            raise RuntimeError("httpx is not installed")
        all_attempts_transport_failed = True
        for method in ("HEAD", "GET"):
            try:
                response = self._client.request(method, normalized)
                resolved = normalize_url(str(response.url))
                self._record_host_result(host, transport_failed=False)
                return resolved or normalized
            except httpx_mod.HTTPError as exc:
                status_code = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
                if status_code:
                    all_attempts_transport_failed = False
                if method == "HEAD":
                    continue
                break
        self._record_host_result(
            host,
            transport_failed=all_attempts_transport_failed,
        )
        return normalized

    def resolve(self, url: str) -> str:
        normalized = normalize_url(url)
        if not is_supported_redirect_url(normalized):
            return normalized
        owner = False
        host = self._redirect_host(normalized)
        wait_event: threading.Event | None = None
        with self._lock:
            cached = self._cache.get(normalized)
            if cached is not None:
                self._cache_hits += 1
                return cached
            wait_event = self._inflight.get(normalized)
            if wait_event is None:
                if host in self._unreachable_hosts:
                    self._short_circuits += 1
                    return normalized
                wait_event = threading.Event()
                self._inflight[normalized] = wait_event
                owner = True
        if not owner:
            wait_event.wait(timeout=float(self._timeout_s))
            with self._lock:
                cached = self._cache.get(normalized, normalized)
                self._cache_hits += 1
                return cached
        resolved = self._resolve_with_client(normalized)
        with self._lock:
            self._cache[normalized] = resolved
            if resolved and resolved != normalized:
                self._resolved_count += 1
            done_event = self._inflight.pop(normalized, None)
            if done_event is not None:
                done_event.set()
        return resolved

    def snapshot_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "cacheHits": int(self._cache_hits),
                "resolvedCount": int(self._resolved_count),
                "transportFailures": int(self._transport_failures),
                "shortCircuits": int(self._short_circuits),
                "unreachableHosts": len(self._unreachable_hosts),
            }

    def seed_cache(self, cache: dict[str, str] | None) -> None:
        if not isinstance(cache, dict) or not cache:
            return
        with self._lock:
            for key, value in cache.items():
                normalized_key = normalize_url(key)
                normalized_value = normalize_url(value)
                if not normalized_key or not normalized_value:
                    continue
                if not is_supported_redirect_url(normalized_key):
                    continue
                if normalized_key == normalized_value:
                    continue
                self._cache[normalized_key] = normalized_value

    def snapshot_cache(self) -> dict[str, str]:
        with self._lock:
            return {
                key: value for key, value in self._cache.items() if key and value and key != value
            }

    def close(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.close()
        except _EXPECTED_TRANSPORT_CLOSE_EXCEPTIONS:
            return


def build_redirect_resolver(
    *,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    max_connections: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    initial_cache: dict[str, str] | None = None,
) -> PooledRedirectResolver:
    return PooledRedirectResolver(
        timeout_s=timeout_s,
        max_connections=max_connections,
        initial_cache=initial_cache,
    )


def default_fetch_text(url: str, timeout_s: int, request: RequestConfig | None = None) -> str:
    effective_request = request or default_request_config(timeout_s=timeout_s)
    headers = build_fetch_headers(url, effective_request)
    sink = effective_request.capture_response_headers
    if sink is not None:
        text, response_headers = common_fetch_text_with_response_headers(
            url, timeout_s, headers=headers
        )
        _record_response_validators(sink, 200, response_headers)
        return text
    return common_default_fetch_text(url, timeout_s, headers=headers)


async def async_fetch_text_httpx(
    client: Any,
    url: str,
    timeout_s: int,
    *,
    request: RequestConfig | None = None,
) -> str:
    if httpx is None:
        raise RuntimeError("httpx is not installed")
    timeout = httpx.Timeout(float(max(1, timeout_s)))
    try:
        headers = build_fetch_headers(url, request or default_request_config(timeout_s=timeout_s))
        response = await client.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        if request is not None and request.capture_response_headers is not None:
            _record_response_validators(
                request.capture_response_headers, int(response.status_code or 0), response.headers
            )
        # ponytail: host-specific cap for heavy outlier listings (603 MiB observed).
        try:
            from src.jobs.common.http import fetch_max_bytes_for_url

            max_bytes = fetch_max_bytes_for_url(url)
        except Exception:
            max_bytes = common_fetch_max_bytes()
        content = bytearray()
        # ponytail: streamed read-at-most cap; see common.http.fetch_max_bytes.
        async for chunk in response.aiter_bytes():
            if len(content) >= max_bytes:
                break
            content.extend(chunk[: max(0, max_bytes - len(content))])
        return bytes(content).decode("utf-8", errors="replace")
    except httpx.HTTPStatusError as exc:
        code = int(getattr(exc.response, "status_code", 0) or 0)
        location = str(exc.response.headers.get("Location") or "")
        raise HttpStatusError(code, url, location=location) from exc
    except httpx.InvalidURL as exc:
        raise RuntimeError(f"Network error for {url}: {exc}") from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"Network error for {url}: {exc}") from exc


class AsyncHttpTextFetcher:
    def __init__(self, *, max_connections: int = DEFAULT_ADAPTER_HTTP_CONCURRENCY) -> None:
        httpx_mod = httpx
        if httpx_mod is None:
            raise RuntimeError("httpx is not installed")
        self._max_connections = max(1, int(max_connections or 1))
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ready = threading.Event()
        self._closed = False
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise RuntimeError("Async HTTP loop initialization timed out")

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        httpx_mod = httpx
        if httpx_mod is None:
            self._ready.set()
            return
        # ponytail: http2 multiplexes provider boards on the same host (e.g. boards.greenhouse.io)
        # through one TLS connection — stdlib-only win before per-board parallel fanout.
        try:
            try:
                self._client = httpx_mod.AsyncClient(
                    follow_redirects=True,
                    headers=DEFAULT_HTTP_HEADERS,
                    limits=httpx_mod.Limits(
                        max_keepalive_connections=self._max_connections,
                        max_connections=max(self._max_connections * 2, self._max_connections),
                    ),
                    http2=True,
                )
            except (TypeError, ImportError):
                self._client = httpx_mod.AsyncClient(
                    follow_redirects=True,
                    headers=DEFAULT_HTTP_HEADERS,
                    limits=httpx_mod.Limits(
                        max_keepalive_connections=self._max_connections,
                        max_connections=max(self._max_connections * 2, self._max_connections),
                    ),
                )
        except ImportError:
            self._ready.set()
            return
        except _EXPECTED_TRANSPORT_CLOSE_EXCEPTIONS:
            self._ready.set()
            return
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            try:
                self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            except _EXPECTED_TRANSPORT_CLOSE_EXCEPTIONS:
                pass
            asyncio.set_event_loop(None)
            self._loop.close()

    async def _fetch(self, url: str, timeout_s: int, request: RequestConfig | None = None) -> str:
        return await async_fetch_text_httpx(self._client, url, timeout_s, request=request)

    async def _aclose(self) -> None:
        await self._client.aclose()

    def fetch_text(self, url: str, timeout_s: int, request: RequestConfig | None = None) -> str:
        if self._closed:
            raise RuntimeError("Async HTTP fetcher is closed")
        future = asyncio.run_coroutine_threadsafe(
            self._fetch(url, timeout_s, request=request), self._loop
        )
        return str(future.result())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close_coro = self._aclose()
        try:
            future = asyncio.run_coroutine_threadsafe(close_coro, self._loop)
        except _EXPECTED_ASYNC_TRANSPORT_CLOSE_EXCEPTIONS:
            close_coro.close()
        except BaseException:
            close_coro.close()
            raise
        else:
            try:
                future.result(timeout=5)
            except _EXPECTED_ASYNC_TRANSPORT_CLOSE_EXCEPTIONS:
                pass
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except _EXPECTED_TRANSPORT_CLOSE_EXCEPTIONS:
            pass
        self._thread.join(timeout=2)


def fetch_with_retries(
    url: str,
    fetch_text: Callable[[str, int], str],
    *,
    request: RequestConfig | None = None,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    retries: int = DEFAULT_RETRIES,
    backoff_s: float = DEFAULT_BACKOFF_S,
) -> str:
    effective_fetch = fetch_text
    if request is not None:
        effective_fetch = make_fetch_text(fetch_text, request=request)
    return common_fetch_with_retries(url, effective_fetch, timeout_s, retries, backoff_s)


def make_fetch_text(
    fetch_text: Callable[..., str],
    *,
    request: RequestConfig,
) -> Callable[[str, int], str]:
    def _wrapped(url: str, timeout_s: int) -> str:
        try:
            return fetch_text(url, timeout_s, request=request)
        except TypeError:
            return fetch_text(url, timeout_s)

    return _wrapped


def resolve_fetch_text_impl(
    *,
    fetch_text: Callable[[str, int], str] = default_fetch_text,
    fetch_strategy: str = DEFAULT_FETCH_STRATEGY,
    adapter_http_concurrency: int = DEFAULT_ADAPTER_HTTP_CONCURRENCY,
) -> tuple[Callable[[str, int], str], str, Any]:
    strategy = norm_text(fetch_strategy)
    chosen = "urllib"
    async_fetcher: AsyncHttpTextFetcher | None = None
    if fetch_text is not default_fetch_text and fetch_text is not common_default_fetch_text:
        return fetch_text, "custom", async_fetcher
    if strategy in {"http", "auto"} and httpx is not None:
        try:
            async_fetcher = AsyncHttpTextFetcher(max_connections=adapter_http_concurrency)
            chosen = "httpx_async"
            return async_fetcher.fetch_text, chosen, async_fetcher
        except _EXPECTED_ASYNC_TRANSPORT_CLOSE_EXCEPTIONS:
            pass
    return default_fetch_text, chosen, async_fetcher
