"""Centralized transport helpers for jobs fetching."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any

from src.jobs.adapters import community
from src.jobs.common.fetch import fetch_with_retries as common_fetch_with_retries
from src.jobs.common.http import HttpStatusError
from src.jobs.common.http import default_fetch_text as common_default_fetch_text
from src.jobs.models import RequestConfig
from src.jobs.text_utils import norm_text
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
    import httpx as httpx
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
) -> dict[str, Any]:
    clean_etag = str(etag or "").strip()
    clean_last_modified = str(last_modified or "").strip()
    if not clean_etag and not clean_last_modified:
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
    if clean_last_modified:
        headers["If-Modified-Since"] = clean_last_modified
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


class PooledRedirectResolver:
    def __init__(
        self,
        *,
        timeout_s: int = DEFAULT_TIMEOUT_S,
        max_connections: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
        initial_cache: dict[str, str] | None = None,
    ) -> None:
        self._timeout_s = max(1, int(timeout_s or DEFAULT_TIMEOUT_S))
        self._cache: dict[str, str] = {}
        self._inflight: dict[str, threading.Event] = {}
        self._lock = threading.Lock()
        self._cache_hits = 0
        self._resolved_count = 0
        self._client = None
        if httpx is not None:
            try:
                self._client = httpx.Client(
                    follow_redirects=True,
                    headers=DEFAULT_REDIRECT_HEADERS,
                    timeout=httpx.Timeout(float(self._timeout_s)),
                    limits=httpx.Limits(
                        max_keepalive_connections=max(1, int(max_connections or 1)),
                        max_connections=max(2, int(max_connections or 1) * 2),
                    ),
                )
            except _EXPECTED_TRANSPORT_CLOSE_EXCEPTIONS:
                self._client = None
        if isinstance(initial_cache, dict) and initial_cache:
            self.seed_cache(initial_cache)

    def _resolve_with_client(self, normalized: str) -> str:
        if self._client is None:
            return resolve_supported_redirect_url(normalized, timeout_s=self._timeout_s)
        last_error: Exception | None = None
        for method in ("HEAD", "GET"):
            try:
                response = self._client.request(method, normalized)
                resolved = normalize_url(str(response.url))
                return resolved or normalized
            except httpx.HTTPError as exc:
                last_error = exc
                status_code = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
                if method == "HEAD" and status_code in {400, 403, 405, 429, 500, 501, 503}:
                    continue
                if method == "HEAD":
                    continue
                break
        _ = last_error
        return normalized

    def resolve(self, url: str) -> str:
        normalized = normalize_url(url)
        if not is_supported_redirect_url(normalized):
            return normalized
        owner = False
        wait_event: threading.Event | None = None
        with self._lock:
            cached = self._cache.get(normalized)
            if cached is not None:
                self._cache_hits += 1
                return cached
            wait_event = self._inflight.get(normalized)
            if wait_event is None:
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
    headers = build_headers(request or default_request_config(timeout_s=timeout_s))
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
        headers = build_headers(request or default_request_config(timeout_s=timeout_s))
        response = await client.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        return str(response.text)
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
        self._client = httpx_mod.AsyncClient(
            follow_redirects=True,
            headers=DEFAULT_HTTP_HEADERS,
            limits=httpx_mod.Limits(
                max_keepalive_connections=self._max_connections,
                max_connections=max(self._max_connections * 2, self._max_connections),
            ),
        )
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
