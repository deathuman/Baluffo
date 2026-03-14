"""Centralized transport helpers for jobs fetching."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import replace
from typing import Any, Callable, Dict, Optional, Tuple

from scripts.jobs import common
from scripts.jobs.models import RequestConfig

DEFAULT_TIMEOUT_S = common.DEFAULT_TIMEOUT_S
DEFAULT_RETRIES = common.DEFAULT_RETRIES
DEFAULT_BACKOFF_S = common.DEFAULT_BACKOFF_S
DEFAULT_FETCH_STRATEGY = common.DEFAULT_FETCH_STRATEGY
DEFAULT_ADAPTER_HTTP_CONCURRENCY = common.DEFAULT_ADAPTER_HTTP_CONCURRENCY
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = common.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_HTTP_HEADERS = dict(common.DEFAULT_HTTP_HEADERS)
DEFAULT_REDIRECT_HEADERS = dict(common.DEFAULT_REDIRECT_HEADERS)
SUPPORTED_REDIRECT_HOSTS = common.SUPPORTED_REDIRECT_HOSTS
httpx = common.httpx


def default_request_config(
    *,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    headers: Dict[str, str] | None = None,
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


def with_proxy(request: RequestConfig, proxy_url: str) -> RequestConfig:
    return replace(request, proxy_url=str(proxy_url or ""))


def build_headers(request: RequestConfig) -> Dict[str, str]:
    headers = dict(DEFAULT_HTTP_HEADERS)
    headers.update(request.headers)
    if request.user_agent:
        headers["User-Agent"] = request.user_agent
    return headers


def normalize_url(url: Any) -> str:
    return common.normalize_url(url)


def fingerprint_url(url: Any) -> str:
    return common.fingerprint_url(url)


def is_supported_redirect_url(url: Any) -> bool:
    return common.is_supported_redirect_url(url)


def resolve_supported_redirect_url(url: Any, *, timeout_s: int = DEFAULT_TIMEOUT_S) -> str:
    return common.resolve_supported_redirect_url(url, timeout_s=timeout_s)


class PooledRedirectResolver:
    def __init__(
        self,
        *,
        timeout_s: int = DEFAULT_TIMEOUT_S,
        max_connections: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    ) -> None:
        self._timeout_s = max(1, int(timeout_s or DEFAULT_TIMEOUT_S))
        self._cache: Dict[str, str] = {}
        self._inflight: Dict[str, threading.Event] = {}
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
            except Exception:  # noqa: BLE001
                self._client = None

    def _resolve_with_client(self, normalized: str) -> str:
        if self._client is None:
            return resolve_supported_redirect_url(normalized, timeout_s=self._timeout_s)
        last_error: Optional[Exception] = None
        for method in ("HEAD", "GET"):
            try:
                response = self._client.request(method, normalized)
                resolved = normalize_url(str(response.url))
                return resolved or normalized
            except Exception as exc:  # noqa: BLE001
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
        wait_event: Optional[threading.Event] = None
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

    def snapshot_stats(self) -> Dict[str, int]:
        with self._lock:
            return {
                "cacheHits": int(self._cache_hits),
                "resolvedCount": int(self._resolved_count),
            }

    def close(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass


def build_redirect_resolver(
    *,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    max_connections: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
) -> PooledRedirectResolver:
    return PooledRedirectResolver(timeout_s=timeout_s, max_connections=max_connections)


def default_fetch_text(url: str, timeout_s: int, request: RequestConfig | None = None) -> str:
    _ = request
    return common.default_fetch_text(url, timeout_s)


class AsyncHttpTextFetcher:
    def __init__(self, *, max_connections: int = DEFAULT_ADAPTER_HTTP_CONCURRENCY) -> None:
        if httpx is None:
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
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            headers=DEFAULT_HTTP_HEADERS,
            limits=httpx.Limits(
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
            except Exception:  # noqa: BLE001
                pass
            asyncio.set_event_loop(None)
            self._loop.close()

    async def _fetch(self, url: str, timeout_s: int) -> str:
        timeout = httpx.Timeout(float(max(1, timeout_s)))
        try:
            response = await self._client.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as exc:
            code = int(getattr(exc.response, "status_code", 0) or 0)
            raise RuntimeError(f"HTTP {code} for {url}") from exc
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Network error for {url}: {exc}") from exc

    async def _aclose(self) -> None:
        await self._client.aclose()

    def fetch_text(self, url: str, timeout_s: int) -> str:
        if self._closed:
            raise RuntimeError("Async HTTP fetcher is closed")
        future = asyncio.run_coroutine_threadsafe(self._fetch(url, timeout_s), self._loop)
        return str(future.result())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            future = asyncio.run_coroutine_threadsafe(self._aclose(), self._loop)
            future.result(timeout=5)
        except Exception:  # noqa: BLE001
            pass
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:  # noqa: BLE001
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
    return common.fetch_with_retries(url, effective_fetch, timeout_s, retries, backoff_s)


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
) -> Tuple[Callable[[str, int], str], str, Any]:
    strategy = common.norm_text(fetch_strategy)
    chosen = "urllib"
    async_fetcher: Optional[AsyncHttpTextFetcher] = None
    if fetch_text is not default_fetch_text and fetch_text is not common.default_fetch_text:
        return fetch_text, "custom", async_fetcher
    if strategy in {"http", "auto"} and httpx is not None:
        try:
            async_fetcher = AsyncHttpTextFetcher(max_connections=adapter_http_concurrency)
            chosen = "httpx_async"
            return async_fetcher.fetch_text, chosen, async_fetcher
        except Exception:  # noqa: BLE001
            pass
    return default_fetch_text, chosen, async_fetcher

