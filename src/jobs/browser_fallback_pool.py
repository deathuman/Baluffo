"""Shared Chromium pool for browser fetch fallbacks.

One lazy Chromium per fetch stage, running on a dedicated dispatcher thread
with an asyncio loop. Callers submit fetches from any worker thread via the
synchronous ``fetch()``; each fetch gets a fresh BrowserContext so cookies/
localStorage never bleed across servers (same isolation as launch-per-call).

Why a dispatcher thread: Playwright's sync API is greenlet-bound to the
thread that starts it, so a pool-of-one cannot be shared across the fetch
ThreadPoolExecutor workers. Async contexts on one loop preserve concurrency
without per-call browser launches.

Crash handling: a dead browser marks the pool unavailable; the resulting
error string matches is_browser_fallback_environment_error tokens, so the
existing BrowserFallbackCircuitBreaker owns the 30-min cooldown.

AI boundary owns: pooled Chromium lifetime and per-call context acquisition.
AI boundary implement in: this file for pool mechanics; fallback gating stays in browser_fallback.py.
AI boundary search before contracts: pipeline source loop, source execution stage, source_check_http.
AI boundary verify: `npm run lint:repo-guardrails` plus tests/test_browser_fallback_pool.py.
"""

from __future__ import annotations

import asyncio
import atexit
import os
import threading
import time
from typing import Any

from src.bridge.source_check_http import normalize_browser_fallback_error

_BROWSER_POOL_DISABLE_VALUES = {"0", "false", "no", "off"}
_POOL_THREAD_NAME = "baluffo-browser-pool"
_POOL_THREAD_JOIN_TIMEOUT_S = 5.0


class _PoolMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.values: dict[str, Any] = {
            "pool_startup_ms": 0,
            "pool_acquisitions": 0,
            "pool_relaunch_count": 0,
        }

    def incr(self, key: str, delta: int = 1) -> None:
        with self._lock:
            self.values[key] = int(self.values.get(key) or 0) + delta

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self.values)


def browser_pool_enabled(env: dict[str, str] | None = None) -> bool:
    values = os.environ if env is None else env
    raw = str(values.get("BALUFFO_BROWSER_POOL") or "").strip().lower()
    return raw not in _BROWSER_POOL_DISABLE_VALUES


class BrowserFallbackPool:
    """Lazy single-browser pool; fresh BrowserContext per fetch call."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._playwright = None
        self._browser = None
        self._available = True
        self._start_lock = threading.Lock()
        self.metrics = _PoolMetrics()
        atexit.register(self._atexit_close)

    def _ensure_started(self) -> None:
        if not self._available:
            raise RuntimeError("browser fallback unavailable (browser has been closed)")
        if self._loop is not None:
            return
        with self._start_lock:
            if self._loop is not None:
                return
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(
                target=self._loop.run_forever,
                name=_POOL_THREAD_NAME,
                daemon=True,
            )
            self._thread.start()
            t0 = time.monotonic()
            future = asyncio.run_coroutine_threadsafe(self._start_browser(), self._loop)
            try:
                future.result(timeout=120)
            except BaseException:
                self._hard_close_locked()
                raise
            self.metrics.incr("pool_startup_ms", int((time.monotonic() - t0) * 1000))

    async def _start_browser(self) -> None:
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)

    async def _fetch(self, url: str, timeout_s: int) -> str:
        context = await self._browser.new_context()
        try:
            page = await context.new_page()
            try:
                try:
                    await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=max(1, int(timeout_s)) * 1000,
                    )
                except BaseException as exc:
                    if self._browser_death_marker(exc):
                        self._mark_unavailable()
                    raise
                return await page.content() or ""
            finally:
                try:
                    await page.close()
                except Exception:
                    pass
        finally:
            try:
                await context.close()
            except Exception:
                pass

    @staticmethod
    def _browser_death_marker(exc: BaseException) -> bool:
        text = str(exc).lower()
        return any(
            token in text
            for token in (
                "browser has been closed",
                "target closed",
                "browser closed",
                "crashed",
            )
        )

    def _mark_unavailable(self) -> None:
        with self._start_lock:
            if self._available:
                self._available = False

    def fetch(self, url: str, timeout_s: int) -> tuple[str, str]:
        """Sync entry-point for worker threads; returns (html, error)."""
        try:
            self._ensure_started()
            future = asyncio.run_coroutine_threadsafe(self._fetch(url, timeout_s), self._loop)
            html = future.result(timeout=max(1, int(timeout_s)) + 30)
        except BaseException as exc:
            return "", normalize_browser_fallback_error(str(exc))
        self.metrics.incr("pool_acquisitions")
        if not html:
            return "", "browser fallback returned empty content"
        return html, ""

    def close(self) -> None:
        with self._start_lock:
            self._hard_close_locked()

    def _hard_close_locked(self) -> None:
        loop = self._loop
        thread = self._thread
        self._loop = None
        self._thread = None
        self._playwright = None
        self._browser = None
        self._available = True
        if loop is None:
            return
        try:
            future = asyncio.run_coroutine_threadsafe(self._shutdown(), loop)
            future.result(timeout=10)
        except BaseException:
            pass
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=_POOL_THREAD_JOIN_TIMEOUT_S)

    async def _shutdown(self) -> None:
        if self._browser is not None:
            try:
                await self._browser.close()
            except BaseException:
                pass
        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except BaseException:
                pass

    def _atexit_close(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


__all__ = ["BrowserFallbackPool", "browser_pool_enabled"]
