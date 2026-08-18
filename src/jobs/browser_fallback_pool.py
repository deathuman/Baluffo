"""Shared browser pool for fetch fallbacks.

One lazy browser per fetch stage, running on a dedicated dispatcher thread
with an asyncio loop. Callers submit fetches from any worker thread via the
synchronous ``fetch()``; each fetch gets a fresh BrowserContext so cookies/
localStorage never bleed across servers (same isolation as launch-per-call).

Backends: ``chromium`` (default) launches a pooled headless Chromium via
Playwright; ``obscura`` (spike, dev-only) connects over CDP to an
``obscura serve`` subprocess (Rust engine, ~30 MB). The obscura binary is
not bundled yet — set ``BALUFFO_OBSCURA_BIN`` for the spike; missing binary
surfaces as a fetch error and the circuit breaker owns retries.

Why a dispatcher thread: Playwright's sync API is greenlet-bound to the
thread that starts it, so a pool-of-one cannot be shared across the fetch
ThreadPoolExecutor workers. Async contexts on one loop preserve concurrency
without per-call browser launches.

Crash handling: a dead browser marks the pool unavailable; the resulting
error string matches is_browser_fallback_environment_error tokens, so the
existing BrowserFallbackCircuitBreaker owns the 30-min cooldown.

AI boundary owns: pooled browser lifetime and per-call context acquisition.
AI boundary implement in: this file for pool mechanics; fallback gating stays in browser_fallback.py.
AI boundary search before contracts: pipeline source loop, source execution stage, source_check_http.
AI boundary verify: `npm run lint:repo-guardrails` plus tests/test_browser_fallback_pool.py.
"""

from __future__ import annotations

import asyncio
import atexit
import os
import random
import shlex
import subprocess
import threading
import time
from typing import Any

from src.bridge.source_check_http import normalize_browser_fallback_error

_BROWSER_POOL_DISABLE_VALUES = {"0", "false", "no", "off"}
_POOL_THREAD_NAME = "baluffo-browser-pool"
_POOL_THREAD_JOIN_TIMEOUT_S = 5.0

_BACKEND_CHROMIUM = "chromium"
_BACKEND_OBSCURA = "obscura"
_BACKEND_ENV = "BALUFFO_BROWSER_FALLBACK_BACKEND"
_OBSCURA_BIN_ENV = "BALUFFO_OBSCURA_BIN"
_OBSCURA_EXTRA_ARGS_ENV = "BALUFFO_OBSCURA_EXTRA_ARGS"
_OBSCURA_CONNECT_TIMEOUT_S = 20.0
_OBSCURA_PORT_MIN = 42000
_OBSCURA_PORT_MAX = 49000


class _PoolMetrics:
    def __init__(self, *, backend: str = _BACKEND_CHROMIUM) -> None:
        self._lock = threading.Lock()
        self.values: dict[str, Any] = {
            "pool_startup_ms": 0,
            "pool_acquisitions": 0,
            "pool_relaunch_count": 0,
            "backend": backend,
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


def browser_fallback_backend(env: dict[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    raw = str(values.get(_BACKEND_ENV) or "").strip().lower()
    return raw if raw in {_BACKEND_CHROMIUM, _BACKEND_OBSCURA} else _BACKEND_CHROMIUM


class BrowserFallbackPool:
    """Lazy single-browser pool; fresh BrowserContext per fetch call."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._playwright: Any = None
        self._browser: Any = None
        self._backend = browser_fallback_backend()
        self._obscura_proc: subprocess.Popen | None = None
        self._available = True
        self._start_lock = threading.Lock()
        self.metrics = _PoolMetrics(backend=self._backend)
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
            loop = self._loop
            self._thread = threading.Thread(
                target=loop.run_forever,
                name=_POOL_THREAD_NAME,
                daemon=True,
            )
            self._thread.start()
            t0 = time.monotonic()
            future = asyncio.run_coroutine_threadsafe(self._start_browser(), loop)
            try:
                future.result(timeout=120)
            except BaseException:
                self._hard_close_locked()
                raise
            self.metrics.incr("pool_startup_ms", int((time.monotonic() - t0) * 1000))

    async def _start_browser(self) -> None:
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        if self._backend == _BACKEND_OBSCURA:
            self._browser = await self._connect_obscura()
            return
        self._browser = await self._playwright.chromium.launch(headless=True)

    async def _connect_obscura(self) -> Any:
        """Connect to a subprocess ``obscura serve`` over CDP (spike, dev-only)."""
        binary = str(os.environ.get(_OBSCURA_BIN_ENV) or "").strip()
        if not binary:
            raise RuntimeError(
                f"browser fallback backend 'obscura' requires the {_OBSCURA_BIN_ENV} env var"
            )
        extra_args = shlex.split(str(os.environ.get(_OBSCURA_EXTRA_ARGS_ENV) or "").strip())
        last_error: BaseException | None = None
        for _attempt in range(2):
            port = random.randint(_OBSCURA_PORT_MIN, _OBSCURA_PORT_MAX)
            proc = subprocess.Popen(
                [binary, "serve", "--port", str(port), *extra_args],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self._obscura_proc = proc
            endpoint = f"ws://127.0.0.1:{port}/devtools/browser"
            try:
                deadline = time.monotonic() + _OBSCURA_CONNECT_TIMEOUT_S
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        last_error = RuntimeError(
                            "obscura serve exited before accepting connections (port busy?)"
                        )
                        break
                    try:
                        return await self._playwright.chromium.connect_over_cdp(endpoint)
                    except BaseException as exc:
                        last_error = exc
                        await asyncio.sleep(0.25)
            except BaseException:
                proc.terminate()
                self._obscura_proc = None
                raise
            proc.terminate()
            self._obscura_proc = None
        raise last_error or RuntimeError("obscura serve failed to start")

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
                "connection closed",
                "connection lost",
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
            loop = self._loop
            if loop is None:
                raise RuntimeError("browser pool event loop not started")
            future = asyncio.run_coroutine_threadsafe(self._fetch(url, timeout_s), loop)
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
        self._available = True
        if loop is None:
            return
        # Grab the live handles BEFORE dropping our references: _shutdown
        # runs on the pool loop thread, and nulling these first would leave
        # playwright's node-driver subprocess and its pipe transports
        # abandoned mid-read, so their __del__ later emits "unclosed
        # transport" ResourceWarnings (PytestUnraisableExceptionWarning on
        # Windows proactor, where the pipe handle is already gone by GC
        # time and repr() in the warning raises ValueError).
        browser = self._browser
        playwright = self._playwright
        self._browser = None
        self._playwright = None
        try:
            future = asyncio.run_coroutine_threadsafe(self._shutdown(browser, playwright), loop)
            future.result(timeout=10)
        except BaseException:
            pass
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=_POOL_THREAD_JOIN_TIMEOUT_S)
        try:
            loop.close()
        except BaseException:
            pass

    async def _shutdown(self, browser: Any, playwright: Any) -> None:
        # Closing the browser and stopping playwright drives the driver
        # process to exit and its stdin/stdout pipe transports to close via
        # asyncio's own shutdown path (SubprocessStreamProtocol
        # ._maybe_close_transport). Skipping this is what left unclosed
        # transports behind at GC time.
        if browser is not None:
            try:
                await asyncio.wait_for(browser.close(), timeout=5)
            except BaseException:
                pass
        if playwright is not None:
            try:
                await asyncio.wait_for(playwright.stop(), timeout=5)
            except BaseException:
                pass
        proc = self._obscura_proc
        self._obscura_proc = None
        if proc is not None:
            try:
                proc.terminate()
            except BaseException:
                pass
        for task in list(asyncio.all_tasks()):
            if task is not asyncio.current_task():
                task.cancel()

    def _atexit_close(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


__all__ = ["BrowserFallbackPool", "browser_fallback_backend", "browser_pool_enabled"]
