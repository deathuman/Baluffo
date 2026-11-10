"""Shared browser pool for fetch fallbacks.

One lazy browser per fetch stage, running on a dedicated dispatcher thread
with an asyncio loop. Callers submit fetches from any worker thread via the
synchronous ``fetch()``; each fetch gets a fresh BrowserContext so cookies/
localStorage never bleed across servers (same isolation as launch-per-call).

Backend: a pooled headless Chromium launched via Playwright. (The ``obscura``
CDP engine swap was A/B'd in 2026-08 and retired: Chromium was 3.7 min faster
on a full-cold fetch at comparable memory, and the binary was never bundled.)

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
import threading
import time
from typing import Any

from src.bridge.source_check_http import normalize_browser_fallback_error
from src.shared.browser_launch_args import browser_fallback_chromium_args
from src.shared.utils import now_iso

_BROWSER_POOL_DISABLE_VALUES = {"0", "false", "no", "off"}
_POOL_THREAD_NAME = "baluffo-browser-pool"
_POOL_THREAD_JOIN_TIMEOUT_S = 5.0
# ponytail: per-acquisition growth inside the pooled Chromium/Node driver pinned
# full-cold fetches at tight cgroup ceilings — contexts are closed per call but
# driver/browser footprint still climbs. Recycle the browser every N successful
# acquisitions (relaunch ≈560 ms vs 90–215 s legacy launch-per-call).
_BROWSER_POOL_RECYCLE_ENV = "BALUFFO_BROWSER_POOL_RECYCLE_ACQUISITIONS"
_BROWSER_POOL_RECYCLE_DEFAULT = 20

# A health probe must be cheap and must not depend on a real board. A data URL
# exercises the browser's page/navigation path without network access; the
# marker lets the probe verify that the document actually rendered.
BROWSER_HEALTH_PROBE_URL = (
    "data:text/html,<html><body><div id='baluffo-browser-health'>"
    "baluffo-browser-health</div></body></html>"
)
BROWSER_HEALTH_PROBE_TIMEOUT_S = 5

# Capture limits for the rendered-JSON lane: a page's own data responses are small and few,
# and a cap keeps one pathological app from pinning the renderer's memory.
CAPTURE_SETTLE_MS = 1500
CAPTURED_PAYLOAD_LIMIT = 24
CAPTURED_BODY_BYTE_LIMIT = 4 * 1024 * 1024


class _PoolMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.values: dict[str, Any] = {
            "pool_startup_ms": 0,
            "pool_acquisitions": 0,
            "pool_relaunch_count": 0,
            "last_failure_stage": "",
            "last_failure_at": "",
            "last_launch_error": "",
            "last_launch_error_type": "",
            "last_navigation_error": "",
            "last_navigation_error_type": "",
        }

    def incr(self, key: str, delta: int = 1) -> None:
        with self._lock:
            self.values[key] = int(self.values.get(key) or 0) + delta

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self.values[key] = value

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self.values)


def browser_pool_enabled(env: dict[str, str] | None = None) -> bool:
    values = os.environ if env is None else env
    raw = str(values.get("BALUFFO_BROWSER_POOL") or "").strip().lower()
    return raw not in _BROWSER_POOL_DISABLE_VALUES


def browser_pool_recycle_acquisitions(env: dict[str, str] | None = None) -> int:
    """Acquisitions after which the pooled browser is relaunched (0 = never)."""
    values = os.environ if env is None else env
    raw = str(values.get(_BROWSER_POOL_RECYCLE_ENV) or "").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return _BROWSER_POOL_RECYCLE_DEFAULT


class BrowserFallbackPool:
    """Lazy single-browser pool; fresh BrowserContext per fetch call."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._playwright: Any = None
        self._browser: Any = None
        self._available = True
        self._start_lock = threading.Lock()
        self._diagnostics_lock = threading.Lock()
        self._last_launch_error = ""
        self._last_launch_error_type = ""
        self._last_navigation_error = ""
        self._last_navigation_error_type = ""
        self._last_failure_stage = ""
        self._last_failure_at = ""
        self.metrics = _PoolMetrics()
        self._recycle_after = browser_pool_recycle_acquisitions()
        self._acquisitions_since_launch = 0
        atexit.register(self._atexit_close)

    def _ensure_started(self) -> None:
        if not self._available:
            # This is a pool-lifecycle error, not a Playwright launch error; recording it
            # here would overwrite the real launch/navigation failure that closed the pool.
            raise RuntimeError("browser fallback unavailable (browser has been closed)")
        if self._browser is not None and self._loop is not None:
            return
        with self._start_lock:
            if self._browser is not None and self._loop is not None:
                return
            if self._loop is None:
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
                except BaseException as exc:
                    self._record_failure("launch", exc)
                    self._hard_close_locked()
                    raise
                self.metrics.incr("pool_startup_ms", int((time.monotonic() - t0) * 1000))
            else:
                # planned recycle: loop alive, browser dropped — relaunch only.
                future = asyncio.run_coroutine_threadsafe(self._start_browser(), self._loop)
                try:
                    future.result(timeout=120)
                except BaseException as exc:
                    self._record_failure("launch", exc)
                    self._hard_close_locked()
                    raise

    def _maybe_recycle_browser(self) -> None:
        """Relaunch the pooled browser after N acquisitions (0 disables)."""
        limit = self._recycle_after
        if limit <= 0 or self._acquisitions_since_launch < limit:
            return
        with self._start_lock:
            if (
                self._acquisitions_since_launch < limit
                or self._browser is None
                or self._loop is None
            ):
                return
            browser = self._browser
            playwright = self._playwright
            self._browser = None
            self._playwright = None
            self._acquisitions_since_launch = 0
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._shutdown(browser, playwright), self._loop
            )
            future.result(timeout=15)
        except BaseException:
            pass
        self.metrics.incr("pool_relaunch_count")
        # Next fetch() re-launches a fresh browser via the loop-alive branch of
        # _ensure_started; concurrent callers block on the start lock meanwhile.

    async def _start_browser(self) -> None:
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        # ponytail: bound pooled-Chromium footprint inside tight cgroups —
        # renderer pages are separate processes the body-cap cannot reach;
        # heavy SPA renders were pinning 2.5g seats during full-coverage fetch.
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=browser_fallback_chromium_args(),
        )

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
                    self._record_failure("navigation", exc)
                    if self._browser_death_marker(exc):
                        self._mark_unavailable()
                    raise
                html = await page.content() or ""
                # ponytail: cap browser-fetched HTML like the httpx path; outlier
                # listings (603 MiB) would otherwise pin the pool browser's renderer.
                try:
                    from src.jobs.common.http import fetch_max_bytes_for_url

                    cap = fetch_max_bytes_for_url(url)
                except Exception:
                    cap = 20 * 1024 * 1024
                if len(html) > cap:
                    html = html[:cap]
                return html
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

    def _record_failure(self, stage: str, exc: BaseException) -> None:
        """Keep the original Playwright error before the breaker normalizes it.

        ``normalize_browser_fallback_error`` intentionally collapses transport
        failures to one stable token, which is right for circuit-breaker policy
        but erases whether Chromium failed to launch or the page failed to
        navigate. The raw message and type are retained here for the health
        probe/run report; they never replace the normalized fallback contract.
        """
        message = str(exc).strip() or type(exc).__name__
        error_type = type(exc).__name__
        stamp = now_iso()
        with self._diagnostics_lock:
            self._last_failure_stage = stage
            self._last_failure_at = stamp
            if stage == "launch":
                self._last_launch_error = message
                self._last_launch_error_type = error_type
            elif stage == "navigation":
                self._last_navigation_error = message
                self._last_navigation_error_type = error_type
        self.metrics.set("last_failure_stage", stage)
        self.metrics.set("last_failure_at", stamp)
        if stage == "launch":
            self.metrics.set("last_launch_error", message)
            self.metrics.set("last_launch_error_type", error_type)
        elif stage == "navigation":
            self.metrics.set("last_navigation_error", message)
            self.metrics.set("last_navigation_error_type", error_type)

    def _failure_snapshot(self) -> dict[str, Any]:
        with self._diagnostics_lock:
            return {
                "lastFailureStage": self._last_failure_stage,
                "lastFailureAt": self._last_failure_at,
                "lastLaunchError": self._last_launch_error,
                "lastLaunchErrorType": self._last_launch_error_type,
                "lastNavigationError": self._last_navigation_error,
                "lastNavigationErrorType": self._last_navigation_error_type,
            }

    async def _probe_page(self, timeout_s: int) -> None:
        """Render a local marker page on the live browser (no network)."""
        browser = self._browser
        if browser is None:
            raise RuntimeError("browser is not started")
        context = await browser.new_context()
        try:
            page = await context.new_page()
            try:
                await page.goto(
                    BROWSER_HEALTH_PROBE_URL,
                    wait_until="domcontentloaded",
                    timeout=max(1, int(timeout_s)) * 1000,
                )
                rendered = await page.text_content("#baluffo-browser-health") or ""
                if "baluffo-browser-health" not in rendered:
                    raise RuntimeError("browser health probe rendered no marker")
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

    def _health_result(
        self,
        *,
        ok: bool,
        stage: str,
        started: float,
        exc: BaseException | None = None,
    ) -> dict[str, Any]:
        result = {
            "ok": bool(ok),
            "stage": stage,
            "error": "" if ok else (str(exc).strip() if exc is not None else ""),
            "errorType": "" if ok else (type(exc).__name__ if exc is not None else ""),
            "durationMs": max(0, int((time.monotonic() - started) * 1000)),
            "probeUrl": BROWSER_HEALTH_PROBE_URL,
            **self._failure_snapshot(),
        }
        self.metrics.set("last_health_probe", dict(result))
        return result

    def health_probe(self, timeout_s: int = BROWSER_HEALTH_PROBE_TIMEOUT_S) -> dict[str, Any]:
        """Check whether Chromium can launch and navigate, and preserve either error.

        The probe is deliberately independent of any board URL so a failed
        fallback can distinguish a broken browser environment from a page that
        merely failed to load. It re-launches after a browser-death marker,
        because reporting the marker itself would not answer whether the
        environment is still usable.
        """
        started = time.monotonic()
        effective_timeout_s = max(1, min(int(timeout_s or BROWSER_HEALTH_PROBE_TIMEOUT_S), 30))
        self.metrics.incr("pool_health_probes")
        if not self._available:
            # _hard_close_locked assumes _start_lock. Re-check under the lock so a
            # concurrent close/restart is not torn down by a stale availability read.
            with self._start_lock:
                if not self._available:
                    self._hard_close_locked()
        try:
            self._ensure_started()
        except BaseException as exc:
            return self._health_result(ok=False, stage="launch", started=started, exc=exc)
        loop = self._loop
        if loop is None:
            loop_exc = RuntimeError("browser pool event loop not started")
            self._record_failure("launch", loop_exc)
            return self._health_result(ok=False, stage="launch", started=started, exc=loop_exc)
        try:
            future = asyncio.run_coroutine_threadsafe(self._probe_page(effective_timeout_s), loop)
            future.result(timeout=effective_timeout_s + 5)
        except BaseException as exc:
            self._record_failure("navigation", exc)
            if self._browser_death_marker(exc):
                self._mark_unavailable()
            return self._health_result(ok=False, stage="navigation", started=started, exc=exc)
        return self._health_result(ok=True, stage="ready", started=started)

    async def _capture_json_response(self, response: Any, captured: list[tuple[str, str]]) -> None:
        """Record one JSON response worth keeping. A failed sub-request is not a failure.

        The render may still be worth reading and the other payloads are still worth keeping,
        so every failure here returns rather than propagating.
        """
        if len(captured) >= CAPTURED_PAYLOAD_LIMIT:
            return
        try:
            if int(getattr(response, "status", 0) or 0) != 200:
                return
            headers = response.headers or {}
            if "json" not in str(headers.get("content-type", "")).lower():
                return
            body = await response.body()
        except Exception:
            return
        if not body or len(body) > CAPTURED_BODY_BYTE_LIMIT:
            return
        captured.append((str(response.url), body.decode("utf-8", errors="replace")))

    async def _settle_page(self, page: Any, settle_ms: int) -> None:
        """Let the page's own data requests land.

        This is what the plain render lacks: it returns on ``domcontentloaded``, before the
        request a single-page board actually renders from has been made. Network-idle is
        best-effort because a chatty page never reaches it, hence the fixed wait as fallback.
        """
        if int(settle_ms) <= 0:
            return
        try:
            await page.wait_for_load_state("networkidle", timeout=max(1, int(settle_ms)) * 2)
        except Exception:
            try:
                await page.wait_for_timeout(max(0, int(settle_ms)))
            except Exception:
                pass

    async def _fetch_captured(
        self, url: str, timeout_s: int, *, settle_ms: int
    ) -> tuple[str, list[tuple[str, str]]]:
        """Render ``url`` and return its HTML plus the JSON responses it fetched.

        A single-page app's openings are not in its markup -- they arrive in the XHR the page
        makes after load, which is why the existing render finds almost nothing on those
        boards. Capturing the responses is the host-agnostic way to reach them: no per-platform
        parser, and a board that changes platform keeps working.
        """
        captured: list[tuple[str, str]] = []
        context = await self._browser.new_context()
        try:
            page = await context.new_page()
            listener = lambda response: self._capture_json_response(response, captured)
            page.on("response", listener)
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
                await self._settle_page(page, settle_ms)
                html = await page.content() or ""
            finally:
                try:
                    page.remove_listener("response", listener)
                except Exception:
                    pass
                try:
                    await page.close()
                except Exception:
                    pass
            return self._cap_html(html, url), captured
        finally:
            try:
                await context.close()
            except Exception:
                pass

    @staticmethod
    def _cap_html(html: str, url: str) -> str:
        """Cap browser-fetched HTML like the httpx path.

        ponytail: outlier listings (603 MiB) would otherwise pin the pool browser's renderer,
        and a captured-payload run reads the body into memory besides.
        """
        try:
            from src.jobs.common.http import fetch_max_bytes_for_url

            cap = fetch_max_bytes_for_url(url)
        except Exception:
            cap = 20 * 1024 * 1024
        return html[:cap] if len(html) > cap else html

    def fetch(self, url: str, timeout_s: int) -> tuple[str, str]:
        """Sync entry-point for worker threads; returns (html, error)."""
        html, error, _captured = self.fetch_captured(url, timeout_s, settle_ms=0)
        return html, error

    def fetch_captured(
        self, url: str, timeout_s: int, *, settle_ms: int = CAPTURE_SETTLE_MS
    ) -> tuple[str, str, list[tuple[str, str]]]:
        """Sync entry-point returning ``(html, error, [(payload_url, body), ...])``.

        Payloads are returned even when the HTML is empty: a board whose listings live
        entirely in an XHR is the case this exists for, and an empty render is its normal
        outcome rather than a failure.
        """
        self._maybe_recycle_browser()
        try:
            self._ensure_started()
        except BaseException as exc:
            return "", normalize_browser_fallback_error(str(exc)), []
        try:
            loop = self._loop
            if loop is None:
                raise RuntimeError("browser pool event loop not started")
            future = asyncio.run_coroutine_threadsafe(
                self._fetch_captured(url, timeout_s, settle_ms=settle_ms), loop
            )
            html, captured = future.result(timeout=max(1, int(timeout_s)) + 30)
        except BaseException as exc:
            self._record_failure("navigation", exc)
            return "", normalize_browser_fallback_error(str(exc)), []
        self.metrics.incr("pool_acquisitions")
        with self._start_lock:
            self._acquisitions_since_launch += 1
        if not html:
            return "", "browser fallback returned empty content", captured
        return html, "", captured

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
        for task in list(asyncio.all_tasks()):
            if task is not asyncio.current_task():
                task.cancel()

    def _atexit_close(self) -> None:
        try:
            self.close()
        except BaseException:
            pass


__all__ = ["BrowserFallbackPool", "browser_pool_enabled"]
