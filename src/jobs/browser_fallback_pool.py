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

Crash handling: a death marker tears down the pooled browser and retries the
fetch once on a fresh one, bounded to two relaunches per death episode and
counted as ``pool_relaunch_on_death``. A successful acquisition resets that
relaunch budget, so two early deaths do not close the lane for the rest of the
run. The first death also earns one immediate health probe, capped per run,
to distinguish a broken replacement from a transient page failure. If that probe
still cannot render, the fetch returns the environment error immediately rather
than spending the second relaunch on a retry that cannot work. A browser that
keeps failing to launch closes the lane for the breaker's cooldown instead of
latching it for the run; the resulting error string matches
is_browser_fallback_environment_error tokens, so the existing
BrowserFallbackCircuitBreaker still owns the 30-min policy.

AI boundary owns: pooled browser lifetime and per-call context acquisition.
AI boundary implement in: this file for pool mechanics; fallback gating stays in browser_fallback.py.
AI boundary search before contracts: pipeline source loop, source execution stage, source_check_http.
AI boundary verify: `npm run lint:repo-guardrails` plus tests/test_browser_fallback_pool.py and tests/test_browser_fallback_death_breaker.py.
"""

from __future__ import annotations

import asyncio
import atexit
import os
import threading
import time
from typing import Any

from src.bridge.source_check_http import (
    is_browser_fallback_environment_error,
    normalize_browser_fallback_error,
)
from src.jobs.browser_death_forensics import (
    BrowserStderrSink,
    browser_forensics_enabled,
    browser_raw_stderr_chromium_args,
    browser_stderr_chromium_args,
    build_browser_forensics,
    capture_browser_environment,
    capture_browser_stderr_for_driver,
    cleanup_browser_stderr_file,
    prepare_browser_stderr_file,
    read_browser_stderr,
)
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
# A death marker earns one fresh browser and one retry, but a crash loop must not
# spend the run launching Chromium: two relaunches per death episode. A
# successful acquisition starts a new death episode and resets this budget.
_BROWSER_POOL_RELAUNCH_LIMIT = 2
# One health probe immediately after the first death distinguishes a broken
# replacement from a transient page failure. A probe is a browser launch, so cap
# the recovery probes for the run; unlike the relaunch budget, this is not reset
# by an acquisition.
_BROWSER_POOL_RECOVERY_PROBE_LIMIT = 1
# Consecutive launch failures before the lane closes. One failed start is a launch
# error the breaker handles; repeated failures mean the environment is unusable and
# the lane should idle rather than relaunch for every source.
_BROWSER_POOL_LAUNCH_FAILURE_LIMIT = 3
# Mirrors DEFAULT_BROWSER_FALLBACK_COOLDOWN_MINUTES: the pool never closes the lane
# for longer than the circuit breaker that owns fallback policy.
_BROWSER_POOL_UNAVAILABLE_COOLDOWN_S = 30 * 60.0

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
            "pool_relaunch_on_death": 0,
            "pool_recovery_probes": 0,
            "last_failure_stage": "",
            "last_failure_at": "",
            "last_launch_error": "",
            "last_launch_error_type": "",
            "last_navigation_error": "",
            "last_navigation_error_type": "",
            "last_disconnect_at": "",
            "last_disconnect_during_probe": False,
            "last_disconnect_intentional": False,
            "disconnect_count": 0,
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


class BrowserStageError(RuntimeError):
    """A browser failure tagged with the step that raised it.

    A ``new_context()``/``new_page()`` failure used to surface as ``navigation``,
    so a browser that died before any page existed read as a page that failed to
    load. The tag keeps the original exception (and its type) for the report.
    """

    def __init__(self, stage: str, error: BaseException) -> None:
        super().__init__(str(error).strip() or type(error).__name__)
        self.stage = stage
        self.original = error


def _stage_error(exc: BaseException) -> tuple[str, BaseException]:
    """Split a stage-tagged failure into its stage and the original exception."""
    if isinstance(exc, BrowserStageError):
        return exc.stage, exc.original
    return "navigation", exc


async def _open_page(browser: Any) -> tuple[Any, Any]:
    """A fresh browser context and page, tagged ``context`` when either fails.

    ``new_context()``/``new_page()`` fail before a page exists, so they are not
    navigation failures; the caller owns both handles and closes them.
    """
    try:
        context = await browser.new_context()
    except BaseException as exc:
        raise BrowserStageError("context", exc) from exc
    try:
        page = await context.new_page()
    except BaseException as exc:
        try:
            await context.close()
        except Exception:
            pass
        raise BrowserStageError("context", exc) from exc
    return context, page


class BrowserFallbackPool:
    """Lazy single-browser pool; fresh BrowserContext per fetch call."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._playwright: Any = None
        self._browser: Any = None
        # S2.3: no sticky availability latch. A death demands one relaunch; repeated
        # launch failures close the lane only for the breaker's cooldown.
        self._pending_relaunch = False
        self._relaunches_on_death = 0
        self._recovery_probes = 0
        self._consecutive_launch_failures = 0
        self._unavailable_until_monotonic = 0.0
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
        # S2.5: keep the death observation independent of the Playwright error. The
        # browser log sink is bounded and removed when the browser is closed.
        self._forensics_enabled = browser_forensics_enabled()
        self._browser_stderr_path = ""
        self._browser_stderr_sink: BrowserStderrSink | None = None
        self._browser_stderr_driver: dict[str, Any] = {}
        self._probe_active = False
        self._closing_browser = False
        self._disconnect_lock = threading.Lock()
        self._last_disconnect_at = ""
        self._last_disconnect_during_probe = False
        self._last_disconnect_intentional = False
        self._disconnect_count = 0
        self._browser_environment_baseline: dict[str, Any] | None = None
        # A death is followed by a relaunch that resets the live disconnect
        # snapshot and deletes the Chromium log. Preserve the death block so
        # the later health probe can still report what killed the browser.
        self._last_death_forensics: dict[str, Any] = {}
        atexit.register(self._atexit_close)

    def _ensure_started(self) -> None:
        self._raise_if_lane_closed()
        if (
            self._browser is not None
            and self._loop is not None
            and not self._pending_relaunch
            and self._browser_connected(self._browser)
        ):
            return
        with self._start_lock:
            self._raise_if_lane_closed()
            if self._browser is not None and self._loop is not None:
                if not self._pending_relaunch and self._browser_connected(self._browser):
                    return
                # S2.2: a non-None handle can be a closed browser (0.3.062's observed
                # failure was at new_page). Drop it and launch a fresh one below.
                self._remember_death_forensics()
                self._hard_close_locked()
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
                    self._note_launch_failure(exc)
                    raise
                self.metrics.incr("pool_startup_ms", int((time.monotonic() - t0) * 1000))
            else:
                # planned recycle: loop alive, browser dropped — relaunch only.
                future = asyncio.run_coroutine_threadsafe(self._start_browser(), self._loop)
                try:
                    future.result(timeout=120)
                except BaseException as exc:
                    self._note_launch_failure(exc)
                    raise
            self._note_launch_success()

    def _raise_if_lane_closed(self) -> None:
        remaining_s = self._unavailable_until_monotonic - time.monotonic()
        if remaining_s > 0:
            raise RuntimeError(
                "browser fallback unavailable (relaunch cooldown "
                f"{int(remaining_s + 0.999)}s remaining)"
            )

    @staticmethod
    def _browser_connected(browser: Any) -> bool:
        """A handle is usable only while Chromium still reports it connected."""
        is_connected = getattr(browser, "is_connected", None)
        if not callable(is_connected):
            return True
        try:
            return bool(is_connected())
        except BaseException:
            return False

    def _note_launch_success(self) -> None:
        self._pending_relaunch = False
        self._consecutive_launch_failures = 0
        self._unavailable_until_monotonic = 0.0

    def _note_launch_failure(self, exc: BaseException) -> None:
        """Count one failed start; close the lane only after repeated failures."""
        self._record_failure("launch", exc)
        self._consecutive_launch_failures += 1
        if self._consecutive_launch_failures >= _BROWSER_POOL_LAUNCH_FAILURE_LIMIT:
            self._unavailable_until_monotonic = (
                time.monotonic() + _BROWSER_POOL_UNAVAILABLE_COOLDOWN_S
            )
        # A failed launch can still have written crashpad/GPU lines to stderr. Keep
        # them before _hard_close_locked closes the sink and its driver.
        self._remember_death_forensics()
        self._hard_close_locked()

    def _relaunch_after_death(self) -> None:
        """Tear down a dead browser so the next ``_ensure_started`` launches fresh."""
        with self._start_lock:
            self._relaunches_on_death += 1
            self.metrics.incr("pool_relaunch_on_death")
            self._pending_relaunch = False
            self._remember_death_forensics()
            self._hard_close_locked()

    def _note_acquisition_success(self) -> None:
        """A served fetch ends the death episode that preceded it.

        The relaunch budget is per death episode, not per pool lifetime: without
        this reset two deaths early in a long run permanently spent the budget,
        so the next death closed the lane for the breaker's 30-minute cooldown.
        Recovery probes stay capped for the run because each one is a browser
        launch and two early deaths must not turn into an unbounded probe loop.
        """
        with self._start_lock:
            self._acquisitions_since_launch += 1
            self._relaunches_on_death = 0

    def _probe_after_death(self) -> dict[str, Any] | None:
        """Probe the replacement browser once after the first death of the run.

        The fetch loop uses this result: a replacement that still cannot render
        returns an environment error instead of spending the second relaunch on
        a retry with no evidence it can work.
        """
        with self._start_lock:
            if self._recovery_probes >= _BROWSER_POOL_RECOVERY_PROBE_LIMIT:
                return None
            self._recovery_probes += 1
        self.metrics.incr("pool_recovery_probes")
        try:
            return self.health_probe()
        except BaseException as exc:
            return {
                "ok": False,
                "stage": "probe",
                "error": str(exc).strip() or type(exc).__name__,
                "errorType": type(exc).__name__,
                "durationMs": 0,
            }

    @staticmethod
    def _probe_failure_error(health: dict[str, Any]) -> str:
        """Turn a failed replacement-probe result into a breaker-owned error."""
        detail = normalize_browser_fallback_error(str(health.get("error") or "").strip())
        if is_browser_fallback_environment_error(detail):
            return detail
        stage = str(health.get("stage") or "unknown").strip() or "unknown"
        return (
            "browser fallback unavailable "
            f"(post-death health probe failed at {stage}: {detail or 'no error detail'})"
        )

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

        self._reset_disconnect_snapshot()
        self._browser_environment_baseline = self._forensics_start()
        args = browser_fallback_chromium_args()
        self._close_browser_stderr_sink()
        cleanup_browser_stderr_file(self._browser_stderr_path)
        self._browser_stderr_path = ""
        stderr_path = ""
        sink: BrowserStderrSink | None = None
        if self._forensics_enabled:
            try:
                sink = BrowserStderrSink()
            except OSError:
                sink = None
            self._browser_stderr_sink = sink
        if sink is not None:
            raw_stderr = False
            with capture_browser_stderr_for_driver(sink) as driver_state:
                self._playwright = await async_playwright().start()
                raw_stderr = bool(driver_state.get("active"))
            # Keep whether the sink engaged, why not, and which module/version was
            # probed: an empty Chromium log file must say why it is empty.
            self._browser_stderr_driver = dict(driver_state)
            if raw_stderr:
                sink.detach_writer()
                args.extend(browser_raw_stderr_chromium_args())
            else:
                # A future Playwright may not expose the driver hooks. Keep the
                # old bounded Chromium log file as the fallback rather than losing
                # stderr evidence entirely.
                self._close_browser_stderr_sink()
        else:
            self._playwright = await async_playwright().start()
        if self._forensics_enabled and self._browser_stderr_sink is None:
            try:
                stderr_path = prepare_browser_stderr_file()
            except OSError:
                stderr_path = ""
            args.extend(browser_stderr_chromium_args(stderr_path))
        self._browser_stderr_path = stderr_path
        # ponytail: bound pooled-Chromium footprint inside tight cgroups —
        # renderer pages are separate processes the body-cap cannot reach;
        # heavy SPA renders were pinning 2.5g seats during full-coverage fetch.
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=args,
        )
        self._attach_browser_listener(self._browser)

    def _attach_browser_listener(self, browser: Any) -> None:
        """Record Chromium's own disconnected event without altering pool policy."""
        on = getattr(browser, "on", None)
        if not callable(on):
            return
        try:
            on("disconnected", self._on_browser_disconnected)
        except Exception:
            pass

    def _on_browser_disconnected(self, _browser: Any = None) -> None:
        self._record_disconnect(intentional=bool(self._closing_browser))

    def _reset_disconnect_snapshot(self) -> None:
        with self._disconnect_lock:
            self._last_disconnect_at = ""
            self._last_disconnect_during_probe = False
            self._last_disconnect_intentional = False
            self._disconnect_count = 0
        self.metrics.set("last_disconnect_at", "")
        self.metrics.set("last_disconnect_during_probe", False)
        self.metrics.set("last_disconnect_intentional", False)
        self.metrics.set("disconnect_count", 0)

    def _record_disconnect(self, *, intentional: bool) -> None:
        stamp = now_iso()
        with self._disconnect_lock:
            if self._last_disconnect_at:
                return
            self._last_disconnect_at = stamp
            self._last_disconnect_during_probe = bool(self._probe_active)
            self._last_disconnect_intentional = bool(intentional)
            self._disconnect_count += 1
        self.metrics.set("last_disconnect_at", stamp)
        self.metrics.set("last_disconnect_during_probe", bool(self._probe_active))
        self.metrics.set("last_disconnect_intentional", bool(intentional))
        self.metrics.set("disconnect_count", self._disconnect_count)

    def _record_disconnect_if_dead(self) -> None:
        browser = self._browser
        if browser is not None and not self._browser_connected(browser):
            self._record_disconnect(intentional=False)

    def _set_probe_active(self, active: bool) -> None:
        with self._disconnect_lock:
            self._probe_active = bool(active)

    def _disconnect_snapshot(self) -> dict[str, Any]:
        with self._disconnect_lock:
            return {
                "observed": bool(self._last_disconnect_at),
                "at": self._last_disconnect_at,
                "duringProbe": bool(self._last_disconnect_during_probe),
                "intentional": bool(self._last_disconnect_intentional),
                "count": int(self._disconnect_count),
            }

    def _forensics_start(self) -> dict[str, Any] | None:
        if not self._forensics_enabled:
            return None
        try:
            return capture_browser_environment()
        except Exception:
            return {"capturedAt": now_iso()}

    def _browser_stderr_snapshot(self) -> dict[str, Any]:
        """Prefer the live raw-stderr ring; keep the log-file fallback for old pools/tests."""
        sink = self._browser_stderr_sink
        snapshot: dict[str, Any] = {}
        if sink is not None:
            try:
                snapshot = sink.snapshot()
            except Exception:
                snapshot = {}
        if not snapshot:
            snapshot = read_browser_stderr(self._browser_stderr_path)
        driver = dict(self._browser_stderr_driver)
        if driver:
            snapshot["driver"] = driver
        return snapshot

    def _close_browser_stderr_sink(self) -> None:
        sink = self._browser_stderr_sink
        self._browser_stderr_sink = None
        if sink is not None:
            try:
                sink.close()
            except Exception:
                pass

    def _remember_death_forensics(self) -> None:
        """Preserve the death block before a relaunch clears its live evidence."""
        if not self._forensics_enabled:
            return
        self._record_disconnect_if_dead()
        after: dict[str, Any] = {}
        try:
            after = capture_browser_environment()
        except Exception:
            pass
        try:
            stderr = self._browser_stderr_snapshot()
        except Exception:
            stderr = {}
        try:
            forensics = build_browser_forensics(
                self._browser_environment_baseline,
                after,
                browser_stderr=stderr,
                disconnect=self._disconnect_snapshot(),
            )
        except Exception:
            return
        if not forensics.get("available"):
            return
        with self._diagnostics_lock:
            self._last_death_forensics = forensics

    def _forensics_snapshot(self, before: dict[str, Any] | None) -> dict[str, Any]:
        if not self._forensics_enabled:
            return {}
        with self._diagnostics_lock:
            death = dict(self._last_death_forensics)
        if death:
            return death
        after: dict[str, Any] = {}
        try:
            after = capture_browser_environment()
        except Exception:
            pass
        try:
            stderr = self._browser_stderr_snapshot()
        except Exception:
            stderr = {}
        try:
            return build_browser_forensics(
                before,
                after,
                browser_stderr=stderr,
                disconnect=self._disconnect_snapshot(),
            )
        except Exception:
            return {
                "available": False,
                "capturedAt": now_iso(),
                "cgroup": {},
                "shm": {},
                "browserStderr": stderr,
                "disconnect": self._disconnect_snapshot(),
            }

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
        # The observed container failure arrives as TargetClosedError from new_page;
        # its message tokens are not guaranteed, so match the type as well.
        if type(exc).__name__ == "TargetClosedError":
            return True
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
        """Record a death marker: this handle is dead, the lane is not.

        The old ``_available`` flag was itself a latch: one death made every later
        call raise until the probe or ``close()`` reset the pool. The next start now
        drops the dead handle and relaunches instead, bounded per pool.
        """
        with self._start_lock:
            self._pending_relaunch = True

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
            if stage in {"launch", "context"}:
                self._last_launch_error = message
                self._last_launch_error_type = error_type
            elif stage == "navigation":
                self._last_navigation_error = message
                self._last_navigation_error_type = error_type
        self.metrics.set("last_failure_stage", stage)
        self.metrics.set("last_failure_at", stamp)
        if stage in {"launch", "context"}:
            self.metrics.set("last_launch_error", message)
            self.metrics.set("last_launch_error_type", error_type)
        elif stage == "navigation":
            self.metrics.set("last_navigation_error", message)
            self.metrics.set("last_navigation_error_type", error_type)
        if stage in {"navigation", "context"}:
            # A TargetClosedError at new_page may arrive before the disconnected
            # callback. Record the same signal here so the health result is not
            # empty just because Playwright delivered the exception first.
            self._record_disconnect_if_dead()
            if self._browser_death_marker(exc):
                self._remember_death_forensics()

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
        context, page = await _open_page(browser)
        try:
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
        environment_before: dict[str, Any] | None = None,
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
        forensics = self._forensics_snapshot(environment_before)
        if forensics:
            result["forensics"] = forensics
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
        environment_before = self._forensics_start()
        self.metrics.incr("pool_health_probes")
        self._set_probe_active(True)
        try:
            try:
                self._ensure_started()
            except BaseException as exc:
                return self._health_result(
                    ok=False,
                    stage="launch",
                    started=started,
                    exc=exc,
                    environment_before=environment_before,
                )
            loop = self._loop
            if loop is None:
                loop_exc = RuntimeError("browser pool event loop not started")
                self._record_failure("launch", loop_exc)
                return self._health_result(
                    ok=False,
                    stage="launch",
                    started=started,
                    exc=loop_exc,
                    environment_before=environment_before,
                )
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._probe_page(effective_timeout_s), loop
                )
                future.result(timeout=effective_timeout_s + 5)
            except BaseException as exc:
                stage, error = _stage_error(exc)
                self._record_failure(stage, error)
                if self._browser_death_marker(error):
                    self._mark_unavailable()
                return self._health_result(
                    ok=False,
                    stage=stage,
                    started=started,
                    exc=error,
                    environment_before=environment_before,
                )
            return self._health_result(
                ok=True,
                stage="ready",
                started=started,
                environment_before=environment_before,
            )
        finally:
            self._set_probe_active(False)

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
        context, page = await _open_page(self._browser)
        try:
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
        while True:
            try:
                loop = self._loop
                if loop is None:
                    raise RuntimeError("browser pool event loop not started")
                future = asyncio.run_coroutine_threadsafe(
                    self._fetch_captured(url, timeout_s, settle_ms=settle_ms), loop
                )
                html, captured = future.result(timeout=max(1, int(timeout_s)) + 30)
                break
            except BaseException as exc:
                stage, error = _stage_error(exc)
                if (
                    self._browser_death_marker(error)
                    and self._relaunches_on_death < _BROWSER_POOL_RELAUNCH_LIMIT
                ):
                    # A new_context/new_page/goto failure on a closed browser is a
                    # death, not a board failure: relaunch and retry this fetch once.
                    self._relaunch_after_death()
                    try:
                        self._ensure_started()
                    except BaseException as launch_exc:
                        # _ensure_started already recorded the launch stage; a start
                        # failure is not a navigation failure.
                        return "", normalize_browser_fallback_error(str(launch_exc)), []
                    # A fresh browser may have launched into the same broken
                    # environment. Probe once, immediately, before spending the
                    # second relaunch on a retry that cannot render. A failed
                    # probe returns now; the breaker owns the cooldown policy.
                    probe_health = self._probe_after_death()
                    if probe_health is not None and not probe_health.get("ok"):
                        return "", self._probe_failure_error(probe_health), []
                    continue
                if self._browser_death_marker(error):
                    # Relaunch budget spent: idle the lane for the breaker's cooldown
                    # instead of launching Chromium once per remaining source.
                    with self._start_lock:
                        self._unavailable_until_monotonic = (
                            time.monotonic() + _BROWSER_POOL_UNAVAILABLE_COOLDOWN_S
                        )
                self._record_failure(stage, error)
                return "", normalize_browser_fallback_error(str(error)), []
        self.metrics.incr("pool_acquisitions")
        self._note_acquisition_success()
        if not html:
            return "", "browser fallback returned empty content", captured
        return html, "", captured

    def close(self) -> None:
        with self._start_lock:
            self._hard_close_locked()
            self._pending_relaunch = False
            self._relaunches_on_death = 0
            self._recovery_probes = 0
            self._consecutive_launch_failures = 0
            self._unavailable_until_monotonic = 0.0
        with self._diagnostics_lock:
            self._last_death_forensics = {}
        self._browser_environment_baseline = None

    def _hard_close_locked(self) -> None:
        loop = self._loop
        thread = self._thread
        self._loop = None
        self._thread = None
        if loop is None:
            self._close_browser_stderr_sink()
            cleanup_browser_stderr_file(self._browser_stderr_path)
            self._browser_stderr_path = ""
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
        self._closing_browser = True
        try:
            future = asyncio.run_coroutine_threadsafe(self._shutdown(browser, playwright), loop)
            future.result(timeout=10)
        except BaseException:
            pass
        finally:
            self._closing_browser = False
        self._close_browser_stderr_sink()
        cleanup_browser_stderr_file(self._browser_stderr_path)
        self._browser_stderr_path = ""
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
