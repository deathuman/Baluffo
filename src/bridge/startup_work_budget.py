"""First-paint work budget for the bridge's heavy startup work.

The bridge used to start its expensive startup work the moment the API object
existed: the overdue availability catch-up forked ten workers, the sync pull
parsed the source-registry artifact, and journal compaction rewrote evidence
files. All of it ran on its own threads *while* the page issued its first
request burst, so the request threads queued behind GIL-bound parses and store
writes and the window showed an empty shell for seconds.

This module makes the ordering explicit instead of accidental. Heavy work is
*admitted* only once the first request burst has gone quiet, so the first page is
answered before the startup machinery starts competing with it. A launch with no
page (headless, CLI, container smoke) still progresses: the barrier also opens
when the budget expires.

Admission is a barrier, not a delay: opening it is idempotent and the reason is
recorded (`request-quiet` or `budget-expired`), so the startup journal says why
heavy work began when it did. Deferred work is serialized on one thread, so the
heavy parse happens once and never in parallel with itself; an item that fails
the way a startup step can fail is recorded and skipped rather than taking the
queue down with it, while an unforeseen crash is recorded as `unexpected` and
still surfaces as a thread error: it must not be quiet.

AI boundary owns: the admission barrier, the deferred queue, and its metrics.
AI boundary implement in: this file for admission policy; the work itself stays in its owning service leaf.
AI boundary search before contracts: bridge accept loop, availability catch-up, entrypoint startup hooks.
AI boundary verify: `npm run lint:repo-guardrails` plus focused startup budget tests.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from typing import Any

from src.shared.utils import int_or_default

# How long heavy startup work waits for the first page before it is admitted
# anyway. Long enough to cover the page's first request burst on a cold start,
# short enough that a headless launch loses no meaningful time.
DEFAULT_BUDGET_MS = 2000

# Silence after the last accepted request that counts as "the burst is done".
# Must exceed the accept loop's poll interval (0.25 s) or the barrier would open
# between two requests of the same burst.
DEFAULT_QUIET_MS = 350

# Diagnostics override for the admission budget: `0` admits heavy work at bind
# (the pre-budget behavior, useful as a control measurement) and a large value
# holds it back longer.
BUDGET_ENV = "BALUFFO_STARTUP_WORK_BUDGET_MS"

_MAX_QUEUED_WORK = 32

# Deferred work failures are recorded, never raised: a broken item must not stop
# the work queued behind it.
_WORK_ERRORS = (OSError, RuntimeError, TypeError, ValueError)


class StartupWorkBudget:
    """Admission gate for heavy startup work, driven by the accept loop."""

    def __init__(
        self,
        *,
        budget_ms: int = DEFAULT_BUDGET_MS,
        quiet_ms: int = DEFAULT_QUIET_MS,
        clock: Callable[[], float] = time.perf_counter,
        metric_sink: Callable[[str, dict[str, Any] | None], None] | None = None,
    ) -> None:
        self._clock = clock
        self._budget_s = max(0.0, float(budget_ms) / 1000.0)
        self._quiet_s = max(0.0, float(quiet_ms) / 1000.0)
        self._metric_sink = metric_sink
        self._lock = threading.Lock()
        self._barrier = threading.Event()
        self._started_at: float | None = None
        self._last_request_at: float | None = None
        self._accepted_requests = 0
        self._reason = ""
        self._waited_ms = 0
        self._queue: list[tuple[str, Callable[[], Any]]] = []
        self._queued_labels: set[str] = set()
        self._worker: threading.Thread | None = None

    # -- admission -------------------------------------------------------

    def start(self) -> None:
        """Begin the budget window. Called once the listener is bound."""
        with self._lock:
            if self._started_at is None:
                self._started_at = self._clock()

    def is_started(self) -> bool:
        with self._lock:
            return self._started_at is not None

    def is_open(self) -> bool:
        return self._barrier.is_set()

    def note_accepted(self, accepted_count: int) -> bool:
        """Feed the cumulative accepted-request count; True when it opened the barrier."""
        now = self._clock()
        opened = False
        with self._lock:
            if self._barrier.is_set():
                return False
            if self._started_at is None:
                self._started_at = now
            count = max(0, int(accepted_count))
            if count > self._accepted_requests:
                self._accepted_requests = count
                self._last_request_at = now
            elif self._last_request_at is not None and now - self._last_request_at >= self._quiet_s:
                opened = self._open_locked("request-quiet", now)
            # The budget bounds admission even when requests never pause: a burst
            # that outlives it must not keep heavy work out forever.
            if not opened and now - float(self._started_at) >= self._budget_s:
                opened = self._open_locked("budget-expired", now)
        if opened:
            self._emit_opened()
        return bool(opened)

    def open(self, reason: str) -> bool:
        """Open the barrier explicitly (tests, or a page-reported first paint)."""
        with self._lock:
            if self._barrier.is_set():
                return False
            opened = self._open_locked(str(reason or "explicit"), self._clock())
        if opened:
            self._emit_opened()
        return bool(opened)

    def wait_for_admission(self, timeout_s: float | None = None) -> str:
        """Block until the barrier opens or the budget runs out.

        Returns `not-armed` when the listener never bound, so a caller that runs
        outside a served bridge is not delayed by a barrier nobody can open.
        """
        if not self.is_started():
            return "not-armed"
        if self._barrier.is_set():
            return "opened"
        timeout = self._budget_s if timeout_s is None else max(0.0, float(timeout_s))
        if self._barrier.wait(timeout=timeout):
            return "opened"
        with self._lock:
            opened = self._open_locked("wait-timeout", self._clock())
        if opened:
            self._emit_opened()
        return "budget-expired"

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "barrierOpen": self._barrier.is_set(),
                "reason": self._reason,
                "waitedMs": self._waited_ms,
                "acceptedRequests": self._accepted_requests,
                "quietMs": int(round(self._quiet_s * 1000)),
                "budgetMs": int(round(self._budget_s * 1000)),
                "pendingLabels": [label for label, _ in self._queue],
            }

    # -- deferred work ---------------------------------------------------

    def submit(self, label: str, work: Callable[[], Any]) -> bool:
        """Queue work behind the barrier. Returns False for a duplicate label."""
        key = str(label or "").strip()
        if not key or not callable(work):
            return False
        with self._lock:
            if key in self._queued_labels:
                return False
            if len(self._queue) >= _MAX_QUEUED_WORK:
                return False
            self._queued_labels.add(key)
            self._queue.append((key, work))
            if self._worker is None or not self._worker.is_alive():
                self._worker = threading.Thread(
                    target=self._run_worker,
                    name="baluffo-startup-work",
                    daemon=True,
                )
                self._worker.start()
        return True

    def _run_worker(self) -> None:
        self.wait_for_admission()
        while True:
            with self._lock:
                if not self._queue:
                    return
                label, work = self._queue.pop(0)
            started = self._clock()
            self._emit("desktop_bridge_deferred_startup_work_started", {"label": label})
            # `unexpected` is overwritten by `ok` or by the failure text; an
            # unforeseen exception leaves it in place, emits its row, and then
            # propagates so the traceback is not swallowed.
            outcome = "unexpected"
            try:
                work()
                outcome = "ok"
            except _WORK_ERRORS as exc:
                outcome = str(exc) or type(exc).__name__
            finally:
                self._emit(
                    "desktop_bridge_deferred_startup_work_finished",
                    {
                        "label": label,
                        "durationMs": int(round((self._clock() - started) * 1000)),
                        "ok": outcome == "ok",
                        "error": "" if outcome == "ok" else outcome,
                    },
                )

    def _open_locked(self, reason: str, now: float) -> bool:
        if self._barrier.is_set():
            return False
        self._reason = reason
        started_at = self._started_at if self._started_at is not None else now
        self._waited_ms = int(round((now - float(started_at)) * 1000))
        self._barrier.set()
        return True

    def _emit_opened(self) -> None:
        self._emit("desktop_bridge_first_paint_barrier_opened", self.snapshot())

    def _emit(self, event: str, payload: dict[str, Any] | None) -> None:
        sink = self._metric_sink
        if sink is None:
            return
        try:
            sink(event, payload)
        except _WORK_ERRORS:
            return


_BUDGET = StartupWorkBudget()


def _env_budget_ms(default: int) -> int:
    raw = str(os.environ.get(BUDGET_ENV) or "").strip()
    if not raw:
        return default
    return max(0, int_or_default(raw, default))


def configure_startup_work_budget(
    *,
    budget_ms: int | None = None,
    quiet_ms: int = DEFAULT_QUIET_MS,
    metric_sink: Callable[[str, dict[str, Any] | None], None] | None = None,
) -> StartupWorkBudget:
    """Install the process budget: called once, before the listener binds."""
    global _BUDGET
    resolved_budget = budget_ms if budget_ms is not None else _env_budget_ms(DEFAULT_BUDGET_MS)
    _BUDGET = StartupWorkBudget(
        budget_ms=resolved_budget,
        quiet_ms=quiet_ms,
        metric_sink=metric_sink,
    )
    return _BUDGET


def reset_startup_work_budget() -> StartupWorkBudget:
    """Drop any barrier/queue state so a test starts from a clean process."""
    return configure_startup_work_budget()


def start_startup_work_budget() -> None:
    _BUDGET.start()


def note_accepted_requests(accepted_count: int) -> bool:
    return _BUDGET.note_accepted(accepted_count)


def wait_for_first_paint(timeout_s: float | None = None) -> str:
    return _BUDGET.wait_for_admission(timeout_s=timeout_s)


def submit_startup_work(label: str, work: Callable[[], Any]) -> bool:
    return _BUDGET.submit(label, work)


def open_first_paint(reason: str = "explicit") -> bool:
    return _BUDGET.open(reason)


def startup_work_budget_snapshot() -> dict[str, Any]:
    return _BUDGET.snapshot()
