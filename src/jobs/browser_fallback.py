"""Browser fallback circuit-breaker helpers for jobs adapters.

AI boundary owns: browser fallback circuit-breaker state and adapter fallback gating.
AI boundary implement in: this file for fallback circuit policy; browser execution lives in adapter/runtime callers.
AI boundary search before contracts: static runtime support, source execution loop, and browser fallback tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused browser fallback tests.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Any

from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.text_utils import clean_text
from src.shared.utils import now_iso

TryPlaywrightFn = Callable[[str, int], tuple[str, str]]

BROWSER_FALLBACK_STATE_KEY = "__browser_fallback__"
DEFAULT_BROWSER_FALLBACK_COOLDOWN_MINUTES = 30


def is_browser_fallback_environment_error(error_text: str) -> bool:
    text = clean_text(error_text).lower()
    if not text:
        return False
    tokens = (
        "browser fallback unavailable",
        "playwright is not installed",
        "spawn eperm",
        "permission denied",
        "access is denied",
        "operation not permitted",
        "failed to launch browser",
        "cannot launch browser",
        "could not find browser",
        "browser_type.launch",
        "executable doesn't exist",
        "executable does not exist",
        "worker spawn blocked",
        "write epipe",
        "broken pipe",
        "pipetransport",
        "target closed",
        "transport closed",
        "connection closed",
        "browser has been closed",
    )
    return any(token in text for token in tokens)


@dataclass
class BrowserFallbackCircuitBreaker:
    cooldown_minutes: int = DEFAULT_BROWSER_FALLBACK_COOLDOWN_MINUTES
    disabled_until_at: str = ""
    last_attempt_at: str = ""
    last_failure_at: str = ""
    last_success_at: str = ""
    last_error: str = ""
    failure_count: int = 0
    # Per-run demand accounting (saturation visibility, 2026-09-15): every
    # wrapped call is an attempt; the breaker refusing one in cooldown is
    # refused demand that used to surface only as a silent got_html=False
    # escalation log line. Attempts = refused + served; served splits into
    # with-html and empty outcomes.
    demand_attempts: int = 0
    demand_refused: int = 0
    demand_served_with_html: int = 0
    demand_served_empty: int = 0
    _lock: Lock = field(default_factory=Lock, repr=False, compare=False)

    @classmethod
    def from_state(
        cls,
        source_state_rows: dict[str, dict[str, Any]] | None,
        *,
        cooldown_minutes: int = DEFAULT_BROWSER_FALLBACK_COOLDOWN_MINUTES,
    ) -> BrowserFallbackCircuitBreaker:
        entry = {}
        if isinstance(source_state_rows, dict):
            raw = source_state_rows.get(BROWSER_FALLBACK_STATE_KEY)
            if isinstance(raw, dict):
                entry = raw
        return cls(
            cooldown_minutes=max(0, int(cooldown_minutes or 0)),
            disabled_until_at=clean_text(entry.get("browserFallbackQuarantinedUntilAt")),
            last_attempt_at=clean_text(entry.get("browserFallbackLastAttemptAt")),
            last_failure_at=clean_text(entry.get("browserFallbackLastFailureAt")),
            last_success_at=clean_text(entry.get("browserFallbackLastSuccessAt")),
            last_error=clean_text(entry.get("browserFallbackLastError")),
            failure_count=max(0, int(entry.get("browserFallbackFailureCount") or 0)),
        )

    def _disabled_until_dt(self) -> datetime | None:
        return parse_datetime(self.disabled_until_at)

    def is_available(self, *, now: datetime | None = None) -> bool:
        until = self._disabled_until_dt()
        if until is None:
            return True
        current = now or datetime.now(UTC)
        return until <= current

    def wrap(self, try_playwright: TryPlaywrightFn) -> TryPlaywrightFn:
        def _wrapped(url: str, timeout_s: int) -> tuple[str, str]:
            now_dt = datetime.now(UTC)
            stamp = now_iso()
            with self._lock:
                self.demand_attempts += 1
                if not self.is_available(now=now_dt):
                    self.last_attempt_at = stamp
                    self.demand_refused += 1
                    return "", "browser fallback unavailable (cooldown active)"
                self.last_attempt_at = stamp
            try:
                html, error = try_playwright(url, timeout_s)
            except (OSError, RuntimeError, ValueError) as exc:
                html = ""
                error = str(exc)
            if html and not clean_text(error):
                with self._lock:
                    self.last_success_at = stamp
                    self.last_error = ""
                    self.failure_count = 0
                    self.disabled_until_at = ""
                    self.demand_served_with_html += 1
                return html, ""
            if is_browser_fallback_environment_error(error):
                cooldown_minutes = max(0, int(self.cooldown_minutes or 0))
                with self._lock:
                    self.failure_count += 1
                    self.last_failure_at = stamp
                    self.last_error = clean_text(error)
                    if cooldown_minutes > 0:
                        self.disabled_until_at = (
                            now_dt + timedelta(minutes=cooldown_minutes)
                        ).isoformat()
                    self.demand_served_empty += 1
            else:
                with self._lock:
                    self.demand_served_empty += 1
            return html, clean_text(error)

        return _wrapped

    def to_state_row(self) -> dict[str, Any]:
        row: dict[str, Any] = {}
        failure_count = int(self.failure_count or 0)
        if failure_count > 0:
            row["browserFallbackFailureCount"] = failure_count
        if clean_text(self.disabled_until_at):
            row["browserFallbackQuarantinedUntilAt"] = clean_text(self.disabled_until_at)
        if clean_text(self.last_failure_at):
            row["browserFallbackLastFailureAt"] = clean_text(self.last_failure_at)
        if clean_text(self.last_error):
            row["browserFallbackLastError"] = clean_text(self.last_error)
        return row
