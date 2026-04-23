from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.jobs.browser_fallback import (
    BROWSER_FALLBACK_STATE_KEY,
    BrowserFallbackCircuitBreaker,
)
from src.jobs.text_utils import clean_text


def apply_browser_escalation_state(
    entry: dict[str, Any],
    *,
    report: dict[str, Any],
    finished_at: str,
    circuit_breaker_cooldown_minutes: int,
) -> None:
    browser_eligible = bool(report.get("browserEscalationEligible"))
    browser_enabled = bool(report.get("browserEscalationEnabled"))
    browser_reason = clean_text(report.get("browserEscalationEligibilityReason"))
    if browser_eligible:
        entry["browserEscalationEligible"] = True
        entry["browserEscalationEligibleAt"] = finished_at
        if browser_reason:
            entry["browserEscalationEligibilityReason"] = browser_reason
    elif entry.get("browserEscalationEligible"):
        entry.pop("browserEscalationEligible", None)
        entry.pop("browserEscalationEligibleAt", None)
        entry.pop("browserEscalationEligibilityReason", None)

    if not browser_enabled:
        return
    attempt_fingerprint = clean_text(report.get("sourceFingerprint")) or clean_text(
        entry.get("lastFingerprint")
    )
    attempt_listing_fingerprint = clean_text(report.get("listingFingerprint")) or clean_text(
        entry.get("lastListingFingerprint")
    )
    entry["browserEscalationLastAttemptAt"] = finished_at
    if attempt_fingerprint:
        entry["browserEscalationLastAttemptFingerprint"] = attempt_fingerprint
    if attempt_listing_fingerprint:
        entry["browserEscalationLastAttemptListingFingerprint"] = attempt_listing_fingerprint
    if entry["lastStatus"] == "ok" and entry["lastKeptCount"] > 0:
        entry["browserEscalationLastSuccessAt"] = finished_at
        entry["browserEscalationFailureCount"] = 0
        for key in (
            "browserEscalationLastFailureAt",
            "browserEscalationLastError",
            "browserEscalationQuarantinedUntilAt",
            "browserEscalationEligible",
            "browserEscalationEligibleAt",
            "browserEscalationEligibilityReason",
        ):
            entry.pop(key, None)
        return
    entry["browserEscalationFailureCount"] = int(entry.get("browserEscalationFailureCount") or 0) + 1
    entry["browserEscalationLastFailureAt"] = finished_at
    entry["browserEscalationLastError"] = clean_text(report.get("error"))
    if circuit_breaker_cooldown_minutes > 0:
        entry["browserEscalationQuarantinedUntilAt"] = (
            datetime.now(UTC) + timedelta(minutes=circuit_breaker_cooldown_minutes)
        ).isoformat()


def browser_fallback_state_row(
    source_state_rows: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    if not isinstance(source_state_rows, dict):
        return {}
    entry = source_state_rows.get(BROWSER_FALLBACK_STATE_KEY)
    return dict(entry) if isinstance(entry, dict) else {}


def build_browser_fallback_circuit_breaker(
    source_state_rows: dict[str, dict[str, Any]] | None,
    *,
    cooldown_minutes: int,
) -> BrowserFallbackCircuitBreaker:
    return BrowserFallbackCircuitBreaker.from_state(
        source_state_rows, cooldown_minutes=cooldown_minutes
    )


def set_browser_fallback_state(
    source_state_rows: dict[str, dict[str, Any]],
    browser_state: dict[str, Any],
) -> None:
    if not isinstance(source_state_rows, dict):
        return
    row = dict(browser_state or {})
    if row:
        source_state_rows[BROWSER_FALLBACK_STATE_KEY] = row
    else:
        source_state_rows.pop(BROWSER_FALLBACK_STATE_KEY, None)
