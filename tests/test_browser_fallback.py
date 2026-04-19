from __future__ import annotations

from src.jobs import state as jobs_state
from src.jobs.browser_fallback import (
    BROWSER_FALLBACK_STATE_KEY,
    BrowserFallbackCircuitBreaker,
)


def test_browser_fallback_circuit_breaker_short_circuits_after_environment_failure() -> None:
    calls: list[str] = []
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=15)

    def fake_try_playwright(url: str, timeout_s: int) -> tuple[str, str]:
        calls.append(url)
        return "", "browser fallback unavailable (playwright is not installed)"

    wrapped = breaker.wrap(fake_try_playwright)

    html1, error1 = wrapped("https://example.com/jobs", 5)
    html2, error2 = wrapped("https://example.com/jobs-2", 5)

    assert html1 == ""
    assert "browser fallback unavailable" in error1
    assert html2 == ""
    assert "cooldown active" in error2
    assert calls == ["https://example.com/jobs"]


def test_browser_fallback_state_roundtrip_preserves_cooldown_fields() -> None:
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=15)
    breaker.wrap(lambda _url, _timeout: ("", "browser fallback unavailable (spawn EPERM)"))(
        "https://example.com/jobs", 5
    )
    row = breaker.to_state_row()
    assert row["browserFallbackFailureCount"] == 1
    assert "browserFallbackQuarantinedUntilAt" in row

    restored = BrowserFallbackCircuitBreaker.from_state(
        {BROWSER_FALLBACK_STATE_KEY: row}, cooldown_minutes=15
    )
    assert restored.failure_count == 1
    assert restored.disabled_until_at == row["browserFallbackQuarantinedUntilAt"]


def test_browser_escalation_state_roundtrip_preserves_guard_fields() -> None:
    normalized = jobs_state.normalize_source_state_payload(
        {
            "sources": {
                "crystal_dynamics": {
                    "browserEscalationEligible": True,
                    "browserEscalationEligibleAt": "2026-03-23T10:00:00Z",
                    "browserEscalationEligibilityReason": "js_required",
                    "browserEscalationLastAttemptAt": "2026-03-23T11:00:00Z",
                    "browserEscalationLastAttemptFingerprint": "fp-a",
                    "browserEscalationLastAttemptListingFingerprint": "listing-a",
                    "browserEscalationLastFailureAt": "2026-03-23T11:00:00Z",
                    "browserEscalationFailureCount": 1,
                }
            }
        },
        updated_at="2026-03-23T12:00:00Z",
    )
    source_row = normalized["sources"]["crystal_dynamics"]
    assert source_row["browserEscalationEligible"] is True
    assert source_row["browserEscalationEligibilityReason"] == "js_required"
    assert source_row["browserEscalationFailureCount"] == 1

    assert source_row["browserEscalationLastAttemptFingerprint"] == "fp-a"
    assert source_row["browserEscalationLastAttemptListingFingerprint"] == "listing-a"
    assert source_row["browserEscalationLastAttemptAt"] == "2026-03-23T11:00:00Z"


def test_browser_escalation_state_update_remembers_attempts_and_successes() -> None:
    finished_at = "2026-03-23T12:30:00Z"
    zero_report = {
        "name": "crystal_dynamics",
        "status": "ok",
        "adapter": "static",
        "fetchedCount": 2,
        "keptCount": 0,
        "error": "no jobs extracted from source pages",
        "classification": "js_required",
        "browserEscalationEligible": True,
        "browserEscalationEligibilityReason": "js_required",
        "browserEscalationEnabled": True,
        "listingFingerprint": "listing-a",
        "sourceFingerprint": "",
        "details": [],
    }
    state_rows = jobs_state.update_source_state_rows(
        source_state_rows={"crystal_dynamics": {}},
        source_reports=[zero_report],
        canonical_rows=[],
        finished_at=finished_at,
        circuit_breaker_failures=0,
        circuit_breaker_cooldown_minutes=30,
    )
    entry = state_rows["crystal_dynamics"]
    assert entry["browserEscalationEligible"] is True
    assert entry["browserEscalationLastAttemptAt"] == finished_at
    assert entry["browserEscalationLastAttemptListingFingerprint"] == "listing-a"
    assert entry["browserEscalationFailureCount"] == 1
    assert entry["browserEscalationLastAttemptAt"] == finished_at

    success_finished_at = "2026-03-23T12:45:00Z"
    success_report = {
        "name": "crystal_dynamics",
        "status": "ok",
        "adapter": "static",
        "fetchedCount": 2,
        "keptCount": 3,
        "error": "",
        "classification": "ok_with_jobs",
        "browserEscalationEligible": False,
        "browserEscalationEnabled": True,
        "listingFingerprint": "listing-b",
        "sourceFingerprint": "fp-b",
        "details": [],
    }
    success_state_rows = jobs_state.update_source_state_rows(
        source_state_rows={"crystal_dynamics": dict(entry)},
        source_reports=[success_report],
        canonical_rows=[{"source": "crystal_dynamics"}],
        finished_at=success_finished_at,
        circuit_breaker_failures=0,
        circuit_breaker_cooldown_minutes=30,
    )
    success_entry = success_state_rows["crystal_dynamics"]
    assert success_entry["browserEscalationLastSuccessAt"] == success_finished_at
    assert success_entry["browserEscalationFailureCount"] == 0
    assert "browserEscalationEligible" not in success_entry
