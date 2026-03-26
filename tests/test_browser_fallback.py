from __future__ import annotations

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
