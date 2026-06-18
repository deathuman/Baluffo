from __future__ import annotations

import pytest

from src.source_discovery import orchestrator_probe
from src.source_discovery.orchestrator_runtime import DiscoveryRunState


def test_browser_fallback_controls_disable_fallback_for_invalid_source_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _bad_source_state(*_args: object, **_kwargs: object) -> object:
        raise ValueError("invalid source state")

    monkeypatch.setattr(
        orchestrator_probe.BrowserFallbackCircuitBreaker,
        "from_state",
        _bad_source_state,
    )

    try_playwright, semaphore = orchestrator_probe._browser_fallback_controls(
        state=DiscoveryRunState()
    )

    assert try_playwright is None
    assert semaphore is None


def test_browser_fallback_controls_do_not_hide_unexpected_setup_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _unexpected_failure(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("unexpected browser fallback bug")

    monkeypatch.setattr(
        orchestrator_probe.BrowserFallbackCircuitBreaker,
        "from_state",
        _unexpected_failure,
    )

    with pytest.raises(RuntimeError, match="unexpected browser fallback bug"):
        orchestrator_probe._browser_fallback_controls(state=DiscoveryRunState())
