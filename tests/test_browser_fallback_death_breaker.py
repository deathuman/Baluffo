"""Death-probe sequencing and relaunch-budget tests for BrowserFallbackPool.

These drive ``fetch_captured`` with fake browsers so a death/retry cycle is
deterministic and fast. The pool's real browser launch is covered elsewhere.
"""

from __future__ import annotations

from src.jobs.browser_fallback import is_browser_fallback_environment_error
from src.jobs.browser_fallback_pool import BrowserFallbackPool


class _FakeBrowser:
    def __init__(self) -> None:
        self.closed = False

    def is_connected(self) -> bool:
        return not self.closed

    async def close(self) -> None:
        self.closed = True


class _TargetClosedError(Exception):
    """Playwright's death marker by type, without importing Playwright."""


def _install_fake_launcher(pool: BrowserFallbackPool) -> list[_FakeBrowser]:
    launches: list[_FakeBrowser] = []

    async def start() -> None:
        browser = _FakeBrowser()
        launches.append(browser)
        pool._browser = browser
        pool._playwright = None

    pool._start_browser = start  # type: ignore[method-assign]
    return launches


def _install_fetch_sequence(
    pool: BrowserFallbackPool,
    outcomes: list[str],
    events: list[str],
) -> None:
    remaining = list(outcomes)

    async def fetch(
        _url: str, _timeout_s: int, *, settle_ms: int
    ) -> tuple[str, list[tuple[str, str]]]:
        events.append("fetch")
        outcome = remaining.pop(0)
        if outcome == "death":
            raise _TargetClosedError("Target page, context or browser has been closed")
        return "<html>board</html>", []

    pool._fetch_captured = fetch  # type: ignore[method-assign, assignment]


def _install_probe(
    pool: BrowserFallbackPool,
    events: list[str],
    *,
    ok: bool = True,
    error: str = "",
) -> None:
    def probe(_timeout_s: int = 0) -> dict[str, object]:
        events.append("probe")
        return {
            "ok": ok,
            "stage": "ready" if ok else "navigation",
            "error": "" if ok else error,
            "durationMs": 1,
        }

    pool.health_probe = probe  # type: ignore[method-assign, assignment]


def test_first_death_probes_once_before_retry_and_does_not_probe_twice() -> None:
    pool = BrowserFallbackPool()
    launches = _install_fake_launcher(pool)
    events: list[str] = []
    # Two deaths on the first source, then a success. The second source dies once
    # but must not spend a second recovery probe.
    _install_fetch_sequence(pool, ["death", "death", "ok", "death", "ok"], events)
    _install_probe(pool, events)
    try:
        first = pool.fetch_captured("http://boards.test/first", 5, settle_ms=0)
        second = pool.fetch_captured("http://boards.test/second", 5, settle_ms=0)
        metrics = pool.metrics.snapshot()
        relaunch_budget = pool._relaunches_on_death
        unavailable_until = pool._unavailable_until_monotonic
    finally:
        pool.close()

    assert (first[0], first[1]) == ("<html>board</html>", "")
    assert (second[0], second[1]) == ("<html>board</html>", "")
    assert events[:3] == ["fetch", "probe", "fetch"], (
        "the first death must be probed before the retry, not after the run"
    )
    assert events.count("probe") == 1, "two deaths before a success share one probe budget"
    assert metrics["pool_recovery_probes"] == 1
    assert metrics["pool_relaunch_on_death"] == 3
    assert len(launches) == 4, "each death still relaunches, including the bounded probe"
    assert relaunch_budget == 0
    assert unavailable_until == 0.0, "two early deaths must not close the lane"


def test_failed_post_death_probe_returns_environment_error_without_second_relaunch() -> None:
    pool = BrowserFallbackPool()
    launches = _install_fake_launcher(pool)
    events: list[str] = []
    _install_fetch_sequence(pool, ["death", "ok"], events)
    _install_probe(
        pool,
        events,
        ok=False,
        error="browser health probe rendered no marker",
    )
    try:
        html, error, captured = pool.fetch_captured("http://boards.test/list", 5, settle_ms=0)
        metrics = pool.metrics.snapshot()
        relaunch_budget = pool._relaunches_on_death
    finally:
        pool.close()

    assert (html, captured) == ("", [])
    assert error == (
        "browser fallback unavailable "
        "(post-death health probe failed at navigation: "
        "browser health probe rendered no marker)"
    )
    assert is_browser_fallback_environment_error(error)
    assert events == ["fetch", "probe"], "a failed probe must not trigger the retry fetch"
    assert metrics["pool_recovery_probes"] == 1
    assert metrics["pool_relaunch_on_death"] == 1
    assert len(launches) == 2, "only the replacement launch may be spent"
    assert relaunch_budget == 1, "the second relaunch must remain unspent"


def test_success_resets_the_relaunch_budget_for_later_deaths() -> None:
    pool = BrowserFallbackPool()
    _install_fake_launcher(pool)
    events: list[str] = []
    # Three deaths separated by successful acquisitions. The old lifetime budget
    # closed the lane on the third death even though the browser had served twice.
    _install_fetch_sequence(
        pool,
        ["death", "ok", "death", "ok", "death", "ok"],
        events,
    )
    _install_probe(pool, events)
    results = []
    try:
        for index in range(3):
            results.append(pool.fetch_captured(f"http://boards.test/{index}", 5, settle_ms=0))
            assert pool._relaunches_on_death == 0
        unavailable_until = pool._unavailable_until_monotonic
    finally:
        pool.close()

    assert all(error == "" for _html, error, _captured in results)
    assert unavailable_until == 0.0, "a served acquisition must earn a new relaunch budget"
    assert events.count("probe") == 1, "the recovery-probe cap is per run, not per acquisition"
