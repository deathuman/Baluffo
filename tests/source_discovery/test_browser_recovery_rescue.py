from __future__ import annotations

from typing import Any

from src.source_discovery import browser_recovery


def test_rescued_failed_fetch_urls_only_counts_failed_fetch_candidates() -> None:
    results = [
        (
            {"reasonDetail": "browser_recovery_fetch_failed", "url": "https://a.example/careers"},
            "<html>ok</html>",
            "",
            4,
        ),
        ({"reasonDetail": "js_shell", "url": "https://b.example/jobs"}, "<html>ok</html>", "", 4),
        (
            {"reasonDetail": "browser_recovery_fetch_failed", "url": "https://c.example/jobs"},
            "",
            "timeout",
            2,
        ),
        (
            {"reasonDetail": "browser_recovery_fetch_failed", "url": "https://a.example/careers"},
            "<html>ok</html>",
            "",
            3,
        ),
    ]

    assert browser_recovery.rescued_failed_fetch_urls(results) == ["https://a.example/careers"]


def test_run_browser_recovery_batch_counts_rescued_failures(monkeypatch) -> None:
    rows = [
        {"reasonDetail": "browser_recovery_fetch_failed", "url": "https://studio.example/"},
        {"reasonDetail": "js_shell", "url": "https://other.example/"},
    ]

    async def fake_fetch(_rows, *, timeout_s, browser_fetcher, concurrency):
        return [
            (rows[0], "<html>studio</html>", "", 5),
            (rows[1], "<html>other</html>", "", 5),
        ]

    def fake_analysis(
        _fetch_results: list[browser_recovery.BrowserFetchResult],
        _browser_recovery: dict[str, Any],
        _processed: set[str],
    ) -> browser_recovery.BrowserRecoveryAnalysis:
        return browser_recovery.BrowserRecoveryAnalysis(
            all_candidates=[],
            rendered_probe_results=[],
        )

    monkeypatch.setattr(browser_recovery, "fetch_browser_recovery_pages_async", fake_fetch)
    state: dict[str, Any] = {}

    batch = browser_recovery.run_browser_recovery_batch(
        selected=rows,
        processed=set(),
        browser_recovery=state,
        timeout_s=5,
        fetcher=lambda *_args: "",
        browser_fetcher=lambda *_args: ("", ""),
        concurrency=2,
        analyze_fetches=fake_analysis,
    )

    assert batch.rescued_urls == ["https://studio.example/"]
    assert state["resolvedFetchCount"] == 1
