from __future__ import annotations

import asyncio
import time

from src.source_discovery import browser_recovery


def test_browser_recovery_processed_key_prefers_url_entry_then_name() -> None:
    assert (
        browser_recovery.browser_recovery_processed_key(
            {
                "url": "https://studio.example/jobs",
                "sourceDirectoryEntryUrl": "https://directory.example/studio",
                "name": "Studio",
            }
        )
        == "url:https://studio.example/jobs"
    )
    assert (
        browser_recovery.browser_recovery_processed_key(
            {"sourceDirectoryEntryUrl": "https://directory.example/studio", "name": "Studio"}
        )
        == "entry:https://directory.example/studio"
    )
    assert browser_recovery.browser_recovery_processed_key({"name": "Studio"}) == "Studio"


def test_select_unprocessed_candidates_skips_processed_and_respects_limit() -> None:
    rows = [
        {"url": "https://one.example/jobs"},
        {"url": "https://two.example/jobs"},
        {"url": "https://three.example/jobs"},
    ]
    state = {"processedKeys": ["url:https://one.example/jobs"]}

    selected, processed = browser_recovery.select_unprocessed_candidates(
        rows,
        browser_recovery=state,
        limit=1,
    )

    assert processed == {"url:https://one.example/jobs"}
    assert selected == [{"url": "https://two.example/jobs"}]


def test_browser_fetch_pages_async_honors_concurrency_and_durations() -> None:
    active = 0
    max_active = 0

    def fake_browser(url: str, _timeout_s: int) -> tuple[str, str]:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        time.sleep(0.01)
        active -= 1
        return f"<html>{url}</html>", ""

    results = asyncio.run(
        browser_recovery.fetch_browser_recovery_pages_async(
            [{"url": f"https://studio{index}.example/jobs"} for index in range(4)],
            timeout_s=5,
            browser_fetcher=fake_browser,
            concurrency=2,
        )
    )

    assert len(results) == 4
    assert max_active <= 2
    assert all(duration_ms >= 0 for _row, _html, _error, duration_ms in results)


def test_browser_recovery_samples_are_capped_and_state_updates_counts() -> None:
    state: dict[str, object] = {}
    for index in range(3):
        browser_recovery.append_fetch_sample(
            state,
            source_url=f"https://studio{index}.example/jobs",
            duration_ms=index,
            html="html",
            limit=2,
        )
        browser_recovery.append_failure_sample(
            state,
            {"url": f"https://studio{index}.example/jobs", "error": "blocked"},
            limit=2,
        )
    started = time.perf_counter()
    browser_recovery.update_browser_recovery_state(
        state,
        processed={"url:https://one.example/jobs"},
        started=started,
        candidate_count=3,
        fetchAttempts=2,
        activeCandidates=1,
    )

    assert len(state["fetchSamples"]) == 2
    assert len(state["failureSamples"]) == 2
    assert state["processedKeys"] == ["url:https://one.example/jobs"]
    assert state["processedCount"] == 1
    assert state["candidateCount"] == 3
    assert state["fetchAttempts"] == 2
    assert state["activeCandidates"] == 1
