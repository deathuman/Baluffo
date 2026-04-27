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


def test_run_browser_recovery_batch_calls_fetch_and_analysis_once(monkeypatch) -> None:
    fetch_calls: list[tuple[list[dict[str, str]], int, int]] = []
    analysis_calls: list[tuple[list[browser_recovery.BrowserFetchResult], set[str]]] = []

    async def fake_fetch(
        rows: list[dict[str, str]],
        *,
        timeout_s: int,
        browser_fetcher,
        concurrency: int,
    ) -> list[browser_recovery.BrowserFetchResult]:
        fetch_calls.append((rows, timeout_s, concurrency))
        return [(rows[0], "<html>jobs</html>", "", 12)]

    def fake_analysis(
        fetch_results: list[browser_recovery.BrowserFetchResult],
        _browser_recovery: dict[str, object],
        processed: set[str],
    ) -> browser_recovery.BrowserRecoveryAnalysis:
        processed.add("url:https://studio.example/jobs")
        analysis_calls.append((fetch_results, processed))
        return browser_recovery.BrowserRecoveryAnalysis(
            all_candidates=[{"adapter": "greenhouse", "slug": "studio"}],
            rendered_probe_results=[],
        )

    monkeypatch.setattr(browser_recovery, "fetch_browser_recovery_pages_async", fake_fetch)

    async def fake_probe(*_args, **_kwargs):
        return []

    monkeypatch.setattr(browser_recovery, "probe_candidates_async", fake_probe)

    batch = browser_recovery.run_browser_recovery_batch(
        selected=[{"url": "https://studio.example/jobs"}],
        processed=set(),
        browser_recovery={},
        timeout_s=7,
        fetcher=lambda *_args: "",
        browser_fetcher=lambda *_args: ("", ""),
        concurrency=3,
        analyze_fetches=fake_analysis,
        emit_log=None,
    )

    assert fetch_calls == [([{"url": "https://studio.example/jobs"}], 7, 3)]
    assert len(analysis_calls) == 1
    assert batch.processed == {"url:https://studio.example/jobs"}
    assert batch.fetch_results == [
        ({"url": "https://studio.example/jobs"}, "<html>jobs</html>", "", 12)
    ]
    assert batch.analysis.all_candidates == [{"adapter": "greenhouse", "slug": "studio"}]


def test_run_browser_recovery_batch_skips_probe_for_rendered_validated_candidate(
    monkeypatch,
) -> None:
    probe_calls: list[object] = []
    rendered_result = ({"adapter": "static", "name": "Studio Jobs"}, True, 3, "", 0)

    async def fake_fetch(
        rows: list[dict[str, str]],
        *,
        timeout_s: int,
        browser_fetcher,
        concurrency: int,
    ) -> list[browser_recovery.BrowserFetchResult]:
        return [(rows[0], "<html>jobs</html>", "", 1)]

    def fake_probe(*args, **kwargs):
        probe_calls.append((args, kwargs))
        return []

    monkeypatch.setattr(browser_recovery, "fetch_browser_recovery_pages_async", fake_fetch)
    monkeypatch.setattr(browser_recovery, "probe_candidates_async", fake_probe)

    batch = browser_recovery.run_browser_recovery_batch(
        selected=[{"url": "https://studio.example/jobs"}],
        processed=set(),
        browser_recovery={},
        timeout_s=5,
        fetcher=lambda *_args: "",
        browser_fetcher=lambda *_args: ("", ""),
        concurrency=1,
        analyze_fetches=lambda _results, _state, _processed: (
            browser_recovery.BrowserRecoveryAnalysis(
                all_candidates=[rendered_result[0]],
                rendered_probe_results=[rendered_result],
            )
        ),
    )

    assert batch.probe_candidates == []
    assert batch.probe_results == []
    assert probe_calls == []


def test_run_browser_recovery_batch_probes_non_rendered_candidates(monkeypatch) -> None:
    probe_calls: list[tuple[list[dict[str, str]], int]] = []
    candidate = {"adapter": "greenhouse", "slug": "studio"}
    probe_result = (candidate, True, 2, "", 4)

    async def fake_fetch(
        rows: list[dict[str, str]],
        *,
        timeout_s: int,
        browser_fetcher,
        concurrency: int,
    ) -> list[browser_recovery.BrowserFetchResult]:
        return [(rows[0], "<html>jobs</html>", "", 1)]

    async def fake_probe(
        candidates: list[dict[str, str]],
        *,
        timeout_s: int,
        fetcher,
    ) -> list[tuple[dict[str, str], bool, int, str, int]]:
        probe_calls.append((candidates, timeout_s))
        return [probe_result]

    monkeypatch.setattr(browser_recovery, "fetch_browser_recovery_pages_async", fake_fetch)
    monkeypatch.setattr(browser_recovery, "probe_candidates_async", fake_probe)

    batch = browser_recovery.run_browser_recovery_batch(
        selected=[{"url": "https://studio.example/jobs"}],
        processed=set(),
        browser_recovery={},
        timeout_s=5,
        fetcher=lambda *_args: "",
        browser_fetcher=lambda *_args: ("", ""),
        concurrency=1,
        analyze_fetches=lambda _results, _state, _processed: (
            browser_recovery.BrowserRecoveryAnalysis(
                all_candidates=[candidate],
                rendered_probe_results=[],
            )
        ),
    )

    assert probe_calls == [([candidate], 5)]
    assert batch.probe_candidates == [candidate]
    assert batch.probe_results == [probe_result]


def test_run_browser_recovery_batch_can_use_separate_probe_timeout(monkeypatch) -> None:
    probe_timeouts: list[int] = []
    candidate = {"adapter": "greenhouse", "slug": "studio"}

    async def fake_fetch(
        rows: list[dict[str, str]],
        *,
        timeout_s: int,
        browser_fetcher,
        concurrency: int,
    ) -> list[browser_recovery.BrowserFetchResult]:
        assert timeout_s == 3
        return [(rows[0], "<html>jobs</html>", "", 1)]

    async def fake_probe(
        _candidates: list[dict[str, str]],
        *,
        timeout_s: int,
        fetcher,
    ) -> list[tuple[dict[str, str], bool, int, str, int]]:
        probe_timeouts.append(timeout_s)
        return [(candidate, True, 2, "", 4)]

    monkeypatch.setattr(browser_recovery, "fetch_browser_recovery_pages_async", fake_fetch)
    monkeypatch.setattr(browser_recovery, "probe_candidates_async", fake_probe)

    browser_recovery.run_browser_recovery_batch(
        selected=[{"url": "https://studio.example/jobs"}],
        processed=set(),
        browser_recovery={},
        timeout_s=3,
        probe_timeout_s=11,
        fetcher=lambda *_args: "",
        browser_fetcher=lambda *_args: ("", ""),
        concurrency=1,
        analyze_fetches=lambda _results, _state, _processed: (
            browser_recovery.BrowserRecoveryAnalysis(
                all_candidates=[candidate],
                rendered_probe_results=[],
            )
        ),
    )

    assert probe_timeouts == [11]
