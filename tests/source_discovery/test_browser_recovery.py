from __future__ import annotations

import asyncio
import builtins
import time

from src.source_discovery import browser_recovery


def test_default_browser_fetcher_returns_fallback_when_bridge_unavailable(monkeypatch) -> None:
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "src.bridge.source_check_http":
            raise ImportError("missing playwright helper")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    html, error = browser_recovery.default_browser_fetcher()("https://example.com", 5)

    assert html == ""
    assert error == "browser fallback unavailable (playwright helper is not importable)"


def test_browser_recovery_candidate_row_reproduces_web_search_shape() -> None:
    row = browser_recovery.browser_recovery_candidate_row(
        adapter="web_search",
        discovery_method="web_search",
        name="Studio (Browser Recovery)",
        studio="Studio",
        company="Studio",
        url=" https://studio.example/jobs ",
        source_directory_entry_url=" https://studio.example/jobs ",
        nl_priority=True,
        reason_detail="js_shell",
        error="",
    )

    assert row == {
        "name": "Studio (Browser Recovery)",
        "studio": "Studio",
        "company": "Studio",
        "url": "https://studio.example/jobs",
        "sourceDirectoryEntryUrl": "https://studio.example/jobs",
        "nlPriority": True,
        "discoveryMethod": "web_search",
        "adapter": "web_search",
        "reasonDetail": "js_shell",
        "error": "",
    }


def test_browser_recovery_candidate_row_reproduces_gamedevmap_shape() -> None:
    row = browser_recovery.browser_recovery_candidate_row(
        adapter="gamedevmap",
        name="Studio browser recovery",
        studio="Studio",
        url="https://studio.example",
        source_directory_entry_url="https://www.gamedevmap.com/profile/studio",
        reason_detail="js_shell",
    )

    assert row == {
        "adapter": "gamedevmap",
        "name": "Studio browser recovery",
        "studio": "Studio",
        "url": "https://studio.example",
        "sourceDirectoryEntryUrl": "https://www.gamedevmap.com/profile/studio",
        "reasonDetail": "js_shell",
    }


def test_analyze_browser_recovery_fetch_results_routes_common_analysis_flow() -> None:
    browser_state: dict[str, object] = {}
    processed: set[str] = set()
    rendered_static = {"adapter": "static", "listing_url": "https://one.example/jobs"}
    provider = {"adapter": "greenhouse", "slug": "one"}
    probe_result = (rendered_static, True, 2, "", 0)

    def analyze_success(
        row: dict[str, object],
        source_url: str,
        html: str,
    ) -> browser_recovery.BrowserRecoveryPageAnalysis:
        assert row["studio"] == "One"
        assert source_url == "https://one.example/jobs"
        assert html == "<html>jobs</html>"
        provider["marked"] = True
        rendered_static["marked"] = True
        return browser_recovery.BrowserRecoveryPageAnalysis(
            all_candidates=[provider, rendered_static],
            rendered_static_candidates=[rendered_static],
        )

    def handle_failure(
        _row: dict[str, object],
        source_url: str,
        error: str,
        current_browser_state: dict[str, object],
    ) -> list[dict[str, object]]:
        browser_recovery.append_failure_sample(
            current_browser_state,
            {"url": source_url, "error": error},
        )
        return [{"reasonDetail": "browser_recovery_fetch_failed", "url": source_url}]

    analysis = browser_recovery.analyze_browser_recovery_fetch_results(
        fetch_results=[
            ({"studio": "One", "url": "https://one.example/jobs"}, "<html>jobs</html>", "", 12),
            ({"studio": "Two", "url": "https://two.example/jobs"}, "", "timeout", 3),
        ],
        browser_recovery=browser_state,
        processed=processed,
        analyze_success=analyze_success,
        handle_fetch_failure=handle_failure,
        rendered_static_probe_result=lambda candidate, _url, _html: (
            probe_result if candidate is rendered_static else None
        ),
        finalize_candidates=lambda candidates: (candidates, [{"reasonDetail": "bad_provider"}]),
    )

    assert processed == {"url:https://one.example/jobs", "url:https://two.example/jobs"}
    assert browser_state["fetchSamples"] == [
        {"url": "https://one.example/jobs", "durationMs": 12, "htmlBytes": 17}
    ]
    assert browser_state["failureSamples"] == [
        {"url": "https://two.example/jobs", "error": "timeout"}
    ]
    assert analysis.all_candidates == [provider, rendered_static]
    assert analysis.rendered_probe_results == [probe_result]
    assert analysis.fetch_failures == 1
    assert analysis.rejected_rows == [
        {"reasonDetail": "browser_recovery_fetch_failed", "url": "https://two.example/jobs"},
        {"reasonDetail": "bad_provider"},
    ]


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


def test_browser_recovery_merge_helpers_filter_count_and_update_state() -> None:
    rendered = [({"adapter": "static", "name": "Rendered"}, True, 2, "", 1)]
    probed = [
        ({"adapter": "greenhouse", "name": "Provider"}, True, 3, "", 2),
        ({"adapter": "static", "name": "Zero"}, True, 0, "", 3),
        ({"adapter": "static", "name": "Failed"}, False, 0, "timeout", 4),
    ]
    combined = browser_recovery.combine_probe_results(rendered, probed)
    state: dict[str, object] = {}
    started = time.perf_counter()

    positives = browser_recovery.positive_probe_candidates(
        combined,
        normalize_candidate=lambda candidate, jobs_found: {
            **candidate,
            "jobsFound": jobs_found,
        },
    )
    active_count = browser_recovery.count_recovered_candidates(
        [{"webSearchBrowserRecovery": True}, {"gamedevmapBrowserRecovery": True}, "bad"],
        lambda row: bool(row.get("webSearchBrowserRecovery")),
    )
    browser_recovery.update_browser_recovery_merge_state(
        state,
        processed={"url:https://one.example/jobs"},
        started=started,
        candidate_count=4,
        active_count=active_count,
        probe_candidate_count=2,
        rendered_static_validated=1,
        fetch_attempts=3,
        fetch_failures=1,
        candidate_analysis_count=2,
    )

    assert combined == [*rendered, *probed]
    assert positives == [
        {"adapter": "static", "name": "Rendered", "jobsFound": 2},
        {"adapter": "greenhouse", "name": "Provider", "jobsFound": 3},
    ]
    assert active_count == 1
    assert state["processedKeys"] == ["url:https://one.example/jobs"]
    assert state["candidateCount"] == 4
    assert state["activeCandidates"] == 1
    assert state["probeCandidates"] == 2
    assert state["renderedStaticValidated"] == 1
    assert state["fetchAttempts"] == 3
    assert state["fetchFailures"] == 1
    assert state["candidateAnalysisCount"] == 2


def test_merge_browser_recovery_results_applies_callbacks_and_updates_state() -> None:
    rendered = [({"adapter": "static", "name": "Rendered"}, True, 2, "", 1)]
    probed = [({"adapter": "greenhouse", "name": "Provider"}, True, 3, "", 2)]
    state: dict[str, object] = {}
    merged_results: list[object] = []
    marked: list[tuple[list[object], int]] = []
    active_rows: list[dict[str, object]] = []
    started = time.perf_counter()

    def merge_probe_results(combined):
        merged_results.extend(combined)
        active_rows.extend(
            {"webSearchBrowserRecovery": True, "name": row[0]["name"]} for row in combined if row[1]
        )

    combined, active_count = browser_recovery.merge_browser_recovery_results(
        browser_recovery=state,
        processed={"url:https://rendered.example/jobs"},
        started=started,
        candidate_count=5,
        probe_candidate_count=1,
        rendered_probe_results=rendered,
        probe_results=probed,
        mark_probe_results=lambda results, rendered_count: marked.append(
            (list(results), rendered_count)
        ),
        merge_probe_results=merge_probe_results,
        recovered_rows=lambda: list(active_rows),
        recovered_predicate=lambda row: bool(row.get("webSearchBrowserRecovery")),
        fetch_attempts=2,
        fetch_failures=0,
        candidate_analysis_count=2,
    )

    assert combined == [*rendered, *probed]
    assert merged_results == combined
    assert marked == [(combined, 1)]
    assert active_count == 2
    assert state["processedKeys"] == ["url:https://rendered.example/jobs"]
    assert state["candidateCount"] == 5
    assert state["activeCandidates"] == 2
    assert state["probeCandidates"] == 1
    assert state["renderedStaticValidated"] == 1
    assert state["fetchAttempts"] == 2
    assert state["fetchFailures"] == 0
    assert state["candidateAnalysisCount"] == 2


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
