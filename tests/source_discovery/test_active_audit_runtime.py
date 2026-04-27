from __future__ import annotations

from src.source_discovery.active_audit_runtime import (
    HomepagePageOutcome,
    NoCandidateOutcome,
    run_active_homepage_batch,
)


def _row_url(row: dict[str, object]) -> str:
    return str(row.get("url") or "").strip()


def test_active_homepage_batch_direct_rows_infer_provider_and_skip_fetch() -> None:
    result = run_active_homepage_batch(
        batch_rows=[{"name": "Direct", "url": "https://direct.example/jobs"}],
        homepage_fetch_results=[],
        row_url=_row_url,
        infer_direct_provider=lambda row: {"adapter": "greenhouse", "url": _row_url(row)},
        fetch_failure_rejection=lambda row, fetch: {"reason": "failed"},
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(),
    )

    assert result.provider_candidates == [
        {"adapter": "greenhouse", "url": "https://direct.example/jobs"}
    ]
    assert result.homepages_fetched == 0


def test_active_homepage_batch_fetch_failure_records_failure_and_rejection() -> None:
    failure = {"adapter": "gamedevmap", "stage": "homepage_fetch", "error": "timeout"}
    result = run_active_homepage_batch(
        batch_rows=[],
        homepage_fetch_results=[
            {
                "url": "https://fail.example",
                "payload": {"name": "Fail", "url": "https://fail.example"},
                "ok": False,
                "failure": failure,
            }
        ],
        row_url=_row_url,
        infer_direct_provider=lambda row: None,
        fetch_failure_rejection=lambda row, fetch: {
            "reason": "homepage_fetch_failed",
            "url": _row_url(row),
        },
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(),
    )

    assert result.failures == [failure]
    assert result.rejected_rows == [
        {"reason": "homepage_fetch_failed", "url": "https://fail.example"}
    ]
    assert result.homepages_fetched == 0


def test_active_homepage_batch_success_routes_page_candidates() -> None:
    result = run_active_homepage_batch(
        batch_rows=[],
        homepage_fetch_results=[
            {
                "url": "https://studio.example",
                "payload": {"name": "Studio", "url": "https://studio.example"},
                "ok": True,
                "text": "<html>jobs</html>",
            }
        ],
        row_url=_row_url,
        infer_direct_provider=lambda row: None,
        fetch_failure_rejection=lambda row, fetch: {"reason": "failed"},
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(
            provider_candidates=[{"adapter": "lever", "url": url}],
            static_candidates=[{"adapter": "static", "url": f"{url}/careers"}],
            found_candidates=True,
        ),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(
            rejected_rows=[{"reason": "should_not_run"}]
        ),
    )

    assert result.provider_candidates == [{"adapter": "lever", "url": "https://studio.example"}]
    assert result.static_candidates == [
        {"adapter": "static", "url": "https://studio.example/careers"}
    ]
    assert result.rejected_rows == []
    assert result.homepages_fetched == 1


def test_active_homepage_batch_no_candidate_queues_recovery_without_rejection() -> None:
    result = run_active_homepage_batch(
        batch_rows=[],
        homepage_fetch_results=[
            {
                "url": "https://quiet.example",
                "payload": {"name": "Quiet", "url": "https://quiet.example"},
                "ok": True,
                "text": "<html>No jobs</html>",
            }
        ],
        row_url=_row_url,
        infer_direct_provider=lambda row: None,
        fetch_failure_rejection=lambda row, fetch: {"reason": "failed"},
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(found_candidates=False),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(
            primary_recovery_jobs=[{"url": f"{url}/careers"}],
            browser_recovery_candidates=[{"url": url, "reasonDetail": "js_shell"}],
        ),
    )

    assert result.primary_recovery_jobs == [{"url": "https://quiet.example/careers"}]
    assert result.browser_recovery_candidates == [
        {"url": "https://quiet.example", "reasonDetail": "js_shell"}
    ]
    assert result.rejected_rows == []


def test_active_homepage_batch_no_candidate_can_reject_when_recovery_not_queued() -> None:
    result = run_active_homepage_batch(
        batch_rows=[],
        homepage_fetch_results=[
            {
                "url": "https://quiet.example",
                "payload": {"name": "Quiet", "url": "https://quiet.example"},
                "ok": True,
                "text": "<html>No jobs</html>",
            }
        ],
        row_url=_row_url,
        infer_direct_provider=lambda row: None,
        fetch_failure_rejection=lambda row, fetch: {"reason": "failed"},
        analyze_homepage=lambda row, url, html: HomepagePageOutcome(found_candidates=False),
        handle_no_candidate=lambda row, url, html: NoCandidateOutcome(
            rejected_rows=[{"reason": "no_careers_evidence", "url": url}],
        ),
    )

    assert result.rejected_rows == [
        {"reason": "no_careers_evidence", "url": "https://quiet.example"}
    ]
