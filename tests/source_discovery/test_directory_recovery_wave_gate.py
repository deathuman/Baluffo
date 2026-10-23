from __future__ import annotations

from src.source_discovery.directory_page_recovery import (
    DirectoryRecoveryResult,
    apply_recovery_to_scan_result,
    run_directory_page_recovery,
)
from tests.helpers.bridge_fakes import directory_recovery_request

_request = directory_recovery_request


def _outcome_fetcher(
    outcomes: dict[str, str | BaseException],
) -> tuple[object, list[str]]:
    """A directory fetcher that answers per URL and 404s anything unlisted."""
    calls: list[str] = []

    def fetch(url: str, _timeout: int) -> str:
        calls.append(url)
        outcome = outcomes.get(url)
        if isinstance(outcome, BaseException):
            raise outcome
        if outcome is not None:
            return outcome
        raise RuntimeError(f"Client error '404 Not Found' for url '{url}'")

    return fetch, calls


def test_second_wave_skipped_when_first_wave_generated_guess_answered() -> None:
    """A guessed /careers that answered has read the site; do not try four more spellings."""
    request = _request()
    fetch, calls = _outcome_fetcher(
        {
            "https://studio.example.com/careers": "<html><body>No roles yet</body></html>",
            "https://studio.example.com/jobs": "<html><body>No roles yet</body></html>",
        }
    )

    output = run_directory_page_recovery(
        5,
        [request],
        fetcher=fetch,
        total_concurrency=2,
        per_host_concurrency=1,
        analyze_result=lambda _result, _request: ([], []),
        progress_label="Test",
    )

    assert output.summary["recoveryFetchAttempts"] == 2
    assert output.batch_timing["recoverySkippedByGeneratedAnswered"] == 1
    assert not any(url.endswith("/join-us") for url in calls)
    assert not any(url.endswith("/about/careers") for url in calls)


def test_second_wave_skipped_when_every_generated_guess_is_gone() -> None:
    request = _request()

    def fetch(url: str, _timeout: int) -> str:
        raise RuntimeError("Client error '404 Not Found' for url '" + url + "'")

    output = run_directory_page_recovery(
        5,
        [request],
        fetcher=fetch,
        total_concurrency=2,
        per_host_concurrency=1,
        analyze_result=lambda _result, _request: ([], []),
        progress_label="Test",
    )

    assert output.summary["recoveryFetchAttempts"] == 2
    assert output.batch_timing["recoverySkippedByGeneratedNotFound"] == 1
    assert output.summary["recoveryFailures"] == 2


def test_second_wave_runs_when_first_wave_could_not_read_the_host() -> None:
    """A host that answered nothing (timeout) has not been heard from, so it keeps wave 2."""
    request = _request()
    fetch, calls = _outcome_fetcher(
        {
            "https://studio.example.com/careers": RuntimeError("timed out"),
            "https://studio.example.com/jobs": RuntimeError("timed out"),
            "https://studio.example.com/join-us": "<html><body>No roles</body></html>",
        }
    )

    output = run_directory_page_recovery(
        5,
        [request],
        fetcher=fetch,
        total_concurrency=2,
        per_host_concurrency=1,
        analyze_result=lambda _result, _request: ([], []),
        progress_label="Test",
    )

    assert output.summary["recoveryFetchAttempts"] == 6
    assert any(url.endswith("/join-us") for url in calls)
    assert "recoverySkippedByGeneratedAnswered" not in output.batch_timing


def test_recovery_failure_rows_carry_url_reason_and_url_source() -> None:
    request = _request()

    def fetch(url: str, _timeout: int) -> str:
        status = "410 Gone" if url.endswith("/jobs") else "404 Not Found"
        raise RuntimeError(f"Client error '{status}' for url '{url}'")

    output = run_directory_page_recovery(
        5,
        [request],
        fetcher=fetch,
        total_concurrency=2,
        per_host_concurrency=1,
        analyze_result=lambda _result, _request: ([], []),
        progress_label="Test",
    )

    by_url = {str(row.get("url") or ""): row for row in output.failures}
    careers = by_url["https://studio.example.com/careers"]
    assert careers["stage"] == "recovery_fetch"
    assert careers["recoveryUrlSource"] == "generated_common_path"
    assert careers["recoveryUrlPath"] == "/careers"
    assert careers["sourceDirectoryEntryUrl"] == "https://studio.example.com/"
    assert "404" in careers["error"]
    assert by_url["https://studio.example.com/jobs"]["recoveryUrlPath"] == "/jobs"


def test_recovery_failure_rows_mark_linked_pages_as_html_jobish_links() -> None:
    request = _request(html='<a href="/en/careers">Careers</a>')

    def fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(f"Client error '404 Not Found' for url '{url}'")

    output = run_directory_page_recovery(
        5,
        [request],
        fetcher=fetch,
        total_concurrency=2,
        per_host_concurrency=1,
        analyze_result=lambda _result, _request: ([], []),
        progress_label="Test",
    )

    linked = next(
        row for row in output.failures if str(row.get("url") or "").endswith("/en/careers")
    )
    assert linked["recoveryUrlSource"] == "html_jobish_link"
    assert linked["recoveryUrlPath"] == "/en/careers"


def test_apply_recovery_to_scan_result_carries_recovery_failure_rows() -> None:
    recovery_failure = {
        "name": "Studio",
        "url": "https://studio.example/careers",
        "error": "Client error '404 Not Found'",
        "stage": "recovery_fetch",
        "recoveryUrlSource": "generated_common_path",
        "recoveryUrlPath": "/careers",
    }

    row = apply_recovery_to_scan_result(
        {
            "providerCandidates": [],
            "staticCandidates": [],
            "failures": [{"stage": "website_fetch", "error": "dns"}],
            "summary": {},
            "batchTiming": {},
        },
        DirectoryRecoveryResult(failures=[dict(recovery_failure)]),
    )

    assert row["failures"] == [{"stage": "website_fetch", "error": "dns"}, recovery_failure]
