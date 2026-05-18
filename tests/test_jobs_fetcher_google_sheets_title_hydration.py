import json

from src import jobs_fetcher as jf


def test_canonicalize_google_sheets_rows_hydrates_greenhouse_and_lever_titles() -> None:
    greenhouse_feed = "https://boards-api.greenhouse.io/v1/boards/examplegames/jobs?content=true"
    lever_feed = "https://api.lever.co/v0/postings/coda?mode=json"
    calls: dict[str, int] = {}

    def fake_fetch(url: str, _timeout: int) -> str:
        calls[url] = calls.get(url, 0) + 1
        if url == greenhouse_feed:
            return json.dumps(
                {
                    "jobs": [
                        {
                            "id": 12345,
                            "title": "Senior Product Manager",
                            "absolute_url": "https://job-boards.greenhouse.io/examplegames/jobs/12345",
                        }
                    ]
                }
            )
        if url == lever_feed:
            return json.dumps(
                [
                    {
                        "id": "11111111-2222-3333-4444-555555555555",
                        "text": "Digital Marketing Manager",
                        "hostedUrl": (
                            "https://jobs.lever.co/coda/11111111-2222-3333-4444-555555555555"
                        ),
                    }
                ]
            )
        raise AssertionError(f"Unexpected URL: {url}")

    resolver = jf.GoogleSheetsProviderTitleResolver(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )
    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Product-management",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://job-boards.greenhouse.io/examplegames/jobs/12345",
                "sector": "Game",
            },
            {
                "sourceJobId": "sheet-2",
                "title": "Digital-marketing",
                "company": "Coda",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": ("https://jobs.lever.co/coda/11111111-2222-3333-4444-555555555555"),
                "sector": "Tech",
            },
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
        title_hydration_resolver=resolver,
    )

    assert [row.title for row in canonical_rows] == [
        "Senior Product Manager",
        "Digital Marketing Manager",
    ]
    assert not drop_reasons
    assert stats["title_hydration_candidates"] == 2
    assert stats["title_hydration_feed_fetches"] == 2
    assert stats["title_hydration_repaired"] == 2
    assert stats["title_hydration_missed"] == 0
    assert stats["title_hydration_errors"] == 0
    assert calls == {greenhouse_feed: 1, lever_feed: 1}


def test_canonicalize_google_sheets_rows_reuses_title_hydration_feed_cache() -> None:
    feed_url = "https://boards-api.greenhouse.io/v1/boards/sharedboard/jobs?content=true"
    calls = 0

    def fake_fetch(url: str, _timeout: int) -> str:
        nonlocal calls
        calls += 1
        assert url == feed_url
        return json.dumps(
            {
                "jobs": [
                    {
                        "id": 1,
                        "title": "Senior Product Manager",
                        "absolute_url": "https://job-boards.greenhouse.io/sharedboard/jobs/1",
                    },
                    {
                        "id": 2,
                        "title": "Lifecycle Marketing Manager",
                        "absolute_url": "https://job-boards.greenhouse.io/sharedboard/jobs/2",
                    },
                ]
            }
        )

    resolver = jf.GoogleSheetsProviderTitleResolver(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )
    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Product-management",
                "company": "Example Games",
                "jobLink": "https://job-boards.greenhouse.io/sharedboard/jobs/1",
                "sector": "Game",
            },
            {
                "sourceJobId": "sheet-2",
                "title": "Digital-marketing",
                "company": "Example Games",
                "jobLink": "https://job-boards.greenhouse.io/sharedboard/jobs/2",
                "sector": "Game",
            },
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
        title_hydration_resolver=resolver,
    )

    assert [row.title for row in canonical_rows] == [
        "Senior Product Manager",
        "Lifecycle Marketing Manager",
    ]
    assert not drop_reasons
    assert calls == 1
    assert stats["title_hydration_candidates"] == 2
    assert stats["title_hydration_feed_fetches"] == 1
    assert stats["title_hydration_cache_hits"] == 1
    assert stats["title_hydration_repaired"] == 2


def test_canonicalize_google_sheets_rows_keeps_unsupported_missing_and_failed_hydration() -> None:
    missing_feed = "https://boards-api.greenhouse.io/v1/boards/missingboard/jobs?content=true"
    broken_feed = "https://boards-api.greenhouse.io/v1/boards/brokenboard/jobs?content=true"

    def fake_fetch(url: str, _timeout: int) -> str:
        if url == missing_feed:
            return json.dumps({"jobs": [{"id": 999, "title": "Unrelated Role"}]})
        if url == broken_feed:
            raise RuntimeError("provider unavailable")
        raise AssertionError(f"Unexpected URL: {url}")

    resolver = jf.GoogleSheetsProviderTitleResolver(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )
    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Product-management",
                "company": "Example Games",
                "jobLink": (
                    "https://jobs.ashbyhq.com/examplegames/3eabb716-eaef-432e-ad88-f3e16d01e54b"
                ),
                "sector": "Game",
            },
            {
                "sourceJobId": "sheet-2",
                "title": "Ui-art",
                "company": "Example Games",
                "jobLink": "https://jobs.jobvite.com/examplegames/job/oQ8Nzfw4",
                "sector": "Game",
            },
            {
                "sourceJobId": "sheet-3",
                "title": "Product-management",
                "company": "Example Games",
                "jobLink": "https://job-boards.greenhouse.io/missingboard/jobs/123",
                "sector": "Game",
            },
            {
                "sourceJobId": "sheet-4",
                "title": "Digital-marketing",
                "company": "Example Games",
                "jobLink": "https://job-boards.greenhouse.io/brokenboard/jobs/456",
                "sector": "Game",
            },
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
        title_hydration_resolver=resolver,
    )

    assert [row.title for row in canonical_rows] == [
        "Product-management",
        "Ui-art",
        "Product-management",
        "Digital-marketing",
    ]
    assert not drop_reasons
    assert stats["title_hydration_candidates"] == 2
    assert stats["title_hydration_feed_fetches"] == 2
    assert stats["title_hydration_repaired"] == 0
    assert stats["title_hydration_missed"] == 2
    assert stats["title_hydration_errors"] == 1


def test_canonicalize_google_sheets_rows_drops_provider_hydrated_static_non_openings() -> None:
    def fake_fetch(url: str, _timeout: int) -> str:
        assert url == "https://boards-api.greenhouse.io/v1/boards/examplegames/jobs?content=true"
        return json.dumps(
            {
                "jobs": [
                    {
                        "id": 12345,
                        "title": "General Application",
                        "absolute_url": "https://job-boards.greenhouse.io/examplegames/jobs/12345",
                    }
                ]
            }
        )

    resolver = jf.GoogleSheetsProviderTitleResolver(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )
    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Product-management",
                "company": "Example Games",
                "jobLink": "https://job-boards.greenhouse.io/examplegames/jobs/12345",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
        title_hydration_resolver=resolver,
    )

    assert canonical_rows == []
    assert drop_reasons["non_job_static_page"] == 1
    assert stats["title_hydration_candidates"] == 1
    assert stats["title_hydration_repaired"] == 1
