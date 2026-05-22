import json

from src import jobs_fetcher as jf
from src.jobs.canonicalize import GoogleSheetsCategoryLinkStatusResolver


class _FakeCategoryLinkStatusResolver:
    def __init__(self, statuses: dict[str, int]) -> None:
        self.statuses = statuses
        self.prefetched: list[str] = []
        self.stale_drops = 0

    def prefetch(
        self,
        job_links: list[str],
        *,
        concurrency: int = 1,
        progress_callback=None,
    ) -> None:
        self.prefetched.extend(job_links)
        if progress_callback is not None:
            progress_callback(
                phase_key="checking_category_links",
                phase_label="Checking category links",
                counts=self.snapshot_stats(),
                message="fake progress",
            )

    def is_stale(self, job_link: str) -> bool:
        return int(self.statuses.get(job_link) or 0) in {404, 410}

    def note_stale_drop(self) -> None:
        self.stale_drops += 1

    def snapshot_stats(self) -> dict[str, int]:
        unique_links = len(set(self.prefetched))
        return {
            "category_link_status_candidates": len(self.prefetched),
            "category_link_status_checked": unique_links,
            "category_link_status_cache_hits": max(0, len(self.prefetched) - unique_links),
            "category_link_status_stale_dropped": self.stale_drops,
            "category_link_status_errors": 0,
            "category_link_status_ms": 0,
        }


def test_google_sheets_category_title_repairs_from_safe_url_slug() -> None:
    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Influencer-marketing",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://jobs.example.test/openings/influencer-manager",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == ["Influencer Manager"]
    assert not drop_reasons
    assert stats["category_link_status_candidates"] == 0


def test_google_sheets_unrepaired_category_title_drops_without_status_check() -> None:
    resolver = _FakeCategoryLinkStatusResolver({})

    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Influencer-marketing",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://jobs.example.test/openings/F424388045",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
        category_link_status_resolver=resolver,
    )

    assert canonical_rows == []
    assert drop_reasons == {"google_sheets_category_row": 1}
    assert stats["category_link_status_candidates"] == 0
    assert resolver.prefetched == []


def test_google_sheets_category_title_stale_link_drops_after_url_repair() -> None:
    job_link = "https://jobs.example.test/openings/influencer-manager"
    resolver = _FakeCategoryLinkStatusResolver({job_link: 404})

    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Influencer-marketing",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": job_link,
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
        category_link_status_resolver=resolver,
    )

    assert canonical_rows == []
    assert drop_reasons == {"google_sheets_category_row": 1}
    assert stats["category_link_status_candidates"] == 1
    assert stats["category_link_status_stale_dropped"] == 1


def test_google_sheets_category_title_nonterminal_status_does_not_drop() -> None:
    job_link = "https://jobs.example.test/openings/influencer-manager"
    resolver = _FakeCategoryLinkStatusResolver({job_link: 403})

    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Influencer-marketing",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": job_link,
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
        category_link_status_resolver=resolver,
    )

    assert [row.title for row in canonical_rows] == ["Influencer Manager"]
    assert not drop_reasons
    assert stats["category_link_status_candidates"] == 1
    assert stats["category_link_status_stale_dropped"] == 0


def test_google_sheets_provider_hydrated_category_title_gets_status_checked() -> None:
    job_link = "https://job-boards.greenhouse.io/examplegames/jobs/12345"
    feed_url = "https://boards-api.greenhouse.io/v1/boards/examplegames/jobs?content=true"

    def fake_fetch(url: str, _timeout: int) -> str:
        assert url == feed_url
        return json.dumps(
            {
                "jobs": [
                    {
                        "id": 12345,
                        "title": "Influencer Manager",
                        "absolute_url": job_link,
                    }
                ]
            }
        )

    title_resolver = jf.GoogleSheetsProviderTitleResolver(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )
    status_resolver = _FakeCategoryLinkStatusResolver({job_link: 200})

    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Influencer-marketing",
                "company": "Example Games",
                "jobLink": job_link,
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
        title_hydration_resolver=title_resolver,
        category_link_status_resolver=status_resolver,
    )

    assert [row.title for row in canonical_rows] == ["Influencer Manager"]
    assert not drop_reasons
    assert stats["category_link_status_candidates"] == 1
    assert status_resolver.prefetched == [job_link]


def test_google_sheets_category_status_resolver_error_does_not_drop() -> None:
    job_link = "https://jobs.example.test/openings/influencer-manager"
    resolver = GoogleSheetsCategoryLinkStatusResolver(
        timeout_s=1,
        fetch_status=lambda _url, _timeout: (_ for _ in ()).throw(RuntimeError("timeout")),
    )

    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Influencer-marketing",
                "company": "Example Games",
                "jobLink": job_link,
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
        category_link_status_resolver=resolver,
    )

    assert [row.title for row in canonical_rows] == ["Influencer Manager"]
    assert not drop_reasons
    assert stats["category_link_status_errors"] == 1
    assert stats["category_link_status_stale_dropped"] == 0


def test_google_sheets_normalization_and_category_link_progress_callbacks() -> None:
    progress_events = []

    def progress_callback(**payload):
        progress_events.append(payload)

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": f"sheet-{idx}",
                "title": "Influencer-marketing",
                "company": "Example Games",
                "jobLink": f"https://jobs.example.test/openings/influencer-manager-{idx}",
                "sector": "Game",
            }
            for idx in range(1001)
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
        category_link_status_resolver=_FakeCategoryLinkStatusResolver({}),
        progress_callback=progress_callback,
    )

    assert len(canonical_rows) == 1001
    assert not drop_reasons
    assert any(event.get("phase_key") == "normalizing_rows" for event in progress_events)
    assert any(event.get("phase_key") == "checking_category_links" for event in progress_events)


def test_google_sheets_category_title_smartrecruiters_mismatch_drops_before_repair() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Influencer-marketing",
                "company": "Mighty Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": (
                    "https://jobs.smartrecruiters.com/AbercrombieAndFitchCo/"
                    "744000114604816-hollister-co-brand-representative"
                ),
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons == {"google_sheets_category_row": 1}
