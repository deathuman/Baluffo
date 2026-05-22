from src import jobs_fetcher as jf


class _FakeCategoryLinkStatusResolver:
    def __init__(self, statuses: dict[str, int]) -> None:
        self.statuses = statuses
        self.prefetched: list[str] = []
        self.stale_drops = 0

    def prefetch(self, job_links: list[str], *, concurrency: int = 1) -> None:
        self.prefetched.extend(job_links)

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


def test_google_sheets_category_title_stale_link_drops_before_url_repair() -> None:
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
