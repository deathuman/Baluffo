from unittest import mock

from src import jobs_fetcher as jf
from src.jobs.transport import PooledRedirectResolver


def test_google_sheet_candidate_urls_prefer_export_before_gviz_fallbacks() -> None:
    urls = jf.google_sheet_candidate_urls("sheet-id", "0")
    assert urls[0].endswith("/export?format=csv&gid=0")
    assert urls[1].endswith("/gviz/tq?tqx=out:csv&gid=0")
    assert urls[2].endswith("/pub?output=csv")


def test_parse_google_sheets_csv_prefers_title_over_job_category_in_flattened_headers() -> None:
    csv_text = (
        "Intro row,,,,,,,,,,\n"
        "Company Category,Company Name,Overall Category,Job Category,Title,Job Type,Country,City,Fully Remote?,Job Link,Added\n"
        "Developer,Studio A,Game,Art,Senior Design Director,Full-Time,United States,Los Angeles,Yes,https://gracklehq.com/rd/374557,2026-03-10\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["title"] == "Senior Design Director"
    assert rows[0]["company"] == "Studio A"
    assert rows[0]["jobLink"] == "https://gracklehq.com/rd/374557"


def test_canonicalize_job_with_reason_repairs_unknown_google_sheets_company_from_resolved_link() -> (
    None
):
    normalized, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Technical Director",
            "company": jf.UNKNOWN_COMPANY_LABEL,
            "jobLink": "https://gracklehq.com/rd/372393",
        },
        source="google_sheets",
        fetched_at="2026-03-13T10:00:00Z",
        resolve_redirect_url=lambda _url: (
            "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
        ),
    )
    assert normalized is not None
    assert normalized.company == "Ubisoft2"
    assert normalized.jobLink == "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
    assert reason == ""


def test_pooled_redirect_resolver_skips_self_mappings_when_seeding_and_persisting() -> None:
    redirect_url = "https://gracklehq.com/rd/372393"
    second_redirect_url = "https://gracklehq.com/rd/372394"
    resolved_url = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
    resolver = PooledRedirectResolver(timeout_s=1, max_connections=1)
    try:
        resolver.seed_cache(
            {
                redirect_url: redirect_url,
                second_redirect_url: resolved_url,
            }
        )
        assert resolver.snapshot_cache() == {second_redirect_url: resolved_url}

        with mock.patch.object(resolver, "_resolve_with_client", return_value=redirect_url):
            assert resolver.resolve(redirect_url) == redirect_url

        assert resolver.snapshot_cache() == {second_redirect_url: resolved_url}
    finally:
        resolver.close()
