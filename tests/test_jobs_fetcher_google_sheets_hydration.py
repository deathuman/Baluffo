import json

from src import jobs_fetcher as jf


def test_google_sheets_title_hydration_supports_workable_widget_payload() -> None:
    resolver = jf.GoogleSheetsProviderTitleResolver(
        fetch_text=lambda _url, _timeout: json.dumps(
            {
                "name": "Loop",
                "jobs": [
                    {
                        "shortcode": "97BEDE6E0C",
                        "title": "Senior Product Manager",
                        "url": "https://apply.workable.com/followloop/j/97BEDE6E0C",
                    }
                ],
            }
        ),
        timeout_s=1,
        retries=0,
        backoff_s=0,
    )

    rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "title": "Product-management",
                "company": "Loop",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://apply.workable.com/followloop/j/97BEDE6E0C",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
        title_hydration_resolver=resolver,
    )

    assert not drop_reasons
    assert rows[0].title == "Senior Product Manager"
    assert int(stats.get("title_hydration_repaired") or 0) == 1


def test_google_sheets_title_hydration_supports_ashby_board_html() -> None:
    ashby_html = """
        <script>
        window.__appData = {
          "organization": {"name": "Beyond Sports"},
          "jobBoard": {
            "jobPostings": [
              {
                "id": "1c7aa77f-7570-46d4-9f00-e74a914fcbe8",
                "title": "Senior Gameplay Programmer",
                "locationName": "Remote",
                "workplaceType": "Remote",
                "employmentType": "FullTime",
                "publishedDate": "2026-05-22"
              }
            ]
          }
        };
        </script>
    """
    resolver = jf.GoogleSheetsProviderTitleResolver(
        fetch_text=lambda _url, _timeout: ashby_html,
        timeout_s=1,
        retries=0,
        backoff_s=0,
    )

    rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "title": "Game-programmer",
                "company": "Beyond Sports",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": (
                    "https://jobs.ashbyhq.com/beyondsports/1c7aa77f-7570-46d4-9f00-e74a914fcbe8"
                ),
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
        title_hydration_resolver=resolver,
    )

    assert not drop_reasons
    assert rows[0].title == "Senior Gameplay Programmer"
    assert int(stats.get("title_hydration_repaired") or 0) == 1
