"""Tests for rendered cards and static plugins sheet studio behavior."""

# ruff: noqa: F401
from ._helpers import (
    FIXTURES_DIR,
    AdapterPluginContext,
    Counter,
    GenericCareersSpider,
    HtmlResponse,
    Path,
    Request,
    _fixture,
    _looks_like_location_cell,
    _parse_structured_locations,
    _read_fixture,
    ats_wrappers,
    build_city_garbage_report,
    build_contamination_report,
    build_location_quality_report,
    build_public_text_quality_report,
    classify_job_page,
    default_registry,
    ensure_provider_plugins,
    extract_rendered_card_jobs,
    frontier,
    hashlib,
    jf,
    jfr,
    jobs_canonicalize,
    jobs_common_config,
    jobs_common_registry,
    jobs_dedup,
    jobs_registry,
    jobs_reporting,
    kojima,
    process_detail_link,
    rendered_cards,
    scrapy_runner,
    sheet_studios,
    source_detail_limit_for,
    source_detail_retries_for,
    static_helpers,
    static_scrapy,
    workspace_tmpdir,
)


def test_sheet_studios_enriches_generic_category_rows_from_jobposting_details() -> None:
    listing_html = """
        <html>
          <body>
            <div class="job-category tech">
              <a href="https://www.tetherstudios.com/job/tech"></a>
              <div class="hiring">
                <h1>Tech</h1>
                <h2>1 Open Position</h2>
              </div>
            </div>
            <div class="job-category art">
              <a href="https://www.tetherstudios.com/job/art"></a>
              <div class="hiring">
                <h1>Art</h1>
                <h2>1 Open Position</h2>
              </div>
            </div>
          </body>
        </html>
        """
    detail_htmls = {
        "https://www.tetherstudios.com/careers": listing_html,
        "https://www.tetherstudios.com/job/tech": """
            <html><head><title>Software Engineers</title></head>
            <body>
              <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "JobPosting",
                "title": "Software Engineers",
                "description": "Engineering role",
                "hiringOrganization": {"name": "Tether Studios"},
                "url": "https://www.tetherstudios.com/job/tech"
              }
              </script>
            </body></html>
        """,
        "https://www.tetherstudios.com/job/art": """
            <html><head><title>UI Artist</title></head>
            <body>
              <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "JobPosting",
                "title": "UI Artist",
                "description": "Art role",
                "hiringOrganization": {"name": "Tether Studios"},
                "url": "https://www.tetherstudios.com/job/art"
              }
              </script>
            </body></html>
        """,
    }

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert timeout_s == 5
        return detail_htmls[url]

    rows = sheet_studios.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
        pages=["https://www.tetherstudios.com/careers"],
        source_row={
            "name": "Tether Studios (Sheet)",
            "studio": "Tether Studios",
            "company": "Tether Studios",
            "id": "static:listing_url:https://www.tetherstudios.com/careers",
        },
        parse_jobpostings_from_html=jf.parse_jobpostings_from_html,
    )

    assert len(rows) == 2
    assert {row["title"] for row in rows} == {"Software Engineers", "UI Artist"}
    assert {row["jobLink"] for row in rows} == {
        "https://www.tetherstudios.com/job/tech",
        "https://www.tetherstudios.com/job/art",
    }


def test_sheet_studios_preserves_existing_multi_location_payloads() -> None:
    listing_html = "<html><body><script type='application/ld+json'>{}</script></body></html>"

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert timeout_s == 5
        assert url == "https://example.com/careers/"
        return listing_html

    def fake_parse_jobpostings_from_html(
        *args: object, **kwargs: object
    ) -> list[dict[str, object]]:
        return [
            {
                "sourceJobId": "static:test:multi-location",
                "title": "Technical Artist",
                "company": "Example Studio",
                "city": "Guildford",
                "country": "UK",
                "locations": [
                    {"city": "Guildford", "country": "UK"},
                    {"city": "Utrecht", "country": "NL"},
                ],
                "locationSummary": "Guildford, UK | Utrecht, NL",
                "workType": "Onsite",
                "contractType": "Unknown",
                "jobLink": "https://example.com/jobs/technical-artist",
                "sector": "Game",
                "postedAt": "",
            }
        ]

    rows = sheet_studios.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
        pages=["https://example.com/careers/"],
        source_row={
            "name": "Example Studio (Sheet)",
            "studio": "Example Studio",
            "company": "Example Studio",
            "id": "static:listing_url:https://example.com/careers/",
        },
        parse_jobpostings_from_html=fake_parse_jobpostings_from_html,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["city"] == "Guildford"
    assert row["country"] == "UK"
    assert row["locations"] == [
        {"city": "Guildford", "country": "UK"},
        {"city": "Utrecht", "country": "NL"},
    ]
    assert row["locationSummary"] == "Guildford, UK | Utrecht, NL"


def test_sheet_studios_sanitizes_one_man_studio_listing_city_noise() -> None:
    listing_html = _read_fixture("theonemanstudio_detail_noise.html")

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert timeout_s == 5
        assert url == "https://theonemanstudio.com/careers/"
        return listing_html

    def fake_parse_jobpostings_from_html(
        *args: object, **kwargs: object
    ) -> list[dict[str, object]]:
        return [
            {
                "sourceJobId": "static:test:one-man-studio-noise",
                "title": "Principal Environment Artist",
                "company": "One Man Studio Ltd.",
                "city": "size",
                "country": "Unknown",
                "locations": [{"city": "size", "country": ""}],
                "locationSummary": "size",
                "workType": "Onsite",
                "contractType": "Unknown",
                "jobLink": "https://theonemanstudio.com/jobs/principal-environment-artist",
                "sector": "Game",
                "postedAt": "",
            }
        ]

    rows = sheet_studios.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
        pages=["https://theonemanstudio.com/careers/"],
        source_row={
            "name": "One Man Studio (Sheet)",
            "studio": "One Man Studio",
            "company": "One Man Studio",
            "id": "static:listing_url:https://theonemanstudio.com/careers/",
        },
        parse_jobpostings_from_html=fake_parse_jobpostings_from_html,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["city"] == ""
    assert row["country"] == ""
    assert row["locationSummary"] == ""
    assert row["locations"] == []
