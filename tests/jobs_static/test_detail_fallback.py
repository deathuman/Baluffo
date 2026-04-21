# ruff: noqa: F401
from unittest import mock

import pytest

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
    patch_jobs_fetcher_aliases,
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


def test_job_page_gate_rejects_regular_pages_and_accepts_jobposting_jsonld() -> None:
    regular_html = """
        <html>
          <head><title>About</title></head>
          <body><h1>About</h1><p>About us</p></body>
        </html>
    """
    allowed, reason = classify_job_page(
        regular_html,
        "https://example.com/jobs/about",
        page_title="About",
    )
    assert not allowed
    assert reason == "dead_listing_page"

    job_html = """
        <html>
          <head><title>Software Engineer</title></head>
          <body>
            <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "JobPosting",
              "title": "Software Engineer",
              "url": "/careers/software-engineer",
              "hiringOrganization": {"name": "Example Studio"}
            }
            </script>
          </body>
        </html>
    """
    allowed, reason = classify_job_page(
        job_html,
        "https://example.com/careers/software-engineer",
        page_title="Software Engineer",
    )
    assert allowed
    assert reason == "jobposting_jsonld"


def test_normalize_source_report_row_fills_zero_kept_label_residues() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "static_source::zero_kept_residue",
            "status": "ok",
            "adapter": "static",
            "failureBucket": "",
            "classification": "",
            "zeroKeptClassification": "n/a",
            "fetchedCount": 2,
            "keptCount": 0,
            "error": "",
        }
    )
    assert str(row.get("failureBucket") or "") == "no_openings"
    assert str(row.get("classification") or "") == ""
    assert str(row.get("zeroKeptClassification") or "") == "legit_empty"


def test_normalize_source_report_row_preserves_static_site_changed_url_surface() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "static_source::site_changed",
            "status": "error",
            "adapter": "static",
            "failureBucket": "site_changed",
            "listingUrl": "https://example.com/careers",
            "pages": ["https://example.com/careers", ""],
            "sourceId": "static:site-changed",
            "details": [],
        }
    )
    assert str(row.get("listingUrl") or "") == "https://example.com/careers"
    assert row.get("pages") == ["https://example.com/careers"]
    assert str(row.get("sourceId") or "") == "static:site-changed"

    non_site_changed = jf.normalize_source_report_row(
        {
            "name": "static_source::not_site_changed",
            "status": "ok",
            "adapter": "static",
            "failureBucket": "needs_review",
            "listingUrl": "https://example.com/hidden",
            "pages": ["https://example.com/hidden"],
            "sourceId": "static:not-site-changed",
        }
    )
    assert "listingUrl" not in non_site_changed
    assert "pages" not in non_site_changed
    assert "sourceId" not in non_site_changed


def test_normalize_source_report_row_preserves_static_zero_extract_classification() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "static_source::needs_review",
            "status": "error",
            "adapter": "static",
            "failureBucket": "needs_review",
            "classification": "needs_review",
            "error": "no jobs extracted from source pages",
        }
    )
    assert str(row.get("classification") or "") == "needs_review"
    assert str(row.get("failureBucket") or "") == "needs_review"


def test_parse_jobpostings_from_html_collapses_sequence_text_values() -> None:
    html = """
        <html>
          <body>
            <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "JobPosting",
              "title": [
                "Lead Technical Designer",
                " House of How Games",
                "Job Description"
              ],
              "url": "https://www.houseofhow.com/job/lead-technical-designer",
              "hiringOrganization": {"name": ["House of How"]},
              "jobLocationType": "Onsite",
              "employmentType": "Unknown"
            }
            </script>
          </body>
        </html>
    """

    rows = jf.parse_jobpostings_from_html(
        html,
        base_url="https://www.houseofhow.com/careers/",
        fallback_company="House of How",
        fallback_source_id_prefix="static:House of How (Sheet)",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Lead Technical Designer"
    assert row["company"] == "House of How"
    assert row["jobLink"] == "https://www.houseofhow.com/job/lead-technical-designer"


@pytest.mark.parametrize(
    ("visible_title", "expected_title"),
    [
        ("March 17, 2023 3D Artist", "3D Artist"),
        ("03/17/2023 QA Engineer", "QA Engineer"),
        ("2023-03-17 Marketing Artist", "Marketing Artist"),
    ],
)
def test_rendered_card_jobs_strip_leading_posted_date_prefixes(
    visible_title: str,
    expected_title: str,
) -> None:
    html = f"""
        <a href="https://example.com/jobs/1" target="_blank" class="join_row">
          <div class="d-table-cell vacancy ps-1 pe-3 pe-md-0">{visible_title}</div>
          <div class="d-table-cell vacancy">Filters</div>
        </a>
        """
    rows = extract_rendered_card_jobs(
        html,
        page_url="https://example.com/careers/",
        company="Example Studio",
        source_id="example_rendered_date_prefix",
        allow_any_anchor=True,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == expected_title
    assert row["city"] == ""
    assert row["locationSummary"] == ""


def test_rendered_card_location_cell_heuristic_rejects_role_bleed() -> None:
    assert not _looks_like_location_cell("Artiste technique")
    assert _looks_like_location_cell("Montréal, CA")


def test_rendered_card_structured_locations_deduplicate_variants() -> None:
    locations, work_type, contract_type = _parse_structured_locations(
        ["Gameplay Programmer", "Munich, DE | München, DE", "Full-time"],
        "Gameplay Programmer",
    )
    assert locations == [{"city": "Munich", "country": "DE"}]
    assert work_type == "Full-time"
    assert contract_type == ""


def test_rendered_card_structured_locations_ignore_full_time_noise_cells() -> None:
    locations, work_type, contract_type = _parse_structured_locations(
        ["Environment Artist München", "Full-time", "München, DE", "Onsite"],
        "Environment Artist München",
    )
    assert locations == [{"city": "München", "country": "DE"}]
    assert work_type == "Full-time"
    assert contract_type == ""


def test_static_detail_fallback_does_not_synthesize_rows_from_remote_full_time_noise() -> None:
    detail_html = """
        <html>
          <body>
            <h1>Senior Environment Artist</h1>
            <p>Remote</p>
            <p>Full Time</p>
            <p>About our studio and culture.</p>
          </body>
        </html>
        """

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://example.com/about/senior-environment-artist"
        return detail_html, False

    result = process_detail_link(
        detail="https://example.com/about/senior-environment-artist",
        detail_title="Senior Environment Artist",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="Example Studio",
        source_name="static:listing_url:https://example.com/jobs",
        source={"studio": "Example Studio"},
        ignored_link_titles=set(),
    )

    assert result["rows"] == []
    assert result["parseEmpty"] is True
    assert str(result.get("rejectedClassification") or "") == "dead_listing_page"


def test_static_detail_fallback_enriches_unknown_rows_from_tokyo_detail_block() -> None:
    detail_html = """
        <html>
          <body>
            <h1>背景アーティスト / Environment Artist</h1>
            <p>Office location:</p>
            <p>7F NTF Takebashi Building, 3-15 Kanda Nishiki-cho, Chiyoda-ku, Tokyo</p>
          </body>
        </html>
        """

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://www.kojimaproductions.jp/en/environment-artist"
        return detail_html, False

    with mock.patch(
        "src.jobs.adapters.static_helpers.parse_jobpostings_from_html",
        return_value=[
            {
                "sourceJobId": "static:kojima:environment-artist",
                "title": "背景アーティスト / Environment Artist",
                "company": "KOJIMA PRODUCTIONS",
                "city": "",
                "country": "Unknown",
                "locations": [],
                "locationSummary": "",
                "workType": "",
                "contractType": "",
                "jobLink": "https://www.kojimaproductions.jp/en/environment-artist",
                "sector": "Game",
                "postedAt": "",
            }
        ],
    ):
        result = process_detail_link(
            detail="https://www.kojimaproductions.jp/en/environment-artist",
            detail_title="背景アーティスト / Environment Artist",
            source_started=0.0,
            static_source_time_budget_s=10,
            fetch_html_cached=fake_fetch,
            timeout_s=5,
            detail_retries=0,
            company="KOJIMA PRODUCTIONS",
            source_name="static:listing_url:https://www.kojimaproductions.jp/en/careers",
            source={"studio": "KOJIMA PRODUCTIONS"},
            ignored_link_titles=set(),
        )

    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["city"] == "Tokyo"
    assert row["country"] == "Japan"
    assert row["locationSummary"] == "Tokyo, Japan"
    assert row["locations"] == [{"city": "Tokyo", "country": "Japan"}]


def test_static_detail_fallback_ignores_grid_noise_before_office_location() -> None:
    detail_html = """
        <html>
          <body>
            <h1>Lead Environment Artist</h1>
            <div>grid</div>
            <p>Office location:</p>
            <p>7F NTF Takebashi Building, 3-15 Kanda Nishiki-cho, Chiyoda-ku, Tokyo</p>
          </body>
        </html>
        """

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://area35east.com/jobs/lead-environment-artist"
        return detail_html, False

    result = process_detail_link(
        detail="https://area35east.com/jobs/lead-environment-artist",
        detail_title="Lead Environment Artist",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="Area 35 East",
        source_name="static:listing_url:https://area35east.com/careers",
        source={"studio": "Area 35 East"},
        ignored_link_titles=set(),
    )

    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["city"] == "Tokyo"
    assert row["country"] == "Japan"
    assert row["locationSummary"] == "Tokyo, Japan"
    assert row["locations"] == [{"city": "Tokyo", "country": "Japan"}]


def test_static_detail_fallback_ignores_gutenify_css_noise_on_one_man_studio_page() -> None:
    detail_html = _read_fixture("theonemanstudio_detail_live_shape.html")

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://theonemanstudio.com/jobs/environment-artist"
        return detail_html, False

    result = process_detail_link(
        detail="https://theonemanstudio.com/jobs/environment-artist",
        detail_title="Senior | Mid-level Environment Artist",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="One Man Studio Ltd.",
        source_name="static_source::static:listing_url:https://theonemanstudio.com/careers/",
        source={"studio": "One Man Studio", "company": "One Man Studio"},
        ignored_link_titles=set(),
    )

    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["city"] == ""
    assert row["country"] == "Unknown"
    assert row["locationSummary"] == ""
    assert row["locations"] == []
    assert row["workType"] == "Remote"
    assert row["contractType"] == "Full Time"


def test_static_detail_fallback_ignores_scroll_noise_when_no_location_exists() -> None:
    detail_html = """
        <html>
          <body>
            <h1>PRINCIPAL 3D ENVIRONMENT ARTIST NEW IP</h1>
            <div>Scroll</div>
          </body>
        </html>
        """

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://www.4a-games.com.mt/principal-3d-environment-artist-new-ip"
        return detail_html, False

    result = process_detail_link(
        detail="https://www.4a-games.com.mt/principal-3d-environment-artist-new-ip",
        detail_title="PRINCIPAL 3D ENVIRONMENT ARTIST NEW IP",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="4A Games",
        source_name="static:listing_url:https://www.4a-games.com.mt/careers",
        source={"studio": "4A Games"},
        ignored_link_titles=set(),
    )

    assert all(row.get("city") != "Scroll" for row in result["rows"])
    assert all(row.get("locationSummary") != "Scroll" for row in result["rows"])


def test_static_detail_fallback_rejects_generic_synthesized_titles() -> None:
    empty_html = "<html><body></body></html>"

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://www.tetherstudios.com/job/tech"
        return empty_html, False

    result = process_detail_link(
        detail="https://www.tetherstudios.com/job/tech",
        detail_title="Tech",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="Tether Studios",
        source_name="Tether Studios",
        source={"studio": "Tether Studios"},
        ignored_link_titles=set(),
    )

    assert result["rows"] == []
    assert str(result.get("rejectedClassification") or "") == "dead_listing_page"
    assert "https://www.tetherstudios.com/job/tech" in str(result.get("rejectedExample") or "")


def test_static_detail_fallback_rejects_regular_pages_without_synthesizing_rows() -> None:
    regular_html = """
        <html>
          <head><title>About</title></head>
          <body><h1>About</h1><p>About us</p></body>
        </html>
    """

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://example.com/jobs/about"
        return regular_html, False

    result = process_detail_link(
        detail="https://example.com/jobs/about",
        detail_title="About",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="Example Studio",
        source_name="Example Careers",
        source={"studio": "Example Studio"},
        ignored_link_titles=set(),
    )

    assert result["rows"] == []
    assert str(result.get("rejectedClassification") or "") == "dead_listing_page"
    assert "https://example.com/jobs/about" in str(result.get("rejectedExample") or "")


@pytest.mark.parametrize(
    ("fixture_name", "detail_url", "source_name", "company", "city_token", "title"),
    [
        (
            "theonemanstudio_detail_noise.html",
            "https://theonemanstudio.com/jobs/environment-artist",
            "static:listing_url:https://theonemanstudio.com/careers/",
            "One Man Studio",
            "justification",
            "Senior | Mid-level Environment Artist",
        ),
        (
            "theonemanstudio_detail_noise.html",
            "https://theonemanstudio.com/jobs/principal-environment-artist",
            "static:listing_url:https://theonemanstudio.com/careers/",
            "One Man Studio",
            "space",
            "Principal Environment Artist",
        ),
        (
            "gismart_detail_noise.html",
            "https://gismart.com/vacancy/web-product-designer-testora",
            "static:listing_url:https://gismart.com/careers/",
            "Gismart",
            "Testora",
            "Web/Product Designer (Testora)",
        ),
        (
            "inworld_detail_noise.html",
            "https://jobs.ashbyhq.com/inworld-ai/bf5054ab-ed19-4890-8f0b-ca8a57210e42/application",
            "static:listing_url:https://inworld.ai/careers#job-openings",
            "Inworld AI",
            "Swaziland",
            "Staff / Principal Machine Learning Engineer, Serving - Switzerland",
        ),
        (
            "techland_detail_noise.html",
            "https://techland.net/job-offers/weapon-concept-artist-57",
            "static:listing_url:https://techland.net/job-offers",
            "Techland",
            "event:'pageViewed'",
            "Weapon Concept Artist",
        ),
    ],
)
def test_static_detail_fallback_sanitizes_source_specific_noise_city_values(
    fixture_name: str,
    detail_url: str,
    source_name: str,
    company: str,
    city_token: str,
    title: str,
) -> None:
    detail_html = _read_fixture(fixture_name)

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == detail_url
        return detail_html, False

    with mock.patch(
        "src.jobs.adapters.static_helpers.parse_jobpostings_from_html",
        return_value=[
            {
                "sourceJobId": f"static:test:{city_token}",
                "title": title,
                "company": company,
                "city": city_token,
                "country": "Unknown",
                "locations": [{"city": city_token, "country": ""}],
                "locationSummary": city_token,
                "workType": "Onsite",
                "contractType": "Unknown",
                "jobLink": detail_url,
                "sector": "Game",
                "postedAt": "",
            }
        ],
    ):
        result = process_detail_link(
            detail=detail_url,
            detail_title=title,
            source_started=0.0,
            static_source_time_budget_s=10,
            fetch_html_cached=fake_fetch,
            timeout_s=5,
            detail_retries=0,
            company=company,
            source_name=source_name,
            source={"studio": company},
            ignored_link_titles=set(),
        )

    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["city"] == ""
    assert row["country"] == ""
    assert row["locationSummary"] == ""
    assert row["locations"] == []


def test_static_detail_fallback_treats_remote_only_text_as_work_type_not_location() -> None:
    detail_html = """
        <html>
          <body>
            <h1>Principal Environment Artist</h1>
            <p>Remote, Remote</p>
            <p>Remote</p>
            <p>Full Time</p>
          </body>
        </html>
        """

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://www.onemanstudio.com/jobs/principal-environment-artist"
        return detail_html, False

    result = process_detail_link(
        detail="https://www.onemanstudio.com/jobs/principal-environment-artist",
        detail_title="Principal Environment Artist",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="One Man Studio",
        source_name="static:listing_url:https://theonemanstudio.com/careers/",
        source={"studio": "One Man Studio"},
        ignored_link_titles=set(),
    )

    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["city"] == ""
    assert row["country"] == "Unknown"
    assert row["locationSummary"] == ""
    assert row["locations"] == []
    assert row["workType"] == "Remote"
    assert row["contractType"] == "Full Time"


def test_static_detail_fallback_understands_team_asobi_japanese_location_block() -> None:
    detail_html = """
        <html>
          <body>
            <h1>3D Environment Artist</h1>
            <p>勤務場所</p>
            <p>東京エリア（3ヶ月の試用期間後、リモートワーク可）</p>
          </body>
        </html>
        """

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://job-boards.greenhouse.io/siei/jobs/5524462004"
        return detail_html, False

    result = process_detail_link(
        detail="https://job-boards.greenhouse.io/siei/jobs/5524462004",
        detail_title="3D Environment Artist",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="Team ASOBI",
        source_name="static:listing_url:https://www.teamasobi.com/jobs/",
        source={"studio": "Team ASOBI"},
        ignored_link_titles=set(),
    )

    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["city"] == "Tokyo"
    assert row["country"] == "Japan"
    assert row["locationSummary"] == "Tokyo, Japan"
    assert row["locations"] == [{"city": "Tokyo", "country": "Japan"}]
