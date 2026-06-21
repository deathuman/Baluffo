"""Tests for rendered cards and static plugins extraction behavior."""

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


def test_add_detail_link_rejects_linkedin_job_urls_before_detail_fetch() -> None:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    seen_links: set[str] = set()
    link_rejections: Counter[str] = Counter()

    static_helpers.add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        link_rejections,
        candidate_url="https://www.linkedin.com/jobs/view/1234567890/",
        anchor_text="Senior Engineer",
        enforce_heuristics=False,
        page_url="https://www.example.com/careers",
        source={"company": "Example"},
        default_path_tokens=[],
        default_query_keys=[],
    )

    assert detail_links == []
    assert link_rejections["dead_listing_page"] == 1


def test_add_detail_link_rejects_regular_navigation_titles_before_detail_fetch() -> None:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    seen_links: set[str] = set()
    link_rejections: Counter[str] = Counter()

    static_helpers.add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        link_rejections,
        candidate_url="https://example.com/jobs/about",
        anchor_text="About",
        enforce_heuristics=True,
        page_url="https://example.com/careers",
        source={"company": "Example"},
        default_path_tokens=["/job/", "/jobs/"],
        default_query_keys=["job_id"],
    )

    assert detail_links == []
    assert link_rejections["dead_listing_page"] == 1


def test_add_detail_link_strips_unknown_worlds_trailing_backslash() -> None:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    seen_links: set[str] = set()
    link_rejections: Counter[str] = Counter()

    static_helpers.add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        link_rejections,
        candidate_url="https://boards.greenhouse.io/unknownworlds/jobs/7535230002\\",
        anchor_text="Lead Environment Artist",
        enforce_heuristics=False,
        page_url="https://unknownworlds.com/en/careers",
        source={"company": "Unknown Worlds Entertainment"},
        default_path_tokens=[],
        default_query_keys=[],
    )

    assert detail_links == [
        ("https://boards.greenhouse.io/unknownworlds/jobs/7535230002", "Lead Environment Artist")
    ]
    assert not link_rejections


def test_classify_job_page_rejects_tether_style_regular_pages_as_dead_listing_pages() -> None:
    for url in (
        "https://www.tetherstudios.com/about",
        "https://www.tetherstudios.com/support",
        "https://www.tetherstudios.com/privacy",
        "https://www.tetherstudios.com/all-games",
        "https://www.tetherstudios.com/games/yatzy-royale",
    ):
        job_like, reason = classify_job_page("", url)
        assert not job_like
        assert reason == "dead_listing_page"


def test_extract_rendered_card_jobs_handles_table_row_manual_website_cards() -> None:
    html = """
        <html>
          <body>
            <table class="jobs-table">
              <tbody>
                <tr class="job-row">
                  <td><a href="/jobs/environment-artist">Environment Artist</a></td>
                  <td>Remote</td>
                  <td>Permanent</td>
                  <td><a href="/jobs/environment-artist">Details</a></td>
                </tr>
                <tr class="job-row">
                  <td><a href="/jobs/technical-artist">Technical Artist</a></td>
                  <td>Berlin, Germany</td>
                  <td>Contract</td>
                  <td><a href="/jobs/technical-artist">View Details</a></td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """

    rows = extract_rendered_card_jobs(
        html,
        page_url="https://example.com/careers",
        company="Example Studio",
        source_id="example_manual_table",
        allow_any_anchor=True,
    )
    assert len(rows) == 2
    assert {row["title"] for row in rows} == {"Environment Artist", "Technical Artist"}
    assert {row["jobLink"] for row in rows} == {
        "https://example.com/jobs/environment-artist",
        "https://example.com/jobs/technical-artist",
    }
    assert {row["city"] for row in rows} == {"Remote", "Berlin"}
    assert {row["country"] for row in rows} == {"Remote", "DE"}


def test_extract_rendered_card_jobs_keeps_tether_category_openings() -> None:
    html = """
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

    rows = extract_rendered_card_jobs(
        html,
        page_url="https://www.tetherstudios.com/careers",
        company="Tether Studios",
        source_id="tether_category_openings",
        allow_any_anchor=True,
    )
    assert len(rows) == 2
    assert {row["title"] for row in rows} == {"Tech", "Art"}
    assert {row["jobLink"] for row in rows} == {
        "https://www.tetherstudios.com/job/tech",
        "https://www.tetherstudios.com/job/art",
    }


def test_extract_rendered_card_jobs_parses_stellar_structured_anchor_cells() -> None:
    html = """
        <a href="https://jobs.ashbyhq.com/stellarentertainment/4526ffd2-860e-4e2d-8743-4e637ca0ced6" target="_blank" class="join_row Art United Kingdom">
          <div class="d-table-cell vacancy ps-1 pe-3 pe-md-0">Technical Artist</div>
          <div class="d-table-cell vacancy">Art</div>
          <div class="d-table-cell vacancy text-end pe-1">Guildford, UK | Utrecht, NL</div>
          <div class="d-table-cell d-none">United Kingdom</div>
        </a>
        """
    rows = extract_rendered_card_jobs(
        html,
        page_url="https://stellarentertainment.software/join-us/",
        company="Stellar Entertainment Software",
        source_id="stellar_exact_anchor",
        allow_any_anchor=True,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Technical Artist"
    assert row["city"] == "Guildford"
    assert row["country"] == "UK"
    assert row["locations"] == [
        {"city": "Guildford", "country": "UK"},
        {"city": "Utrecht", "country": "NL"},
    ]
    assert row["locationSummary"] == "Guildford, UK | Utrecht, NL"
    assert (
        row["jobLink"]
        == "https://jobs.ashbyhq.com/stellarentertainment/4526ffd2-860e-4e2d-8743-4e637ca0ced6"
    )


def test_extract_rendered_card_jobs_rejects_campaign_landing_pages() -> None:
    html = """
        <html>
          <body>
            <article class="card">
              <a href="/latin-america-student-and-recent-graduates">
                Latin America Student and Recent Graduates Explore internship and apprenticeship roles across exciting teams including consumer products, entertainment and sports in Latin America! Jan. 14, 2025
              </a>
              <div>Latin America Student and Recent Graduates</div>
            </article>
          </body>
        </html>
        """

    rows = extract_rendered_card_jobs(
        html,
        page_url="https://www.disneycareers.com/en/search-jobs/game/391/1",
        company="Disney",
        source_id="disney_campaign_page",
        allow_any_anchor=True,
    )
    assert rows == []


def test_extract_rendered_card_jobs_rejects_generic_site_pages_with_allow_any_anchor() -> None:
    html = """
        <html>
          <body>
            <article class="card">
              <h3>Yatzy Royale A Classic Puzzle with No Dice</h3>
              <a href="/games/yatzy-royale">Learn More</a>
            </article>
            <article class="card">
              <h3>Tech</h3>
              <a href="/job/tech">Learn More</a>
            </article>
            <article class="card">
              <h3>Art</h3>
              <a href="/job/art">Read More</a>
            </article>
            <article class="card">
              <h3>About Tether Studios</h3>
              <a href="/about">Learn More</a>
            </article>
            <article class="card">
              <h3>Search FAQ</h3>
              <a href="/support">View Details</a>
            </article>
            <article class="card">
              <h3>Privacy Policy</h3>
              <a href="/privacy">Read More</a>
            </article>
            <article class="card">
              <h3>Games</h3>
              <a href="/all-games">Details</a>
            </article>
            <article class="job-card">
              <h3>Business Development Manager</h3>
              <div>Helsinki Metropolitan Area</div>
              <div>Permanent</div>
              <a href="/careers/business-development-manager">Apply Now</a>
            </article>
          </body>
        </html>
        """

    rows = extract_rendered_card_jobs(
        html,
        page_url="https://www.tetherstudios.com/careers",
        company="Tether Studios",
        source_id="tether_mixed_pages",
        allow_any_anchor=True,
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "Business Development Manager"
    assert (
        rows[0]["jobLink"] == "https://www.tetherstudios.com/careers/business-development-manager"
    )


def test_extract_rendered_card_jobs_skips_non_location_cells_before_city_extraction() -> None:
    html = """
        <a href="https://example.com/jobs/1" target="_blank" class="join_row">
          <div class="d-table-cell vacancy ps-1 pe-3 pe-md-0">Gameplay Engineer</div>
          <div class="d-table-cell vacancy">.elementor</div>
          <div class="d-table-cell vacancy">AI Solutions PM</div>
          <div class="d-table-cell vacancy">Administrative & Support Services</div>
          <div class="d-table-cell vacancy">AGREE DISAGREE LEARN MORE</div>
          <div class="d-table-cell vacancy">Assist with outdoor photos</div>
          <div class="d-table-cell vacancy">Art & Animation</div>
          <div class="d-table-cell vacancy text-end pe-1">Berlin, DE</div>
        </a>
        """
    rows = extract_rendered_card_jobs(
        html,
        page_url="https://example.com/careers/",
        company="Example Studio",
        source_id="example_rendered_noise",
        allow_any_anchor=True,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Gameplay Engineer"
    assert row["city"] == "Berlin"
    assert row["country"] == "DE"
    assert row["locations"] == [{"city": "Berlin", "country": "DE"}]
    assert row["locationSummary"] == "Berlin, DE"


def test_rendered_card_family_and_ats_wrapper_do_not_overlap_on_zenimax() -> None:
    ctx = AdapterPluginContext(
        family="static",
        adapter_key="static",
        source_identity="jobs.zenimax.com",
    )
    assert ats_wrappers.can_handle(ctx)
    assert not rendered_cards.can_handle(ctx)
