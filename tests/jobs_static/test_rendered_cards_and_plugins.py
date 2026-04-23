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


def test_stellar_join_us_page_with_ashby_job_anchors_is_not_dead_listed() -> None:
    html = """
        <html>
          <body>
            <article class="careers-listing">
              <a href="https://jobs.ashbyhq.com/stellarentertainment/8615ea53-9992-489f-b2cd-38ede3434679" target="_blank" class="join_row Engineering United Kingdom">
                <div class="d-table-cell vacancy ps-1 pe-3 pe-md-0">Principal Rendering Engineer</div>
                <div class="d-table-cell vacancy">Engineering</div>
                <div class="d-table-cell vacancy text-end pe-1">Guildford, UK | Remote, UK</div>
                <div class="d-table-cell d-none">United Kingdom</div>
              </a>
              <a href="https://jobs.ashbyhq.com/stellarentertainment/393927f5-29cd-492c-b091-7a5eaeab7284" target="_blank" class="join_row Art United Kingdom">
                <div class="d-table-cell vacancy ps-1 pe-3 pe-md-0">Lighting Artist</div>
                <div class="d-table-cell vacancy">Art</div>
                <div class="d-table-cell vacancy text-end pe-1">Guildford, UK | Remote, UK</div>
                <div class="d-table-cell d-none">United Kingdom</div>
              </a>
              <div class="sorry ps-1">Sorry no jobs match your search...</div>
            </article>
          </body>
        </html>
        """

    job_like, reason = classify_job_page(
        html,
        "https://stellarentertainment.software/join-us/",
        page_title="Join us - Stellar Entertainment",
    )
    assert job_like
    assert reason in {"job_listing_anchors", "job_markers"}

    rows = extract_rendered_card_jobs(
        html,
        page_url="https://stellarentertainment.software/join-us/",
        company="Stellar Entertainment",
        source_id="stellar_test",
        allow_any_anchor=True,
    )
    assert len(rows) == 2
    assert any(row["title"].startswith("Principal Rendering Engineer") for row in rows)
    assert any(row["title"].startswith("Lighting Artist") for row in rows)
    assert {row["jobLink"] for row in rows} == {
        "https://jobs.ashbyhq.com/stellarentertainment/8615ea53-9992-489f-b2cd-38ede3434679",
        "https://jobs.ashbyhq.com/stellarentertainment/393927f5-29cd-492c-b091-7a5eaeab7284",
    }


def test_run_static_studio_pages_source_accepts_cdpr_query_key_override() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    base = "https://cdpr-careers-test.example.com/en/jobs"
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Cdprojektred (Manual Website)",
            "studio": "Cdprojektred",
            "adapter": "static",
            "company": "Cdprojektred",
            "pages": [base],
            "detailQueryKeys": ["gh_jid"],
            "enabledByDefault": True,
        }
    ]
    listing = f'<html><body><a href="{base}?gh_jid=1234">Gameplay Engineer</a></body></html>'
    detail = "<html><body><h1>Gameplay Engineer</h1></body></html>"
    try:

        def fake_fetch(url: str, _: int) -> str:
            if url == base:
                return listing
            if url == f"{base}?gh_jid=1234":
                return detail
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert rows[0]["jobLink"] == f"{base}?gh_jid=1234"
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_accepts_larian_uuid_paths_and_rejects_location_pages() -> (
    None
):
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Larian Studios (Manual Website)",
            "studio": "Larian Studios",
            "adapter": "static",
            "company": "Larian Studios",
            "pages": ["https://larian.com/careers"],
            "enabledByDefault": True,
        }
    ]
    listing = (
        "<html><body>"
        '<a href="https://larian.com/careers/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee">Senior Engineer</a>'
        '<a href="https://larian.com/careers/location/gent?location=Gent">Gent</a>'
        "</body></html>"
    )
    detail = "<html><body><h1>Senior Engineer</h1></body></html>"
    try:

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://larian.com/careers":
                return listing
            if url == "https://larian.com/careers/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee":
                return detail
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert (
            rows[0]["jobLink"] == "https://larian.com/careers/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_accepts_remedy_query_key_override() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    base = "https://remedy-careers-test.example.com/careers"
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Remedy Entertainment (Manual Website)",
            "studio": "Remedy Entertainment",
            "adapter": "static",
            "company": "Remedy Entertainment",
            "pages": [base],
            "detailQueryKeys": ["jobid"],
            "enabledByDefault": True,
        }
    ]
    listing = f'<html><body><a href="{base}/open?jobid=42">Rendering Programmer</a></body></html>'
    detail = "<html><body><h1>Rendering Programmer</h1></body></html>"
    try:

        def fake_fetch(url: str, _: int) -> str:
            if url == base:
                return listing
            if url == f"{base}/open?jobid=42":
                return detail
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert rows[0]["jobLink"] == f"{base}/open?jobid=42"
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_accepts_ubisoft_query_key_override() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Ubisoft (Manual Website)",
            "studio": "Ubisoft",
            "adapter": "static",
            "company": "Ubisoft",
            "pages": ["https://www.ubisoft.com/en-us/company/careers/locations/milan"],
            "detailQueryKeys": ["jobid"],
            "enabledByDefault": True,
        }
    ]
    listing = '<html><body><a href="https://www.ubisoft.com/en-us/company/careers/search?jobid=99">Engine Programmer</a></body></html>'
    detail = "<html><body><h1>Engine Programmer</h1></body></html>"
    try:

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://www.ubisoft.com/en-us/company/careers/locations/milan":
                return listing
            if url == "https://www.ubisoft.com/en-us/company/careers/search?jobid=99":
                return detail
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert rows[0]["jobLink"] == "https://www.ubisoft.com/en-us/company/careers/search?jobid=99"
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_activision_plugin_extracts_job_links() -> None:
    source_rows = [
        {
            "name": "Activision (Manual Website)",
            "studio": "Activision",
            "adapter": "static",
            "company": "Activision",
            "pages": ["https://careers.activision.com"],
            "enabledByDefault": True,
            "id": "static:listing_url:https://careers.activision.com",
        }
    ]
    listing_html = """
        <a href="https://careers.activision.com/search-results">Search Jobs</a>
        <a href="https://careers.activision.com/job/R025845/Programmeur-senior-Productivite">Programmeur senior, Productivite</a>
        <a href="https://careers.activision.com/apply?jobSeqNo=ACPUUSR025845EXTERNAL">Apply Now</a>
        """
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: listing_html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=source_rows,
    )
    assert len(rows) == 1
    assert str(rows[0].get("jobLink") or "").endswith(
        "/job/R025845/Programmeur-senior-Productivite"
    )


def test_run_static_studio_pages_source_amber_jobvite_listing_only() -> None:
    html = """
        <a href="/amberstudiocareers/job/oSIbufwZ">
          <div>Senior Unity Game Engineer (Project Based)</div>
          <div>Remote, Brazil</div>
        </a>
        """
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "Amber (Sheet)",
                "studio": "Amber",
                "company": "Amber",
                "pages": ["https://jobs.jobvite.com/amberstudiocareers/search?l=Worldwide"],
                "id": "static:listing_url:https://jobs.jobvite.com/amberstudiocareers/search?l=Worldwide",
            }
        ],
    )
    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://jobs.jobvite.com/amberstudiocareers/job/oSIbufwZ"


def test_run_static_studio_pages_source_amanotes_plugin_extracts_next_data_positions() -> None:
    html = """
        <script id="__NEXT_DATA__" type="application/json">
        {
          "props": {
            "pageProps": {
              "positions": [
                {
                  "title": "Senior Backend Developer (NodeJS)",
                  "location": "HCMC",
                  "type": "Full-time",
                  "team": "Tech",
                  "leverId": "43fa1ef6-a45e-4718-9b8f-022c673632c6",
                  "slug": {"current": "senior-backend-developer"}
                },
                {
                  "title": "[New Games] Game Unit Manager",
                  "location": "HCMC",
                  "type": "Full-time",
                  "team": "Games",
                  "leverId": "cb73238c-a74d-4d0f-9dfd-a4c32e0f1c41",
                  "slug": {"current": "new-games-game-unit-manager"}
                }
              ]
            }
          }
        }
        </script>
        """
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "Amanotes (Sheet)",
                "studio": "Amanotes",
                "company": "Amanotes",
                "pages": ["https://www.careers.amanotes.com/jobs"],
                "id": "static:listing_url:https://www.careers.amanotes.com/jobs",
            }
        ],
    )
    assert [row["title"] for row in rows] == [
        "Senior Backend Developer (NodeJS)",
        "[New Games] Game Unit Manager",
    ]
    assert rows[0]["jobLink"] == (
        "https://www.careers.amanotes.com/jobs/"
        "senior-backend-developer/43fa1ef6-a45e-4718-9b8f-022c673632c6"
    )
    assert rows[0]["city"] == "HCMC"
    assert rows[0]["country"] == "Vietnam"
    assert rows[0]["locations"] == [{"city": "HCMC", "country": "Vietnam"}]
    assert rows[0]["locationSummary"] == "HCMC, Vietnam"
    assert rows[0]["workType"] == ""


def test_run_static_studio_pages_source_amanotes_plugin_preserves_remote_as_work_type() -> None:
    html = """
        <script id="__NEXT_DATA__" type="application/json">
        {
          "props": {
            "pageProps": {
              "positions": [
                {
                  "title": "QA Engineer",
                  "location": "Remote",
                  "type": "Full-time",
                  "team": "Game",
                  "slug": {"current": "qa-engineer"},
                  "leverId": "job-1"
                }
              ]
            }
          }
        }
        </script>
        """
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "Amanotes (Sheet)",
                "studio": "Amanotes",
                "company": "Amanotes",
                "pages": ["https://www.careers.amanotes.com/jobs"],
                "id": "static:listing_url:https://www.careers.amanotes.com/jobs",
            }
        ],
    )
    assert len(rows) == 1
    assert rows[0]["city"] == ""
    assert rows[0]["country"] == ""
    assert rows[0]["locations"] == []
    assert rows[0]["locationSummary"] == ""
    assert rows[0]["workType"] == "Remote"


def test_run_static_studio_pages_source_blizzard_plugin_follows_role_pages_to_search_results() -> (
    None
):
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Blizzard Entertainment (Sheet)",
            "studio": "Blizzard Entertainment",
            "adapter": "static",
            "company": "Blizzard Entertainment",
            "pages": ["https://careers.blizzard.com/global/en"],
            "enabledByDefault": True,
            "id": "static:listing_url:https://careers.blizzard.com/global/en",
        }
    ]
    home_html = '<a href="/global/en/engineering-technology">ENGINEERING & TECHNOLOGY</a>'
    role_html = '<a href="https://careers.blizzard.com/global/en/search-results?rk=l-engineering-technology&sortBy=Most%20relevant">View Open Jobs</a>'
    results_html = """
        <a href="https://careers.blizzard.com/global/en/job/R026699/Software-Engineer-Server-World-of-Warcraft-Irvine-CA">Software Engineer, Server - World of Warcraft | Irvine, CA</a>
        <div>Location Irvine, California, United States of America Posted Date January 30 2026 Category Engineering Job Id R026699</div>
        <a href="https://careers.blizzard.com/global/en/job/R026419/Lead-Systems-Engineer-Unreal-Engine-5">Lead Systems Engineer, Unreal Engine 5</a>
        <div>Location Irvine, California, United States of America Posted Date February 03 2026 Category Engineering Job Id R026419</div>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://careers.blizzard.com/global/en":
            return home_html
        if url == "https://careers.blizzard.com/global/en/engineering-technology":
            return role_html
        if "search-results?rk=l-engineering-technology" in url:
            return results_html
        raise RuntimeError(f"Unexpected URL: {url}")

    try:
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        titles = {str(row.get("title") or "") for row in rows}
        assert "Software Engineer, Server - World of Warcraft | Irvine, CA" in titles
        assert "Lead Systems Engineer, Unreal Engine 5" in titles
        assert len(rows) == 2
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_classifies_ea_as_js_required() -> None:
    sources = [
        {
            "name": "Electronic Arts (Manual Website)",
            "studio": "Electronic Arts",
            "company": "Electronic Arts",
            "adapter": "static",
            "pages": ["https://careers.ea.com/careers"],
            "enabledByDefault": True,
        }
    ]

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=sources,
    )

    assert rows == []
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert str(detail.get("classification") or "") == "js_required"
    assert str(detail.get("failureBucket") or "") == "js_required"
    assert bool(detail.get("browserFallbackRecommended"))


def test_run_static_studio_pages_source_classifies_linkedin_429_as_anti_bot_or_challenge() -> None:
    sources = [
        {
            "name": "LinkedIn Careers",
            "studio": "LinkedIn",
            "adapter": "static",
            "company": "LinkedIn",
            "pages": ["https://www.linkedin.com/jobs/view/123"],
            "enabledByDefault": True,
        }
    ]

    def fake_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(f"HTTP 429 Too Many Requests for {url}")

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=sources,
    )

    assert rows == []
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert str(detail.get("classification") or "") == "anti_bot_or_challenge"
    assert str(detail.get("failureBucket") or "") == "anti_bot_or_challenge"
    assert bool(detail.get("browserFallbackRecommended"))


def test_run_static_studio_pages_source_classifies_sega_as_js_required() -> None:
    sources = [
        {
            "name": "SEGA (Manual Website)",
            "studio": "SEGA",
            "company": "SEGA",
            "adapter": "static",
            "pages": ["https://www.sega.co.jp/en/recruit/"],
            "enabledByDefault": True,
        }
    ]

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=sources,
    )

    assert rows == []
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert str(detail.get("classification") or "") == "js_required"
    assert str(detail.get("failureBucket") or "") == "js_required"
    assert bool(detail.get("browserFallbackRecommended"))


def test_run_static_studio_pages_source_climax_listing_only() -> None:
    html = """
        <a href="https://www.climaxstudios.com/join-our-team/jobs/eD-experienced-games-producer/">
          <h3>Experienced Games Producer</h3>
          <p>Exciting role</p>
          <div>Location London England United Kingdom</div>
          <div>Create, Permanent</div>
        </a>
        """
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "Climax Studios (Sheet)",
                "studio": "Climax Studios",
                "company": "Climax Studios",
                "pages": ["https://www.climaxstudios.com/join-our-team/jobs/"],
                "id": "static:listing_url:https://www.climaxstudios.com/join-our-team/jobs/",
            }
        ],
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "Experienced Games Producer"


def test_run_static_studio_pages_source_globalstep_listing_only() -> None:
    html = """
        <a href="https://globalstep.com/jobs/unity-game-developer/">
          <h2>Unity Game Developer</h2>
          <span>Bucharest - Romania</span>
          <span>3+ Years</span>
          <span>More Details</span>
        </a>
        """
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "GlobalStep (Sheet)",
                "studio": "GlobalStep",
                "company": "GlobalStep",
                "pages": ["https://globalstep.com/careers/"],
                "id": "static:listing_url:https://globalstep.com/careers/",
            }
        ],
    )
    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://globalstep.com/jobs/unity-game-developer/"
    assert rows[0]["city"] == "Bucharest"
    assert rows[0]["country"] == "Romania"


def test_run_static_studio_pages_source_littlechicken_plugin_extracts_listing_cards() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Little Chicken (Manual Website)",
            "studio": "Little Chicken",
            "adapter": "static",
            "company": "Little Chicken",
            "pages": ["https://www.littlechicken.nl/jobs/"],
            "enabledByDefault": True,
            "id": "static:listing_url:https://www.littlechicken.nl/jobs/",
        }
    ]
    listing_html = """
        <article><h2>3D Artist Internship</h2><a href="/job/3d-artist-internship/">Read more</a></article>
        <article><h2>2D Artist Internship</h2><a href="/job/2d-artist-internship/">Read more</a></article>
        <article><h2>QA Tester Internship</h2><a href="/job/qa-tester-internship/">Read more</a></article>
        """
    detail_html = """
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"JobPosting","title":"3D Artist Internship","url":"https://www.littlechicken.nl/job/3d-artist-internship/","hiringOrganization":{"@type":"Organization","name":"Little Chicken"}}
        </script>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://www.littlechicken.nl/jobs/":
            return listing_html
        if "littlechicken.nl/job/" in url:
            return detail_html.replace(
                "3d-artist-internship", url.rstrip("/").split("/")[-1]
            ).replace(
                "3D Artist Internship", url.rstrip("/").split("/")[-1].replace("-", " ").title()
            )
        raise RuntimeError(f"Unexpected URL: {url}")

    try:
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        titles = {str(row.get("title") or "") for row in rows}
        assert "3D Artist Internship" in titles
        assert "2D Artist Internship" in titles
        assert "Qa Tester Internship" in titles
        assert len(rows) == 3
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_loads_kojima_dynamic_listing() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    base_url = "https://kojima-careers-test.example.com/en/careers"
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Kojima Productions (Manual Website)",
            "studio": "Kojima Productions",
            "adapter": "static",
            "company": "Kojima Productions",
            "pages": [base_url],
            "enabledByDefault": True,
        }
    ]
    listing_html = """
        <table>
          <tr class="job-listing-item"><td><a href="/en/game-programmer">Game Programmer</a></td></tr>
          <tr class="job-listing-item"><td><a href="/en/ai-programmer">AI Programmer</a></td></tr>
        </table>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == base_url:
            return listing_html
        if url in {
            "https://kojima-careers-test.example.com/en/game-programmer",
            "https://kojima-careers-test.example.com/en/ai-programmer",
        }:
            return "<html><body><h1>job</h1></body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    try:
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
        titles = {str(row.get("title") or "") for row in rows}
        assert "Game Programmer" in titles
        assert "AI Programmer" in titles
        assert len(rows) == 2
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_milestone_plugin_extracts_intervieweb_iframe() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Milestone (Manual Website)",
            "studio": "Milestone",
            "adapter": "static",
            "company": "Milestone",
            "pages": ["https://milestone.it/careers"],
            "enabledByDefault": True,
            "id": "static:listing_url:https://milestone.it/careers",
        }
    ]
    listing_html = """
        <script src="https://cezanneondemand.intervieweb.it/integration/announces_js.php?lang=en&utype=0&k=abc123&LAC=milestone&d=milestone.it&annType=published&view=list&defgroup=name&gnavenable=1&desc=1&typeView=large"></script>
        """
    iframe_html = """
        <a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=60982&lang=en">Game Designer_tech</a>
        <div>Milano, Italia Design</div>
        <a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=61104&lang=en">JUNIOR IT SERVICE DESK</a>
        <div>Milano, Italia ICT and Information Systems</div>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://milestone.it/careers":
            return listing_html
        if "module=iframeAnnunci" in url and "act1=23" in url:
            return iframe_html
        raise RuntimeError(f"Unexpected URL: {url}")

    try:
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        titles = {str(row.get("title") or "") for row in rows}
        assert "Game Designer_tech" in titles
        assert "JUNIOR IT SERVICE DESK" in titles
        assert len(rows) == 2
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_nacon_plugin_extracts_listing_cards() -> None:
    from src.jobs.adapters.plugins.static.register import register_static_plugins

    register_static_plugins()
    source_rows = [
        {
            "name": "Nacon Studio Milan (Manual Website)",
            "studio": "Nacon Studio Milan",
            "adapter": "static",
            "company": "Nacon Studio Milan",
            "pages": ["https://www.naconstudiomilan.com/careers/"],
            "enabledByDefault": True,
            "id": "static:listing_url:https://www.naconstudiomilan.com/careers",
        }
    ]
    listing_html = """
        <article>
          <h4>Gameplay Designer</h4>
          <p>We are looking for a Gameplay Designer.</p>
          <a href="/careers/gameplay-designer/">Learn more</a>
        </article>
        <article>
          <h4>AI Programmer</h4>
          <p>We are looking for an experienced AI Programmer.</p>
          <a href="/careers/ai-programmer/">Learn more</a>
        </article>
        """
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: listing_html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=source_rows,
    )
    titles = {str(row.get("title") or "") for row in rows}
    assert titles == {"Gameplay Designer", "AI Programmer"}


def test_run_static_studio_pages_source_sheet_studios_uses_rendered_card_fallback() -> None:
    html = """
        <html>
          <body>
            <article class="job-card">
              <h3>Business Development Manager</h3>
              <div>Helsinki Metropolitan Area</div>
              <div>Permanent</div>
              <a href="/open-positions/business-development-manager">Learn More</a>
            </article>
          </body>
        </html>
        """
    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "Rovio Entertainment (Sheet)",
                "studio": "Rovio Entertainment",
                "company": "Rovio Entertainment",
                "pages": ["https://www.rovio.com/open-positions/"],
                "id": "static:listing_url:https://www.rovio.com/open-positions/",
            }
        ],
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "Business Development Manager"
    assert rows[0]["jobLink"] == "https://www.rovio.com/open-positions/business-development-manager"
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert int(detail.get("keptCount") or 0) == 1
    assert str(detail.get("failureBucket") or "") != "js_required"


def test_run_static_studio_pages_source_uses_rendered_card_fallback_for_manual_table_pages() -> (
    None
):
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Example Manual Website (Manual Website)",
            "studio": "Example Manual Website",
            "adapter": "static",
            "company": "Example Manual Website",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
            "id": "static:listing_url:https://example.net/careers",
        }
    ]
    html = """
        <html>
          <body>
            <table class="jobs-table">
              <tbody>
                <tr class="job-row">
                  <td>Environment Artist</td>
                  <td>Remote</td>
                  <td>Permanent</td>
                  <td><a href="/jobs/environment-artist">Read More</a></td>
                </tr>
                <tr class="job-row">
                  <td>Technical Artist</td>
                  <td>Berlin, Germany</td>
                  <td>Contract</td>
                  <td><a href="/jobs/technical-artist">Read More</a></td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://example.net/careers":
            return html
        raise RuntimeError(f"Unexpected URL: {url}")

    try:
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
        assert len(rows) == 2
        assert {row["title"] for row in rows} == {"Environment Artist", "Technical Artist"}
        assert {row["jobLink"] for row in rows} == {
            "https://example.net/jobs/environment-artist",
            "https://example.net/jobs/technical-artist",
        }
        detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[
            0
        ]
        assert int(detail.get("keptCount") or 0) == 2
        assert str(detail.get("failureBucket") or "") != "js_required"
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_kojima_plugin_uses_browser_listing() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Kojimaproductions (Manual Website)",
            "studio": "Kojimaproductions",
            "adapter": "static",
            "company": "Kojimaproductions",
            "pages": ["https://www.kojimaproductions.jp/en/careers"],
            "enabledByDefault": True,
            "id": "static:listing_url:https://www.kojimaproductions.jp/en/careers",
        }
    ]
    listing_html = "<html><body><p>Open Positions</p></body></html>"
    browser_html = """
        <a href="/en/game-programmer">Game Programmer<br>Programming<br>Tokyo, Japan</a>
        <a href="/en/technical-artist">Technical Artist<br>Programming<br>Tokyo, Japan</a>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://www.kojimaproductions.jp/en/careers":
            return listing_html
        raise RuntimeError(f"Unexpected URL: {url}")

    try:
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            try_playwright=lambda _url, _timeout: (browser_html, ""),
        )
        titles = {str(row.get("title") or "") for row in rows}
        assert "Game Programmer" in titles
        assert "Technical Artist" in titles
        assert len(rows) == 2
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev
