"""Tests for rendered cards and static plugins core plugin behavior."""

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
