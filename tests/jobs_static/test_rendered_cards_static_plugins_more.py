"""Tests for rendered cards and static plugins additional plugin behavior."""

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
        if url.startswith("https://example.net/jobs/"):
            return f"<html><body><h1>{url.rsplit('/', 1)[-1]}</h1></body></html>"
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
        links = {row["jobLink"] for row in rows}
        assert "https://example.net/jobs/environment-artist" in links
        assert "https://example.net/jobs/technical-artist" in links
        diagnostics = jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}
        detail = (diagnostics.get("details") or [{}])[0]
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
