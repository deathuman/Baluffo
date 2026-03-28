# ruff: noqa: F403,F405
from tests.jobs_fetcher_helpers import *

patch_jobs_fetcher_aliases()


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


def test_run_static_studio_pages_source_with_fixture() -> None:
    listing = _fixture("littlechicken_jobs_page.html")
    detail = _fixture("littlechicken_job_detail.html")
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use example.net so the generic fallback runs (no static plugin handles it)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Fallback Test Studio",
            "studio": "Fallback Test Studio",
            "adapter": "static",
            "company": "Fallback Test Studio",
            "pages": ["https://example.net/about-us/jobs/"],
            "enabledByDefault": True,
        }
    ]

    try:

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://example.net/about-us/jobs/":
                return listing
            if "/job/" in url:
                return detail
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 2
        assert any("/job/" in (row.get("jobLink") or "") for row in rows)
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_loads_kojima_dynamic_listing() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use a host that has no static plugin so the generic flow runs (no Kojima plugin).
    # Listing HTML already contains job links so we don't need the dynamic POST.
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
        assert "Ai Programmer" in titles
        assert len(rows) == 2
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


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


def test_run_static_studio_pages_source_accepts_larian_uuid_paths_and_rejects_location_pages() -> (
    None
):
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use larian.com so fallback runs and applies /careers/location/ exclusion heuristic (no plugin for larian)
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


def test_run_static_studio_pages_source_accepts_cdpr_query_key_override() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use a host that has no static plugin so the generic flow runs with detailQueryKeys
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


def test_run_static_studio_pages_source_accepts_remedy_query_key_override() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use a host that has no static plugin so the generic flow runs with detailQueryKeys
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


def test_run_static_studio_pages_source_dedupes_candidate_links_before_fetch() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use example.net so the generic fallback runs (no static plugin)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Dedup Test Studio",
            "studio": "Dedup Test Studio",
            "adapter": "static",
            "company": "Dedup Test Studio",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
        }
    ]
    listing = (
        "<html><body>"
        '<div class="job-listing-item"><a href="/job/engine-programmer">Engine Programmer</a></div>'
        '<a href="/job/engine-programmer">Engine Programmer</a>'
        '<script>var detail = "https://example.net/job/engine-programmer";</script>'
        "</body></html>"
    )
    detail = "<html><body><h1>Engine Programmer</h1></body></html>"
    fetch_counts = {"detail": 0}

    try:

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://example.net/careers":
                return listing
            if url == "https://example.net/job/engine-programmer":
                fetch_counts["detail"] += 1
                return detail
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) == 1
        assert fetch_counts["detail"] == 1
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_parallelizes_detail_fetches() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use example.net so the generic fallback runs (no static plugin)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Parallel Static Studio",
            "studio": "Parallel Static Studio",
            "adapter": "static",
            "company": "Parallel Static Studio",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
        }
    ]
    listing = (
        "<html><body>"
        '<a href="/job/a">Role A</a>'
        '<a href="/job/b">Role B</a>'
        '<a href="/job/c">Role C</a>'
        "</body></html>"
    )
    active = 0
    peak = 0
    active_lock = threading.Lock()

    try:

        def fake_fetch(url: str, _: int) -> str:
            nonlocal active, peak
            if url == "https://example.net/careers":
                return listing
            if url in {
                "https://example.net/job/a",
                "https://example.net/job/b",
                "https://example.net/job/c",
            }:
                with active_lock:
                    active += 1
                    peak = max(peak, active)
                time.sleep(0.05)
                with active_lock:
                    active -= 1
                title = url.rsplit("/", 1)[-1].upper()
                return f"<html><body><h1>{title}</h1></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            static_detail_concurrency=3,
        )
        assert len(rows) == 3
        assert peak >= 2
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_scrapy_static_source_handles_malformed_json() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Scrapy Test Studio",
            "studio": "Scrapy Test Studio",
            "adapter": "scrapy_static",
            "pages": ["https://example.com/jobs"],
            "enabledByDefault": True,
        }
    ]
    fake_result = mock.Mock()
    fake_result.stdout = b"not json"
    fake_result.stderr = b"runner stderr"
    fake_result.returncode = 1
    try:
        with mock.patch("subprocess.run", return_value=fake_result):
            with mock.patch.object(jf, "set_source_diagnostics") as diag:
                rows = jf.run_scrapy_static_source(
                    fetch_text=lambda _url, _timeout: "",
                    timeout_s=5,
                    retries=1,
                    backoff_s=1.0,
                )
                assert rows == []
                diag.assert_called_once()
                args, kwargs = diag.call_args
                assert args[0] == "scrapy_static_sources"
                assert kwargs.get("adapter") == "scrapy_static"
                details = kwargs.get("details") or []
                assert details
                assert str(details[0].get("classification") or "") == "parse_error"
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_scrapy_static_registration_in_default_loaders() -> None:
    assert "scrapy_static_sources" in jfr.DEFAULT_SOURCE_LOADER_NAMES
    assert "google_sheets_1er2oaxo" in jfr.DEFAULT_SOURCE_LOADER_NAMES
    assert "google_sheets_1mvqhxat" in jfr.DEFAULT_SOURCE_LOADER_NAMES
    assert jfr.SOURCE_REPORT_META["scrapy_static_sources"]["adapter"] == "scrapy_static"
    assert jfr.SOURCE_REPORT_META["google_sheets_1er2oaxo"]["adapter"] == "csv"
    assert jfr.SOURCE_REPORT_META["google_sheets_1mvqhxat"]["adapter"] == "csv"
    names = [name for name, _ in jf.default_source_loaders()]
    assert "scrapy_static_sources" in names
    assert "google_sheets_1er2oaxo" in names
    assert "google_sheets_1mvqhxat" in names


def test_run_pipeline_writes_browser_fallback_queue() -> None:
    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Valve",
                    "name": "Valve Careers Scrapy",
                    "status": "ok",
                    "fetchedCount": 10,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": True,
                    "top_reject_reasons": ["missing_title:4"],
                    "sourceId": "valve-source-id",
                    "pages": ["https://www.valvesoftware.com/en/jobs"],
                    "stats": {
                        "downloader/request_count": 10,
                        "downloader/response_count": 10,
                        "downloader/response_status_count/200": 10,
                        "retry/count": 0,
                        "item_scraped_count": 0,
                        "candidate_links_found": 8,
                        "detail_pages_visited": 8,
                        "jobs_emitted": 0,
                        "jobs_rejected_validation": 8,
                        "finish_reason": "finished",
                    },
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-scrapy-fallback") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert len(queue_rows) == 1
        assert str(queue_rows[0].get("adapter") or "") == "scrapy_static"
        assert str(queue_rows[0].get("classification") or "") == "needs_review"
        assert str((report.get("outputs") or {}).get("browserFallbackQueue") or "") == str(
            queue_path
        )
        details = ((report.get("sources") or [{}])[0].get("details") or [{}])[0]
        assert str(details.get("classification") or "") == "needs_review"
        assert bool(details.get("browserFallbackRecommended"))


def test_browser_fallback_queue_one_canonical_per_source() -> None:
    """When a source has multiple pages (main + sub-pages), queue gets one row with canonical listing URL."""
    main_url = "https://supercell.com/en/careers/"
    sub_urls = [
        "https://supercell.com/en/careers/joining-supercell/",
        "https://supercell.com/en/careers/our-offices/",
    ]

    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Supercell",
                    "name": "Supercell Careers",
                    "status": "ok",
                    "fetchedCount": 3,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": True,
                    "sourceId": "static:listing_url:https://supercell.com/en/careers/",
                    "pages": [main_url, *sub_urls],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-canonical") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert len(queue_rows) == 1
        assert queue_rows[0].get("page") == main_url
        assert str(queue_rows[0].get("studio") or "") == "Supercell"


def test_browser_fallback_queue_excludes_job_provider_domains() -> None:
    """Sources whose domain has job_provider (e.g. Remedy/Jobylon) are not added to the queue."""
    remedy_url = "https://www.remedygames.com/careers"

    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Remedy",
                    "name": "Remedy Careers",
                    "status": "ok",
                    "fetchedCount": 1,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": True,
                    "sourceId": "static:remedy",
                    "pages": [remedy_url],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-no-job-provider") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        remedy_rows = [r for r in queue_rows if "remedygames" in str(r.get("page") or "")]
        assert len(remedy_rows) == 0


def test_browser_fallback_queue_skips_needs_review_sources() -> None:
    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="multiple",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Nacon Studio Milan",
                    "name": "Nacon Studio Milan",
                    "status": "ok",
                    "fetchedCount": 1,
                    "keptCount": 0,
                    "error": "no jobs extracted from source pages",
                    "classification": "needs_review",
                    "browserFallbackRecommended": False,
                    "sourceId": "static:nacon",
                    "pages": ["https://www.naconstudiomilan.com/careers/"],
                    "stats": {},
                }
            ],
            partial_errors=[],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-browser-queue-skip-parse-zero") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-browser-fallback-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert queue_rows == []


def test_run_pipeline_writes_parser_regression_queue_for_top_level_site_changed_only() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith("https://example.com/careers"):
                return "https://example.com/careers/updated"
            return url

        def close(self) -> None:
            pass

    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="Site Changed Studio",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Studio A",
                    "name": "Site Changed Studio Careers",
                    "status": "ok",
                    "fetchedCount": 6,
                    "keptCount": 0,
                    "error": "",
                    "classification": "needs_review",
                    "browserFallbackRecommended": False,
                    "listingChanged": True,
                    "sourceId": "static:site-changed",
                    "pages": ["https://example.com/careers"],
                    "stats": {},
                },
            ],
            partial_errors=["HTTP 404 Not Found"],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-parser-regression-queue") as tmp:
        out = Path(tmp)
        with mock.patch.object(jf, "build_redirect_resolver", return_value=DummyRedirectResolver()):
            report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("scrapy_static_sources", scraper_loader)],
                show_progress=False,
            )
        queue_path = out / "jobs-parser-regression-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert len(queue_rows) == 1
        assert str(queue_rows[0].get("source") or "") == "Site Changed Studio"
        assert str(queue_rows[0].get("oldUrl") or "") == "https://example.com/careers"
        assert str(queue_rows[0].get("currentUrl") or "") == "https://example.com/careers/updated"
        assert str(queue_rows[0].get("lastStatus") or "") == "ok"
        assert str(queue_rows[0].get("classification") or "") == "site_changed"
        assert str((report.get("outputs") or {}).get("parserRegressionQueue") or "") == str(
            queue_path
        )
        assert str((report.get("outputs") or {}).get("browserFallbackQueue") or "") == str(
            out / "jobs-browser-fallback-queue.json"
        )
        assert int((report.get("healthSummary") or {}).get("siteChangedDiagnosedCount") or 0) == 1
        assert (
            int((report.get("healthSummary") or {}).get("siteChangedMissingOldUrlCount") or 0) == 0
        )
        assert int((report.get("healthSummary") or {}).get("parserRegressionQueueCount") or 0) == 1


def test_build_parser_regression_queue_prefers_listing_url_for_old_url() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith("https://example.com/careers"):
                return "https://example.com/careers/updated"
            return url

    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "scrapy_static_sources",
                "studio": "Site Changed Studio",
                "adapter": "scrapy_static",
                "status": "ok",
                "failureBucket": "site_changed",
                "listingUrl": "https://example.com/careers",
                "sourceId": "static:site-changed",
                "pages": ["https://example.com/careers/ignored"],
                "details": [
                    {
                        "name": "Site Changed Studio Careers",
                        "pages": ["https://example.com/careers/details-ignored"],
                    }
                ],
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=DummyRedirectResolver().resolve,
    )

    assert len(rows) == 1
    assert str(rows[0].get("oldUrl") or "") == "https://example.com/careers"
    assert str(rows[0].get("currentUrl") or "") == "https://example.com/careers/updated"


def test_build_parser_regression_queue_uses_provider_url_for_greenhouse_boards() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith(
                "https://boards-api.greenhouse.io/v1/boards/guerrillagames"
            ):
                return "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs/updated"
            return url

    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "greenhouse_boards",
                "adapter": "greenhouse",
                "status": "ok",
                "failureBucket": "site_changed",
                "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=DummyRedirectResolver().resolve,
    )

    assert len(rows) == 1
    assert (
        str(rows[0].get("oldUrl") or "")
        == "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true"
    )
    assert (
        str(rows[0].get("currentUrl") or "")
        == "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs/updated"
    )


def test_build_parser_regression_queue_uses_provider_url_for_workable_sources() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith(
                "https://apply.workable.com/api/v1/widget/accounts/wargaming"
            ):
                return "https://apply.workable.com/api/v1/widget/accounts/wargaming/jobs"
            return url

    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "workable_sources",
                "adapter": "workable",
                "status": "ok",
                "failureBucket": "site_changed",
                "providerUrl": "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true",
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=DummyRedirectResolver().resolve,
    )

    assert len(rows) == 1
    assert (
        str(rows[0].get("oldUrl") or "")
        == "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true"
    )
    assert (
        str(rows[0].get("currentUrl") or "")
        == "https://apply.workable.com/api/v1/widget/accounts/wargaming/jobs"
    )


def test_build_parser_regression_queue_prefers_listing_url_over_provider_url() -> None:
    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "greenhouse_boards",
                "adapter": "greenhouse",
                "status": "ok",
                "failureBucket": "site_changed",
                "listingUrl": "https://example.com/careers",
                "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=None,
    )

    assert len(rows) == 1
    assert str(rows[0].get("oldUrl") or "") == "https://example.com/careers"


def test_build_parser_regression_queue_does_not_use_error_text_without_provider_url() -> None:
    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "greenhouse_boards",
                "adapter": "greenhouse",
                "status": "ok",
                "failureBucket": "site_changed",
                "error": "HTTP 404 for https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=None,
    )

    assert rows == []


def test_site_changed_provider_url_reconciliation_counts_align() -> None:
    rows = [
        {
            "name": "greenhouse_boards",
            "adapter": "greenhouse",
            "status": "ok",
            "failureBucket": "site_changed",
            "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
        }
    ]

    assert jobs_reporting.count_site_changed_diagnosed_sources(rows) == 1
    assert jobs_reporting.count_site_changed_missing_old_url_sources(rows) == 0
    assert (
        len(
            jobs_reporting.build_parser_regression_queue(
                rows,
                generated_at="2026-03-28T12:00:00+00:00",
                resolve_redirect_url=None,
            )
        )
        == 1
    )


def test_build_parser_regression_queue_projects_listing_changed_to_artifact_flag() -> None:
    class DummyRedirectResolver:
        def resolve(self, url: str) -> str:
            if str(url or "").startswith("https://example.com/careers"):
                return "https://example.com/careers/updated"
            return url

    rows = jobs_reporting.build_parser_regression_queue(
        [
            {
                "name": "scrapy_static_sources",
                "studio": "Site Changed Studio",
                "adapter": "scrapy_static",
                "status": "ok",
                "failureBucket": "site_changed",
                "listingChanged": True,
                "sourceId": "static:site-changed",
                "pages": ["https://example.com/careers"],
                "details": [
                    {
                        "name": "Site Changed Studio Careers",
                        "pages": ["https://example.com/careers"],
                    }
                ],
            }
        ],
        generated_at="2026-03-28T12:00:00+00:00",
        resolve_redirect_url=DummyRedirectResolver().resolve,
    )

    assert len(rows) == 1
    assert str(rows[0].get("source") or "") == "Site Changed Studio"
    assert str(rows[0].get("oldUrl") or "") == "https://example.com/careers"
    assert str(rows[0].get("currentUrl") or "") == "https://example.com/careers/updated"
    assert bool(rows[0].get("listingFingerprintChanged"))


def test_run_pipeline_does_not_enqueue_parser_regression_from_nested_detail_only() -> None:
    def scraper_loader(**_: object):
        jf.set_source_diagnostics(
            "scrapy_static_sources",
            adapter="scrapy_static",
            studio="Nested Detail Studio",
            details=[
                {
                    "adapter": "scrapy_static",
                    "studio": "Nested Detail Studio",
                    "name": "Nested Detail Studio Careers",
                    "status": "ok",
                    "fetchedCount": 6,
                    "keptCount": 0,
                    "error": "",
                    "classification": "site_changed",
                    "browserFallbackRecommended": False,
                    "listingChanged": True,
                    "sourceId": "static:nested-detail",
                    "pages": ["https://example.com/nested-careers"],
                    "stats": {},
                }
            ],
            partial_errors=["no jobs extracted from source pages"],
        )
        return []

    with workspace_tmpdir("jobs-fetcher-parser-regression-queue-nested-detail") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("scrapy_static_sources", scraper_loader)],
            show_progress=False,
        )
        queue_path = out / "jobs-parser-regression-queue.json"
        assert queue_path.exists()
        queue_rows = json.loads(queue_path.read_text(encoding="utf-8"))
        assert queue_rows == []
        assert int((report.get("healthSummary") or {}).get("siteChangedDiagnosedCount") or 0) == 0
        assert (
            int((report.get("healthSummary") or {}).get("siteChangedMissingOldUrlCount") or 0) == 0
        )
        assert int((report.get("healthSummary") or {}).get("parserRegressionQueueCount") or 0) == 0


def test_scrapy_static_zero_extract_weak_path_falls_back_to_needs_review() -> None:
    detail = {
        "adapter": "scrapy_static",
        "studio": "Weak Signal Studio",
        "name": "Weak Signal Studio Careers",
        "status": "ok",
        "fetchedCount": 4,
        "keptCount": 0,
        "error": "",
        "classification": "ok_no_jobs",
        "browserFallbackRecommended": True,
        "signalQuality": "weak",
        "stats": {
            "candidate_links_found": 0,
            "detail_pages_visited": 0,
            "jobs_emitted": 0,
            "jobs_rejected_validation": 0,
        },
    }

    updated = static_scrapy._update_taxonomy_fields(detail)

    assert str(updated.get("classification") or "") == "needs_review"
    assert str(updated.get("failureBucket") or "") == "needs_review"
    assert str(updated.get("zeroKeptClassification") or "") == "legit_empty"
    assert not bool(updated.get("browserFallbackRecommended"))


def test_static_zero_extract_generic_path_falls_back_to_needs_review() -> None:
    detail = {
        "adapter": "static",
        "studio": "Capcom",
        "name": "Capcom Careers",
        "status": "error",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "no jobs extracted from source pages",
        "classification": "",
        "browserFallbackRecommended": False,
        "signalQuality": "strong",
        "stats": {
            "candidate_links_found": 0,
            "detail_pages_visited": 0,
            "jobs_emitted": 0,
            "jobs_rejected_validation": 0,
        },
    }

    updated = static_helpers.update_source_detail_taxonomy(detail)

    assert str(updated.get("classification") or "") == "needs_review"
    assert str(updated.get("failureBucket") or "") == "needs_review"
    assert str(updated.get("zeroKeptClassification") or "") == "needs_review"


def test_static_loader_disables_browser_fallback_after_environment_failure() -> None:
    sources = [
        {
            "name": "Alpha Studio (Manual Website)",
            "studio": "Alpha Studio",
            "adapter": "static",
            "pages": ["https://alpha.example/careers"],
            "enabledByDefault": True,
        },
        {
            "name": "Beta Studio (Manual Website)",
            "studio": "Beta Studio",
            "adapter": "static",
            "pages": ["https://beta.example/careers"],
            "enabledByDefault": True,
        },
    ]
    browser_calls: list[str] = []
    breaker = BrowserFallbackCircuitBreaker(cooldown_minutes=15)

    def fake_try_playwright(url: str, timeout_s: int) -> tuple[str, str]:
        browser_calls.append(url)
        return "", "browser fallback unavailable (playwright is not installed)"

    def failing_fetch_text(_url: str, _timeout: int) -> str:
        raise RuntimeError("HTTP Error 403: forbidden")

    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = sources
    try:
        with pytest.raises(AdapterValidationError):
            jf.run_static_studio_pages_source(
                fetch_text=failing_fetch_text,
                timeout_s=5,
                retries=0,
                backoff_s=0.0,
                source_state_rows={},
                try_playwright=breaker.wrap(fake_try_playwright),
                force_refresh_all=True,
            )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert browser_calls == ["https://alpha.example/careers"]
    state_row = breaker.to_state_row()
    assert state_row["browserFallbackFailureCount"] == 1
    assert "browserFallbackQuarantinedUntilAt" in state_row


def test_run_scrapy_static_source_timeout_is_not_requeued() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Tequilaworks (Manual Website)",
            "studio": "Tequilaworks",
            "adapter": "scrapy_static",
            "pages": ["https://tequilaworks.com/en/careers"],
            "enabledByDefault": True,
        }
    ]
    try:
        with mock.patch(
            "subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="runner", timeout=20)
        ):
            jf.SOURCE_DIAGNOSTICS.clear()
            rows = jf.run_scrapy_static_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=0,
                backoff_s=0,
            )
            assert rows == []
            detail = (
                (jf.SOURCE_DIAGNOSTICS.get("scrapy_static_sources") or {}).get("details") or [{}]
            )[0]
            assert str(detail.get("classification") or "") == "browser_timeout"
            assert not bool(detail.get("browserFallbackRecommended"))
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_scrapy_static_registry_from_browser_queue_collapses_by_source_id() -> None:
    """When the browser queue has multiple rows for the same sourceId, registry has one row per source with best URL."""
    with workspace_tmpdir("jobs-fetcher-registry-collapse") as tmp:
        queue_path = Path(tmp) / "jobs-browser-fallback-queue.json"
        # Same sourceId, two pages (main has shorter path)
        queue_path.write_text(
            json.dumps(
                [
                    {
                        "adapter": "scrapy_static",
                        "sourceId": "static:supercell",
                        "name": "Supercell",
                        "studio": "Supercell",
                        "page": "https://supercell.com/en/careers/joining-supercell/",
                    },
                    {
                        "adapter": "scrapy_static",
                        "sourceId": "static:supercell",
                        "name": "Supercell",
                        "studio": "Supercell",
                        "page": "https://supercell.com/en/careers/",
                    },
                ],
                indent=2,
            ),
            encoding="utf-8",
        )
        with mock.patch.object(jobs_common, "SCRAPY_BROWSER_QUEUE_PATH", queue_path):
            rows = jobs_common.registry_entries("scrapy_static", enabled_only=True)
        assert len(rows) == 1
        assert rows[0].get("pages") == ["https://supercell.com/en/careers/"]
        assert rows[0].get("id") == "static:supercell"


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
