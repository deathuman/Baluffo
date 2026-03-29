# ruff: noqa: F403,F405
from collections import Counter

from src.jobs.adapters.plugins.static import ats_wrappers, rendered_cards
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.plugins.types import AdapterPluginContext
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


def test_js_required_audit_rows_stay_explicit_and_keep_needs_review_bounded() -> None:
    audit_rows = [
        {
            "name": "static_source::combat_waffle_studios",
            "status": "ok",
            "adapter": "static",
            "failureBucket": "",
            "classification": "ok_no_jobs",
            "zeroKeptClassification": "n/a",
            "fetchedCount": 0,
            "keptCount": 0,
            "durationMs": 1_200,
            "error": "static:Combat Waffle Studios (Sheet): no jobs extracted from source pages",
        },
        {
            "name": "static_source::nexus_studios",
            "status": "ok",
            "adapter": "static",
            "failureBucket": "",
            "classification": "ok_no_jobs",
            "zeroKeptClassification": "",
            "fetchedCount": 0,
            "keptCount": 0,
            "durationMs": 1_100,
            "error": "static:Nexus Studios (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::high_5_games",
            "status": "error",
            "adapter": "static",
            "failureBucket": "",
            "classification": "needs_review",
            "zeroKeptClassification": "",
            "fetchedCount": 0,
            "keptCount": 0,
            "durationMs": 1_000,
            "error": "static:High 5 Games (Sheet): no jobs extracted from source pages",
        },
        {
            "name": "static_source::sega",
            "status": "error",
            "adapter": "static",
            "failureBucket": "n/a",
            "classification": "needs_review",
            "zeroKeptClassification": "n/a",
            "fetchedCount": 0,
            "keptCount": 0,
            "durationMs": 900,
            "error": "static:SEGA (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::netease_games",
            "status": "ok",
            "adapter": "static",
            "failureBucket": "",
            "classification": "ok_no_jobs",
            "zeroKeptClassification": "",
            "fetchedCount": 0,
            "keptCount": 0,
            "durationMs": 800,
            "error": "static:Netease Games (Gameprog): no jobs extracted from source pages",
        },
        {
            "name": "static_source::weak_generic_zero_kept",
            "status": "ok",
            "adapter": "static",
            "failureBucket": "",
            "classification": "ok_no_jobs",
            "zeroKeptClassification": "n/a",
            "fetchedCount": 0,
            "keptCount": 0,
            "durationMs": 10,
            "error": "",
        },
    ]

    normalized_rows = [jf.normalize_source_report_row(row) for row in audit_rows]
    breakdown = jobs_reporting.build_unknown_static_breakdown(normalized_rows)

    for row in normalized_rows[:-1]:
        assert str(row.get("failureBucket") or "") == "js_required"
        assert str(row.get("zeroKeptClassification") or "") == "broken_extraction"

    weak_row = normalized_rows[-1]
    assert str(weak_row.get("failureBucket") or "") == "needs_review"
    assert str(weak_row.get("zeroKeptClassification") or "") == "needs_review"

    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 5
    assert breakdown["byShape"]["other_static"]["count"] == 1
    assert breakdown["byShape"]["transport_network"]["count"] == 0
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 0
    assert breakdown["topByWallTime"][0]["name"] == "static_source::combat_waffle_studios"
    assert breakdown["topByWallTime"][-1]["name"] == "static_source::weak_generic_zero_kept"


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


def test_run_static_studio_pages_source_emits_heartbeat_callbacks() -> None:
    listing = _fixture("littlechicken_jobs_page.html")
    detail = _fixture("littlechicken_job_detail.html")
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    heartbeat_calls: list[str] = []
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Fallback Heartbeat Studio",
            "studio": "Fallback Heartbeat Studio",
            "adapter": "static",
            "company": "Fallback Heartbeat Studio",
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
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            heartbeat_callback=lambda: heartbeat_calls.append("beat"),
        )
        assert len(rows) == 2
        assert len(heartbeat_calls) >= 4
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


def test_run_static_studio_pages_source_flattens_slow_tail_with_history() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    source = {
        "name": "Tail Test Studio",
        "studio": "Tail Test Studio",
        "adapter": "static",
        "company": "Tail Test Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    jf.STUDIO_SOURCE_REGISTRY = [source]
    listing_html = (
        "<html><body>"
        + "".join(
            f'<article><h2>Role {i}</h2><a href="/job/{i}">More Details</a></article>'
            for i in range(20)
        )
        + "</body></html>"
    )
    detail_html = "<html><body><h1>Role</h1></body></html>"
    detail_calls = {"count": 0}
    tail_state = {
        "Tail Test Studio": {
            "lastDetailPagesVisited": 42,
            "lastKeptCount": 1,
            "lastDurationMs": 145137,
            "lastDetailYieldPct": 2,
            "lastStageTimingsMs": {"detailFetch": 217029},
        }
    }

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://example.net/careers":
            return listing_html
        if url.startswith("https://example.net/job/"):
            detail_calls["count"] += 1
            time.sleep(0.01)
            return detail_html
        raise RuntimeError(f"Unexpected URL: {url}")

    def run_once(source_state_rows: dict[str, dict[str, object]]) -> tuple[float, int, int]:
        jf.SOURCE_DIAGNOSTICS.clear()
        start = time.perf_counter()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[source],
            diagnostics_name="Tail Test Studio",
            source_state_rows=source_state_rows,
        )
        elapsed = time.perf_counter() - start
        diag = (jf.SOURCE_DIAGNOSTICS.get("Tail Test Studio") or {}).get("details") or []
        stats = (diag[0] if diag else {}).get("stats") or {}
        return elapsed, int(stats.get("detail_pages_visited") or 0), len(rows)

    try:
        control_elapsed, control_detail_pages, control_rows = run_once({})
        tail_elapsed, tail_detail_pages, tail_rows = run_once(tail_state)

        assert control_rows >= 1
        assert tail_rows >= 1
        assert control_detail_pages >= 10
        assert tail_detail_pages <= 6
        assert tail_elapsed < control_elapsed
        assert detail_calls["count"] >= control_detail_pages
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


def test_unknown_static_breakdown_groups_by_shape_and_orders_views() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/a",
            "adapter": "static",
            "studio": "Example A",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 1000,
            "error": "connection timeout while fetching https://example.com/a",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/b",
            "adapter": "static",
            "studio": "Example B",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 300,
            "error": "HTTP 429 Too Many Requests for https://example.com/b",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/c",
            "adapter": "static",
            "studio": "Example C",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 200,
            "error": "static:Example C (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/d",
            "adapter": "static",
            "studio": "Example D",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 75,
            "error": "static:Example D (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/e",
            "adapter": "static",
            "studio": "Example E",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 50,
            "error": "unexpected parser shape with no obvious classification",
        },
        {
            "name": "personio_sources",
            "adapter": "personio",
            "studio": "Personio Example",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 999,
            "error": "personio_sources: HTTP 429 for https://example.personio.de/xml",
        },
        {
            "name": "ok_source",
            "adapter": "static",
            "studio": "Ok Studio",
            "status": "ok",
            "failureBucket": "unknown",
            "durationMs": 12,
            "fetchedCount": 4,
            "keptCount": 0,
            "error": "time_budget_exceeded while fetching https://example.com/f",
            "zeroKeptClassification": "needs_review",
        },
    ]

    breakdown = jobs_reporting.build_unknown_static_breakdown(source_reports)

    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 2
    assert breakdown["byShape"]["transport_network"]["count"] == 2
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 1
    assert breakdown["byShape"]["other_static"]["count"] == 1
    assert (
        breakdown["topByWallTime"][0]["name"]
        == "static_source::static:listing_url:https://example.com/a"
    )
    assert breakdown["topByFrequency"][0]["shape"] == "transport_network"
    assert breakdown["topByFrequency"][0]["count"] == 2
    assert source_reports[0]["failureBucket"] == "unknown"
    assert source_reports[-1]["status"] == "ok"
    assert source_reports[-1]["keptCount"] == 0


def test_build_pipeline_summary_embeds_unknown_static_breakdown_without_affecting_totals() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/a",
            "adapter": "static",
            "studio": "Example A",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 1000,
            "error": "connection timeout while fetching https://example.com/a",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/b",
            "adapter": "static",
            "studio": "Example B",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 300,
            "error": "HTTP 429 Too Many Requests for https://example.com/b",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/c",
            "adapter": "static",
            "studio": "Example C",
            "status": "error",
            "failureBucket": "unknown",
            "durationMs": 200,
            "error": "static:Example C (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "ok_source",
            "adapter": "static",
            "studio": "Ok Studio",
            "status": "ok",
            "failureBucket": "unknown",
            "durationMs": 12,
            "fetchedCount": 4,
            "keptCount": 0,
            "error": "time_budget_exceeded while fetching https://example.com/f",
            "zeroKeptClassification": "needs_review",
        },
    ]

    summary = jobs_reporting.build_pipeline_summary(
        {"inputCount": 0, "mergedCount": 0},
        [],
        source_reports,
        0,
        False,
        1,
        0,
        0,
        json_bytes=123,
        csv_bytes=456,
        light_json_bytes=78,
        lifecycle_counts_map={"active": 0, "likelyRemoved": 0, "archived": 0, "totalTracked": 0},
    )

    breakdown = summary.get("unknownStaticBreakdown") or {}
    assert summary["rawFetched"] == 4
    assert summary["successfulSources"] == 1
    assert summary["failedSources"] == 3
    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["byShape"]["transport_network"]["count"] == 2
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 1
    assert breakdown["byShape"]["other_static"]["count"] == 0
    assert len(breakdown["topByWallTime"]) == 4
    assert len(breakdown["topByFrequency"]) == 4


def test_needs_review_breakdown_groups_by_shape_and_orders_views() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/a",
            "adapter": "static",
            "studio": "Example A",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 1000,
            "error": "static:Example A (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/b",
            "adapter": "static",
            "studio": "Example B",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 300,
            "error": "time_budget_exceeded while fetching https://example.com/b",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/c",
            "adapter": "static",
            "studio": "Example C",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 200,
            "error": "HTTP 429 Too Many Requests for https://example.com/c",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/d",
            "adapter": "static",
            "studio": "Example D",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 150,
            "error": "site changed redirect after page move",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/e",
            "adapter": "static",
            "studio": "Example E",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "n/a",
            "keptCount": 0,
            "durationMs": 75,
            "error": "unexpected parser shape with no obvious classification",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/f",
            "adapter": "static",
            "studio": "Example F",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 50,
            "error": "unhelpful zero-kept outcome with no clear clues",
        },
    ]

    breakdown = jobs_reporting.build_needs_review_breakdown(source_reports)

    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["byShape"]["transport_network"]["count"] == 1
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 1
    assert breakdown["byShape"]["site_changed"]["count"] == 1
    assert breakdown["byShape"]["blank_residue"]["count"] == 1
    assert breakdown["byShape"]["ambiguous_review"]["count"] == 1
    assert breakdown["topByWallTime"][0]["name"] == "static_source::static:listing_url:https://example.com/a"
    assert breakdown["topByFrequency"][0]["shape"] == "no_jobs_extracted"


def test_build_pipeline_summary_embeds_needs_review_breakdown_without_affecting_totals() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/a",
            "adapter": "static",
            "studio": "Example A",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 1000,
            "error": "static:Example A (Manual Website): no jobs extracted from source pages",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/b",
            "adapter": "static",
            "studio": "Example B",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 300,
            "error": "time_budget_exceeded while fetching https://example.com/b",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/c",
            "adapter": "static",
            "studio": "Example C",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 200,
            "error": "HTTP 429 Too Many Requests for https://example.com/c",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/d",
            "adapter": "static",
            "studio": "Example D",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 150,
            "error": "site changed redirect after page move",
        },
    ]

    summary = jobs_reporting.build_pipeline_summary(
        {"inputCount": 0, "mergedCount": 0},
        [],
        source_reports,
        0,
        False,
        1,
        0,
        0,
        json_bytes=123,
        csv_bytes=456,
        light_json_bytes=78,
        lifecycle_counts_map={"active": 0, "likelyRemoved": 0, "archived": 0, "totalTracked": 0},
    )

    breakdown = summary.get("needsReviewBreakdown") or {}
    assert summary["rawFetched"] == 0
    assert summary["successfulSources"] == 0
    assert summary["failedSources"] == 4
    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["byShape"]["transport_network"]["count"] == 1
    assert breakdown["byShape"]["anti_bot_challenge"]["count"] == 1
    assert breakdown["byShape"]["site_changed"]["count"] == 1
    assert breakdown["byShape"]["blank_residue"]["count"] == 0
    assert breakdown["byShape"]["ambiguous_review"]["count"] == 0


def test_blank_residue_breakdown_ignores_success_rows_and_tracks_true_zero_kept_residue() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/success",
            "adapter": "static",
            "studio": "Example Success",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "",
            "keptCount": 5,
            "durationMs": 900,
            "error": "",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/blank",
            "adapter": "static",
            "studio": "Example Blank",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "",
            "keptCount": 0,
            "durationMs": 800,
            "error": "time_budget_exceeded while fetching https://example.com/blank",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/explicit",
            "adapter": "static",
            "studio": "Example Explicit",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "durationMs": 700,
            "error": "manual site with ambiguous zero-kept outcome",
        },
    ]

    breakdown = jobs_reporting.build_blank_residue_breakdown(source_reports)

    assert breakdown["byShape"]["blank_residue"]["count"] == 1
    assert breakdown["topByWallTime"][0]["name"] == "static_source::static:listing_url:https://example.com/blank"
    assert all(
        row["name"] != "static_source::static:listing_url:https://example.com/success"
        for row in breakdown["topByWallTime"]
    )


def test_build_pipeline_summary_embeds_blank_residue_breakdown_without_affecting_totals() -> None:
    source_reports = [
        {
            "name": "static_source::static:listing_url:https://example.com/success",
            "adapter": "static",
            "studio": "Example Success",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "",
            "keptCount": 5,
            "fetchedCount": 6,
            "durationMs": 900,
            "error": "",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/blank",
            "adapter": "static",
            "studio": "Example Blank",
            "status": "ok",
            "failureBucket": "",
            "zeroKeptClassification": "",
            "keptCount": 0,
            "fetchedCount": 0,
            "durationMs": 800,
            "error": "time_budget_exceeded while fetching https://example.com/blank",
        },
        {
            "name": "static_source::static:listing_url:https://example.com/explicit",
            "adapter": "static",
            "studio": "Example Explicit",
            "status": "error",
            "failureBucket": "needs_review",
            "zeroKeptClassification": "needs_review",
            "keptCount": 0,
            "fetchedCount": 0,
            "durationMs": 700,
            "error": "manual site with ambiguous zero-kept outcome",
        },
    ]

    summary = jobs_reporting.build_pipeline_summary(
        {"inputCount": 0, "mergedCount": 0},
        [],
        source_reports,
        0,
        False,
        1,
        0,
        0,
        json_bytes=123,
        csv_bytes=456,
        light_json_bytes=78,
        lifecycle_counts_map={"active": 0, "likelyRemoved": 0, "archived": 0, "totalTracked": 0},
    )

    breakdown = summary.get("blankResidueBreakdown") or {}
    assert summary["rawFetched"] == 6
    assert summary["successfulSources"] == 2
    assert summary["failedSources"] == 1
    assert breakdown["byShape"]["blank_residue"]["count"] == 1
    assert breakdown["topByWallTime"][0]["name"] == "static_source::static:listing_url:https://example.com/blank"
    assert all(
        row["name"] != "static_source::static:listing_url:https://example.com/success"
        for row in breakdown["topByWallTime"]
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


def test_static_manual_no_jobs_surface_as_js_required() -> None:
    detail = {
        "adapter": "static",
        "studio": "Frontier Developments",
        "name": "Frontier Developments Careers",
        "status": "error",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "static:Frontier Developments (Sheet): no jobs extracted from source pages",
        "classification": "needs_review",
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
    normalized = jf.normalize_source_report_row(updated)
    breakdown = jobs_reporting.build_unknown_static_breakdown([normalized])

    assert str(updated.get("classification") or "") == "js_required"
    assert str(updated.get("failureBucket") or "") == "js_required"
    assert str(updated.get("zeroKeptClassification") or "") == "broken_extraction"
    assert str(normalized.get("classification") or "") == "js_required"
    assert str(normalized.get("failureBucket") or "") == "js_required"
    assert str(normalized.get("zeroKeptClassification") or "") == "broken_extraction"
    assert breakdown["byShape"]["no_jobs_extracted"]["count"] == 1
    assert breakdown["topByWallTime"][0]["name"] == "Frontier Developments Careers"


def test_static_zero_extract_linkedin_429_promotes_to_anti_bot_or_challenge() -> None:
    detail = {
        "adapter": "static",
        "studio": "Nexus Studios",
        "name": "Nexus Studios Careers",
        "status": "error",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "HTTP 429 Too Many Requests for https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search",
        "classification": "rate_limited",
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

    assert str(updated.get("classification") or "") == "anti_bot_or_challenge"
    assert str(updated.get("failureBucket") or "") == "anti_bot_or_challenge"
    assert str(updated.get("zeroKeptClassification") or "") == "broken_extraction"
    assert bool(updated.get("browserFallbackRecommended"))


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


def test_static_repeat_offender_no_jobs_surface_as_js_required() -> None:
    cases = [
        (
            "Electronic Arts",
            "static:Electronic Arts (Manual Website): no jobs extracted from source pages",
            "static:electronic arts (manual website): no jobs extracted from source pages",
        ),
        (
            "SEGA",
            "static:SEGA (Manual Website): no jobs extracted from source pages",
            "static:sega (manual website): no jobs extracted from source pages",
        ),
        (
            "Capcom",
            "static:Capcom (Sheet): no jobs extracted from source pages",
            "static:capcom (sheet): no jobs extracted from source pages",
        ),
        (
            "Stormind",
            "static:Stormind Games (Gameprog): no jobs extracted from source pages",
            "static:stormind games (gameprog): no jobs extracted from source pages",
        ),
        (
            "Unknown Worlds",
            "static:Unknown Worlds Entertainment (Sheet): no jobs extracted from source pages",
            "static:unknown worlds entertainment (sheet): no jobs extracted from source pages",
        ),
    ]

    for source_name, error, expected_error in cases:
        detail = {
            "adapter": "static",
            "studio": source_name,
            "name": f"{source_name} Careers",
            "status": "error",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": error,
            "classification": "needs_review",
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
        normalized = jf.normalize_source_report_row(updated)

        assert str(updated.get("classification") or "") == "js_required"
        assert str(updated.get("failureBucket") or "") == "js_required"
        assert str(normalized.get("classification") or "") == "js_required"
        assert str(normalized.get("failureBucket") or "") == "js_required"
        assert str(normalized.get("error") or "").lower() == expected_error


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
    assert link_rejections["non_job_url"] == 1


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


def test_run_static_studio_pages_source_rendered_cards_extracts_indie_job_cards() -> None:
    html = """
        <html>
          <body>
            <article class="job-card">
              <div>Black Beach Studio is hiring a Video Producer to work from Anywhere</div>
              <a href="/work-with/black-beach-studio/video-producer">Learn More</a>
            </article>
            <article class="job-card">
              <div>Romero Games is hiring a Senior Gameplay Programmer in Galway</div>
              <a href="/work-with/romero-games/senior-gameplay-programmer">View Job</a>
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
                "name": "Work With Indies (Manual Website)",
                "studio": "Work With Indies",
                "company": "Work With Indies",
                "pages": ["https://www.workwithindies.com/work-with/black-beach-studio"],
                "id": "static:listing_url:https://www.workwithindies.com/work-with/black-beach-studio",
            }
        ],
    )
    assert len(rows) == 2
    assert {row["title"] for row in rows} == {"Video Producer", "Senior Gameplay Programmer"}
    assert {row["jobLink"] for row in rows} == {
        "https://www.workwithindies.com/work-with/black-beach-studio/video-producer",
        "https://www.workwithindies.com/work-with/romero-games/senior-gameplay-programmer",
    }
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert int(detail.get("keptCount") or 0) == 2
    assert str(detail.get("failureBucket") or "") != "js_required"


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
    assert {row["country"] for row in rows} == {"Unknown"}


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


def test_run_static_studio_pages_source_ats_wrapper_extracts_greenhouse_cards() -> None:
    html = """
        <html>
          <body>
            <article class="job-card">
              <h4>Senior Technical Gameplay Animator</h4>
              <div>California, US</div>
              <a href="https://www.naughtydog.com/greenhouse/job/5645048004?gh_jid=5645048004">APPLY NOW</a>
            </article>
            <article class="job-card">
              <h4>Senior Sound Designer Contingent</h4>
              <div>California, US</div>
              <a href="https://www.naughtydog.com/greenhouse/job/5749070004?gh_jid=5749070004">APPLY NOW</a>
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
                "name": "Naughty Dog Careers",
                "studio": "Naughty Dog",
                "company": "Naughty Dog",
                "pages": ["https://www.naughtydog.com/openings?department=naughty-dog"],
                "id": "static:listing_url:https://www.naughtydog.com/openings?department=naughty-dog",
            }
        ],
    )
    assert len(rows) == 2
    assert {row["title"] for row in rows} == {
        "Senior Technical Gameplay Animator",
        "Senior Sound Designer Contingent",
    }
    assert {row["jobLink"] for row in rows} == {
        "https://www.naughtydog.com/greenhouse/job/5645048004?gh_jid=5645048004",
        "https://www.naughtydog.com/greenhouse/job/5749070004?gh_jid=5749070004",
    }
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert int(detail.get("keptCount") or 0) == 2
    assert str(detail.get("failureBucket") or "") != "js_required"


def test_rendered_card_family_and_ats_wrapper_do_not_overlap_on_zenimax() -> None:
    ctx = AdapterPluginContext(
        family="static",
        adapter_key="static",
        source_identity="jobs.zenimax.com",
    )
    assert ats_wrappers.can_handle(ctx)
    assert not rendered_cards.can_handle(ctx)


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
