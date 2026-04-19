# ruff: noqa: F403, F405
import json
import subprocess
import threading
import time
from unittest import mock

import pytest

from src.exceptions import AdapterValidationError
from src.jobs.browser_fallback import BrowserFallbackCircuitBreaker

from ._helpers import *  # noqa: F401,F403


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
        with (
            mock.patch("subprocess.run", return_value=fake_result),
            mock.patch.object(
                static_scrapy, "registry_entries", return_value=list(jf.STUDIO_SOURCE_REGISTRY)
            ),
            mock.patch.object(static_scrapy, "set_source_diagnostics") as diag,
        ):
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


def test_run_static_frontier_source_skips_non_location_dd_values() -> None:
    html = """
        <html>
          <body>
            <li>
              <div class="c-careers-job-listing__department-list-detail">
                <h3>Senior Gameplay Programmer</h3>
                <dd>AI Solutions PM</dd>
                <dd>Administrative & Support Services</dd>
                <dd>Full Time</dd>
                <dd>Tokyo</dd>
                <a href="/careers/jobs/senior-gameplay-programmer">Details</a>
              </div>
            </li>
          </body>
        </html>
        """
    rows = frontier.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://www.frontier.co.uk/careers"],
        source_row={"name": "Frontier Developments", "id": "frontier"},
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Senior Gameplay Programmer"
    assert row["city"] == "Tokyo"
    assert row["country"] == "Japan"


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


def test_run_static_studio_pages_source_dedupes_candidate_links_before_fetch() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use example.net so the generic listing-only fallback runs (no static plugin)
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
        assert fetch_counts["detail"] == 0
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


def test_run_static_studio_pages_source_emits_incremental_detail_batch_progress() -> None:
    listing = _fixture("littlechicken_jobs_page.html")
    detail = _fixture("littlechicken_job_detail.html")
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    progress_events: list[dict[str, object]] = []
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Fallback Progress Studio",
            "studio": "Fallback Progress Studio",
            "adapter": "static",
            "company": "Fallback Progress Studio",
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
            progress_callback=lambda **kwargs: progress_events.append(dict(kwargs)),
        )
        assert len(rows) == 2
        assert any(
            str(event.get("phase_key") or "") == "static_detail_traversal"
            and int((event.get("counts") or {}).get("detailPagesFetched") or 0) == 1
            and str(event.get("target_label") or "") == "Detail fetch 1/2"
            for event in progress_events
        )
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
        assert control_detail_pages == 0
        assert tail_detail_pages == 0
        assert detail_calls["count"] == 0
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_force_refresh_all_reprocesses_detail_links() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Force Refresh Studio",
            "studio": "Force Refresh Studio",
            "adapter": "static",
            "company": "Force Refresh Studio",
            "pages": ["https://target.example/careers"],
            "enabledByDefault": True,
        },
        {
            "name": "Control Studio",
            "studio": "Control Studio",
            "adapter": "static",
            "company": "Control Studio",
            "pages": ["https://control.example/careers"],
            "enabledByDefault": True,
        },
    ]
    target_listing = (
        '<html><body><a href="/job/software-engineer">Software Engineer</a></body></html>'
    )
    control_listing = (
        "<html><head>"
        '<script type="application/ld+json">'
        '{"@context":"https://schema.org","@type":"JobPosting","title":"Control Role",'
        '"hiringOrganization":{"name":"Control Studio"},'
        '"jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},'
        '"url":"https://control.example/job/control-role"}'
        "</script>"
        "</head><body></body></html>"
    )
    target_fingerprint = hashlib.sha1(target_listing.encode("utf-8")).hexdigest()
    source_state_rows = {"Force Refresh Studio": {"lastListingFingerprint": target_fingerprint}}
    detail_calls = {"count": 0}

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://target.example/careers":
            return target_listing
        if url == "https://control.example/careers":
            return control_listing
        if url == "https://target.example/job/software-engineer":
            return "<html><body><h1>Software Engineer</h1></body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_process_detail_html(**kwargs: object) -> dict[str, object]:
        detail_calls["count"] += 1
        return {
            "rows": [
                {
                    "sourceJobId": "static:Force Refresh Studio:target",
                    "title": "Software Engineer",
                    "company": "Force Refresh Studio",
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "",
                    "jobLink": "https://target.example/job/software-engineer",
                    "sector": "Game",
                    "postedAt": "",
                    "adapter": "static",
                    "studio": "Force Refresh Studio",
                }
            ],
            "parseEmpty": False,
            "fetchMs": 0,
            "parseMs": 0,
            "cacheHit": False,
            "rejectedClassification": "",
            "rejectedExample": "",
        }

    try:
        with mock.patch("src.jobs.adapters.static.extract_rendered_card_jobs", return_value=[]):
            with mock.patch(
                "src.jobs.adapters.static.process_detail_html",
                side_effect=fake_process_detail_html,
            ):
                rows_no_refresh = jf.run_static_studio_pages_source(
                    fetch_text=fake_fetch,
                    timeout_s=5,
                    retries=0,
                    backoff_s=0,
                    sources=list(jf.STUDIO_SOURCE_REGISTRY),
                    source_state_rows=source_state_rows,
                    force_refresh_all=False,
                )
                rows_force_refresh = jf.run_static_studio_pages_source(
                    fetch_text=fake_fetch,
                    timeout_s=5,
                    retries=0,
                    backoff_s=0,
                    sources=list(jf.STUDIO_SOURCE_REGISTRY),
                    source_state_rows=source_state_rows,
                    force_refresh_all=True,
                )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert detail_calls["count"] == 1
    assert len(rows_no_refresh) == 1
    assert len(rows_force_refresh) == 2
    assert any(
        str(row.get("jobLink") or "") == "https://target.example/job/software-engineer"
        for row in rows_force_refresh
    )


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


def test_run_static_studio_pages_source_parallelizes_detail_fetches() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    # Use example.net so the generic listing-only fallback runs (no static plugin)
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


def test_run_static_studio_pages_source_enforces_hard_budget_and_preserves_partial_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Budget Studio",
            "studio": "Budget Studio",
            "adapter": "static",
            "company": "Budget Studio",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
        }
    ]
    listing_html = (
        "<html><body>"
        '<a href="/job/a">Role A</a>'
        '<a href="/job/b">Role B</a>'
        '<a href="/job/c">Role C</a>'
        "</body></html>"
    )
    monkeypatch.setenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S", "5")
    fetched_detail_urls: list[str] = []

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.net/careers":
                return listing_html
            if url.startswith("https://example.net/job/"):
                fetched_detail_urls.append(url)
                time.sleep(2.2)
                title = url.rsplit("/", 1)[-1].upper()
                return f"<html><body><h1>{title} Engineer</h1></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        started = time.perf_counter()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            static_detail_concurrency=1,
        )
        elapsed = time.perf_counter() - started
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert len(rows) == 2
    assert fetched_detail_urls == [
        "https://example.net/job/a",
        "https://example.net/job/b",
    ]
    assert elapsed < 5.6


def test_run_static_studio_pages_source_parallelizes_listing_fetches() -> None:
    source_row = {
        "name": "Parallel Listing Studio",
        "studio": "Parallel Listing Studio",
        "adapter": "static",
        "company": "Parallel Listing Studio",
        "pages": [
            "https://example.net/jobs/page-a",
            "https://example.net/jobs/page-b",
            "https://example.net/jobs/page-c",
        ],
        "enabledByDefault": True,
    }
    page_html = {
        "https://example.net/jobs/page-a": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role A",
            "hiringOrganization":{"name":"Parallel Listing Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-a"}
            </script></head><body></body></html>
        """,
        "https://example.net/jobs/page-b": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role B",
            "hiringOrganization":{"name":"Parallel Listing Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-b"}
            </script></head><body></body></html>
        """,
        "https://example.net/jobs/page-c": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role C",
            "hiringOrganization":{"name":"Parallel Listing Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-c"}
            </script></head><body></body></html>
        """,
    }
    active = 0
    peak = 0
    active_lock = threading.Lock()

    def fake_fetch(url: str, _: int) -> str:
        nonlocal active, peak
        with active_lock:
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(0.05)
            return page_html[url]
        finally:
            with active_lock:
                active -= 1

    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
    )

    assert len(rows) == 3
    assert peak >= 2


def test_run_static_studio_pages_source_uses_async_listing_fetch_when_provided() -> None:
    source_row = {
        "name": "Async Listing Studio",
        "studio": "Async Listing Studio",
        "adapter": "static",
        "company": "Async Listing Studio",
        "pages": ["https://example.net/jobs"],
        "enabledByDefault": True,
    }
    async_calls: list[tuple[str, int]] = []

    async def fake_listing_async_fetch(
        _client: object, _job: dict[str, object], url: str, timeout_s: int
    ) -> str:
        async_calls.append((url, timeout_s))
        return """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Async Role",
            "hiringOrganization":{"name":"Async Listing Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/async-role"}
            </script></head><body></body></html>
        """

    def failing_sync_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(f"unexpected sync fetch: {url}")

    rows = jf.run_static_studio_pages_source(
        fetch_text=failing_sync_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
        listing_async_fetch=fake_listing_async_fetch,
    )

    assert len(rows) == 1
    assert async_calls == [("https://example.net/jobs", 5)]


def test_run_static_studio_pages_source_rejects_obvious_off_target_detail_links() -> None:
    source_row = {
        "name": "Off Target Studio",
        "studio": "Off Target Studio",
        "adapter": "static",
        "company": "Off Target Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    listing_html = """
        <html>
          <head>
            <script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Platform Engineer",
            "hiringOrganization":{"name":"Off Target Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/platform-engineer"}
            </script>
          </head>
          <body>
            <a href="https://www.youtube.com/watch?v=abc">Video</a>
            <a href="https://example.net/legal/privacy-policy">Privacy</a>
            <a href="https://forms.gle/example">Form</a>
          </body>
        </html>
    """
    fetched_urls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        fetched_urls.append(url)
        if url == "https://example.net/careers":
            return listing_html
        raise RuntimeError(f"Unexpected URL: {url}")

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
    )

    assert len(rows) == 1
    assert fetched_urls == ["https://example.net/careers"]
    details = jf.SOURCE_DIAGNOSTICS.get("static_studio_pages", {}).get("details") or []
    assert details
    assert int((details[0].get("loss") or {}).get("staticNonJobUrlRejected") or 0) >= 3


def test_run_static_studio_pages_source_zero_yield_listing_falls_through_to_needs_review() -> None:
    source_row = {
        "name": "Provider Zero Yield Studio",
        "studio": "Provider Zero Yield Studio",
        "adapter": "static",
        "company": "Provider Zero Yield Studio",
        "pages": ["https://jobs.workdayjobs.com/provider-zero-yield"],
        "enabledByDefault": True,
    }
    fetch_calls: list[str] = []
    jf.SOURCE_DIAGNOSTICS.clear()

    def fake_fetch(url: str, _timeout: int) -> str:
        fetch_calls.append(url)
        return "<html><body><h1>Join our team</h1><p>No openings right now.</p></body></html>"

    with pytest.raises(AdapterValidationError):
        jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=2,
            backoff_s=0,
            sources=[source_row],
        )

    assert fetch_calls == ["https://jobs.workdayjobs.com/provider-zero-yield"]
    details = jf.SOURCE_DIAGNOSTICS.get("static_studio_pages", {}).get("details") or []
    assert details
    assert str((details[0].get("stats") or {}).get("listing_terminal_reason") or "") == ""
    assert details[0].get("classification") == "site_changed"


def test_run_static_studio_pages_source_keeps_post_listing_detail_tail() -> None:
    source_row = {
        "name": "Post Listing Tail Studio",
        "studio": "Post Listing Tail Studio",
        "adapter": "static",
        "company": "Post Listing Tail Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    listing_html = """
        <html>
          <head><script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"Listed Role",
          "hiringOrganization":{"name":"Post Listing Tail Studio"},
          "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
          "url":"https://example.net/jobs/listed-role"}
          </script></head>
          <body>
            <a href="/job/a">Role A</a>
            <a href="/job/b">Role B</a>
            <a href="/job/c">Role C</a>
          </body>
        </html>
    """
    fetched_detail_urls: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        if url == "https://example.net/careers":
            return listing_html
        if url.startswith("https://example.net/job/"):
            fetched_detail_urls.append(url)
            time.sleep(1.2)
            title = url.rsplit("/", 1)[-1].upper()
            return f"<html><body><h1>{title} Engineer</h1></body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
        static_detail_concurrency=1,
    )

    assert len(rows) >= 1
    assert fetched_detail_urls == [
        "https://example.net/job/a",
        "https://example.net/job/b",
        "https://example.net/job/c",
    ]


def test_run_static_studio_pages_source_records_listing_browser_fallback_terminal_reason() -> None:
    source_row = {
        "name": "Fallback Empty Listing Studio",
        "studio": "Fallback Empty Listing Studio",
        "adapter": "static",
        "company": "Fallback Empty Listing Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    fetch_calls: list[str] = []
    playwright_calls: list[tuple[str, int]] = []
    jf.SOURCE_DIAGNOSTICS.clear()

    def fake_fetch(url: str, _timeout: int) -> str:
        fetch_calls.append(url)
        raise TimeoutError("timed out")

    def fake_try_playwright(url: str, timeout_s: int) -> tuple[str, str]:
        playwright_calls.append((url, timeout_s))
        return "", ""

    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=2,
        backoff_s=0,
        sources=[source_row],
        try_playwright=fake_try_playwright,
    )

    assert rows == []
    assert fetch_calls == [
        "https://example.net/careers",
        "https://example.net/careers",
        "https://example.net/careers",
    ]
    assert len(playwright_calls) == 1
    details = jf.SOURCE_DIAGNOSTICS.get("static_studio_pages", {}).get("details") or []
    assert details
    stats = details[0].get("stats") or {}
    assert int(stats.get("listing_browser_fallbacks") or 0) == 1
    assert stats.get("listing_terminal_reason") == "browser_fallback_empty"


def test_run_static_studio_pages_source_emits_incremental_listing_batch_progress() -> None:
    source_row = {
        "name": "Listing Progress Studio",
        "studio": "Listing Progress Studio",
        "adapter": "static",
        "company": "Listing Progress Studio",
        "pages": [
            "https://example.net/jobs/page-a",
            "https://example.net/jobs/page-b",
            "https://example.net/jobs/page-c",
        ],
        "enabledByDefault": True,
    }
    page_html = {
        "https://example.net/jobs/page-a": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role A",
            "hiringOrganization":{"name":"Listing Progress Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-a"}
            </script></head><body></body></html>
        """,
        "https://example.net/jobs/page-b": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role B",
            "hiringOrganization":{"name":"Listing Progress Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-b"}
            </script></head><body></body></html>
        """,
        "https://example.net/jobs/page-c": """
            <html><head><script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Role C",
            "hiringOrganization":{"name":"Listing Progress Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/role-c"}
            </script></head><body></body></html>
        """,
    }
    progress_events: list[dict[str, object]] = []

    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda url, _timeout: page_html[url],
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
        progress_callback=lambda **kwargs: progress_events.append(dict(kwargs)),
    )

    assert len(rows) == 3
    assert any(
        str(event.get("phase_key") or "") == "static_listing_fetch"
        and int((event.get("counts") or {}).get("listingPagesFetched") or 0) == 1
        and str(event.get("target_label") or "") == "Listing fetch 1/3"
        for event in progress_events
    )


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


def test_run_static_studio_pages_source_text_detail_page_overrides_listing_category_city() -> None:
    listing_html = """
        <html>
          <body>
            <section>
              <h2>Art</h2>
              <a href="/en/jobs?job_id=49">
                <div class="d-table-cell vacancy ps-1 pe-3 pe-md-0">3D Environment Artist</div>
                <div class="d-table-cell vacancy">Art</div>
              </a>
            </section>
          </body>
        </html>
        """
    detail_html = """
        <html>
          <body>
            <h1>3D Environment Artist</h1>
            <p>We work together in person, in Bellevue, WA, USA</p>
          </body>
        </html>
        """
    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda url, _timeout: listing_html if url.endswith("/en/jobs") else detail_html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "Valve Software (Manual Website)",
                "studio": "Valve Software",
                "company": "Valve Software",
                "pages": ["https://www.valvesoftware.com/en/jobs"],
                "id": "static:listing_url:https://www.valvesoftware.com/en/jobs",
            }
        ],
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "3D Environment Artist"
    assert row["city"] == "Bellevue"
    assert row["country"] == "US"
    assert row["locationSummary"] == "Bellevue, US"
    assert row["locations"] == [{"city": "Bellevue", "country": "US"}]
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert int(detail.get("keptCount") or 0) == 1


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


@pytest.mark.parametrize(
    "noise_line",
    ["Apr. 06", "AI Solutions PM", "Assist with outdoor photos"],
)
def test_run_static_studio_pages_source_kojima_blank_city_for_noise_trailing_line(
    noise_line: str,
) -> None:
    html = f"""
        <html>
          <body>
            <a href="/en/careers/123">
              <span>Senior Programmer</span><br />
              <span>Programming</span><br />
              <span>{noise_line}</span>
            </a>
          </body>
        </html>
        """
    rows = kojima.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://www.kojimaproductions.jp/en/careers"],
        source_row={"name": "Kojima Productions", "id": "kojima"},
        parse_jobpostings_from_html=lambda *args, **kwargs: [],
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Senior Programmer"
    assert row["city"] == ""
    assert row["country"] == "Japan"


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


def test_static_source_rejects_regular_pages_as_dead_listing_pages() -> None:
    listing_html = """
        <html>
          <body>
            <a href="/jobs/about">Senior Engineer</a>
          </body>
        </html>
    """
    detail_html = """
        <html>
          <head><title>About</title></head>
          <body><h1>About</h1><p>About us</p></body>
        </html>
    """
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Example Studio (Manual Website)",
            "studio": "Example Studio",
            "adapter": "static",
            "company": "Example Studio",
            "pages": ["https://example.com/careers"],
            "enabledByDefault": True,
        }
    ]

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.com/careers":
                return listing_html
            if url == "https://example.com/jobs/about":
                return detail_html
            raise RuntimeError(f"Unexpected URL: {url}")

        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )
        assert rows == []
        detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[
            0
        ]
        assert str(detail.get("classification") or "") == "dead_listing_page"
        assert int(detail.get("deadListingPageCount") or 0) == 1
        examples = detail.get("deadListingPageExamples")
        assert isinstance(examples, list) and examples
        normalized = jf.normalize_source_report_row(detail)
        assert str(normalized.get("classification") or "") == "dead_listing_page"
        assert int(normalized.get("deadListingPageCount") or 0) == 1
        assert isinstance(normalized.get("deadListingPageExamples"), list)
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_static_studio_pages_source_keeps_scanning_after_repeated_dead_detail_pages() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Dead Detail Studio",
            "studio": "Dead Detail Studio",
            "adapter": "static",
            "company": "Dead Detail Studio",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
        }
    ]
    listing_html = (
        "<html><body>"
        '<a href="/job/1"><span></span></a>'
        '<a href="/job/2"><span></span></a>'
        '<a href="/job/3"><span></span></a>'
        '<a href="/job/4"><span></span></a>'
        "</body></html>"
    )
    detail_fetches: list[str] = []

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.net/careers":
                return listing_html
            if url.startswith("https://example.net/job/"):
                detail_fetches.append(url)
                return "<html><head><title>About</title></head><body><p>About us</p></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            static_detail_concurrency=1,
        )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert rows == []
    assert sorted(detail_fetches) == [
        "https://example.net/job/1",
        "https://example.net/job/2",
        "https://example.net/job/3",
        "https://example.net/job/4",
    ]


def test_run_static_studio_pages_source_empty_detail_batches_do_not_stop_remaining_candidates() -> (
    None
):
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Adaptive Detail Studio",
            "studio": "Adaptive Detail Studio",
            "adapter": "static",
            "company": "Adaptive Detail Studio",
            "pages": ["https://example.net/careers"],
            "enabledByDefault": True,
        }
    ]
    listing_html = (
        "<html><body>"
        '<a href="/job/1"><span></span></a>'
        '<a href="/job/2"><span></span></a>'
        '<a href="/job/3"><span></span></a>'
        '<a href="/job/4"><span></span></a>'
        '<a href="/job/5"><span></span></a>'
        '<a href="/job/6"><span></span></a>'
        "</body></html>"
    )
    detail_fetches: list[str] = []

    try:

        def fake_fetch(url: str, _timeout: int) -> str:
            if url == "https://example.net/careers":
                return listing_html
            if url.startswith("https://example.net/job/"):
                detail_fetches.append(url)
                return "<html><head><title>About</title></head><body><p>About us</p></body></html>"
            raise RuntimeError(f"Unexpected URL: {url}")

        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            static_detail_concurrency=3,
        )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev

    assert rows == []
    assert detail_fetches == [
        "https://example.net/job/1",
        "https://example.net/job/2",
        "https://example.net/job/3",
        "https://example.net/job/4",
        "https://example.net/job/5",
        "https://example.net/job/6",
    ]
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
    assert int(stats.get("detail_batch_count") or 0) == 2
    assert int(stats.get("detail_pages_skipped_by_adaptive_stop") or 0) == 0


def test_run_static_studio_pages_source_listing_rows_do_not_cap_residual_detail_batches() -> None:
    source_row = {
        "name": "Listing Wins Studio",
        "studio": "Listing Wins Studio",
        "adapter": "static",
        "company": "Listing Wins Studio",
        "pages": ["https://example.net/careers"],
        "enabledByDefault": True,
    }
    listing_html = """
        <html>
          <head>
            <script type="application/ld+json">
            {"@context":"https://schema.org","@type":"JobPosting","title":"Platform Engineer",
            "hiringOrganization":{"name":"Listing Wins Studio"},
            "jobLocation":{"address":{"addressLocality":"Remote","addressCountry":"US"}},
            "url":"https://example.net/jobs/platform-engineer"}
            </script>
          </head>
          <body>
            <a href="/job/a">Role A</a>
            <a href="/job/b">Role B</a>
            <a href="/job/c">Role C</a>
            <a href="/job/d">Role D</a>
          </body>
        </html>
    """
    detail_fetches: list[str] = []

    def fake_fetch(url: str, _timeout: int) -> str:
        if url == "https://example.net/careers":
            return listing_html
        if url.startswith("https://example.net/job/"):
            detail_fetches.append(url)
            title = url.rsplit("/", 1)[-1].upper()
            return f"<html><body><h1>{title} Engineer</h1></body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    jf.SOURCE_DIAGNOSTICS.clear()
    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[source_row],
        static_detail_concurrency=6,
    )

    assert len(rows) == 5
    assert sorted(detail_fetches) == [
        "https://example.net/job/a",
        "https://example.net/job/b",
        "https://example.net/job/c",
        "https://example.net/job/d",
    ]
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    stats = detail.get("stats") if isinstance(detail.get("stats"), dict) else {}
    assert int(stats.get("detail_batch_count") or 0) == 1


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
        with mock.patch.object(jobs_common_registry, "SCRAPY_BROWSER_QUEUE_PATH", queue_path):
            rows = jobs_registry.registry_entries("scrapy_static", enabled_only=True)
        assert len(rows) == 1
        assert rows[0].get("pages") == ["https://supercell.com/en/careers/"]
        assert rows[0].get("id") == "static:supercell"
