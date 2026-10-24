# ruff: noqa: F401
from unittest import mock

from ._helpers import hashlib, jf, process_detail_link


def test_static_detail_returns_nested_candidates_for_exact_category_container_page() -> None:
    category_html = """
        <html>
          <body>
            <div>https://example.com/jobs/vfx-artist</div>
            <div>https://example.com/jobs/lead-vfx-artist</div>
          </body>
        </html>
        """

    def fake_fetch(
        url: str, _remaining_budget_s: float | None = None, **kwargs: object
    ) -> tuple[str, bool]:
        assert url == "https://example.com/department/vfx"
        return category_html, False

    result = process_detail_link(
        detail="https://example.com/department/vfx",
        detail_title="VFX",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fake_fetch,
        timeout_s=5,
        detail_retries=0,
        company="Example Studio",
        source_name="Example Studio",
        source={"studio": "Example Studio"},
        ignored_link_titles=set(),
    )

    assert result["rows"] == []
    assert {
        str((item or {}).get("url") or "") for item in (result.get("nestedDetailLinks") or [])
    } == {
        "https://example.com/jobs/vfx-artist",
        "https://example.com/jobs/lead-vfx-artist",
    }


def test_run_static_studio_pages_source_repairs_exact_category_parsed_listing_row_from_detail() -> (
    None
):
    listing_url = "https://example.net/careers"
    detail_url = "https://jobs.example.workable.com/j/qa"
    detail_html = """
        <html><body>
          <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"Senior QA Analyst","url":"https://jobs.example.workable.com/j/qa","hiringOrganization":{"name":"Example Studio"}}
          </script>
        </body></html>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == listing_url:
            return "<html><body></body></html>"
        if url == detail_url:
            return detail_html
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_listing_parse(
        html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str
    ) -> list[dict[str, object]]:
        del html, fallback_company, fallback_source_id_prefix
        if base_url != listing_url:
            return []
        return [_static_row("QA", detail_url)]

    with mock.patch(
        "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
        side_effect=fake_listing_parse,
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Exact Category Parsed Listing Studio", listing_url)],
        )

    assert [row["title"] for row in rows] == ["Senior QA Analyst"]


def test_run_static_studio_pages_source_follows_one_nested_category_hop() -> None:
    listing_url = "https://example.net/careers"
    category_url = "https://example.net/department/vfx"
    child_urls = {
        "https://example.net/jobs/vfx-artist": "VFX Artist",
        "https://example.net/jobs/lead-vfx-artist": "Lead VFX Artist",
    }
    category_html = """
        <html><body>
          <div>https://example.net/jobs/vfx-artist</div>
          <div>https://example.net/jobs/lead-vfx-artist</div>
        </body></html>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == listing_url:
            return "<html><body></body></html>"
        if url == category_url:
            return category_html
        if url in child_urls:
            return (
                '<html><body><script type="application/ld+json">'
                f'{{"@context":"https://schema.org","@type":"JobPosting","title":"{child_urls[url]}","url":"{url}","hiringOrganization":{{"name":"Example Studio"}}}}'
                "</script></body></html>"
            )
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_listing_parse(
        html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str
    ) -> list[dict[str, object]]:
        del html, fallback_company, fallback_source_id_prefix
        if base_url != listing_url:
            return []
        return [_static_row("VFX", category_url)]

    with mock.patch(
        "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
        side_effect=fake_listing_parse,
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Nested Category Studio", listing_url)],
        )

    assert sorted(row["title"] for row in rows) == ["Lead VFX Artist", "VFX Artist"]


def test_run_static_studio_pages_source_rejects_unresolved_exact_category_parent_page() -> None:
    listing_url = "https://example.net/careers"
    category_url = "https://example.net/team/legal"
    info_html = """
        <html>
          <head><title>Legal</title></head>
          <body><h1>Legal</h1><p>Meet the legal team.</p></body>
        </html>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == listing_url:
            return "<html><body></body></html>"
        if url == category_url:
            return info_html
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_listing_parse(
        html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str
    ) -> list[dict[str, object]]:
        del html, fallback_company, fallback_source_id_prefix
        if base_url != listing_url:
            return []
        return [_static_row("Legal", category_url)]

    jf.SOURCE_DIAGNOSTICS.clear()
    with mock.patch(
        "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
        side_effect=fake_listing_parse,
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Exact Category Dead Page Studio", listing_url)],
        )

    assert rows == []
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert str(detail.get("classification") or "") == "dead_listing_page"


def test_run_static_studio_pages_source_exact_category_rows_bypass_listing_only_and_fingerprint_skip() -> (
    None
):
    listing_url = "https://jobs.jobvite.com/example/search?l=Worldwide"
    detail_url = "https://jobs.jobvite.com/example/job/o1"
    listing_html = "<html><body><div>listing</div></body></html>"
    detail_html = """
        <html><body>
          <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"QA Analyst","url":"https://jobs.jobvite.com/example/job/o1","hiringOrganization":{"name":"Listing Only Studio"}}
          </script>
        </body></html>
        """
    source_state_rows = {
        "Listing Only Studio": {
            "lastListingFingerprint": hashlib.sha1(listing_html.encode("utf-8")).hexdigest()
        }
    }

    def fake_fetch(url: str, _: int) -> str:
        if url == listing_url:
            return listing_html
        if url == detail_url:
            return detail_html
        raise RuntimeError(f"Unexpected URL: {url}")

    with mock.patch(
        "src.jobs.adapters.static_listing.extract_rendered_card_jobs",
        return_value=[{"title": "QA", "jobLink": detail_url}],
    ):
        jf.SOURCE_DIAGNOSTICS.clear()
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Listing Only Studio", listing_url)],
            source_state_rows=source_state_rows,
            force_refresh_all=False,
        )

    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    stats = detail.get("stats") or {}
    assert [row["title"] for row in rows] == ["QA Analyst"]
    assert str(detail.get("cacheDecision") or "") != "listing_only"
    assert int(stats.get("detail_pages_visited") or 0) >= 1


def test_run_static_studio_pages_source_repairs_generic_container_row_from_detail() -> None:
    listing_url = "https://example.net/careers"
    detail_url = "https://example.net/careers/creative"
    detail_html = """
        <html><body>
          <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"Creative Producer","url":"https://example.net/careers/creative","hiringOrganization":{"name":"Example Games Studio"}}
          </script>
        </body></html>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == listing_url:
            return "<html><body></body></html>"
        if url == detail_url:
            return detail_html
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_listing_parse(
        html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str
    ) -> list[dict[str, object]]:
        del html, fallback_company, fallback_source_id_prefix
        if base_url != listing_url:
            return []
        return [_static_row("Creative", detail_url)]

    with mock.patch(
        "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
        side_effect=fake_listing_parse,
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Generic Container Detail Studio", listing_url)],
        )

    assert [row["title"] for row in rows] == ["Creative Producer"]


def test_run_static_studio_pages_source_follows_generic_container_nested_hop() -> None:
    listing_url = "https://example.net/careers"
    category_url = "https://example.net/careers/function-3d"
    child_url = "https://example.net/jobs/3d-artist"
    category_html = f'<html><body><a href="{child_url}">3D Artist</a></body></html>'
    child_html = """
        <html><body>
          <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"3D Artist","url":"https://example.net/jobs/3d-artist","hiringOrganization":{"name":"Example Studio"}}
          </script>
        </body></html>
        """

    def fake_fetch(url: str, _: int) -> str:
        if url == listing_url:
            return "<html><body></body></html>"
        if url == category_url:
            return category_html
        if url == child_url:
            return child_html
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_listing_parse(
        html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str
    ) -> list[dict[str, object]]:
        del html, fallback_company, fallback_source_id_prefix
        if base_url != listing_url:
            return []
        return [_static_row("3D", category_url)]

    with mock.patch(
        "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
        side_effect=fake_listing_parse,
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Generic Container Nested Studio", listing_url)],
        )

    assert [row["title"] for row in rows] == ["3D Artist"]


def test_run_static_studio_pages_source_rejects_unresolved_generic_container_page() -> None:
    listing_url = "https://example.net/careers"
    category_url = "https://example.net/careers/analytics"

    def fake_fetch(url: str, _: int) -> str:
        if url == listing_url:
            return "<html><body></body></html>"
        if url == category_url:
            return "<html><head><title>Analytics</title></head><body>Team overview</body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_listing_parse(
        html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str
    ) -> list[dict[str, object]]:
        del html, fallback_company, fallback_source_id_prefix
        if base_url != listing_url:
            return []
        return [_static_row("Analytics", category_url)]

    jf.SOURCE_DIAGNOSTICS.clear()
    with mock.patch(
        "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
        side_effect=fake_listing_parse,
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Generic Container Dead Page Studio", listing_url)],
        )

    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert rows == []
    assert str(detail.get("classification") or "") == "dead_listing_page"


def test_run_static_studio_pages_source_keeps_real_roles_with_container_words() -> None:
    listing_url = "https://example.net/careers"
    role_url = "https://example.net/careers/creative-producer"

    def fake_fetch(url: str, _: int) -> str:
        if url == listing_url:
            return "<html><body></body></html>"
        raise RuntimeError(f"Unexpected URL: {url}")

    def fake_listing_parse(
        html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str
    ) -> list[dict[str, object]]:
        del html, fallback_company, fallback_source_id_prefix
        if base_url != listing_url:
            return []
        return [_static_row("Creative Producer", role_url)]

    with mock.patch(
        "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
        side_effect=fake_listing_parse,
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Real Creative Role Studio", listing_url)],
        )

    assert [row["title"] for row in rows] == ["Creative Producer"]


def _source(name: str, listing_url: str) -> dict[str, object]:
    return {
        "name": name,
        "studio": "Example Games Studio",
        "company": "Example Games Studio",
        "adapter": "static",
        "pages": [listing_url],
        "id": f"static:listing_url:{listing_url}",
    }


def _static_row(title: str, url: str) -> dict[str, object]:
    return {
        "sourceJobId": f"static:example:{title.lower()}",
        "title": title,
        "company": "Example Games Studio",
        "city": "",
        "country": "Unknown",
        "locations": [],
        "locationSummary": "",
        "workType": "",
        "contractType": "",
        "jobLink": url,
        "sector": "Game",
        "postedAt": "",
    }


def test_run_static_studio_pages_source_unchanged_304_skips_fetch_and_parse() -> None:
    """A board that kept jobs and answers 304 is not fetched, parsed or rendered.

    2026-10-23: static sources never sent validators, so every one of the 1,587
    boards that keep nothing re-downloaded and re-parsed its listing each run.
    """
    from datetime import UTC, datetime, timedelta

    listing_url = "https://example.net/careers"
    fetched: list[str] = []

    def fake_fetch(url: str, _: int) -> str:
        fetched.append(url)
        return "<html><body></body></html>"

    def fail_parse(**_kwargs: object) -> list[dict[str, object]]:
        raise AssertionError("an unchanged listing must not be parsed")

    source_state_rows = {
        "Unchanged Studio": {
            "lastStatus": "ok",
            "lastKeptCount": 12,
            "lastSuccessAt": (datetime.now(UTC) - timedelta(hours=20)).isoformat(),
            "lastHttpEtag": '"etag-1"',
        }
    }

    jf.SOURCE_DIAGNOSTICS.clear()
    with (
        mock.patch(
            "src.jobs.adapters.static_listing_flow.conditional_revalidate_url",
            return_value={
                "supported": True,
                "notModified": True,
                "statusCode": 304,
                "etag": '"etag-1"',
                "lastModified": "",
            },
        ) as revalidate,
        mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            side_effect=fail_parse,
        ),
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Unchanged Studio", listing_url)],
            source_state_rows=source_state_rows,
            force_refresh_all=False,
        )

    assert rows == []
    assert fetched == []
    assert revalidate.call_args.kwargs["etag"] == '"etag-1"'
    assert revalidate.call_args.kwargs["probe"] is True
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert detail.get("exclusionReason") == "cache_not_modified_304"


def test_run_static_studio_pages_source_304_on_a_multi_page_board_still_fetches() -> None:
    """Page 1 answering 304 says nothing about the pages behind it."""
    from datetime import UTC, datetime, timedelta

    first_url = "https://example.net/careers"
    second_url = "https://example.net/careers/page/2"
    fetched: list[str] = []
    probed: list[str] = []

    def fake_fetch(url: str, _: int) -> str:
        fetched.append(url)
        return "<html><body></body></html>"

    def fake_probe(url: str, _timeout: int, **_kwargs: object) -> dict[str, object]:
        probed.append(url)
        if url == first_url:
            return {
                "supported": True,
                "notModified": True,
                "statusCode": 304,
                "etag": '"etag-1"',
                "lastModified": "",
            }
        return {
            "supported": True,
            "notModified": False,
            "statusCode": 200,
            "etag": '"etag-2"',
            "lastModified": "",
        }

    source_state_rows = {
        "Two Page Studio": {
            "lastStatus": "ok",
            "lastKeptCount": 4,
            "lastSuccessAt": (datetime.now(UTC) - timedelta(hours=20)).isoformat(),
            "lastHttpEtag": '"etag-1"',
        }
    }
    source = _source("Two Page Studio", first_url)
    source["pages"] = [first_url, second_url]

    jf.SOURCE_DIAGNOSTICS.clear()
    with (
        mock.patch(
            "src.jobs.adapters.static_listing_flow.conditional_revalidate_url",
            side_effect=fake_probe,
        ),
        mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_static_row("QA Analyst", "https://example.net/careers/qa")],
        ),
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[source],
            source_state_rows=source_state_rows,
            force_refresh_all=False,
        )

    assert [row["title"] for row in rows] == ["QA Analyst"]
    assert probed == [first_url, second_url], "page 2 must be asked, not assumed"
    assert first_url in fetched
    assert second_url in fetched
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert detail.get("exclusionReason") != "cache_not_modified_304"


def test_run_static_studio_pages_source_rendered_card_board_keeps_the_full_read() -> None:
    """A JS shell answers 304 forever, so rendered-card boards must never take the shortcut."""
    from datetime import UTC, datetime, timedelta

    listing_url = "https://www.applovin.com/careers"
    fetched: list[str] = []

    def fake_fetch(url: str, _: int) -> str:
        fetched.append(url)
        return "<html><body><div id='root'></div></body></html>"

    source_state_rows = {
        "AppLovin": {
            "lastStatus": "ok",
            "lastKeptCount": 9,
            "lastSuccessAt": (datetime.now(UTC) - timedelta(hours=20)).isoformat(),
            "lastHttpEtag": '"shell-1"',
        }
    }

    jf.SOURCE_DIAGNOSTICS.clear()
    with mock.patch(
        "src.jobs.adapters.static_listing_flow.conditional_revalidate_url",
        return_value={
            "supported": True,
            "notModified": True,
            "statusCode": 304,
            "etag": '"shell-1"',
            "lastModified": "",
        },
    ) as revalidate:
        jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("AppLovin", listing_url)],
            source_state_rows=source_state_rows,
            force_refresh_all=False,
        )

    assert not revalidate.called
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert detail.get("exclusionReason") != "cache_not_modified_304"


def test_run_static_studio_pages_source_zero_kept_board_takes_the_probe() -> None:
    """A board that kept nothing asks the same question instead of re-reading itself."""
    from datetime import UTC, datetime, timedelta

    listing_url = "https://example.org/openings"
    fetched: list[str] = []

    def fake_fetch(url: str, _: int) -> str:
        fetched.append(url)
        return "<html><body></body></html>"

    def fail_parse(**_kwargs: object) -> list[dict[str, object]]:
        raise AssertionError("an unchanged empty listing must not be parsed")

    source_state_rows = {
        "Empty Studio": {
            "lastStatus": "ok",
            "lastKeptCount": 0,
            "consecutiveZeroKept": 3,
            "lastSuccessAt": (datetime.now(UTC) - timedelta(hours=20)).isoformat(),
            "lastHttpEtag": '"etag-empty"',
        }
    }

    jf.SOURCE_DIAGNOSTICS.clear()
    with (
        mock.patch(
            "src.jobs.adapters.static_listing_flow.conditional_revalidate_url",
            return_value={
                "supported": True,
                "notModified": True,
                "statusCode": 304,
                "etag": '"etag-empty"',
                "lastModified": "",
            },
        ) as revalidate,
        mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            side_effect=fail_parse,
        ),
    ):
        rows = jf.run_static_studio_pages_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=[_source("Empty Studio", listing_url)],
            source_state_rows=source_state_rows,
            force_refresh_all=False,
        )

    assert rows == []
    assert fetched == []
    assert revalidate.call_args.kwargs["etag"] == '"etag-empty"'
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    assert detail.get("exclusionReason") == "cache_not_modified_304"
