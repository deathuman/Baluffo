from ._helpers import jf, sheet_studios


def test_sheet_studios_uses_shared_static_fetch_helper_for_listing_and_details() -> None:
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
          </body>
        </html>
        """
    detail_html = """
        <html><body>
          <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": "JobPosting",
            "title": "Software Engineer",
            "hiringOrganization": {"name": "Tether Studios"},
            "url": "https://www.tetherstudios.com/job/tech"
          }
          </script>
        </body></html>
        """
    helper_calls: list[str] = []

    def fail_direct_fetch(url: str, timeout_s: int) -> str:
        raise AssertionError(f"direct fetch should not be used for {url} at {timeout_s}")

    def fake_fetch_html_cached(url: str, **_: object) -> tuple[str, bool]:
        helper_calls.append(url)
        if url == "https://www.tetherstudios.com/careers":
            return listing_html, False
        if url == "https://www.tetherstudios.com/job/tech":
            return detail_html, False
        raise AssertionError(f"unexpected helper url: {url}")

    rows = sheet_studios.run(
        fetch_text=fail_direct_fetch,
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
        fetch_html_cached=fake_fetch_html_cached,
    )

    assert [row["title"] for row in rows] == ["Software Engineer"]
    assert helper_calls == [
        "https://www.tetherstudios.com/careers",
        "https://www.tetherstudios.com/job/tech",
    ]
