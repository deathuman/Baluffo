from __future__ import annotations

from ._helpers import jf


def test_rendered_card_plugin_repairs_exact_category_rows_from_details() -> None:
    listing_url = "https://careers.bohemia.net/positions"
    detail_url = "https://careers.bohemia.net/en/open-positions/intermediate-qa-tester-mohbgv73"
    listing_html = f"""
        <html><body>
          <article class="job-card">
            <a href="{detail_url}">
              <h2>Quality Assurance</h2>
            </a>
          </article>
        </body></html>
        """
    detail_html = f"""
        <html><body>
          <script type="application/ld+json">
          {{
            "@context": "https://schema.org",
            "@type": "JobPosting",
            "title": "Intermediate QA Tester",
            "description": "QA role",
            "hiringOrganization": {{"name": "Bohemia Interactive"}},
            "url": "{detail_url}"
          }}
          </script>
        </body></html>
        """

    def fake_fetch(url: str, _timeout: int) -> str:
        if url == listing_url:
            return listing_html
        if url == detail_url:
            return detail_html
        raise AssertionError(f"Unexpected URL: {url}")

    rows = jf.run_static_studio_pages_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "Bohemia Interactive",
                "studio": "Bohemia Interactive",
                "company": "Bohemia Interactive",
                "adapter": "static",
                "pages": [listing_url],
                "enabledByDefault": True,
            }
        ],
        force_refresh_all=True,
    )

    assert [row["title"] for row in rows] == ["Intermediate QA Tester"]
    assert [row["jobLink"] for row in rows] == [detail_url]
