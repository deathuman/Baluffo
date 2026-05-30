from __future__ import annotations

from ._helpers import jf, sheet_studios


def test_sheet_studios_enriches_static_container_artifact_rows_from_details() -> None:
    listing_html = """
        <html>
          <body>
            <div class="job-category creative">
              <a href="https://example.com/careers/creative"></a>
              <div class="hiring">
                <h1>Creative</h1>
                <h2>1 Open Position</h2>
              </div>
            </div>
          </body>
        </html>
        """
    detail_htmls = {
        "https://example.com/careers": listing_html,
        "https://example.com/careers/creative": """
            <html><body>
              <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "JobPosting",
                "title": "Creative Producer",
                "description": "Production role",
                "hiringOrganization": {"name": "Example Studio"},
                "url": "https://example.com/careers/creative"
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
        pages=["https://example.com/careers"],
        source_row={
            "name": "Example Studio (Sheet)",
            "studio": "Example Studio",
            "company": "Example Studio",
            "id": "static:listing_url:https://example.com/careers",
        },
        parse_jobpostings_from_html=jf.parse_jobpostings_from_html,
    )

    assert [row["title"] for row in rows] == ["Creative Producer"]
