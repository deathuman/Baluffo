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


def test_sheet_studios_enriches_structured_container_artifact_rows_from_details() -> None:
    listing_url = "https://example.com/careers"
    detail_url = "https://example.com/jobs/intermediate-qa-tester"
    detail_htmls = {
        listing_url: "<html><body><script type='application/ld+json'>{}</script></body></html>",
        detail_url: """
            <html><body>
              <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "JobPosting",
                "title": "Intermediate QA Tester",
                "description": "QA role",
                "hiringOrganization": {"name": "Example Studio"},
                "jobLocation": {
                  "@type": "Place",
                  "address": {
                    "@type": "PostalAddress",
                    "addressLocality": "Prague",
                    "addressCountry": "Czechia"
                  }
                },
                "url": "https://example.com/jobs/intermediate-qa-tester"
              }
              </script>
            </body></html>
        """,
    }

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert timeout_s == 5
        return detail_htmls[url]

    def fake_listing_parse(*args: object, **kwargs: object) -> list[dict[str, object]]:
        return [
            {
                "title": "Quality Assurance",
                "company": "Example Studio",
                "jobLink": detail_url,
                "city": "Prague",
                "country": "Czechia",
            }
        ]

    rows = sheet_studios.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
        pages=[listing_url],
        source_row={
            "name": "Example Studio (Sheet)",
            "studio": "Example Studio",
            "company": "Example Studio",
            "id": "static:listing_url:https://example.com/careers",
        },
        parse_jobpostings_from_html=fake_listing_parse,
    )

    assert [row["title"] for row in rows] == ["Intermediate QA Tester"]
    assert [row["jobLink"] for row in rows] == [detail_url]


def test_sheet_studios_drops_unresolved_structured_container_artifact_rows() -> None:
    listing_url = "https://example.com/careers"
    detail_url = "https://example.com/careers/marketing"
    detail_htmls = {
        listing_url: "<html><body><script type='application/ld+json'>{}</script></body></html>",
        detail_url: """
            <html><body>
              <h1>Marketing</h1>
              <p>Explore current marketing openings from this category page.</p>
            </body></html>
        """,
    }

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert timeout_s == 5
        return detail_htmls[url]

    def fake_listing_parse(*args: object, **kwargs: object) -> list[dict[str, object]]:
        return [
            {
                "title": "Marketing",
                "company": "Example Studio",
                "jobLink": detail_url,
                "city": "Remote",
                "country": "",
            }
        ]

    rows = sheet_studios.run(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
        pages=[listing_url],
        source_row={
            "name": "Example Studio (Sheet)",
            "studio": "Example Studio",
            "company": "Example Studio",
            "id": "static:listing_url:https://example.com/careers",
        },
        parse_jobpostings_from_html=fake_listing_parse,
    )

    assert rows == []
