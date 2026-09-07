"""Page-controlled hrefs must never crash a source: safe_page_urljoin guards the join seam."""

from __future__ import annotations

from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.static_runtime_support import safe_page_urljoin

ZOHO_POISON_HREF = "https://'+$ESAPI.encoder().encodeForHTMLAttribute(data['website'])+'"
WIX_BRACKET_HREF = "http://[cdn_template_directory]/images/jobs/product-manager.jpg"


def test_poison_template_shape_returns_empty() -> None:
    assert (
        safe_page_urljoin("https://bkomstudios.zohorecruit.com/jobs/careers", ZOHO_POISON_HREF)
        == ""
    )


def test_bracket_host_shape_returns_empty() -> None:
    assert safe_page_urljoin("https://www.plexonic.com/jobs/", WIX_BRACKET_HREF) == ""
    assert safe_page_urljoin("https://woostergames.com", "https://[bad_v6_host]/x") == ""


def test_healthy_relative_and_absolute_links_survive() -> None:
    base = "https://jobs.example.com/careers"
    assert safe_page_urljoin(base, "/jobs/123") == "https://jobs.example.com/jobs/123"
    assert safe_page_urljoin(base, "https://other.example.com/x") == "https://other.example.com/x"
    assert safe_page_urljoin(base, "") == "https://jobs.example.com/careers"


def test_rendered_card_extraction_skips_poison_anchor_without_raising() -> None:
    html = """
        <html>
          <body>
            <table class="jobs-table">
              <tbody>
                <tr class="job-row">
                  <td><a href="https://'+$ESAPI.encoder().encodeForHTMLAttribute(data['website'])+'">Environment Artist</a></td>
                  <td>Remote</td>
                  <td>Permanent</td>
                </tr>
                <tr class="job-row">
                  <td><a href="https://[cdn_template_directory]/jobs/2">Technical Artist</a></td>
                  <td>Berlin, Germany</td>
                  <td>Contract</td>
                </tr>
                <tr class="job-row">
                  <td><a href="/jobs/tools-programmer">Tools Programmer</a></td>
                  <td>Montreal, Canada</td>
                  <td>Permanent</td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """

    rows = extract_rendered_card_jobs(
        html,
        page_url="https://bkomstudios.zohorecruit.com/jobs/careers",
        company="BKOM Studios",
        source_id="static:listing_url:https://bkomstudios.zohorecruit.com/jobs/careers",
        allow_any_anchor=True,
    )
    # The two poison anchors are skipped like any non-link text; the healthy row parses.
    assert [row["jobLink"] for row in rows] == [
        "https://bkomstudios.zohorecruit.com/jobs/tools-programmer"
    ]
    assert rows[0]["title"] == "Tools Programmer"


def test_rendered_card_extraction_still_yields_healthy_board() -> None:
    html = """
        <html>
          <body>
            <table class="jobs-table">
              <tbody>
                <tr class="job-row">
                  <td><a href="/jobs/environment-artist">Environment Artist</a></td>
                  <td>Remote</td>
                  <td>Permanent</td>
                </tr>
                <tr class="job-row">
                  <td><a href="/jobs/technical-artist">Technical Artist</a></td>
                  <td>Berlin, Germany</td>
                  <td>Contract</td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """

    rows = extract_rendered_card_jobs(
        html,
        page_url="https://bkomstudios.zohorecruit.com/jobs/careers",
        company="BKOM Studios",
        source_id="static:listing_url:https://bkomstudios.zohorecruit.com/jobs/careers",
        allow_any_anchor=True,
    )
    assert [row["jobLink"] for row in rows] == [
        "https://bkomstudios.zohorecruit.com/jobs/environment-artist",
        "https://bkomstudios.zohorecruit.com/jobs/technical-artist",
    ]
    assert {row["title"] for row in rows} == {"Environment Artist", "Technical Artist"}
