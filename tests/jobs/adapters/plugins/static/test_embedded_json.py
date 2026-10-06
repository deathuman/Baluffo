"""Embedded-JSON row extraction: the shapes boards embed, and the junk they embed too.

The lane exists for boards whose listings are JSON inside the page rather than anchors --
Bungie's careers page carries its Greenhouse rows verbatim inside ``__NEXT_DATA__``, and
the measured zero set was full of pages whose only job data never renders as links. The
same payloads also carry CMS assets and nav entries, which is why the posting-shape guard
is as load-bearing as the extraction itself.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static.embedded_json import extract_embedded_json_rows

_BUNGIE_STYLE = """
<html><body><div id="root"></div>
<script id="__NEXT_DATA__" type="application/json">
{"props": {"pageProps": {
  "greenhouseData": {
    "jobs": [
      {"absolute_url": "https://job-boards.greenhouse.io/bungie/jobs/1",
       "company_name": "Bungie", "location": "Bellevue, WA",
       "first_published": "2026-09-01T10:00:00Z", "id": 1, "title": "Gameplay Engineer"},
      {"absolute_url": "https://job-boards.greenhouse.io/bungie/jobs/2",
       "company_name": "Bungie", "location": "United States, Remote",
       "first_published": "2026-08-20T10:00:00Z", "id": 2, "title": "Art Director"}
    ]
  },
  "appData": {
    "assets": [
      {"title": "Hero image", "url": "https://cdn.example/hero.png",
       "fileName": "hero.png", "contentType": "image/png", "width": 1200, "height": 630}
    ]
  }
}}}
</script>
</body></html>
"""


def test_greenhouse_shaped_rows_inside_next_data_are_extracted() -> None:
    rows = extract_embedded_json_rows(
        _BUNGIE_STYLE, board_url="https://careers.bungie.com/jobs", fallback_company="Bungie"
    )
    assert [row["title"] for row in rows] == ["Gameplay Engineer", "Art Director"]
    first = rows[0]
    assert first["jobLink"] == "https://job-boards.greenhouse.io/bungie/jobs/1"
    assert first["company"] == "Bungie"
    assert first["city"] == "Bellevue"
    assert first["postedAt"] == "2026-09-01T10:00:00Z"
    assert first["sector"] == "Game"
    assert first["sourceJobId"] == "embedded:1"


def test_asset_shaped_nodes_are_not_rows() -> None:
    assets_only = """
    <script id="__NEXT_DATA__" type="application/json">
    {"props": {"pageProps": {"appData": {"assets": [
      {"title": "Hero image", "url": "https://cdn.example/hero.png",
       "fileName": "hero.png", "contentType": "image/png"}
    ]}}}}
    </script>
    """
    assert (
        extract_embedded_json_rows(assets_only, board_url="https://careers.bungie.com/jobs") == []
    )


def test_ld_json_job_posting_blocks_are_rows() -> None:
    html = """
    <script type="application/ld+json">
    {"@context": "https://schema.org", "@type": "JobPosting", "title": "Senior Game Designer",
     "url": "https://studio.example/jobs/1",
     "hiringOrganization": {"name": "Example Games"},
     "jobLocation": {"address": {"addressLocality": "Berlin", "addressCountry": "DE"}},
     "datePosted": "2026-08-01", "employmentType": "Full-time"}
    </script>
    """
    rows = extract_embedded_json_rows(html, board_url="https://studio.example/careers")
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Senior Game Designer"
    assert row["company"] == "Example Games"
    assert row["city"] == "Berlin"
    assert row["contractType"] == "Full-time"
    assert row["postedAt"] == "2026-08-01"


def test_a_generic_script_array_of_job_shaped_objects_is_extracted_and_deduped() -> None:
    html = """
    <script>
    window.__JOBS__ = [
      {"title": "Gameplay Programmer", "url": "/jobs/1"},
      {"title": "Gameplay Programmer", "url": "/jobs/1"}
    ];
    </script>
    """
    rows = extract_embedded_json_rows(html, board_url="https://studio.example/careers")
    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://studio.example/jobs/1"


def test_nav_name_url_pairs_are_not_rows() -> None:
    html = '<script>var nav = [{"name": "Jobs", "url": "/jobs"}, {"name": "Home", "url": "/"}];</script>'
    assert extract_embedded_json_rows(html, board_url="https://studio.example/") == []


def test_a_non_jobposting_ld_json_type_is_not_a_row() -> None:
    html = """
    <script type="application/ld+json">
    {"@type": "Organization", "title": "Example Games", "url": "https://studio.example"}
    </script>
    """
    assert extract_embedded_json_rows(html, board_url="https://studio.example/") == []
