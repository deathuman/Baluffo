"""Lane 2.5 end to end: a listing whose only job data is embedded JSON still yields rows.

The lane runs when the anchor-based lanes found nothing and before the browser-rendered
card lane, because a payload a GET already returned should never need a browser. The
Bungie carve-out rides along: its careers page was a rendered-card host, and its listings
are embedded JSON, so the host rule is gone.
"""

from __future__ import annotations

from ._helpers import jf

_LISTING = """
<html><body><div id="root"></div>
<script id="__NEXT_DATA__" type="application/json">
{"props": {"pageProps": {"greenhouseData": {"jobs": [
  {"absolute_url": "https://job-boards.greenhouse.io/embeddedgames/jobs/1",
   "company_name": "Embedded Games", "location": "Bellevue, WA",
   "id": 1, "title": "Gameplay Engineer"}
]}}}}
</script>
</body></html>
"""


def test_run_static_studio_pages_source_reads_embedded_json_rows() -> None:
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: _LISTING,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[
            {
                "name": "Embedded Games (Manual Website)",
                "studio": "Embedded Games",
                "company": "Embedded Games",
                "adapter": "static",
                "pages": ["https://studio.example/careers"],
                "id": "static:listing_url:https://studio.example/careers",
            }
        ],
    )
    assert [row["title"] for row in rows] == ["Gameplay Engineer"]
    assert rows[0]["jobLink"] == "https://job-boards.greenhouse.io/embeddedgames/jobs/1"
    assert rows[0]["adapter"] == "static"
    assert rows[0]["studio"] == "Embedded Games"


def test_careers_bungie_com_is_no_longer_a_rendered_card_host() -> None:
    """The carve-out: its listings are embedded JSON, so the host rule is gone."""
    from src.jobs.adapters.plugins.static._rendered_cards import _RENDERED_CARD_HOSTS

    assert "careers.bungie.com" not in _RENDERED_CARD_HOSTS
