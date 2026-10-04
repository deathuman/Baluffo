"""A board's listing comes from that board's openings, never from the host's.

`tools/coverage_listing_discovery.py` derives a static board's listing URL by walking back up
the URLs of the openings that were missed on it. Those URLs are collected per host, which is
wrong on a multi-tenant platform: all nine herp.careers boards share one host, so every
board's candidate list held every other tenant's openings and the first ancestor to answer
200 won. That pointed all nine boards at the same listing -- CharacterBank's -- while each
carried a different studio's openings.

Nine boards registering one URL is worse than the error that started it: the board looks
served and every other tenant's openings stay missing behind it.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

from coverage_listing_discovery import (  # noqa: E402
    candidate_listings,
    opening_urls_by_host,
    urls_for_board,
)

# One host, nine tenants, exactly the shape that broke.
_HERP = "https://herp.careers/v1"
_URLS_BY_HOST = opening_urls_by_host(
    [
        {"source_url": f"{_HERP}/{tenant}/e5idJtPEHeQh"}
        for tenant in ("pgrecruit", "zoccon", "fnlinc", "charabank", "nobollelinc")
    ]
)


def _board(tenant: str) -> dict[str, Any]:
    return {"listing_url": f"https://herp.careers/{tenant}", "tenant": tenant}


def test_a_board_only_sees_its_own_openings() -> None:
    for tenant in ("pgrecruit", "zoccon", "fnlinc"):
        urls = urls_for_board(_board(tenant), _URLS_BY_HOST)
        assert urls, tenant
        assert all(tenant in u for u in urls), (tenant, urls)


def test_each_tenant_derives_its_own_listing() -> None:
    listings = {
        tenant: candidate_listings("herp.careers", urls_for_board(_board(tenant), _URLS_BY_HOST))
        for tenant in ("pgrecruit", "zoccon", "fnlinc", "charabank")
    }
    resolved = {tenant: urls[0] for tenant, urls in listings.items() if urls}
    assert len(set(resolved.values())) == len(resolved), resolved
    assert resolved["pgrecruit"] == f"{_HERP}/pgrecruit"
    assert resolved["charabank"] == f"{_HERP}/charabank"


def test_the_job_detail_url_is_never_proposed_as_its_own_listing() -> None:
    for urls in candidate_listings(
        "herp.careers", urls_for_board(_board("pgrecruit"), _URLS_BY_HOST)
    ):
        assert urls != f"{_HERP}/pgrecruit/e5idJtPEHeQh", urls


def test_a_single_site_board_still_uses_every_opening_on_its_host() -> None:
    """With no tenant to filter on, host grouping is the whole signal available."""
    by_host = opening_urls_by_host(
        [
            {"source_url": "https://koeitecmo.co.jp/recruit/career/cg/2d.html"},
            {"source_url": "https://koeitecmo.co.jp/recruit/career/3d-model.html"},
        ]
    )
    board = {"listing_url": "https://koeitecmo.co.jp", "tenant": ""}
    assert len(urls_for_board(board, by_host)) == 2
    assert "https://koeitecmo.co.jp/recruit/career" in candidate_listings(
        "koeitecmo.co.jp", urls_for_board(board, by_host)
    )
