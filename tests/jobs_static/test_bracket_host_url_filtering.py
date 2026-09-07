"""Bracket-host URL candidates must be filtered, not crash the static source.

Regression for the 2026-09-06 'Invalid IPv6 URL' failure class: pages whose
CMS emits unrendered template URLs like ``http://[cdn_template_directory]/...``
(plexonic og:image, Wix/JS-rendered DOM on zwift/take2games/bkom) made the
unguarded ``urlparse`` inside the detail-heuristics filter raise ValueError,
which the static runtime's exception classifier recorded as the source's whole
failure. The board itself was healthy.
"""

from __future__ import annotations

from collections import Counter

from src.jobs.adapters.static_detail_heuristics_filter import (
    add_detail_link,
    is_known_non_job_detail_url,
    is_malformed_or_self_detail_url,
    is_probable_job_detail_url,
)

BRACKET_TEMPLATE_URL = "http://[cdn_template_directory]/images/jobs/product-manager.jpg"


def test_malformed_check_classifies_bracket_host_template_url() -> None:
    assert is_malformed_or_self_detail_url(BRACKET_TEMPLATE_URL) is True


def test_malformed_check_classifies_bracket_host_with_page_url() -> None:
    assert (
        is_malformed_or_self_detail_url(
            BRACKET_TEMPLATE_URL, page_url="https://www.plexonic.com/jobs/"
        )
        is True
    )


def test_non_job_detail_check_survives_bracket_host() -> None:
    assert is_known_non_job_detail_url(BRACKET_TEMPLATE_URL) is True


def test_probable_detail_check_rejects_bracket_host() -> None:
    assert (
        is_probable_job_detail_url(
            BRACKET_TEMPLATE_URL,
            {"name": "Plexonic"},
            default_path_tokens=[],
            default_query_keys=[],
        )
        is False
    )


def test_add_detail_link_rejects_bracket_host_without_raising() -> None:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    seen_links: set[str] = set()
    rejections: Counter[str] = Counter()
    add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        rejections,
        candidate_url=BRACKET_TEMPLATE_URL,
        anchor_text="Lead Game Developer",
        enforce_heuristics=False,
        page_url="https://www.plexonic.com/jobs/",
        source={"name": "Plexonic (GameDevMap)"},
        default_path_tokens=[],
        default_query_keys=[],
    )
    assert detail_links == []
    assert rejections["dead_listing_page"] == 1


def test_add_detail_link_still_accepts_healthy_candidate() -> None:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    seen_links: set[str] = set()
    rejections: Counter[str] = Counter()
    add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        rejections,
        candidate_url="/jobs/lead-game-developer",
        anchor_text="Lead Game Developer",
        enforce_heuristics=False,
        page_url="https://www.plexonic.com/jobs/",
        source={"name": "Plexonic (GameDevMap)"},
        default_path_tokens=["/jobs/"],
        default_query_keys=[],
    )
    assert [link for link, _ in detail_links] == [
        "https://www.plexonic.com/jobs/lead-game-developer"
    ]
    assert not rejections
