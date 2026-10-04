"""A board's JSON API host is not the board, and both spellings must be one board.

A board registered through its JSON endpoint and the same board registered as a career
page are one board, but they parse to different identities unless the API host is folded
onto the canonical career-page host.

Measured on the 528-row curated catalogue: all 39 Ashby boards land as
``ashby:api_url:https://api.ashbyhq.com/posting-api/job-board/<slug>``, which resolved to
``('ashby', 'api.ashbyhq.com', '')`` -- both host and tenant lost -- so all 39 read as
unregistered and 334 openings were reported as missing. Greenhouse, Lever and
SmartRecruiters have the same shape. The delivery report was understating coverage.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

from coverage_board_identity import (  # noqa: E402
    api_path_tenant,
    canonical_board_host,
    registry_identity,
)

# Each pair is the same board written the two ways the registry actually stores it.
_SAME_BOARD = [
    (
        "ashby:api_url:https://api.ashbyhq.com/posting-api/job-board/moonactive",
        "ashby:board_url:https://jobs.ashbyhq.com/moonactive",
        ("ashby", "jobs.ashbyhq.com", "moonactive"),
    ),
    (
        "greenhouse:api_url:https://boards-api.greenhouse.io/v1/boards/samsung/jobs?content=true",
        "greenhouse:slug:samsung",
        ("greenhouse", "job-boards.greenhouse.io", "samsung"),
    ),
    (
        "lever:api_url:https://api.lever.co/v0/postings/amanotes?mode=json",
        "lever:account:amanotes",
        ("lever", "jobs.lever.co", "amanotes"),
    ),
    (
        "smartrecruiters:api_url:https://api.smartrecruiters.com/v1/companies/Bet3651/postings",
        "smartrecruiters:company_id:Bet3651",
        ("smartrecruiters", "jobs.smartrecruiters.com", "bet3651"),
    ),
]


def test_both_spellings_of_a_board_resolve_to_one_identity() -> None:
    for api_id, page_id, expected in _SAME_BOARD:
        assert registry_identity(api_id) == expected, api_id
        assert registry_identity(api_id) == registry_identity(page_id), api_id


def test_api_hosts_fold_onto_their_career_page_host() -> None:
    assert canonical_board_host("api.ashbyhq.com") == ("ashby", "jobs.ashbyhq.com")
    assert canonical_board_host("boards-api.greenhouse.io") == (
        "greenhouse",
        "job-boards.greenhouse.io",
    )
    assert canonical_board_host("api.lever.co") == ("lever", "jobs.lever.co")


def test_a_non_api_host_is_left_alone() -> None:
    for host in ("hrmos.co", "24bitgames.com", "jobs.ea.com", "api.example.com"):
        adapter, canonical = canonical_board_host(host)
        assert (adapter, canonical) == ("", host), host


def test_the_tenant_is_read_out_of_each_api_path_shape() -> None:
    for url, expected in (
        ("https://api.ashbyhq.com/posting-api/job-board/moonactive", "moonactive"),
        ("https://boards-api.greenhouse.io/v1/boards/samsung/jobs?content=true", "samsung"),
        ("https://api.lever.co/v0/postings/amanotes?mode=json", "amanotes"),
        ("https://api.smartrecruiters.com/v1/companies/Bet3651/postings", "bet3651"),
        ("https://apply.workable.com/api/accounts/bandai?embed=true", ""),
    ):
        assert api_path_tenant(url) == expected, url


def test_static_identities_are_unchanged_by_the_api_folding() -> None:
    """Static boards have no JSON API, so the fix must not touch them."""
    for registry_id, expected in (
        ("static:listing_url:https://hrmos.co/pages/capcom/jobs", ("static", "hrmos.co", "capcom")),
        ("static:listing_url:https://24bitgames.com/careers/", ("static", "24bitgames.com", "")),
        (
            "static:listing_url:https://moonton.jobs.feishu.cn/index",
            ("static", "moonton.jobs.feishu.cn", "moonton"),
        ),
        (
            "workday:listing_url:https://tencent.wd1.myworkdayjobs.com/Tencent_Careers",
            ("workday", "tencent.wd1.myworkdayjobs.com", "tencent_careers"),
        ),
    ):
        assert registry_identity(registry_id) == expected, registry_id
