"""Tenant resolution decides whether a board is registered at all.

The failure this file guards against is specific and measured: a multi-tenant
platform collapsed to one host-root row looks served in the registry while every
one of its tenants' openings stays missing. hrmos.co is 31 tenants and 845
openings on the apex host alone, including Capcom, Square Enix, Cygames, Nexon,
Game Freak and Spike Chunsoft.

So these tests assert the tenant *and* the platform split, not just that a
candidate is produced.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "coverage_boards", _ROOT / "tools" / "coverage_boards.py"
)
assert _spec and _spec.loader
boards_mod: ModuleType = importlib.util.module_from_spec(_spec)
sys.modules["coverage_boards"] = boards_mod
_spec.loader.exec_module(boards_mod)


def _miss(url: str, company: str = "Studio", title: str = "Technical Artist") -> dict[str, object]:
    return {"sourceUrl": url, "company": company, "title": title}


# --- Tenant source per platform -------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected_tenant"),
    [
        # seg0: multi-tenant, tenant is the first path segment.
        ("https://job-boards.greenhouse.io/2kczech/jobs/1", "2kczech"),
        ("https://jobs.lever.co/animocabrands/abc-123", "animocabrands"),
        ("https://jobs.ashbyhq.com/arb-interactive/xyz", "arb-interactive"),
        ("https://jobs.jobvite.com/asus/job/oyOpAfw5", "asus"),
        ("https://apply.workable.com/keywords-intl1/j/ABC/", "keywords-intl1"),
        # subdomain: tenant is the leading label, path is the posting.
        ("https://kurogame.jobs.feishu.cn/index/position/769047/detail", "kurogame"),
        ("https://nintendoeurope.csod.com/ux/ats/careersite/1/requisition/503", "nintendoeurope"),
        # after: tenant sits behind a platform-chrome prefix.
        ("https://herp.careers/v1/charabank/f8aZIoadk-3R", "charabank"),
        ("https://app.mokahr.com/social-recruitment/ourpalm/45614#/job/x", "ourpalm"),
        ("https://hrmos.co/pages/capcom/jobs/QA_active_200b_3", "capcom"),
        # host: the subdomain is the tenant by construction, and the path is
        # per-opening, so the host is the board.
        ("https://blooberteam.recruitee.com/api/offers/1", "blooberteam.recruitee.com"),
        ("https://activategames.bamboohr.com/careers/1", "activategames.bamboohr.com"),
    ],
)
def test_tenant_is_resolved_from_where_the_platform_keeps_it(
    url: str, expected_tenant: str
) -> None:
    assert boards_mod.build_candidate(url)["tenant"] == expected_tenant


def test_single_site_careers_host_is_its_own_board() -> None:
    """No tenant segment to find, so the host is the board and not a guess."""
    candidate = boards_mod.build_candidate("https://jobs.ea.com/en_US/careers/JobDetail/X/215618")
    assert candidate["adapter"] == "static"
    assert candidate["tenant"] == "jobs.ea.com"
    assert candidate["id"] == "static:listing_url:https://jobs.ea.com"


def test_locale_segment_is_not_mistaken_for_a_tenant() -> None:
    """``en_US`` is chrome. Stored unlowercased it never matches the lookup."""
    candidate = boards_mod.build_candidate("https://jobs.ea.com/en_US/careers/JobDetail/X/215618")
    assert candidate["tenant"] != "en_US"


# --- Registry id shape, per adapter ---------------------------------------


@pytest.mark.parametrize(
    ("url", "expected_id"),
    [
        ("https://job-boards.greenhouse.io/2kczech/jobs/1", "greenhouse:slug:2kczech"),
        ("https://jobs.lever.co/animocabrands/abc", "lever:account:animocabrands"),
        (
            "https://jobs.ashbyhq.com/voodoo/req/1",
            "ashby:board_url:https://jobs.ashbyhq.com/voodoo",
        ),
        (
            "https://jobs.smartrecruiters.com/Bet3651/123",
            "smartrecruiters:company_id:bet3651",
        ),
        (
            "https://blooberteam.recruitee.com/api/offers/1",
            "recruitee:api_url:https://blooberteam.recruitee.com/api/offers/",
        ),
        (
            "https://activategames.bamboohr.com/careers/1",
            "bamboohr:listing_url:https://activategames.bamboohr.com/careers",
        ),
        (
            "https://lostboysinteractive.applytojob.com/apply/x",
            "jazzhr:board_url:https://lostboysinteractive.applytojob.com/apply",
        ),
        (
            "https://tencent.wd1.myworkdayjobs.com/timi_careers",
            "workday:listing_url:https://tencent.wd1.myworkdayjobs.com/timi_careers",
        ),
    ],
)
def test_registry_id_matches_the_format_the_runtime_already_reads(
    url: str, expected_id: str
) -> None:
    """Each id shape was copied from a live active registry row, not invented.

    A mismatch here does not produce a visible error: it produces a registry row
    that nothing ever fetches, and the openings stay missing.
    """
    assert boards_mod.build_candidate(url)["id"] == expected_id


def test_smartrecruiters_company_id_strips_separators_before_lookup() -> None:
    """The API keys on an alphanumeric id, not the display segment."""
    candidate = boards_mod.build_candidate("https://jobs.smartrecruiters.com/Bet365/1")
    assert candidate["company_id"] == "Bet365"
    assert candidate["api_url"] == "https://api.smartrecruiters.com/v1/companies/Bet365/postings"


# --- Grouping: one row per board, not per opening -------------------------


def test_one_board_row_absorbs_all_of_its_openings() -> None:
    misses = [
        _miss("https://job-boards.greenhouse.io/2kczech/jobs/1"),
        _miss("https://job-boards.greenhouse.io/2kczech/jobs/2"),
        _miss("https://job-boards.greenhouse.io/2kczech/jobs/3"),
    ]
    rows = boards_mod.collect_candidates(misses, registry_ids=set())
    assert len(rows) == 1
    assert rows[0]["missingCount"] == 3


def test_a_multi_tenant_platform_splits_by_tenant() -> None:
    """The measured failure: 31 hrmos tenants, 845 openings, apex host only."""
    misses = [
        _miss(f"https://hrmos.co/pages/{tenant}/jobs/{i}")
        for i, tenant in enumerate(("capcom", "cygames"))
    ]
    misses += [_miss("https://hrmos.co/pages/nexon/jobs/9")]
    rows = boards_mod.collect_candidates(misses, registry_ids=set())
    assert len(rows) == 3
    assert {row["tenant"] for row in rows} == {"capcom", "cygames", "nexon"}


def test_unreadable_vendor_groups_by_tenant_so_the_backlog_is_sized() -> None:
    """Board count is meaningless without an adapter; openings is what to size.

    Collapsing these made 845 hrmos openings read as one board.
    """
    misses = [
        _miss(f"https://hrmos.co/pages/{tenant}/jobs/{i}")
        for i, tenant in enumerate(("capcom", "cygames", "nexon"))
    ]
    rows = boards_mod.collect_candidates(misses, registry_ids=set())
    assert len(rows) == 3
    assert all(row["status"] == boards_mod.STATUS_UNSUPPORTED for row in rows)
    assert sum(row["missingCount"] for row in rows) == 3


def test_unsupported_vendor_is_reported_not_silently_dropped() -> None:
    """An unreadable board is still a missing opening, so it must stay visible."""
    row = boards_mod.build_candidate("https://kurogame.jobs.feishu.cn/index/position/1/detail")
    assert row["status"] == boards_mod.STATUS_UNSUPPORTED
    assert row["adapter"] == "feishu"


def test_unknown_host_falls_back_to_a_scraped_page_not_a_discard() -> None:
    """2,030 active static rows exist; dropping unknown hosts lost 697 boards."""
    candidate = boards_mod.build_candidate("https://careers.some-studio.example/jobs/1")
    assert candidate["adapter"] == "static"
    assert candidate["status"] == boards_mod.STATUS_NEW
    assert candidate["id"] == "static:listing_url:https://careers.some-studio.example"


# --- Dedupe against the live registry ------------------------------------


def test_an_already_registered_board_is_marked_not_re_proposed() -> None:
    """Otherwise every sweep proposes rows the registry already serves."""
    rows = boards_mod.collect_candidates(
        [_miss("https://job-boards.greenhouse.io/2kczech/jobs/1")],
        registry_ids={"greenhouse:slug:2kczech"},
    )
    assert rows[0]["status"] == boards_mod.STATUS_ALREADY


def test_identity_is_the_board_not_the_studio_label() -> None:
    """One board is registered under two labels; a label join calls it missing.

    The registry carries Lost Boys Interactive both plainly and as
    "Lost Boys Interactive (Embracer Group)", so matching on the studio name
    reports a covered board as a gap.
    """
    rows = boards_mod.collect_candidates(
        [_miss("https://job-boards.greenhouse.io/lostboysinteractive/jobs/1", company="Embracer")],
        registry_ids={"greenhouse:slug:lostboysinteractive"},
    )
    assert rows[0]["status"] == boards_mod.STATUS_ALREADY


def test_a_board_keyed_row_does_not_match_a_differently_keyed_one() -> None:
    """Same tenant, different platform: genuinely different boards."""
    rows = boards_mod.collect_candidates(
        [_miss("https://job-boards.greenhouse.io/acme/jobs/1")],
        registry_ids={"lever:account:acme"},
    )
    assert rows[0]["status"] == boards_mod.STATUS_NEW


def test_misses_without_a_url_are_ignored() -> None:
    assert boards_mod.collect_candidates([{"company": "x", "title": "y"}], registry_ids=set()) == []


# --- Summary -------------------------------------------------------------


def test_summary_reports_openings_per_adapter_and_the_vendor_backlog() -> None:
    misses = [
        _miss("https://job-boards.greenhouse.io/a/jobs/1"),
        _miss("https://job-boards.greenhouse.io/a/jobs/2"),
        _miss("https://jobs.lever.co/b/1"),
        _miss("https://kurogame.jobs.feishu.cn/index/position/1/detail"),
    ]
    summary = boards_mod.summarise(boards_mod.collect_candidates(misses, registry_ids=set()))
    assert summary["openingsRecoverable"] == 3
    assert summary["newBoardsByAdapter"] == {"greenhouse": 1, "lever": 1}
    assert summary["unsupportedVendorBacklog"] == {"feishu": {"boards": 1, "openings": 1}}
