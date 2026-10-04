"""Board identity is the join key on both sides of the registry comparison.

It is a leaf on purpose: ``coverage_audit``, ``coverage_boards``,
``coverage_verify`` and ``coverage_probe`` all resolve boards, and they have to
agree exactly. A disagreement is not cosmetic -- it puts an opening in one bucket
during measurement and another during registration, so the phase that was supposed
to fix it investigates the wrong thing.

That is not hypothetical. The audit previously matched a registry row by testing
whether *any* path segment was a substring of *any* registry id. That was loose
enough to label 4,564 of 4,589 ``registered_no_role`` rows as registered when the
boards were never registered at all, which sent phase 2 looking for a collection
bug that did not exist. The real figure is 58.

The three rules below each came from a measurement that contradicted the obvious
implementation.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "tools"))
_spec = importlib.util.spec_from_file_location(
    "coverage_board_identity", _ROOT / "tools" / "coverage_board_identity.py"
)
assert _spec and _spec.loader
ident: ModuleType = importlib.util.module_from_spec(_spec)
sys.modules["coverage_board_identity"] = ident
_spec.loader.exec_module(ident)


# --- URL normalisation is not board identity -----------------------------


def test_match_key_normalisation_keeps_the_query_string() -> None:
    """For Ashby the job id is ``?ashby_jid=``.

    Upstream, 218 distinct openings share one base URL, so dropping the query
    collapses a whole board into one 'matched' opening and hides real gaps.
    """
    assert ident.normalize_url("https://b.ashbyhq.com/x?ashby_jid=a") != ident.normalize_url(
        "https://b.ashbyhq.com/x?ashby_jid=b"
    )


def test_match_key_normalisation_drops_scheme_www_and_fragment() -> None:
    assert ident.normalize_url("https://www.x.com/j/1#top") == ident.normalize_url(
        "http://x.com/j/1"
    )


def test_host_of_strips_www_and_lowercases() -> None:
    assert (
        ident.host_of("https://WWW.Job-boards.Greenhouse.IO/2kczech/jobs/1")
        == "job-boards.greenhouse.io"
    )


# --- Identity is host + tenant, not the id string ------------------------


@pytest.mark.parametrize(
    ("left", "right"),
    [
        # Both are real active static rows for one board.
        ("static:listing_url:https://x.com/jobs", "static:listing_url:https://x.com/jobs/"),
    ],
)
def test_trailing_slash_does_not_split_one_board_into_two(left: str, right: str) -> None:
    """Measured: of 2,030 active static rows, 778 end in ``/`` and 1,252 do not.

    Comparing id strings would call one real board two boards, propose one for
    registration, and leave the other silently serving the openings.
    """
    assert ident.registry_identity(left) == ident.registry_identity(right)
    assert ident.covers_board(right, ident.build_candidate("https://x.com/jobs/1"))


def test_a_slug_only_id_resolves_through_the_canonical_host() -> None:
    """Ids never contain ``job-boards.greenhouse.io``, so a host-only test fails."""
    assert ident.registry_identity("greenhouse:slug:2kczech") == (
        "greenhouse",
        "job-boards.greenhouse.io",
        "2kczech",
    )


def test_identity_does_not_use_the_studio_label() -> None:
    """One board is registered as both 'Lost Boys Interactive' and '(Embracer Group)'."""
    assert ident.registry_identity(
        "ashby:board_url:https://jobs.ashbyhq.com/lostboysinteractive"
    ) == (
        "ashby",
        "jobs.ashbyhq.com",
        "lostboysinteractive",
    )


def test_the_same_tenant_on_two_platforms_is_two_boards() -> None:
    greenhouse = ident.build_candidate("https://job-boards.greenhouse.io/acme/jobs/1")
    lever = ident.build_candidate("https://jobs.lever.co/acme/abc")
    assert ident.candidate_identity(greenhouse) != ident.candidate_identity(lever)


def test_an_unparseable_id_yields_no_identity() -> None:
    assert ident.registry_identity("nonsense") is None
    assert ident.registry_identity("") is None


def test_a_different_tenant_is_not_covered() -> None:
    assert not ident.covers_board(
        "greenhouse:slug:other",
        ident.build_candidate("https://job-boards.greenhouse.io/acme/jobs/1"),
    )


# --- Tenant position per platform ----------------------------------------


@pytest.mark.parametrize(
    ("url", "adapter", "tenant"),
    [
        ("https://job-boards.greenhouse.io/2kczech/jobs/1", "greenhouse", "2kczech"),
        ("https://jobs.lever.co/animocabrands/x", "lever", "animocabrands"),
        ("https://jobs.jobvite.com/asus/job/oyOpAfw5", "static", "asus"),
        ("https://herp.careers/v1/charabank/f8aZIoadk", "static", "charabank"),
        ("https://app.mokahr.com/social-recruitment/ourpalm/45614#/job/x", "static", "ourpalm"),
        ("https://hrmos.co/pages/capcom/jobs/QA_active_200b_3", "static", "capcom"),
        ("https://kurogame.jobs.feishu.cn/index/position/1/detail", "feishu", "kurogame"),
        (
            "https://nintendoeurope.csod.com/ux/ats/careersite/1/requisition/503",
            "csod",
            "nintendoeurope",
        ),
        (
            "https://blooberteam.recruitee.com/api/offers/1",
            "recruitee",
            "blooberteam.recruitee.com",
        ),
        ("https://jobs.ea.com/en_US/careers/JobDetail/X/215618", "static", "jobs.ea.com"),
    ],
)
def test_tenant_is_taken_from_where_the_platform_keeps_it(
    url: str, adapter: str, tenant: str
) -> None:
    candidate = ident.build_candidate(url)
    assert candidate["adapter"] == adapter
    assert candidate["tenant"] == tenant


def test_hrmos_splits_into_31_tenants_not_one_board() -> None:
    """The measured failure this module exists to prevent.

    A host-root rule reported Capcom, Square Enix, Cygames, Nexon, Game Freak and
    Spike Chunsoft as a single served board while all 845 openings stayed missing.
    """
    tenants = {
        ident.build_candidate(f"https://hrmos.co/pages/{t}/jobs/{i}")["tenant"]
        for i, t in enumerate(("capcom", "square-enix", "cygames", "nexon", "gamefreak"))
    }
    assert len(tenants) == 5


def test_a_locale_segment_is_not_taken_for_a_tenant() -> None:
    """``en_US`` is chrome, and the skip set is stored lowercased to match."""
    assert (
        ident.build_candidate("https://jobs.ea.com/en_US/careers/JobDetail/X/1")["tenant"]
        == "jobs.ea.com"
    )


# --- Registry id shapes ---------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected_id"),
    [
        ("https://job-boards.greenhouse.io/2kczech/jobs/1", "greenhouse:slug:2kczech"),
        ("https://jobs.lever.co/animocabrands/abc", "lever:account:animocabrands"),
        (
            "https://jobs.ashbyhq.com/voodoo/req/1",
            "ashby:board_url:https://jobs.ashbyhq.com/voodoo",
        ),
        ("https://jobs.smartrecruiters.com/Bet3651/123", "smartrecruiters:company_id:bet3651"),
        (
            "https://blooberteam.recruitee.com/api/offers/1",
            "recruitee:api_url:https://blooberteam.recruitee.com/api/offers/",
        ),
        (
            "https://lostboysinteractive.applytojob.com/apply/x",
            "jazzhr:board_url:https://lostboysinteractive.applytojob.com/apply",
        ),
        (
            "https://activategames.bamboohr.com/careers/1",
            "bamboohr:listing_url:https://activategames.bamboohr.com/careers",
        ),
        (
            "https://tencent.wd1.myworkdayjobs.com/timi_careers",
            "workday:listing_url:https://tencent.wd1.myworkdayjobs.com/timi_careers",
        ),
    ],
)
def test_emitted_id_matches_a_live_registry_row_not_an_invention(
    url: str, expected_id: str
) -> None:
    """A mismatch produces no error: it produces a row nothing ever fetches."""
    assert ident.build_candidate(url)["id"] == expected_id


def test_unreadable_vendors_are_reported_not_dropped() -> None:
    """An unreadable board is still a missing opening, so it must stay countable."""
    for url, vendor in (
        ("https://kurogame.jobs.feishu.cn/index/position/1/detail", "feishu"),
        ("https://nintendoeurope.csod.com/ux/ats/careersite/1/requisition/503", "csod"),
    ):
        candidate = ident.build_candidate(url)
        assert candidate["adapter"] == vendor
        assert candidate["status"] == ident.STATUS_UNSUPPORTED


def test_hrmos_is_a_static_platform_not_an_unsupported_vendor() -> None:
    """hrmos was labelled a vendor needing an adapter; it is collected by a static plugin.

    `plugins/static/hrmos.py` keys on the same host and reads the same tenant listing pages,
    and four of its tenants were already registered as static rows with one keeping 340 jobs
    in production. Calling the host "hrmos" made all 32 tenants `unsupported_vendor`, which
    is what first presented 845 openings as needing a vendor integration, and then left the
    28 rows registered as static unable to match a candidate whose adapter read "hrmos".
    """
    candidate = ident.build_candidate("https://hrmos.co/pages/capcom/jobs/1")
    assert candidate["adapter"] == "static"
    assert candidate["status"] == ident.STATUS_NEW
    assert candidate["tenant"] == "capcom", candidate


def test_one_board_on_three_greenhouse_hosts_is_one_identity() -> None:
    """Greenhouse serves the same board on three hosts, one of them the EU domain."""
    identities = {
        ident.candidate_identity(ident.build_candidate(f"https://{host}/applovin/jobs/4716410006"))
        for host in (
            "boards.greenhouse.io",
            "job-boards.greenhouse.io",
            "job-boards.eu.greenhouse.io",
        )
    }
    assert identities == {("greenhouse", "job-boards.greenhouse.io", "applovin")}, identities


def test_unknown_host_is_scrapeable_rather_than_discarded() -> None:
    """2,030 active static rows exist; dropping unknown hosts lost 697 boards."""
    candidate = ident.build_candidate("https://careers.some-studio.example/jobs/1")
    assert candidate["adapter"] == "static"
    assert candidate["status"] == ident.STATUS_NEW


def test_registry_identities_accepts_both_shapes() -> None:
    parsed = ident.registry_identities({"greenhouse:slug:acme", ("lever", "jobs.lever.co", "x")})
    assert parsed == {
        ("greenhouse", "job-boards.greenhouse.io", "acme"),
        ("lever", "jobs.lever.co", "x"),
    }
