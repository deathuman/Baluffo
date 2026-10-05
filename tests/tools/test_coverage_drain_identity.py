"""Registration must resolve a board by host + tenant whether or not it carries a URL.

**193 of the 695 curated boards carry no `listing_url` at all.** They identify the way the
registry does, by adapter plus tenant::

    {"adapter": "smartrecruiters", "company_id": "Bet3651", "coverageAuditOpenings": 176}

Keying those on `listing_url` resolved every one of them to `("", "")`, matched no registry
row, and reported a gap of **197 boards / 2,223 openings**. The gap does not exist: measured
against the drained registry with identity resolved from the tenant field, those rows match
at 190 of 193, with every provider adapter essentially complete and a residue of about three
boards genuinely unregistered.

That was the fourth wrong number this harness produced from one mistake -- applying a single
identity rule to rows that do not share a shape -- so the rules below are pinned as
invariants. They do not assert a count, because a count is what moved.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tools"))


def _load():
    spec = importlib.util.spec_from_file_location(
        "coverage_drain_identity_under_test", _ROOT / "tools" / "coverage_drain.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cd = _load()


def _curated(**fields):
    row = {
        "adapter": "",
        "coverageAuditOpenings": 1,
        "name": "board",
        "studio": "studio",
    }
    row.update(fields)
    return row


# --- a URL-bearing board resolves through the URL rule ------------------------------


def test_url_bearing_board_resolves_by_host_and_tenant():
    identity = cd.curated_identity(
        _curated(adapter="static", listing_url="https://careers.ea.com/jobs")
    )
    host, tenant = identity
    assert host == "careers.ea.com"
    # A static board is one board per host, so an empty tenant is the correct answer here.
    # Inventing a tenant (the earlier `tenant == host` bug) made 57 of 695 boards report
    # as registered when 498 were, because it stopped agreeing with `registry_identity`.
    assert tenant == ""


def test_url_on_a_multi_tenant_platform_yields_a_tenant():
    """A static-hosted board on Workday is one of many tenants on that host."""
    host, tenant = cd.curated_identity(
        _curated(adapter="static", listing_url="https://studio-a.wd5.myworkdayjobs.com/en-US/x")
    )
    assert host and tenant, "a shared-platform board must separate by tenant"


def test_url_bearing_board_ignores_its_tenant_field():
    """The URL is the identity when there is one.

    A board can carry a stale tenant field alongside its URL. Preferring the field would
    make two identical boards compare unequal whenever their stale values disagree.
    """
    with_field = cd.curated_identity(
        _curated(adapter="static", listing_url="https://careers.ea.com/jobs", company_id="stale")
    )
    without_field = cd.curated_identity(
        _curated(adapter="static", listing_url="https://careers.ea.com/jobs")
    )
    assert with_field == without_field


# --- a URL-less provider board resolves through the registry's own id grammar ---------


@pytest.mark.parametrize(
    ("adapter", "field", "value"),
    [
        ("smartrecruiters", "company_id", "ubisoft2"),
        ("greenhouse", "slug", "koeitecmo"),
        ("workable", "account", "riotgames"),
        ("lever", "account", "niantic"),
        ("ashby", "board_url", "playstation"),
    ],
)
def test_url_less_provider_board_resolves_to_its_tenant(adapter, field, value):
    """The curated row and the registry row must agree, for every provider shape."""
    host, tenant = cd.curated_identity(_curated(adapter=adapter, **{field: value}))
    assert tenant == value.lower(), "tenant must come from the provider's own field"

    registry = cd.registry_identity(f"{adapter}:{field}:{value}")
    assert registry is not None, f"{adapter} id grammar must parse"
    assert (registry[1], registry[2]) == (host, tenant), (
        "curated and registry must resolve identically, or the board reads as unregistered"
    )


def test_url_less_board_never_resolves_to_empty_when_it_names_a_tenant():
    host, tenant = cd.curated_identity(_curated(adapter="smartrecruiters", company_id="Bet3651"))
    assert host and tenant, "a provider row naming its tenant is identifiable"
    assert host == "jobs.smartrecruiters.com"


def test_url_less_board_falls_back_to_a_url_shaped_field():
    """`api_url` is a real shape in the curated table; it identifies the board."""
    with_api = cd.curated_identity(
        _curated(
            adapter="", api_url="https://api.smartrecruiters.com/v1/companies/Bet3651/postings"
        )
    )
    assert with_api[0], "an api_url must yield a host"
    assert with_api == cd.curated_identity(
        _curated(
            adapter="", listing_url="https://api.smartrecruiters.com/v1/companies/Bet3651/postings"
        )
    )


# --- an unidentifiable board must not borrow an identity ------------------------------


def test_unidentifiable_board_resolves_to_empty_and_registers_false():
    """The failure mode this pins: `("", "")` must read as "unknown", never as a match.

    Every registry row carries a non-empty host, so the empty identity cannot collide with
    a real one. Asserted explicitly because an empty identity matching an empty identity is
    exactly the bug -- it reports a gap that does not exist.
    """
    report = cd.registration_report(
        curated=[_curated(adapter="", name="nameless")],
        active=[],
        pending=[],
    )
    row = report[0]
    assert cd.report_identity(row) == ("", "")
    assert row["registered"] is False


def test_registry_rows_never_yield_an_empty_identity():
    """If any real registry row resolved to ("", "") it would match every unidentifiable
    curated board. Pin that this cannot happen."""
    report = cd.registration_report(
        curated=[_curated(adapter="smartrecruiters", company_id="ubisoft2")],
        active=[{"id": "smartrecruiters:company_id:ubisoft2", "adapter": "smartrecruiters"}],
        pending=[],
    )
    assert report[0]["registered"] is True
    assert cd.report_identity(report[0]) != ("", "")


# --- identity is host + tenant, never either alone -----------------------------------


def test_two_tenants_on_one_provider_host_are_distinct_boards():
    """Greenhouse serves every studio from one host. Host alone would call 52 boards one."""
    a = cd.curated_identity(_curated(adapter="greenhouse", slug="koeitecmo"))
    b = cd.curated_identity(_curated(adapter="greenhouse", slug="sega"))
    assert a[0] == b[0], "same host is the point"
    assert a[1] != b[1], "tenant must separate them"


def test_tenant_is_case_insensitive():
    """The registry lowercases path segments; a case difference is not a different board."""
    lower = cd.curated_identity(_curated(adapter="greenhouse", slug="koeitecmo"))
    upper = cd.curated_identity(_curated(adapter="greenhouse", slug="KoeiTecmo"))
    assert lower == upper


def test_platform_is_not_tenant():
    """The adapter names the platform, never the studio.

    Workday and BambooHR host hundreds of unrelated studios. Deriving tenancy from the
    declared adapter collapses every board on the platform into one key -- which is how 66
    cross-tenant duplicate claims survived long enough to suppress 64 boards.
    """
    a = cd.curated_identity(
        _curated(adapter="workday", listing_url="https://studio-a.wd5.myworkdayjobs.com/jobs")
    )
    b = cd.curated_identity(
        _curated(adapter="workday", listing_url="https://studio-b.wd5.myworkdayjobs.com/jobs")
    )
    assert a[0] != b[0], "tenancy comes from the host, not from `workday`"


# --- the report row carries its resolved identity ------------------------------------


def test_report_row_carries_the_resolved_identity():
    """Downstream must not re-derive identity from fields the report row does not carry.

    A report row has no `company_id`. Re-deriving on it yields ("", ""), which reads as
    "not registered" -- the original bug, one layer downstream of where it started.
    """
    report = cd.registration_report(
        curated=[_curated(adapter="smartrecruiters", company_id="ubisoft2", openings=333)],
        active=[{"id": "smartrecruiters:company_id:ubisoft2", "adapter": "smartrecruiters"}],
        pending=[],
    )
    row = report[0]
    assert row["host"], "host must be stored on the report row"
    assert row["tenant"] == "ubisoft2"
    assert "company_id" not in row, "the tenant field is deliberately not carried through"
    assert cd.report_identity(row) == cd.curated_identity(
        _curated(adapter="smartrecruiters", company_id="ubisoft2")
    )


def test_report_identity_falls_back_for_hand_built_rows():
    """Several tests and any ad-hoc reader construct a report row from `listing_url` alone.

    The fallback must apply the same rule, or the same board reads as registered or
    unregistered depending on which row shape the caller happened to build.
    """
    bare = {"adapter": "static", "listing_url": "https://careers.ea.com/jobs"}
    assert cd.report_identity(bare) == cd.curated_identity(bare)


def test_registration_matches_a_url_less_board_against_its_registry_row():
    """The end-to-end behaviour, stated as a property rather than a count."""
    report = cd.registration_report(
        curated=[
            _curated(adapter="workable", account="riotgames", openings=176),
            _curated(adapter="workable", account="studioghibli", openings=40),
        ],
        active=[
            {"id": "workable:account:riotgames", "adapter": "workable"},
        ],
        pending=[],
    )
    matched, unmatched = report
    assert matched["registered"] is True
    assert unmatched["registered"] is False, (
        "one tenant present must not make every tenant on the platform present"
    )
