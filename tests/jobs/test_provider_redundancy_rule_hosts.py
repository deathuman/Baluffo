"""Provider redundancy rules must match the hosts the studios actually use.

`REDUNDANT_STATIC_IF_PROVIDER` suppresses a static board when a provider board already
serves it. A rule whose host list misses the real host is worse than no rule: the static
row is never recognised as provider-served, so it keeps being fetched by the wrong adapter.

Ubisoft is the measured case. Its rule listed `ubisoft.com` and `www.ubisoft.com`, but
every careers board is a regional subdomain. On 2026-10-04 the static adapter spent
2,760 seconds on `toronto.ubisoft.com/jobs` and kept **1** job, from a 223 KB page that
serves **zero** job links, while the SmartRecruiters board (`Ubisoft2`, 333 postings,
including Berlin) covers the same studios.

This is the same shape as the Greenhouse EU defect: a host pattern that does not match
the address the studio publishes.
"""

from __future__ import annotations

from src.jobs.common.registry import _host_matches_pattern, _matches_redundant_static_rule
from src.jobs.common.registry_defaults import REDUNDANT_STATIC_IF_PROVIDER


def _rule_for(host: str) -> dict:
    for rule in REDUNDANT_STATIC_IF_PROVIDER:
        if any(_host_matches_pattern(host, pattern) for pattern in rule["hosts"]):
            return rule
    return {}


def test_regional_careers_subdomains_match_the_provider_rule() -> None:
    """The defect: every real Ubisoft board matched nothing."""
    for host in (
        "toronto.ubisoft.com",
        "berlin.ubisoft.com",
        "mainz.ubisoft.com",
        "duesseldorf.ubisoft.com",
        "saguenay.ubisoft.com",
        "stockholm.ubisoft.com",
        "winnipeg.ubisoft.com",
    ):
        rule = _rule_for(host)
        assert rule, f"{host} matched no redundancy rule"
        assert rule["adapter"] == "smartrecruiters", host
        assert rule["provider_id_value"] == "Ubisoft2", host


def test_the_apex_and_www_still_match() -> None:
    """Widening must not drop the hosts the rule already covered."""
    for host in ("ubisoft.com", "www.ubisoft.com"):
        assert _rule_for(host)["provider_id_value"] == "Ubisoft2", host


def test_a_ubisoft_subdomain_is_suppressed_once_the_provider_row_exists() -> None:
    """The rule has to actually fire, not merely match."""
    provider_keys = {("smartrecruiters", "Ubisoft2")}

    regional = _matches_redundant_static_rule(
        {"adapter": "static", "listing_url": "https://toronto.ubisoft.com/jobs"},
        REDUNDANT_STATIC_IF_PROVIDER,
        provider_keys,
    )
    apex = _matches_redundant_static_rule(
        {"adapter": "static", "listing_url": "https://www.ubisoft.com/en-us/company/careers"},
        REDUNDANT_STATIC_IF_PROVIDER,
        provider_keys,
    )

    assert regional, "a regional careers board must be recognised as provider-served"
    assert apex


def test_without_the_provider_row_the_static_board_is_kept() -> None:
    """Suppression is conditional on the provider row existing."""
    assert not _matches_redundant_static_rule(
        {"adapter": "static", "listing_url": "https://toronto.ubisoft.com/jobs"},
        REDUNDANT_STATIC_IF_PROVIDER,
        set(),
    )


def test_an_unrelated_studio_is_not_swallowed_by_a_widened_rule() -> None:
    """A subdomain glob must not become a blanket suppression for the whole domain."""
    for host in ("ubisoft.com.evil.example", "notubisoft.com", "ubisoft-community.net"):
        assert not _host_matches_pattern(host, "*.ubisoft.com"), host
