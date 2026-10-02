"""``likelyProviders`` is a prior for provider probing, not a ceiling.

An explicit provider list used to be treated as exhaustive: discovery probed
exactly those adapters and nothing else. When a studio moves its ATS, the seed
keeps naming the old vendor, the old board 404s, and the new one is never
proposed - Voodoo's board moved from Lever to Ashby and discovery could not
follow it.

The fix widens the list only when the seed's own ``careersUrl`` already points at
another supported provider's host, so a seed consistent with its pinned vendor is
left exactly as it was.
"""

from typing import Any

import pytest

from src.source_discovery import config as sdcfg
from src.source_discovery.provider_patterns import likely_providers_for_seed


def _explicit_only(seed: dict[str, Any]) -> list[str]:
    """The pre-change contract, for measuring the delta."""
    explicit = [
        str(item).strip().lower()
        for item in (seed.get("likelyProviders") or [])
        if str(item).strip()
    ]
    if explicit:
        return [item for item in explicit if item in sdcfg.SUPPORTED_PROVIDERS or item == "static"]
    providers = {"greenhouse", "workable", "teamtailor"}
    if not bool(seed.get("nlPriority")):
        providers.update({"lever", "smartrecruiters", "ashby", "recruitee", "pinpoint"})
    return [item for item in sdcfg.SUPPORTED_PROVIDERS if item in providers]


def test_provider_list_is_widened_when_the_careers_url_points_elsewhere() -> None:
    drifted: dict[str, Any] = {
        "studio": "Drifted Co",
        "aliases": ["drifted-co"],
        "nlPriority": False,
        "likelyProviders": ["lever"],
        "careersUrl": "https://jobs.ashbyhq.com/driftedco",
    }
    assert likely_providers_for_seed(drifted) == ["lever", "ashby"]


def test_provider_list_is_not_widened_for_a_consistent_seed() -> None:
    """A seed whose careersUrl matches its pinned vendor is untouched."""
    consistent: dict[str, Any] = {
        "studio": "Fine Co",
        "aliases": ["fine-co"],
        "nlPriority": False,
        "likelyProviders": ["lever"],
        "careersUrl": "https://jobs.lever.co/fineco",
    }
    assert likely_providers_for_seed(consistent) == ["lever"]


def test_provider_list_is_not_widened_without_a_careers_url() -> None:
    """No host evidence means no widening, so the list stays authoritative."""
    no_url: dict[str, Any] = {
        "studio": "Bare Co",
        "aliases": ["bare-co"],
        "nlPriority": False,
        "likelyProviders": ["lever"],
    }
    assert likely_providers_for_seed(no_url) == ["lever"]


def test_widening_preserves_order_and_never_drops_a_pinned_provider() -> None:
    seed: dict[str, Any] = {
        "studio": "Both Co",
        "aliases": ["both-co"],
        "nlPriority": False,
        "likelyProviders": ["greenhouse", "lever"],
        "careersUrl": "https://jobs.ashbyhq.com/bothco",
    }
    providers = likely_providers_for_seed(seed)
    # Pinned providers keep their declared order and come first; the implied
    # provider is appended rather than substituted.
    assert providers[:2] == ["greenhouse", "lever"]
    assert "ashby" in providers
    assert len(providers) == len(set(providers))


def test_widening_ignores_static_pseudo_provider_hosts() -> None:
    """``static`` is allowed in the list but is not a probeable ATS host."""
    seed: dict[str, Any] = {
        "studio": "Site Co",
        "aliases": ["site-co"],
        "nlPriority": False,
        "likelyProviders": ["static"],
        "careersUrl": "https://site-co.example/careers",
    }
    assert likely_providers_for_seed(seed) == ["static"]


def test_no_shipped_seed_changes_its_provider_set() -> None:
    """All 34 catalog seeds are already host-consistent, so the fix is a no-op.

    This is the blast-radius guard. Unconditional widening would have added
    lever/smartrecruiters/ashby/recruitee/pinpoint to the 15 ``static``-pinned
    seeds; gating on host evidence changes none of them today, and only helps a
    seed that actually drifts later.
    """
    seeds = sdcfg.load_studio_seeds()
    changed = [
        seed.get("studio")
        for seed in seeds
        if likely_providers_for_seed(seed) != _explicit_only(seed)
    ]
    assert changed == [], f"these shipped seeds changed provider set: {changed}"


@pytest.mark.parametrize(
    ("host_url", "expected_provider"),
    [
        ("https://jobs.ashbyhq.com/example", "ashby"),
        ("https://boards.greenhouse.io/example", "greenhouse"),
        ("https://jobs.lever.co/example", "lever"),
        ("https://jobs.smartrecruiters.com/Example", "smartrecruiters"),
    ],
)
def test_implied_provider_is_derived_from_the_careers_url_host(
    host_url: str, expected_provider: str
) -> None:
    seed: dict[str, Any] = {
        "studio": "Example Studio",
        "aliases": ["example-studio"],
        "nlPriority": False,
        "likelyProviders": ["lever"],
        "careersUrl": host_url,
    }
    assert expected_provider in likely_providers_for_seed(seed)
