"""The rebrand test must be a label match, and must not fire on an acquisition.

Splitting the redirect class on a registrable-domain label match is what separates
`www.bungie.net -> careers.bungie.com` (Bungie still hiring, on a new address) from
`exozet.com -> endava.com` (Endava bought Exozet, and the studio no longer exists as a board).

The asymmetry is the whole point. Filing a rebrand as "gone" loses a live board -- six studios
in this data, one of them a board the catalogue already tracks. Filing an acquisition as a
rebrand re-points a dead board at a live one and credits it with someone else's openings. So
the rule is as narrow as it can be while still catching the real cases: same label, different
registrable domain, nothing else.

These tests also pin that a **public suffix list is required**. Without one, `.co.za` and
`.nl` are ambiguous, and a naive last-two-labels split invents registrable domains -- which
would make unrelated hosts look like rebrands of each other.
"""

from __future__ import annotations

import pytest

from src.source_discovery.rebrand_detection import (
    REBRANDED,
    is_rebrand,
    probe_rebranded_target,
)

# --- the six real rebrands ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("source_host", "target_host"),
    [
        ("www.bungie.net", "careers.bungie.com"),
        ("www.traviangames.de", "www.traviangames.com"),
        ("www.talespin.company", "www.talespin.com"),
        ("gabagoogames.ca", "www.gabagoogames.com"),
        ("starklearning.nl", "starklearning.eu"),
        ("www.softgames.de", "www.softgames.com"),
        ("www.massivemonster.co", "massivemonster.com"),
        ("www.joybits.org", "joybits.games"),
    ],
)
def test_a_studio_on_a_new_domain_is_a_rebrand(source_host, target_host):
    assert is_rebrand(source_host, target_host)


def test_the_classification_reports_site_rebranded_for_those():
    """End to end through the classifier, since that is what the fetch report will show."""
    from src.source_discovery.redirect_classification import classify_cross_site_redirect

    verdict = classify_cross_site_redirect(
        "https://www.bungie.net/jobs", "https://careers.bungie.com"
    )
    assert verdict.kind == REBRANDED
    assert verdict.target_url == "https://careers.bungie.com"
    assert verdict.adapter == "", "a rebrand is not a platform move"


# --- acquisitions must NOT be rebrands --------------------------------------------------


@pytest.mark.parametrize(
    ("source_host", "target_host", "why"),
    [
        ("exozet.com", "endava.com", "acquired by a differently-named company"),
        ("echtragames.com", "www.zynga.com", "acquired by Zynga"),
        (
            "amazongames.com",
            "www.amazongamestudios.com",
            "label differs: amazongames vs amazongamestudios",
        ),
        ("game-labs.net", "www.stillfront.com", "acquired by Stillfront"),
        ("foxandsheep.com", "www.linkedin.com", "the studio closed; linkedin is not a board"),
        (
            "wildcardstudios.com",
            "wildcardmobile.com",
            "label differs: wildcardstudios vs wildcardmobile",
        ),
    ],
)
def test_an_acquisition_is_not_a_rebrand(source_host, target_host, why):
    """The direction that matters: a rebrand here would credit a dead board with live jobs."""
    assert not is_rebrand(source_host, target_host), why


# --- same-registrable-domain moves are not rebrands --------------------------------------


@pytest.mark.parametrize(
    ("source_host", "target_host", "why"),
    [
        ("invisiblewalls.co", "jobs.invisiblewalls.co", "subdomain of the same domain"),
        ("careers.acme.com", "www.careers.acme.com", "www is not a domain change"),
        ("careers.acme.com", "careers.acme.com", "identical"),
    ],
)
def test_a_same_domain_move_is_not_a_rebrand(source_host, target_host, why):
    assert not is_rebrand(source_host, target_host), why


# --- the public suffix list is load-bearing ---------------------------------------------


def test_a_multi_part_suffix_is_resolved_correctly():
    """`seamonster.co.za` and `seamonster.digital` share a label; a naive split invents domains.

    Splitting on the last two labels would read `co.za` as the suffix and derive
    `seamonster.co` -- which is not a registrable domain at all, and would make the comparison
    meaningless in both directions.
    """
    assert is_rebrand("www.seamonster.co.za", "www.seamonster.digital")


def test_a_single_label_suffix_still_works():
    assert is_rebrand("gabagoogames.ca", "www.gabagoogames.com")


def test_unrelated_hosts_with_a_shared_suffix_are_not_rebrands():
    assert not is_rebrand("bungie.net", "traviangames.com")


def test_empty_or_hostile_input_is_inert():
    for source, target in (("", ""), ("bungie.net", ""), ("", "bungie.com"), ("...", "...")):
        assert not is_rebrand(source, target)


# --- the probe gate: a rebrand is not yet a re-point -------------------------------------


def test_a_rebrand_is_confirmed_only_when_the_new_address_lists_jobs():
    """The classification says "same studio, new address". The probe says "worth re-pointing".

    Kept separate on purpose: a label match is cheap and always available, while this costs a
    fetch and can come back empty. Several redirect targets in this data are Steam store pages,
    so an unconfirmed rebrand must not become a live board.
    """
    confirmed = probe_rebranded_target(
        "https://careers.bungie.com", detector=lambda url: {"jobLinks": ["/job/1", "/job/2"]}
    )
    assert confirmed.confirmed
    assert "job links" in confirmed.reason


def test_a_new_address_with_no_jobs_is_not_confirmed():
    empty = probe_rebranded_target("https://x.example", detector=lambda url: {"jobLinks": []})
    assert not empty.confirmed
    assert "no job links" in empty.reason


def test_a_failed_probe_is_never_permission():
    """A raising detector is no evidence, not a yes."""

    def boom(url):
        raise TimeoutError("probe timed out")

    result = probe_rebranded_target("https://x.example", detector=boom)
    assert not result.confirmed
    assert "probe failed" in result.reason


@pytest.mark.parametrize(
    ("detector", "why"),
    [
        (lambda url: "", "empty body"),
        (lambda url: None, "None body"),
        (lambda url: {}, "no jobLinks key"),
    ],
)
def test_shapeless_probe_results_are_never_confirmation(detector, why):
    assert not probe_rebranded_target("https://x.example", detector=detector).confirmed, why


def test_a_missing_target_url_is_inert():
    assert not probe_rebranded_target("", detector=lambda url: {"jobLinks": ["x"]}).confirmed
