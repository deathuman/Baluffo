"""Personio operates two domains, and a board is real on either.

`validate_candidate_for_probe` accepted only `jobs.personio.de`, so every `.jobs.personio.com`
board was refused as an invalid host before it was ever fetched. All nine of the ones this
effort registered -- Altagram Group, Aesir, Weltenbauer, nitrado, Bigpoint, GVC2U, Trip.com
Games, Remote Control and United Games -- served a real `<workzag-jobs>` feed that the
runtime's parser reads, and all nine registered zero.

This is the third time a hardcoded vendor assumption turned a working board into a recorded
zero, after Workday's GET-instead-of-POST and BambooHR's `/careers` instead of
`/careers/list`. The runtime itself never had the restriction: `provider_personio` fetches
`feed_url` directly and only the probe's validator stood in the way.
"""

from __future__ import annotations

from typing import Any

from src.source_discovery.probe import validate_candidate_for_probe

_COM = "https://altagramgroup.jobs.personio.com/xml"
_DE = "https://stratosphere-games.jobs.personio.de/xml"


def _candidate(feed_url: str) -> dict[str, Any]:
    return {"adapter": "personio", "feed_url": feed_url, "studio": "Example"}


def test_both_personio_domains_are_valid() -> None:
    """The German domain is the one the live registry carries; the `.com` is the same product."""
    for feed_url in (_COM, _DE, "https://yager.jobs.personio.de/xml"):
        valid, reason = validate_candidate_for_probe(_candidate(feed_url))
        assert valid, f"{feed_url}: {reason}"


def test_a_personio_lookalike_host_is_still_refused() -> None:
    """Widening the domains must not accept a host that merely mentions personio."""
    for feed_url in (
        "https://personio.com/xml",
        "https://jobs.personio.com.evil.test/xml",
        "https://notpersonio.de/xml",
        "",
    ):
        valid, _reason = validate_candidate_for_probe(_candidate(feed_url))
        assert not valid, feed_url


def test_personio_is_still_validated_at_all() -> None:
    """A guard on the guard: the branch must not have become unconditional."""
    valid, reason = validate_candidate_for_probe({"adapter": "personio"})
    assert not valid
    assert reason == "invalid personio host"
