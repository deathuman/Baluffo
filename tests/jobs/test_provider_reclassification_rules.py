"""Every static-to-provider reclassification must name a provider row that exists.

A host rule with no matching provider row is a silent no-op: the static board keeps
being scraped by the wrong adapter, which is the failure the rules exist to stop. The
other direction matters as much: a rule whose host is an aggregator or a vendor demo
tenant suppresses a board no provider covers and loses its jobs.

The 11 cases below were verified against the live board APIs on 2026-11-18. The 13
signature matches that failed validation are asserted absent so they cannot be added
back by accident.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.jobs.common.registry import (
    _host_matches_pattern,
    _matches_redundant_static_rule,
    registry_entries,
)
from src.jobs.common.registry_defaults import (
    DEFAULT_STUDIO_SOURCE_REGISTRY,
    REDUNDANT_STATIC_IF_PROVIDER,
)

LOCATOR_FIELD = {
    "greenhouse": "slug",
    "lever": "account",
    "workable": "account",
    "smartrecruiters": "company_id",
    "bamboohr": "adapter",
}

CASES: tuple[dict[str, str], ...] = (
    {
        "host": "endlessstudios.com",
        "url": "https://about.endlessstudios.com/careers.html",
        "adapter": "bamboohr",
        "tenant": "endlessstudios",
    },
    {
        "host": "lbcstudios.ca",
        "url": "https://lbcstudios.ca/careers/",
        "adapter": "bamboohr",
        "tenant": "lbcstudiosca",
    },
    {
        "host": "high-voltage.com",
        "url": "https://www.high-voltage.com/careers/",
        "adapter": "bamboohr",
        "tenant": "highvoltagesoftware",
    },
    {
        "host": "keenswh.com",
        "url": "https://www.keenswh.com/careers/",
        "adapter": "greenhouse",
        "tenant": "kswh",
    },
    {
        "host": "neowiz.com",
        "url": "https://www.neowiz.com/en/career",
        "adapter": "lever",
        "tenant": "neowiz",
    },
    {
        "host": "play.co",
        "url": "https://www.play.co/career",
        "adapter": "lever",
        "tenant": "playco",
    },
    {
        "host": "skydance.com",
        "url": "https://skydance.com/careers/",
        "adapter": "lever",
        "tenant": "skydance",
    },
    {
        "host": "dont-nod.com",
        "url": "https://dont-nod.com/en/offers/",
        "adapter": "smartrecruiters",
        "tenant": "dontnod",
    },
    {
        "host": "unchained-entertainment.com",
        "url": "https://www.unchained-entertainment.com",
        "adapter": "smartrecruiters",
        "tenant": "citystateentertainmentllc",
    },
    {
        "host": "anzu.io",
        "url": "https://www.anzu.io/careers",
        "adapter": "workable",
        "tenant": "anzu",
    },
    {
        "host": "square-enix-games.com",
        "url": "https://www.square-enix-games.com/en_us/careers",
        "adapter": "workable",
        "tenant": "square-enix",
    },
)

REJECTED_HOSTS: tuple[str, ...] = (
    "lazyapply.com",  # job aggregator, not 2K's board
    "thirtyfirstunion.com",  # greenhouse tenant resolved to a generic "users"
    "weekend.com",  # greenhouse tenant "volleythat" 404s
    "nvizzio.com",  # SmartRecruiters "job-widget" demo tenant
    "heavyiron.games",  # Workable "www" demo tenant
    "promineostudios.com",  # Workable "www" demo tenant
    "felixandpaul.com",  # BambooHR "resources" marketing tenant
    "quanticdream.com",  # Lever tenant 404s
    "codewizards.io",  # Teamtailor vendor host, no tenant
    "join.portalone.com",  # Teamtailor vendor host, no tenant
    "storm8.com",  # Teamtailor signature, no extractable tenant
    "fingersoft.com",  # Teamtailor signature, no extractable tenant
    "liongamelion.com",  # Teamtailor tenant 404s
    "cae.com",  # Workday board rate-limited at validation; not shipped
    "meowwolf.com",  # Workday board rate-limited at validation; not shipped
)


def _rule_for(host: str) -> dict[str, Any]:
    for rule in REDUNDANT_STATIC_IF_PROVIDER:
        if any(_host_matches_pattern(host, pattern) for pattern in rule.get("hosts") or []):
            return rule
    return {}


def _provider_row_matches(row: dict[str, Any], case: dict[str, str]) -> bool:
    if str(row.get("adapter") or "") != case["adapter"]:
        return False
    if case["adapter"] == "bamboohr":
        listing = str(row.get("listing_url") or "")
        return listing.startswith(f"https://{case['tenant']}.bamboohr.com/")
    field = LOCATOR_FIELD[case["adapter"]]
    return str(row.get(field) or "").lower() == case["tenant"].lower()


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["host"])
def test_reclassification_names_a_provider_row_that_exists(case: dict[str, str]) -> None:
    entries = registry_entries(
        case["adapter"],
        studio_source_registry=DEFAULT_STUDIO_SOURCE_REGISTRY,
        redundant_static_rules=REDUNDANT_STATIC_IF_PROVIDER,
    )
    assert any(_provider_row_matches(row, case) for row in entries), (
        f"{case['host']} points at {case['adapter']}/{case['tenant']}, "
        "but no such provider row is registered"
    )


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["host"])
def test_reclassification_rule_fires_for_the_studio_host(case: dict[str, str]) -> None:
    rule = _rule_for(case["host"])
    assert rule, f"{case['host']} matched no redundancy rule"
    assert rule["adapter"] == case["adapter"]

    provider_keys = {(case["adapter"], str(rule["provider_id_value"]))}
    assert _matches_redundant_static_rule(
        {"adapter": "static", "pages": [case["url"]]},
        REDUNDANT_STATIC_IF_PROVIDER,
        provider_keys,
    )


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["host"])
def test_mapped_static_row_is_suppressed_only_with_the_provider_row(
    case: dict[str, str],
) -> None:
    static_row = {
        "name": "studio-static",
        "studio": "Studio",
        "adapter": "static",
        "pages": [case["url"]],
        "enabledByDefault": True,
    }
    with_provider = [*DEFAULT_STUDIO_SOURCE_REGISTRY, static_row]
    without_provider = [
        row for row in DEFAULT_STUDIO_SOURCE_REGISTRY if row.get("adapter") != case["adapter"]
    ] + [static_row]

    suppressed = registry_entries(
        "static",
        studio_source_registry=with_provider,
        redundant_static_rules=REDUNDANT_STATIC_IF_PROVIDER,
    )
    assert "studio-static" not in [row["name"] for row in suppressed]

    kept = registry_entries(
        "static",
        studio_source_registry=without_provider,
        redundant_static_rules=REDUNDANT_STATIC_IF_PROVIDER,
    )
    assert "studio-static" in [row["name"] for row in kept]


def test_unmapped_static_host_is_kept() -> None:
    static_row = {
        "name": "control-static",
        "studio": "Control",
        "adapter": "static",
        "pages": ["https://example.com/careers"],
        "enabledByDefault": True,
    }
    kept = registry_entries(
        "static",
        studio_source_registry=[*DEFAULT_STUDIO_SOURCE_REGISTRY, static_row],
        redundant_static_rules=REDUNDANT_STATIC_IF_PROVIDER,
    )
    assert "control-static" in [row["name"] for row in kept]


@pytest.mark.parametrize("host", REJECTED_HOSTS)
def test_rejected_signature_matches_are_not_reclassified(host: str) -> None:
    assert _rule_for(host) == {}, (
        f"{host} failed board validation on 2026-11-18 and must stay on its static row"
    )
