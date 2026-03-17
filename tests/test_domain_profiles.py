"""Tests for domain_profiles listing URL helpers."""

from __future__ import annotations

from src.scrapers.domain_profiles import (
    domain_profile_for_url,
    is_likely_listing_url,
    pick_canonical_listing_url,
)


def test_is_likely_listing_url_empty_profile_accepts() -> None:
    """Empty profile has no exclusions, so any URL is listing-like."""
    assert is_likely_listing_url("https://example.com/careers/", {}) is True
    assert is_likely_listing_url("https://supercell.com/en/careers/our-offices/", {}) is True


def test_is_likely_listing_url_rejects_excluded_tokens() -> None:
    """Profile with exclude_listing_path_tokens rejects paths containing them."""
    profile = {"exclude_listing_path_tokens": ["/our-offices", "/joining-supercell"]}
    assert is_likely_listing_url("https://supercell.com/en/careers/", profile) is True
    assert is_likely_listing_url("https://supercell.com/en/careers/our-offices/", profile) is False
    assert is_likely_listing_url("https://supercell.com/en/careers/joining-supercell/", profile) is False
    assert is_likely_listing_url("https://supercell.com/en/careers/living-helsinki/", profile) is True  # not in list


def test_pick_canonical_listing_url_returns_shortest_listing_like() -> None:
    """Among listing-like URLs, returns the one with shortest path (main careers page)."""
    main = "https://supercell.com/en/careers/"
    sub1 = "https://supercell.com/en/careers/joining-supercell/"
    sub2 = "https://supercell.com/en/careers/our-offices/"
    # supercell.com profile has exclude_listing_path_tokens for joining-supercell, our-offices, etc.
    pages = [main, sub1, sub2]
    got = pick_canonical_listing_url(pages)
    assert got == main


def test_pick_canonical_listing_url_fallback_when_all_excluded() -> None:
    """When all pages are excluded, returns first page (backward compatible)."""
    profile = {"exclude_listing_path_tokens": ["/careers"]}
    # Mock: we need a domain that has exclusions covering everything. Use a list where all have /our-offices.
    pages = [
        "https://supercell.com/en/careers/our-offices/",
        "https://supercell.com/en/careers/joining-supercell/",
    ]
    # supercell profile excludes both; so listing_like is [] and we fall back to first.
    got = pick_canonical_listing_url(pages)
    assert got == pages[0]


def test_pick_canonical_listing_url_empty_returns_none() -> None:
    assert pick_canonical_listing_url([]) is None


def test_pick_canonical_listing_url_single_page() -> None:
    url = "https://www.valvesoftware.com/en/jobs"
    assert pick_canonical_listing_url([url]) == url


def test_domain_profile_for_url_supercell_has_exclude_listing_tokens() -> None:
    """Supercell profile includes exclude_listing_path_tokens."""
    profile = domain_profile_for_url("https://supercell.com/en/careers/")
    tokens = profile.get("exclude_listing_path_tokens") or []
    assert "/our-offices" in tokens
    assert "/joining-supercell" in tokens


def test_pick_canonical_listing_url_activision_root_becomes_search_results() -> None:
    """Activision profile has canonical_listing_path; root URL is resolved to /search-results."""
    root = "https://careers.activision.com"
    pages = [root]
    got = pick_canonical_listing_url(pages)
    assert got == "https://careers.activision.com/search-results"
    # With trailing slash
    got2 = pick_canonical_listing_url(["https://careers.activision.com/"])
    assert got2 == "https://careers.activision.com/search-results"


def test_domain_profile_for_url_activision_has_canonical_listing_path() -> None:
    """Activision profile includes canonical_listing_path for /search-results."""
    profile = domain_profile_for_url("https://careers.activision.com/")
    assert profile.get("canonical_listing_path") == "/search-results"
