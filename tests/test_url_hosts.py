from src import url_hosts
from src.url_hosts import (
    host_matches_domain,
    host_matches_domain_pattern,
    host_matches_subdomain,
    normalized_host,
    url_host_matches_domain,
)

# Every shape a caller hands `normalized_host`: hosts, domain patterns, empty and missing values,
# falsy non-strings, numbers, bytes and a tuple. The memo must not change any of them.
_NORMALIZED_HOST_VALUES = (
    "Example.COM.",
    "  apply.workable.com  ",
    ".recruitee.com",
    "jobs.greenhouse.io.",
    "",
    None,
    0,
    12,
    b"ABC",
    ("a", "b"),
)


def test_host_matches_domain_requires_domain_boundary() -> None:
    assert host_matches_domain("jobs.greenhouse.io", "greenhouse.io") is True
    assert host_matches_domain("greenhouse.io", "greenhouse.io") is True
    assert host_matches_domain("evilgreenhouse.io", "greenhouse.io") is False
    assert host_matches_domain("greenhouse.io.evil.example", "greenhouse.io") is False


def test_host_matches_subdomain_excludes_bare_domain() -> None:
    assert host_matches_subdomain("studio.teamtailor.com", "teamtailor.com") is True
    assert host_matches_subdomain("teamtailor.com", "teamtailor.com") is False


def test_host_matches_domain_pattern_honors_leading_dot() -> None:
    assert host_matches_domain_pattern("studio.recruitee.com", ".recruitee.com") is True
    assert host_matches_domain_pattern("recruitee.com", ".recruitee.com") is False
    assert host_matches_domain_pattern("apply.workable.com", "apply.workable.com") is True


def test_url_host_matches_domain_uses_parsed_hostname() -> None:
    assert url_host_matches_domain("https://jobs.smartrecruiters.com/Studio", "smartrecruiters.com")
    assert not url_host_matches_domain(
        "https://example.com/path?next=jobs.smartrecruiters.com",
        "smartrecruiters.com",
    )


def test_normalized_host_matches_the_transform_it_memoizes() -> None:
    """The memo returns exactly what the uncached expression returns, for every input shape."""
    for value in _NORMALIZED_HOST_VALUES:
        assert normalized_host(value) == str(value or "").strip().lower().rstrip("."), value


def test_normalized_host_serves_a_repeat_from_its_cache() -> None:
    """Failing-before: there was no memo, so `_normalized_host_text` did not exist to count."""
    url_hosts._normalized_host_text.cache_clear()
    try:
        first = normalized_host("Jobs.Example.COM.")
        second = normalized_host("Jobs.Example.COM.")
        info = url_hosts._normalized_host_text.cache_info()
    finally:
        url_hosts._normalized_host_text.cache_clear()
    assert first == second == "jobs.example.com"
    assert (info.hits, info.misses) == (1, 1), info


def test_normalized_host_cache_is_bounded() -> None:
    """A long-lived app process audits for hours, so the memo may not grow without a bound."""
    maxsize = url_hosts._normalized_host_text.cache_info().maxsize
    assert isinstance(maxsize, int) and 0 < maxsize <= 1 << 20, maxsize
    assert len(_NORMALIZED_HOST_VALUES) < maxsize
