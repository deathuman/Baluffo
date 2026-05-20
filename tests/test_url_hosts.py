from src.url_hosts import (
    host_matches_domain,
    host_matches_domain_pattern,
    host_matches_subdomain,
    url_host_matches_domain,
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
