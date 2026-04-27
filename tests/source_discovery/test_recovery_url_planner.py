from __future__ import annotations

from src.source_discovery import recovery_url_planner


def test_common_recovery_urls_preserve_origin_and_path_order() -> None:
    urls = recovery_url_planner.common_recovery_urls(
        "https://studio.example.com/about",
        ("/careers", "/jobs", "/company/careers"),
    )

    assert urls == [
        "https://studio.example.com/careers",
        "https://studio.example.com/jobs",
        "https://studio.example.com/company/careers",
    ]


def test_common_recovery_urls_skip_blocked_profile_hosts() -> None:
    urls = recovery_url_planner.common_recovery_urls(
        "https://linktr.ee/studio",
        ("/careers", "/jobs"),
        blocked_hosts={"linktr.ee"},
    )

    assert urls == []


def test_recovery_urls_extract_same_party_jobish_links_and_bound_results() -> None:
    html = """
    <a href="/jobs#openings">Jobs</a>
    <script>{"url":"https://careers.example.com/join-us#team"}</script>
    <a href="https://other.example/jobs">Other</a>
    """

    urls = recovery_url_planner.recovery_urls(
        "https://www.example.com",
        html,
        paths=("/careers", "/jobs"),
        limit=3,
    )

    assert urls == [
        "https://www.example.com/jobs",
        "https://careers.example.com/join-us",
        "https://www.example.com/careers",
    ]


def test_recovery_urls_exclude_cross_party_links() -> None:
    html = '<a href="https://external.example.org/jobs">External Jobs</a>'

    urls = recovery_url_planner.recovery_urls(
        "https://studio.example.com",
        html,
        paths=("/careers",),
        limit=3,
    )

    assert urls == ["https://studio.example.com/careers"]
