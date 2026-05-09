from __future__ import annotations

from src.source_discovery import build_static_candidate_from_page


def test_build_static_candidate_from_page_rejects_plain_careers_landing() -> None:
    row = build_static_candidate_from_page(
        "https://www.krafton.com/careers/",
        """
        <a href="/careers/people/">People</a>
        <a href="/careers/life/">Life</a>
        <a href="/careers/jobs/">Jobs</a>
        """,
        studio="Krafton",
        nl_priority=False,
        discovery_method="gamedevmap",
    )

    assert row is None
