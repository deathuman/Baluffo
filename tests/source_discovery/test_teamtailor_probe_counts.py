from __future__ import annotations

from src.source_discovery.probe import parse_probe_count


def test_teamtailor_probe_count_uses_visible_total_when_listing_is_paginated() -> None:
    html = """
    <main>
      <p>25 jobs</p>
      <a href="https://career.example/jobs/manager-product-launch">Manager, Product Launch</a>
      <a href="https://career.example/jobs/open-application">Open application</a>
      <a href="/jobs?page=2">Show 5 more</a>
    </main>
    """

    assert parse_probe_count("teamtailor", html, base_url="https://career.example/jobs") == 25
