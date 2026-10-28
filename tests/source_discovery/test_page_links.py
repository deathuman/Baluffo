"""Oracles for the per-body link index.

`PageLinks` exists to replace repeated scans, not to change what those scans mean. So each view is
pinned against the public extractor it stands in for, over a body that exercises every branch
(relative and absolute hrefs, a redirect wrapper, an entity, an escaped slash, a fragment, a
`mailto:` and a `javascript:` value), and the reuse rule is pinned in both directions: one body is
walked once, and an equal-but-distinct body is a different page.

Verify:
  python -m pytest tests/source_discovery/test_page_links.py -q
"""

from __future__ import annotations

import re

from src.shared.regex import find_urls_in_text
from src.source_discovery import page_links as page_links_module
from src.source_discovery.page_links import PageLinks, page_links_for
from src.source_discovery.recovery_url_planner import html_url_candidates
from src.source_discovery.web_search_extract import (
    extract_jobish_links,
    extract_links_from_html,
    href_values,
)

BASE = "https://studio.example.com/"

BODY = (
    "<html><body>"
    '<a href="/careers">Careers</a>'
    '<a href="https://boards.greenhouse.io/acme">Jobs</a>'
    '<a href="https://www.google.com/url?q=https://jobs.lever.co/acme&sa=D">Lever</a>'
    '<a href="https://www.google.com/url?q=https%3A%2F%2Fapply.workable.com%2Facme%2F">Workable</a>'
    '<a href="https://studio.example.com/jobs?team=art&amp;level=senior">Jobs by team</a>'
    '<a href="https://cdn.example.com\\/jobs\\/123">Escaped</a>'
    '<a href="#top">Top</a>'
    '<a href="mailto:jobs@studio.example.com">Mail</a>'
    '<a href="javascript:void(0)">JS</a>'
    '<script>window.jobs = "https://jobs.ashbyhq.com/acme";</script>'
    "</body></html>"
)

PLAIN_BODY = (
    "<html><body>"
    '<a href="/careers">Careers</a>'
    '<a href="https://boards.greenhouse.io/acme">Jobs</a>'
    "</body></html>"
)


def test_links_match_the_public_extractor() -> None:
    links = PageLinks(BODY).links()
    assert links == extract_links_from_html(BODY)
    assert links  # the fixture must exercise the extractor


def test_absolute_urls_match_the_shared_extractor() -> None:
    assert PageLinks(BODY).absolute_urls() == find_urls_in_text(BODY)


def test_jobish_links_match_the_public_extractor_per_base_url() -> None:
    links = PageLinks(BODY)
    assert links.jobish(BASE) == extract_jobish_links(BODY, BASE)
    other = "https://studio.example.com/careers"
    assert links.jobish(other) == extract_jobish_links(BODY, other)


def test_lowered_matches_the_plain_lower_on_the_body() -> None:
    assert PageLinks(BODY).lowered() == BODY.lower()
    assert PageLinks("").lowered() == ""


def test_url_candidates_match_the_planner_on_an_escaped_body() -> None:
    """An entity or `\\/` means the planner's own transform, not the raw scans."""
    links = PageLinks(BODY)
    assert links.url_candidates() == html_url_candidates(BODY)
    assert any("team=art&level=senior" in url for url in links.url_candidates())


def test_url_candidates_match_the_planner_with_a_provider_pattern() -> None:
    pattern = re.compile(r"https://boards\.greenhouse\.io/[^\"'\\s<]+")
    assert PageLinks(BODY).url_candidates(pattern) == html_url_candidates(
        BODY, provider_url_pattern=pattern
    )


def test_one_body_is_walked_once_across_every_view(monkeypatch) -> None:
    real = page_links_module.href_values
    calls: list[int] = []

    def _counting(body: str) -> list[str]:
        calls.append(len(body))
        return real(body)

    monkeypatch.setattr(page_links_module, "href_values", _counting)
    links = PageLinks(PLAIN_BODY)
    links.links()
    links.jobish(BASE)
    links.jobish("https://studio.example.com/jobs")
    links.url_candidates()
    links.url_candidates()

    assert calls == [len(PLAIN_BODY)], f"expected one href scan, got {len(calls)}"


def test_equal_but_distinct_bodies_are_two_pages(monkeypatch) -> None:
    real = page_links_module.href_values
    calls: list[int] = []

    def _counting(body: str) -> list[str]:
        calls.append(len(body))
        return real(body)

    monkeypatch.setattr(page_links_module, "href_values", _counting)
    # `PLAIN_BODY[:-1] + ">"` is a real second object: one-element ``str.join`` returns its
    # input, so it cannot stand in for a rebuilt body.
    rebuilt = PLAIN_BODY[:-1] + ">"
    first = PageLinks(rebuilt)
    second = PageLinks(PLAIN_BODY)
    assert first.body is not second.body
    first.links()
    second.links()

    assert len(calls) == 2


def test_page_links_for_reuses_only_the_identical_body() -> None:
    body = PLAIN_BODY[:-1] + ">"
    index = PageLinks(body)

    assert body == PLAIN_BODY and body is not PLAIN_BODY
    assert page_links_for(body, index) is index
    assert page_links_for(PLAIN_BODY, index) is not index
    assert page_links_for(body, None) is not index
    assert page_links_for(body, None).links() == index.links()


def test_href_values_are_scanned_case_insensitively_in_both_quote_styles() -> None:
    body = "<A HREF='/one'>1</A><b href=\"/two\">2</b>"
    assert list(href_values(body)) == ["/one", "/two"]
