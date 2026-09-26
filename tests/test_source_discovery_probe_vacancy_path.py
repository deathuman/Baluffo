"""The probe's detail-path regex must match the singular `/vacancy/` spelling.

`vacancies?` expands to "vacanc" + "ie" + "s" + an optional trailing "s", so it accepted
`/vacancies/<slug>` and `/vacanciess/<slug>` but **not** `/vacancy/<slug>`. Boards publishing at the
singular path were counted as zero by the probe while the fetch adapter collected them fine through
the listing-row lane -- the same split as the About Fun finding: real jobs, wrong discovery number.

Recovered by the fix, measured live against a known-good control:
  gismart.com/careers              0 -> 26 detail links, all own-host
  playground-games.com/careers     0 -> 12 detail links, all own-host

`/careers/<slug>` and `/career/<slug>` are deliberately NOT matched by the detail regex; those go
through `_is_same_listing_detail_link`, which is same-host by construction. Asserting that here stops
a future "fix" from quietly widening this pattern into the career paths.
"""

from __future__ import annotations

import re

from src.source_discovery.probe import (
    _STATIC_DETAIL_PATH_RE,
    _STATIC_LISTING_PATH_RE,
    _static_detail_links,
    static_probe_evidence,
)


def test_singular_vacancy_detail_path_is_recognised() -> None:
    assert _STATIC_DETAIL_PATH_RE.search("/vacancy/creative-production-lead")
    assert _STATIC_DETAIL_PATH_RE.search("/vacancy/130603393")
    assert _STATIC_DETAIL_PATH_RE.search("/vacancy/25")


def test_plural_vacancy_detail_path_still_recognised() -> None:
    assert _STATIC_DETAIL_PATH_RE.search("/vacancies/lead-environment-artist")
    assert _STATIC_DETAIL_PATH_RE.search("/vacancies/qa-lead")


def test_nonsense_double_plural_is_rejected() -> None:
    """`vacancies?` matched `/vacanciess/`; `vacanc(?:y|ies)` does not."""
    assert not _STATIC_DETAIL_PATH_RE.search("/vacanciess/lead")
    assert not _STATIC_DETAIL_PATH_RE.search("/vacanies/lead")
    assert not _STATIC_DETAIL_PATH_RE.search("/vacanc/lead")


def test_untouched_detail_shapes_are_unaffected() -> None:
    for path in (
        "/jobs/senior-engineer",
        "/job/12345",
        "/positions/qa",
        "/position/qa",
        "/openings/artist",
        "/opening/artist",
    ):
        assert _STATIC_DETAIL_PATH_RE.search(path), path
    for path in ("/about/team", "/news/2026", "/blog/post-1", "/press/some-release"):
        assert not _STATIC_DETAIL_PATH_RE.search(path), path


def test_career_paths_are_not_in_the_detail_regex() -> None:
    """Career detail links are the same-host listing path's job, not this pattern's."""
    assert not _STATIC_DETAIL_PATH_RE.search("/careers/artist")
    assert not _STATIC_DETAIL_PATH_RE.search("/career/artist")


def test_listing_path_regex_accepts_both_vacancy_spellings() -> None:
    assert _STATIC_LISTING_PATH_RE.search("/vacancy")
    assert _STATIC_LISTING_PATH_RE.search("/vacancies")
    assert _STATIC_LISTING_PATH_RE.search("/vacancies/")
    assert not _STATIC_LISTING_PATH_RE.search("/about")


def test_singular_vacancy_links_reach_the_probe_evidence() -> None:
    html = """
    <html><body>
      <a href="/vacancy/creative-production-lead-radiotech">Creative Production Lead</a>
      <a href="/vacancy/talent-acquisition-manager">Talent Acquisition Manager</a>
    </body></html>
    """
    links = _static_detail_links(html, "https://gismart.com/careers")

    assert "https://gismart.com/vacancy/creative-production-lead-radiotech" in links
    assert "https://gismart.com/vacancy/talent-acquisition-manager" in links
    evidence = static_probe_evidence(html, "https://gismart.com/careers")
    assert evidence.count == 2


def test_the_old_pattern_could_not_match_the_singular_spelling() -> None:
    """Why the fix exists, stated as a fact about the previous pattern rather than a claim."""
    old = re.compile(r"(?i)/(?:vacancies?)/[^/?#]+")

    assert not old.search("/vacancy/lead")
    assert old.search("/vacancies/lead")
