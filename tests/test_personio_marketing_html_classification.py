"""Personio marketing-HTML classification gate (parser / runner / probe).

Dead board slugs (e.g. innogames.jobs.personio.de/xml) serve Personio's
marketing HTML page instead of the workzag XML feed. The 2026-09-05 T2 probe
surfaced this as the cryptic expat error "not well-formed: line 5, column
328658" — the offset lands deep inside the marketing page's minified CSS
(line 5 is a ~363k-char style blob in a 1.7MB Next.js document), telling the
operator nothing. Parser, runner, and probe now share one predicate so all
three surfaces report the same true story.
"""

from __future__ import annotations

import pytest

from src.jobs.adapters.parsers.personio import (
    looks_like_personio_marketing_html,
    parse_personio_feed_xml,
)
from src.source_discovery import probe

# Real-world shape captured 2026-09-05 from innogames/travian
# *.jobs.personio.{de,com}/xml (1,726,566 bytes, sha1 06831ce4e2ee): a Next.js
# marketing page whose line 5 is a ~363k-char minified CSS blob — the exact
# offset behind the "line 5, column 328658" probe failures.
_MARKETING_PAGE_HEAD = (
    '<!DOCTYPE html><html lang="en"><head><meta charSet="utf-8" '
    'data-next-head=""/><script id="usercentrics-cmp" async="" '
    'defer=""></script><title data-next-head="">Championing every side of '
    "HR&#x27;s evolution | Personio</title></head>"
)
_MARKETING_CSS_LINE = ".x{--tw-ring-shadow:var(--tw-ring-offset-shadow,0 0 #0000)}" * 400


def _marketing_page() -> str:
    return f"{_MARKETING_PAGE_HEAD}\n\n<style>{_MARKETING_CSS_LINE}</style>\n<body></body></html>"


def test_predicate_flags_marketing_html_shapes() -> None:
    assert looks_like_personio_marketing_html(_marketing_page())
    assert looks_like_personio_marketing_html("<html><body>anything</body></html>")
    assert looks_like_personio_marketing_html("<p>HR und Lohnbuchhaltung endlich vereint</p>")


def test_predicate_accepts_valid_feed_payloads() -> None:
    feed = (
        '<?xml version="1.0" encoding="UTF-8"?>\n\n<workzag-jobs>\n\n<position>\n'
        "    <id>2717976</id>\n    <name>Spieler &#38; Team Lead</name>\n"
        "</position>\n</workzag-jobs>"
    )
    assert not looks_like_personio_marketing_html(feed)
    assert not looks_like_personio_marketing_html("")
    assert not looks_like_personio_marketing_html("<workzag-jobs /")


def test_parser_returns_no_rows_for_marketing_page_instead_of_expat_noise() -> None:
    assert parse_personio_feed_xml(_marketing_page(), source_name="InnoGames") == []


def test_parser_still_parses_entity_bearing_job_titles() -> None:
    feed = (
        '<?xml version="1.0" encoding="UTF-8"?>\n\n<workzag-jobs>\n\n<position>\n'
        "    <id>2717976</id>\n    <name>Deutschtrainer &#38; Community Manager</name>\n"
        "    <subcompany>Travian Games GmbH</subcompany>\n"
        "    <office>Munich, Germany</office>\n"
        "</position>\n</workzag-jobs>"
    )
    rows = parse_personio_feed_xml(feed, source_name="Travian")
    assert len(rows) == 1
    assert rows[0]["title"] == "Deutschtrainer & Community Manager"


def test_probe_reports_marketing_redirect_not_expat_offset() -> None:
    with pytest.raises(ValueError, match="personio feed redirected to marketing site"):
        probe.parse_probe_count("personio", _marketing_page())


def test_probe_still_counts_valid_feed_positions() -> None:
    feed = "<workzag-jobs><position/><position/><position/></workzag-jobs>"
    assert probe.parse_probe_count("personio", feed) == 3


def test_probe_candidate_failure_names_marketing_site_not_line_offset() -> None:
    def marketing_fetch(_url: str, _timeout: int) -> str:
        return _marketing_page()

    ok, count, error = probe.probe_candidate(
        {"adapter": "personio", "feed_url": "https://dead-slug.jobs.personio.de/xml"},
        timeout_s=5,
        fetcher=marketing_fetch,
    )

    assert ok is False
    assert count == 0
    assert "marketing site" in error
    assert "line 5" not in error
    assert "not well-formed" not in error


def test_probe_genuinely_malformed_xml_keeps_xml_error() -> None:
    with pytest.raises(ValueError, match="invalid personio XML"):
        probe.parse_probe_count(
            "personio", "<workzag-jobs><position>\x08</position></workzag-jobs>"
        )
