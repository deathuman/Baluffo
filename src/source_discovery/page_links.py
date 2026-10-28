"""One body walk per page, shared by every discovery extractor.

A no-candidate page used to be walked ten times: the provider pass scanned it for hrefs and
absolute URLs, the explicit-careers pass scanned it for hrefs again, the reason detail scanned it
again, and the recovery planner scanned it four more times for hrefs and twice for URLs — all on
the same string. `PageLinks` derives each view **once per body** and hands it to every step that
needs it. Its identity is the body object, so two equal strings stay two pages and a caller that
rebuilds a body pays its own scan rather than reading another page's links.

AI boundary owns: the per-body link index and its lazily derived views.
AI boundary implement in: this file for body-level extraction reuse; URL semantics stay in web_search_extract and recovery_url_planner.
AI boundary search before contracts: web_search_extract, recovery_url_planner, page_analysis, and the GameDevMap page outcome.
AI boundary verify: `python -m pytest tests/source_discovery/test_page_links.py -q`.
"""

from __future__ import annotations

import html as html_lib
from re import Pattern
from urllib.parse import unquote

from src.shared.regex import find_urls_in_text

from .web_search_extract import href_values, jobish_links_from_hrefs, links_from_hrefs


class PageLinks:
    """Lazily derived link views of one fetched body; each is computed at most once."""

    __slots__ = (
        "body",
        "_absolute",
        "_hrefs",
        "_jobish",
        "_links",
        "_lowered",
        "_url_candidates",
    )

    def __init__(self, body: str) -> None:
        self.body = body
        self._hrefs: tuple[str, ...] | None = None
        self._links: list[str] | None = None
        self._absolute: list[str] | None = None
        self._lowered: str | None = None
        self._jobish: dict[str, list[str]] = {}
        self._url_candidates: list[str] | None = None

    def hrefs(self) -> tuple[str, ...]:
        """The raw quoted `href` values, in document order."""
        if self._hrefs is None:
            self._hrefs = tuple(href_values(self.body))
        return self._hrefs

    def links(self) -> list[str]:
        """What `extract_links_from_html` returns for this body."""
        if self._links is None:
            self._links = links_from_hrefs(self.hrefs())
        return list(self._links)

    def absolute_urls(self) -> list[str]:
        """What `find_urls_in_text` returns for this body."""
        if self._absolute is None:
            self._absolute = find_urls_in_text(str(self.body or ""))
        return list(self._absolute)

    def jobish(self, base_url: str) -> list[str]:
        """What `extract_jobish_links(html, base_url)` returns for this body."""
        cached = self._jobish.get(base_url)
        if cached is None:
            cached = jobish_links_from_hrefs(self.hrefs(), base_url)
            self._jobish[base_url] = cached
        return list(cached)

    def lowered(self) -> str:
        """The body lowercased once, for the keyword and js-shell tests."""
        if self._lowered is None:
            self._lowered = str(self.body or "").lower()
        return self._lowered

    def url_candidates(self, provider_url_pattern: Pattern[str] | None = None) -> list[str]:
        """What `recovery_url_planner.html_url_candidates` returns for this body.

        The unescape/replace is an identity on most bodies (`html.unescape` returns its input when
        there is no `&`, `str.replace` when nothing matched), so the href and URL scans are reused
        rather than repeated on a copy.
        """
        if provider_url_pattern is None and self._url_candidates is not None:
            return list(self._url_candidates)
        text = html_lib.unescape(str(self.body or "")).replace("\\/", "/")
        if text is self.body:
            candidates = [*self.links(), *self.absolute_urls()]
        else:
            candidates = [*links_from_hrefs(href_values(text)), *find_urls_in_text(text)]
        if provider_url_pattern is not None:
            candidates.extend(unquote(raw) for raw in provider_url_pattern.findall(text))
        out: list[str] = []
        seen = set()
        for raw in candidates:
            url = str(raw or "").strip().strip("\"'.,;)")
            if not url or url in seen:
                continue
            seen.add(url)
            out.append(url)
        if provider_url_pattern is None:
            self._url_candidates = out
        return list(out)


def page_links_for(body: str, page_links: PageLinks | None = None) -> PageLinks:
    """The caller's index when it belongs to this exact body, else a fresh one."""
    if page_links is not None and page_links.body is body:
        return page_links
    return PageLinks(body)
