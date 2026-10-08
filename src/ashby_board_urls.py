"""Resolve an Ashby board's job-board slug and its posting-API URL.

Ashby board pages are client-rendered: ``https://jobs.ashbyhq.com/<slug>`` serves
markup with zero ``/job/`` anchors, so anchor counting reads every Ashby board as
empty. The posting API at ``api.ashbyhq.com/posting-api/job-board/<slug>`` is the
same data the page renders from, and it answers for every board -- measured on
2026-10-08: voodoo 122 jobs, thatgamecompany 40, supercell 32, all with 0
server-side detail links.

Both the fetch adapters and the discovery probe resolve the slug here, because a
probe that counts anchors and a fetch that reads JSON will disagree about the
same board, and the disagreement is read as "this board has no jobs".

This module is a leaf: it imports nothing from ``src.jobs`` or
``src.source_discovery`` so either side can depend on it without an import cycle.
"""

from __future__ import annotations

from urllib.parse import urlparse

ASHBY_BOARD_HOST_SUFFIX = "jobs.ashbyhq.com"
ASHBY_API_HOST_SUFFIX = "api.ashbyhq.com"
ASHBY_POSTING_API_TEMPLATE = "https://api.ashbyhq.com/posting-api/job-board/{slug}"


def ashby_board_slug(value: object) -> str:
    """The job-board slug for a board URL, an API URL, or a bare slug.

    Accepts every shape the codebase carries for an Ashby board:
    ``jobs.ashbyhq.com/<slug>``, ``.../<slug>/jobs``,
    ``api.ashbyhq.com/posting-api/job-board/<slug>``, and the slug alone.
    """
    text = str(value or "").strip()
    if not text:
        return ""

    if "://" not in text:
        # A bare value is a slug unless it was written as a path.
        return text.strip().strip("/").split("/")[0] if not text.strip().startswith("/") else ""

    parsed = urlparse(text)
    host = parsed.hostname or ""
    path = parsed.path or ""
    if not host.endswith(ASHBY_BOARD_HOST_SUFFIX) and not host.endswith(ASHBY_API_HOST_SUFFIX):
        return ""
    # The posting-API path is `/posting-api/job-board/<slug>`; the board page path is
    # `/<slug>` and may carry a trailing `/jobs` segment, which is a page, not a slug.
    if host.endswith(ASHBY_API_HOST_SUFFIX):
        marker = "/posting-api/job-board/"
        index = path.find(marker)
        if index < 0:
            return ""
        return _first_segment(path[index + len(marker) :])

    segments = [seg for seg in path.split("/") if seg]
    if segments and segments[-1].lower() == "jobs":
        segments = segments[:-1]
    return _first_segment("/".join(segments))


def _first_segment(path: str) -> str:
    segments = [seg for seg in path.split("/") if seg]
    return segments[0].strip() if segments else ""


def ashby_posting_api_url(value: object) -> str:
    """The posting-API URL for a board URL, API URL, or slug; empty when unresolvable."""
    slug = ashby_board_slug(value)
    return ASHBY_POSTING_API_TEMPLATE.format(slug=slug) if slug else ""


def ashby_source_api_url(source: dict[str, object]) -> str:
    """The posting-API URL a registry row or candidate should be read from.

    Prefers a URL the row already carries, so a curated row that was built with
    its API URL keeps it, and falls back to deriving it from the board URL. Ashby
    rows in the wild carry only ``board_url``; without this derivation every one
    of them reads an empty page.
    """
    for key in ("api_url", "board_url", "careersUrl", "listing_url"):
        url = ashby_posting_api_url(source.get(key))
        if url:
            return url
    return ""
