"""Resolve a Dover board's slug and the careers-page API URLs it is read from.

Dover board pages are client-rendered: ``https://app.dover.com/jobs/<slug>`` serves a
~4.4KB SPA shell whose only job links come from a JSON API, so anchor counting reads
every Dover board as empty. The public careers-page API is the same data the page
renders from, and it answers without authentication -- measured on 2026-10-14:
``hatsulabs`` 2 jobs and ``backbone`` 1 job, both from
``/api/v1/careers-page/<client_id>/jobs``.

Resolution is **two steps**, because the endpoints are keyed differently:

1. ``GET /api/v1/careers-page-slug/<slug>`` -> ``{"id": "<client_id>", "name": ...}``
2. ``GET /api/v1/careers-page/<client_id>/jobs`` -> ``{"count": N, "results": [...]}``

The global ``/api/v1/job-board/jobs/`` endpoint lists *every* Dover board (483 rows on
2026-10-14) and ignores a ``?client=`` filter, so it cannot be used for one studio.

Both the fetch adapter and the discovery probe resolve the slug here, because a probe
that counts anchors and a fetch that reads JSON will disagree about the same board, and
the disagreement is read as "this board has no jobs" (the Ashby lesson).

This module is a leaf: it imports nothing from ``src.jobs`` or
``src.source_discovery`` so either side can depend on it without an import cycle.
"""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlparse

DOVER_HOST = "app.dover.com"
DOVER_SLUG_API_TEMPLATE = "https://{host}/api/v1/careers-page-slug/{slug}"
DOVER_JOBS_API_TEMPLATE = "https://{host}/api/v1/careers-page/{client_id}/jobs"
#: Path prefixes a Dover board URL is seen under. `/jobs/<slug>` is the public board;
#: `/apply/<slug>/<job_id>` is a single-opening apply link.
DOVER_BOARD_PATH_PREFIXES = ("/jobs/", "/apply/")


def dover_board_slug(value: object) -> str:
    """The Dover board slug for a board URL, an API URL, or a bare slug.

    Accepts every shape the codebase carries for a Dover board:
    ``app.dover.com/jobs/<slug>``, ``app.dover.com/apply/<slug>/<job_id>``, the API
    URLs below, and the slug alone.
    """
    text = str(value or "").strip()
    if not text:
        return ""

    if "://" not in text:
        # A bare value is a slug unless it was written as a path.
        if text.startswith("/"):
            return _slug_from_path(text)
        return text.strip().strip("/").split("/")[0]

    parsed = urlparse(text)
    host = (parsed.hostname or "").lower()
    if host != DOVER_HOST:
        return ""
    path = parsed.path or ""

    # `careers-page-slug/<slug>` and `careers-page/<client_id>/jobs` are the API shapes;
    # the slug is the segment after the marker, and a client id is not a slug.
    marker = "/api/v1/careers-page-slug/"
    index = path.find(marker)
    if index >= 0:
        return _first_segment(path[index + len(marker) :])

    return _slug_from_path(path)


def _slug_from_path(path: str) -> str:
    for prefix in DOVER_BOARD_PATH_PREFIXES:
        index = path.find(prefix)
        if index >= 0:
            return _first_segment(path[index + len(prefix) :])
    return ""


def _first_segment(path: str) -> str:
    segments = [seg for seg in path.split("/") if seg]
    return segments[0].strip() if segments else ""


def dover_slug_api_url(value: object) -> str:
    """The slug-resolution URL for a board URL, API URL, or slug; empty when unresolvable."""
    slug = dover_board_slug(value)
    return DOVER_SLUG_API_TEMPLATE.format(host=DOVER_HOST, slug=slug) if slug else ""


def dover_jobs_api_url(client_id: object) -> str:
    """The jobs URL for a Dover ``client_id``; empty when it is blank.

    Note the absence of a trailing slash: ``.../jobs/`` is routed to the SPA shell and
    returns HTML, not the JSON payload.
    """
    client = str(client_id or "").strip()
    return DOVER_JOBS_API_TEMPLATE.format(host=DOVER_HOST, client_id=client) if client else ""


def dover_source_slug_url(source: Mapping[str, object]) -> str:
    """The slug-resolution URL a registry row or candidate should be read from.

    Prefers a URL the row already carries, so a curated row built with its API URL keeps
    it, and falls back to deriving it from the board URL. Dover rows in the wild carry
    ``listing_url`` / ``board_url`` / ``careersUrl`` as ``app.dover.com/jobs/<slug>``.
    """
    for key in ("api_url", "board_url", "careersUrl", "listing_url"):
        url = dover_slug_api_url(source.get(key))
        if url:
            return url
    return ""
