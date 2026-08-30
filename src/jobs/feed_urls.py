"""Shared leaf: WordPress-style feed URL builders.

Single source of truth for the two feed URL shapes consumed by the static feed
plugins (``jobs.adapters.plugins.static._feed_postings`` and its spec-driven
leaves) and the discovery feed probe (``source_discovery.wordpress_feed_probe``),
so the plugin and discovery sides can never drift apart:

* :func:`site_feed_url` — the site-wide feed rooted at the origin
  (``<scheme>://<host>/feed/``), used for mixed news feeds (arsanesia,
  petprojectgames) and as the default feed-mode builder.
* :func:`page_relative_feed_url` — a dedicated feed rooted at the page's own
  path (e.g. Sandsoft's ``/careers/feed/``); the same shape the discovery probe
  checks before a zero-kept candidate is escalated to the browser pool.

AI boundary owns: feed URL shape construction shared by the static plugins and
the discovery feed probe.
"""

from __future__ import annotations

from urllib.parse import urlparse

from src.jobs.text_utils import clean_text


def site_feed_url(page_url: str) -> str:
    """WordPress site feed for a page's origin (``<scheme>://<host>/feed/``)."""
    try:
        parsed = urlparse(clean_text(page_url))
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}/feed/"


def page_relative_feed_url(page_url: str, *, feed_name: str = "feed") -> str:
    """Feed rooted at the page's own path (e.g. ``<page>/careers/feed/``).

    Handles the trailing-slash variants and the already-``/<feed>`` form so a
    dedicated jobs feed living next to the careers page (like Sandsoft's
    ``/careers/feed/``) reuses the same fetch/parse wiring.
    """
    base = clean_text(page_url).rstrip("/")
    if not base:
        return ""
    feed = clean_text(feed_name).strip("/")
    if base.endswith("/" + feed):
        return base + "/"
    return f"{base}/{feed}/"


def site_rss_url(page_url: str) -> str:
    """Tumblr/Ghost-style site feed at the origin's ``/rss`` (no trailing slash)."""
    try:
        parsed = urlparse(clean_text(page_url))
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}/rss"
