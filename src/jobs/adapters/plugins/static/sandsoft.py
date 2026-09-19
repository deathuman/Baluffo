"""Sandsoft static plugin: recover jobs from the dedicated ``/careers/feed/`` RSS feed.

Sandsoft's ``/careers/`` listing is a jQuery-era JS shell, but a server-rendered RSS
feed of every posting lives at ``/careers/feed/``. Each ``<item>`` carries a real
``<title>`` and ``<link>`` to the posting detail page, and the feed is jobs-only, so
the conservative role-keyword filter is disabled.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static._feed_postings import page_relative_feed_url
from src.jobs.adapters.plugins.static._runner import static_feed_leaf

_SPEC, can_handle, run = static_feed_leaf(
    source_id="sandsoft",
    default_company="Sandsoft",
    identities=("sandsoft.com", "www.sandsoft.com"),
    feed_url_builder=page_relative_feed_url,
    filter_feed_keywords=False,
)
