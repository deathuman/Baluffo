"""Pet Project Games static plugin: recover the job post from the site-wide WordPress feed.

Pet Project Games exposes its only recoverable job signal as a single posting mixed into
the site's news feed (``<origin>/feed/``), so every item is passed through the conservative
role-keyword filter before it can become a row.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static._feed_postings import site_feed_url
from src.jobs.adapters.plugins.static._runner import static_feed_leaf

_SPEC, can_handle, run = static_feed_leaf(
    source_id="petprojectgames",
    default_company="Pet Project Games",
    identities=("petprojectgames.com", "www.petprojectgames.com"),
    feed_url_builder=site_feed_url,
)
