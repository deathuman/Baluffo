"""The Good Evil static plugin: recover the open German internship from the Tumblr feed.

The Good Evil (Cologne) publishes its job postings on its Tumblr ``/rss`` feed. The
studio's current opening — ``Pflichtpraktikum Game-Design od. Programmierung`` — is a
persistent listing (the same post maintained since ~2019, re-dated when hiring; the
"Jobs, Jobs, Jobs" roundup confirms it is currently open). The feed is mixed studio
news in German, so items pass through the conservative role-posting gate, which was
extended with a minimal German vocabulary for exactly this board.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static._runner import static_feed_leaf
from src.jobs.feed_urls import site_rss_url

_SPEC, can_handle, run = static_feed_leaf(
    source_id="thegoodevil",
    default_company="The Good Evil",
    identities=("thegoodevil.com", "www.thegoodevil.com"),
    feed_url_builder=site_rss_url,
)
