"""The Good Evil static plugin: recover the open German internship from the Tumblr feed.

The Good Evil (Cologne) publishes its job postings on its Tumblr ``/rss`` feed. The
studio's current opening — ``Pflichtpraktikum Game-Design od. Programmierung`` — is a
persistent listing (the same post maintained since ~2019, re-dated when hiring; the
"Jobs, Jobs, Jobs" roundup confirms it is currently open). The feed is mixed studio
news in German, so items pass through the conservative role-posting gate, which was
extended with a minimal German vocabulary for exactly this board.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static._runner import (
    SimpleStaticPlugin,
    simple_static_run,
    static_identity_handler,
)
from src.jobs.feed_urls import site_rss_url

_SPEC = SimpleStaticPlugin(
    source_id="thegoodevil",
    default_company="The Good Evil",
    feed_url_builder=site_rss_url,
    filter_feed_keywords=True,
)

can_handle = static_identity_handler("thegoodevil.com", "www.thegoodevil.com")

run = simple_static_run(_SPEC, parse_html=None)
