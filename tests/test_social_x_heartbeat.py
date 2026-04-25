import json

from src import jobs_fetcher as jf


def test_run_social_x_source_forwards_heartbeat_to_rss_fallback() -> None:
    social_cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "x": {
            "enabled": True,
            "queries": ["#gamedevjobs"],
            "maxPostsPerQuery": 5,
            "api": {
                "enabled": True,
                "endpoint": "https://api.x.com/2/tweets/search/recent",
                "bearerTokenEnv": "BALUFFO_X_BEARER_TOKEN",
            },
            "scraperFallback": {"enabled": False, "endpoint": ""},
            "rssFallback": {"enabled": True, "instances": ["https://nitter.net"]},
        },
    }
    rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
  <item>
    <title>We're hiring a Technical Artist at Nova Studio</title>
    <link>https://nitter.net/nova/status/42</link>
    <description>Apply https://careers.nova.dev/ta</description>
    <pubDate>Mon, 09 Mar 2026 11:05:00 GMT</pubDate>
  </item>
</channel></rss>"""

    def fake_fetch(url: str, _: int) -> str:
        if "nitter.net/search/rss" in url:
            return rss
        raise RuntimeError(f"Unhandled URL: {url}")

    heartbeat_calls: list[str] = []
    rows = jf.run_social_x_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        social_config=social_cfg,
        heartbeat_callback=lambda: heartbeat_calls.append("tick"),
    )

    assert len(rows) == 1
    assert "careers.nova.dev" in rows[0]["jobLink"]
    assert heartbeat_calls == ["tick"]


def test_run_social_x_source_forwards_heartbeat_to_scraper_fallback() -> None:
    social_cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "x": {
            "enabled": True,
            "queries": ["#gamedevjobs"],
            "maxPostsPerQuery": 5,
            "api": {
                "enabled": False,
                "endpoint": "",
                "bearerTokenEnv": "BALUFFO_X_BEARER_TOKEN",
            },
            "scraperFallback": {"enabled": True, "endpoint": "https://example.local/x-search"},
            "rssFallback": {"enabled": False, "instances": []},
        },
    }
    payload = {
        "data": [
            {
                "id": "988",
                "text": (
                    "Moonshot Games is hiring gameplay engineers. "
                    "Apply at https://moonshotgames.com"
                ),
                "created_at": "2026-03-09T11:00:00Z",
            }
        ]
    }

    def fake_fetch(url: str, _: int) -> str:
        if "example.local/x-search" in url:
            return json.dumps(payload)
        raise RuntimeError(f"Unhandled URL: {url}")

    heartbeat_calls: list[str] = []
    rows = jf.run_social_x_source(
        fetch_text=fake_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        social_config=social_cfg,
        heartbeat_callback=lambda: heartbeat_calls.append("tick"),
    )

    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://moonshotgames.com/"
    assert heartbeat_calls == ["tick"]
