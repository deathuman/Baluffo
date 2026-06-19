from __future__ import annotations

from unittest import mock

import pytest

from src.exceptions import AdapterValidationError
from src.jobs.adapters import social
from src.jobs.common.diagnostics import SOURCE_DIAGNOSTICS


def _social_x_config() -> dict[str, object]:
    return {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "x": {
            "enabled": True,
            "queries": ["#gamedevjobs"],
            "maxPostsPerQuery": 5,
            "api": {"enabled": False, "endpoint": ""},
            "scraperFallback": {"enabled": True, "endpoint": "https://example.local/x-search"},
            "rssFallback": {"enabled": False, "instances": []},
        },
    }


def _mastodon_config() -> dict[str, object]:
    return {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "mastodon": {
            "enabled": True,
            "instances": ["https://mastodon.example"],
            "hashtags": ["gamedevjobs"],
            "maxPostsPerTag": 5,
        },
    }


def test_social_x_keeps_expected_fetch_failure_as_source_error() -> None:
    def failing_fetch(_url: str, _timeout: int) -> str:
        raise OSError("network unavailable")

    with pytest.raises(AdapterValidationError, match="network unavailable"):
        social.run_social_x_source(
            fetch_text=failing_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=_social_x_config(),
        )

    details = SOURCE_DIAGNOSTICS["social_x"]["details"]
    assert details[0]["status"] == "error"
    assert "network unavailable" in details[0]["error"]


def test_social_x_does_not_hide_programming_failures() -> None:
    def failing_fetch(_url: str, _timeout: int) -> str:
        raise AssertionError("bad social x invariant")

    with pytest.raises(AssertionError, match="bad social x invariant"):
        social.run_social_x_source(
            fetch_text=failing_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=_social_x_config(),
        )


def test_social_mastodon_does_not_hide_programming_failures() -> None:
    def failing_fetch(_url: str, _timeout: int) -> str:
        raise AssertionError("bad mastodon invariant")

    with pytest.raises(AssertionError, match="bad mastodon invariant"):
        social.run_social_mastodon_source(
            fetch_text=failing_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=_mastodon_config(),
        )


def test_social_reddit_does_not_hide_plugin_programming_failures() -> None:
    class BrokenPlugin:
        def run(self, **_kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("bad reddit plugin invariant")

    with (
        mock.patch.object(social, "ensure_social_plugins"),
        mock.patch.object(social.default_registry, "select", return_value=(BrokenPlugin(), None)),
        pytest.raises(AssertionError, match="bad reddit plugin invariant"),
    ):
        social.run_social_reddit_source(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config={
                "enabled": True,
                "minConfidence": 20,
                "rejectForHirePosts": True,
                "reddit": {
                    "enabled": True,
                    "subreddits": ["gamedev"],
                    "maxPostsPerSubreddit": 5,
                    "rssFallback": True,
                    "htmlFallback": True,
                    "rateLimitDelay": 0,
                },
            },
        )
