"""Test Reddit adapter functionality and error handling."""

from __future__ import annotations

import json
from collections.abc import Callable
from unittest.mock import patch

import pytest

import src.jobs.adapters.plugins.social.register as register_module
from src.exceptions import AdapterValidationError
from src.jobs.adapters.plugins.social.register import _run_reddit
from src.jobs.common.config import SOURCE_DIAGNOSTICS


def _reddit_config(*, subreddits: list[str], **reddit_overrides: object) -> dict[str, object]:
    reddit_config = {
        "enabled": True,
        "subreddits": subreddits,
        "maxPostsPerSubreddit": 5,
        "rssFallback": True,
        "htmlFallback": False,
        "rateLimitDelay": 0.0,
    }
    reddit_config.update(reddit_overrides)
    return {
        "enabled": True,
        "reddit": reddit_config,
    }


@pytest.fixture
def configure_reddit(monkeypatch: pytest.MonkeyPatch) -> Callable[..., dict[str, object]]:
    def _configure(*, subreddits: list[str], **reddit_overrides: object) -> dict[str, object]:
        social_config = _reddit_config(subreddits=subreddits, **reddit_overrides)
        SOURCE_DIAGNOSTICS.clear()
        monkeypatch.setattr(register_module, "_SOCIAL_CONFIG", social_config)
        return social_config

    return _configure


@pytest.fixture
def passthrough_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fetch_with_retries(
        url: str,
        fetch_text,
        timeout_s: int,
        retries: int,
        backoff_s: float,
        heartbeat_callback=None,
    ) -> str:
        _ = (retries, backoff_s)
        if callable(heartbeat_callback):
            heartbeat_callback()
        return fetch_text(url, timeout_s)

    monkeypatch.setattr(register_module, "fetch_with_retries", _fetch_with_retries)


@pytest.mark.parametrize(
    ("subreddit", "expected_studio"),
    [
        ("gamedev", "reddit/gamedev"),
        ("gameDevClassifieds", "reddit/gameDevClassifieds"),
        ("gamedevjobs", "reddit/gamedevjobs"),
        ("INAT", "reddit/INAT"),
        ("gamejobs", "reddit/gamejobs"),
        ("indiegaming", "reddit/indiegaming"),
    ],
)
def test_reddit_adapter_loads_configured_subreddits_without_retry_sleep(
    configure_reddit: Callable[..., dict[str, object]],
    passthrough_retries: None,
    subreddit: str,
    expected_studio: str,
) -> None:
    configure_reddit(subreddits=[subreddit], rssFallback=True, htmlFallback=False)

    def fake_fetch(url: str, timeout: int) -> str:
        _ = timeout
        if url.endswith(".rss"):
            return "<rss><channel></channel></rss>"
        return json.dumps({"data": {"children": []}})

    jobs = _run_reddit(fetch_text=fake_fetch, timeout_s=10, retries=2, backoff_s=1.0)

    assert jobs == []
    details = SOURCE_DIAGNOSTICS["social_reddit"]["details"]
    assert len(details) == 1
    assert details[0]["studio"] == expected_studio
    assert details[0]["status"] == "ok"


def test_reddit_adapter_configuration():
    """Committed Reddit pilot config should stay narrow and include HTML fallback."""
    from src.jobs.registry import load_social_config

    config = load_social_config(enabled=True)
    reddit_config = config.get("reddit") or {}
    subreddits = reddit_config.get("subreddits") or []

    assert reddit_config.get("enabled") is True
    assert subreddits == [
        "gamedev",
        "gameDevClassifieds",
        "gamedevjobs",
        "INAT",
        "gamejobs",
        "indiegaming",
    ]
    assert reddit_config.get("rssFallback") is True
    assert reddit_config.get("htmlFallback") is True


def test_reddit_adapter_error_handling(
    configure_reddit: Callable[..., dict[str, object]],
    passthrough_retries: None,
) -> None:
    """Test that Reddit adapter handles errors gracefully."""
    configure_reddit(subreddits=["gamedev"], maxPostsPerSubreddit=1, rssFallback=True)

    def failing_fetch(url: str, timeout: int) -> str:
        _ = (url, timeout)
        raise Exception("Network error: Connection refused")

    with pytest.raises(AdapterValidationError):
        _run_reddit(fetch_text=failing_fetch, timeout_s=10, retries=2, backoff_s=1.0)

    details = SOURCE_DIAGNOSTICS["social_reddit"]["details"]
    assert len(details) == 1
    assert details[0]["status"] == "error"
    assert "Network error" in details[0]["error"]


def test_reddit_adapter_rate_limiting(
    configure_reddit: Callable[..., dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that Reddit adapter implements rate limiting."""
    configure_reddit(
        subreddits=["gamedev", "gameDevClassifieds"],
        maxPostsPerSubreddit=1,
        rssFallback=False,
        htmlFallback=False,
        rateLimitDelay=0.01,
    )

    sleep_calls: list[float] = []
    fetch_calls: list[str] = []

    def mock_fetch(url: str, timeout: int) -> str:
        _ = timeout
        fetch_calls.append(url)
        return "{}"

    def mock_fetch_with_retries(
        url: str,
        fetch_text,
        timeout_s: int,
        retries: int,
        backoff_s: float,
        heartbeat_callback=None,
    ) -> str:
        _ = (timeout_s, retries, backoff_s)
        assert heartbeat_callback is None or callable(heartbeat_callback)
        return fetch_text(url, 10)

    monkeypatch.setattr(register_module, "fetch_with_retries", mock_fetch_with_retries)
    monkeypatch.setattr(register_module.time, "sleep", lambda delay: sleep_calls.append(delay))

    _run_reddit(fetch_text=mock_fetch, timeout_s=10, retries=2, backoff_s=1.0)

    assert len(fetch_calls) == 2
    assert sleep_calls == [0.01]


def test_reddit_adapter_heartbeats_during_fetch_attempts(
    configure_reddit: Callable[..., dict[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that Reddit adapter emits heartbeat signals during long fetch attempts."""
    configure_reddit(
        subreddits=["gamedev"],
        maxPostsPerSubreddit=1,
        rssFallback=False,
        htmlFallback=False,
        rateLimitDelay=0.0,
    )
    heartbeat_calls: list[str] = []

    def mock_fetch(url: str, timeout: int) -> str:
        assert timeout == 10
        return "{}"

    def mock_fetch_with_retries(
        url: str,
        fetch_text,
        timeout_s: int,
        retries: int,
        backoff_s: float,
        heartbeat_callback=None,
    ) -> str:
        assert callable(heartbeat_callback)
        heartbeat_callback()
        return fetch_text(url, timeout_s)

    monkeypatch.setattr(register_module, "fetch_with_retries", mock_fetch_with_retries)

    _run_reddit(
        fetch_text=mock_fetch,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        heartbeat_callback=lambda: heartbeat_calls.append("tick"),
    )

    assert len(heartbeat_calls) >= 2


def test_reddit_adapter_reports_reject_reason_counts(
    configure_reddit: Callable[..., dict[str, object]],
) -> None:
    """Rejected posts should expose reason counts in diagnostics for later auditing."""
    social_config = configure_reddit(
        subreddits=["gamedev"],
        maxPostsPerSubreddit=5,
        rssFallback=False,
        htmlFallback=False,
        rateLimitDelay=0.0,
    )
    social_config["minConfidence"] = 20
    social_config["rejectForHirePosts"] = True
    payload = {
        "data": {
            "children": [
                {
                    "data": {
                        "id": "good123",
                        "title": "We're hiring a Unity Technical Artist at Nebula Games",
                        "selftext": "Apply https://jobs.nebula.dev/ta",
                        "link_flair_text": "Hiring",
                        "permalink": "/r/gamedev/comments/good123/test/",
                        "url": "https://www.reddit.com/r/gamedev/comments/good123/test/",
                        "created_utc": 1700000000,
                        "author": "nebula_hr",
                    }
                },
                {
                    "data": {
                        "id": "bad123",
                        "title": "Why is nobody hiring gameplay programmers anymore?",
                        "selftext": "This industry is rough",
                        "link_flair_text": "Discussion",
                        "permalink": "/r/gamedev/comments/bad123/test/",
                        "url": "https://www.reddit.com/r/gamedev/comments/bad123/test/",
                        "created_utc": 1700000000,
                        "author": "someone",
                    }
                },
            ]
        }
    }

    with patch(
        "src.jobs.adapters.plugins.social.register.fetch_with_retries",
        return_value=json.dumps(payload),
    ):
        jobs = _run_reddit(
            fetch_text=lambda url, timeout: json.dumps(payload),
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
        )

    assert len(jobs) == 1
    details = SOURCE_DIAGNOSTICS["social_reddit"]["details"]
    assert len(details) == 1
    assert details[0]["keptCount"] == 1
    assert details[0]["rejectReasonCounts"]["not_hiring_or_layoff"] == 1
