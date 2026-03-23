"""Test Reddit adapter functionality and error handling."""

from __future__ import annotations

import json
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from src.exceptions import AdapterValidationError
from src.jobs.adapters.plugins.social.register import _run_reddit, set_source_diagnostics
from src.jobs.models import RawJob
from src.jobs.common.config import SOURCE_DIAGNOSTICS


def test_reddit_adapter_loads_all_subreddits():
    """Test that Reddit adapter processes all 6 configured subreddits."""
    social_config = {
        "enabled": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev", "gameDevClassifieds", "gamedevjobs", "INAT", "gamejobs", "indiegaming"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": True,
            "htmlFallback": False,
        }
    }
    
    # Mock the social config
    import src.jobs.adapters.plugins.social.register as register_module
    original_social_config = register_module._SOCIAL_CONFIG
    register_module._SOCIAL_CONFIG = social_config
    
    try:
        # This should not raise an error and should process all subreddits
        jobs = _run_reddit(
            fetch_text=lambda url, timeout: "{}",
            timeout_s=10,
            retries=2,
            backoff_s=1.0
        )
        
        # Verify diagnostics are set correctly
        assert "social_reddit" in SOURCE_DIAGNOSTICS
        details = SOURCE_DIAGNOSTICS["social_reddit"]["details"]
        
        # Should have 6 entries (one for each subreddit)
        assert len(details) == 6
        
        # Verify all subreddits are processed
        subreddit_names = [entry["studio"] for entry in details]
        expected_studios = [
            "reddit/gamedev",
            "reddit/gameDevClassifieds", 
            "reddit/gamedevjobs",
            "reddit/INAT",
            "reddit/gamejobs",
            "reddit/indiegaming"
        ]
        assert set(subreddit_names) == set(expected_studios)
        
    finally:
        # Restore original config
        register_module._SOCIAL_CONFIG = original_social_config


def test_reddit_adapter_configuration():
    """Default config should not poll broad Reddit discussion sources."""
    from src.jobs.registry import load_social_config
    
    config = load_social_config(enabled=True)
    reddit_config = config.get("reddit") or {}
    subreddits = reddit_config.get("subreddits") or []
    
    assert reddit_config.get("enabled") is False
    assert len(subreddits) == 0
    expected_subreddits = []
    assert subreddits == expected_subreddits


def test_reddit_adapter_error_handling():
    """Test that Reddit adapter handles errors gracefully."""
    social_config = {
        "enabled": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 1,
            "rssFallback": True,
            "htmlFallback": False,
        }
    }
    
    def failing_fetch(url: str, timeout: int) -> str:
        # Always fail with a network error
        raise Exception("Network error: Connection refused")
    
    # Mock the social config
    import src.jobs.adapters.plugins.social.register as register_module
    original_social_config = register_module._SOCIAL_CONFIG
    register_module._SOCIAL_CONFIG = social_config
    
    try:
        with pytest.raises(AdapterValidationError):
            _run_reddit(
                fetch_text=failing_fetch,
                timeout_s=10,
                retries=2,
                backoff_s=1.0
            )
        
        # Verify diagnostics show errors
        assert "social_reddit" in SOURCE_DIAGNOSTICS
        details = SOURCE_DIAGNOSTICS["social_reddit"]["details"]
        assert len(details) == 1
        assert details[0]["status"] == "error"
        assert "Network error" in details[0]["error"]
        
    finally:
        # Restore original config
        register_module._SOCIAL_CONFIG = original_social_config


def test_reddit_adapter_rate_limiting():
    """Test that Reddit adapter implements rate limiting."""
    import time
    from unittest.mock import patch
    
    social_config = {
        "enabled": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev", "gameDevClassifieds"],
            "maxPostsPerSubreddit": 1,
            "rssFallback": False,
            "htmlFallback": False,
            "rateLimitDelay": 0.01,  # Very short delay for testing
        }
    }
    
    # Track fetch calls
    fetch_calls = []
    
    def mock_fetch(url: str, timeout: int) -> str:
        fetch_calls.append(time.time())
        return "{}"
    
    # Mock the social config
    import src.jobs.adapters.plugins.social.register as register_module
    original_social_config = register_module._SOCIAL_CONFIG
    register_module._SOCIAL_CONFIG = social_config
    
    try:
        def mock_fetch_with_retries(url: str, fetch_text, timeout_s: int, retries: int, backoff_s: float) -> str:
            fetch_calls.append(time.time())
            return "{}"

        with patch('src.jobs.adapters.plugins.social.register.fetch_with_retries', mock_fetch_with_retries):
            _run_reddit(
                fetch_text=mock_fetch,
                timeout_s=10,
                retries=2,
                backoff_s=1.0
            )
        
        # One JSON attempt per subreddit when RSS fallback is disabled.
        assert len(fetch_calls) == 2
        
        # Verify there was a delay between calls
        if len(fetch_calls) > 1:
            delay = fetch_calls[1] - fetch_calls[0]
            assert delay >= 0.01  # Should respect rate limit delay
            
    finally:
        # Restore original config
        register_module._SOCIAL_CONFIG = original_social_config


def test_reddit_adapter_reports_reject_reason_counts():
    """Rejected posts should expose reason counts in diagnostics for later auditing."""
    social_config = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": False,
            "htmlFallback": False,
            "rateLimitDelay": 0,
        }
    }
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

    import src.jobs.adapters.plugins.social.register as register_module
    original_social_config = register_module._SOCIAL_CONFIG
    register_module._SOCIAL_CONFIG = social_config

    try:
        with patch("src.jobs.adapters.plugins.social.register.fetch_with_retries", return_value=json.dumps(payload)):
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
    finally:
        register_module._SOCIAL_CONFIG = original_social_config
