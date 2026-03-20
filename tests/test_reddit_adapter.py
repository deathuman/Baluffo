"""Test Reddit adapter functionality and error handling."""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

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
    """Test that Reddit configuration includes all expected subreddits."""
    from src.jobs.registry import load_social_config
    
    config = load_social_config(enabled=True)
    reddit_config = config.get("reddit") or {}
    subreddits = reddit_config.get("subreddits") or []
    
    # Verify we have all 6 subreddits
    assert len(subreddits) == 6
    expected_subreddits = ["gamedev", "gameDevClassifieds", "gamedevjobs", "INAT", "gamejobs", "indiegaming"]
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
        jobs = _run_reddit(
            fetch_text=failing_fetch,
            timeout_s=10,
            retries=2,
            backoff_s=1.0
        )
        
        # Should return empty list when all sources fail
        assert len(jobs) == 0
        
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
            "rssFallback": True,
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
        start_time = time.time()
        with patch('src.jobs.adapters.plugins.social.register.fetch_with_retries', mock_fetch):
            _run_reddit(
                fetch_text=mock_fetch,
                timeout_s=10,
                retries=2,
                backoff_s=1.0
            )
        
        # Should have made 2 fetch calls (one per subreddit)
        assert len(fetch_calls) == 2
        
        # Verify there was a delay between calls
        if len(fetch_calls) > 1:
            delay = fetch_calls[1] - fetch_calls[0]
            assert delay >= 0.01  # Should respect rate limit delay
            
    finally:
        # Restore original config
        register_module._SOCIAL_CONFIG = original_social_config