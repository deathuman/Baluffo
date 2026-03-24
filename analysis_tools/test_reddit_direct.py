#!/usr/bin/env python3
"""
Direct Reddit Adapter Test
Tests the Reddit adapter functionality directly without the full pipeline.
"""

import json
import os
import sys
import time

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_reddit_adapter_direct():
    """Test Reddit adapter with mock data."""
    print("Testing Reddit Adapter Directly...")
    print("=" * 50)
    
    try:
        # Import required modules
        from src.jobs.adapters.plugins.social.register import _run_reddit, ensure_registered
        from src.jobs.common.config import SOURCE_DIAGNOSTICS
        from src.jobs.registry import load_social_config
        
        # Load social config
        config = load_social_config(enabled=True)
        reddit_config = config.get("reddit") or {}
        subreddits = reddit_config.get("subreddits") or []
        
        print(f"✓ Configuration loaded: {len(subreddits)} subreddits")
        print(f"  Subreddits: {', '.join(subreddits)}")
        
        # Register the adapter
        ensure_registered(social_config=config)
        print("✓ Reddit adapter registered")
        
        # Mock fetch function that returns Reddit JSON data
        def mock_fetch_text(url, timeout):
            print(f"  Fetching: {url}")
            
            # Mock Reddit JSON response
            if "new.json" in url:
                mock_data = {
                    "data": {
                        "children": [
                            {
                                "data": {
                                    "id": f"test_{int(time.time())}",
                                    "title": "We're hiring a Game Developer at Indie Studio",
                                    "selftext": "Apply at https://indiestudio.com/jobs or email careers@indiestudio.com",
                                    "link_flair_text": "Hiring",
                                    "permalink": "/r/gamedev/comments/test123/job/",
                                    "url": "https://www.reddit.com/r/gamedev/comments/test123/job/",
                                    "created_utc": time.time(),
                                    "author": "indie_hr",
                                }
                            }
                        ]
                    }
                }
                return json.dumps(mock_data)
            
            # Mock RSS response
            elif "new.rss" in url:
                rss_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
<title>reddit.com: new posts in {url.split('/')[-2]}</title>
<description>new posts in {url.split('/')[-2]}</description>
<item>
<title>We're hiring a Game Developer</title>
<link>https://www.reddit.com/r/gamedev/comments/test123/job/</link>
<description>Apply at https://indiestudio.com/jobs</description>
<pubDate>{time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime())}</pubDate>
</item>
</channel>
</rss>"""
                return rss_content
            
            return ""
        
        # Test with just one subreddit to speed up testing
        test_config = {
            "enabled": True,
            "reddit": {
                "enabled": True,
                "subreddits": ["gamedev"],  # Just test one
                "maxPostsPerSubreddit": 1,
                "rssFallback": True,
                "htmlFallback": False,
                "rateLimitDelay": 0.1,  # Fast for testing
            }
        }
        
        # Override the social config temporarily
        import src.jobs.adapters.plugins.social.register as register_module
        original_social_config = register_module._SOCIAL_CONFIG
        register_module._SOCIAL_CONFIG = test_config
        
        try:
            print("\nRunning Reddit adapter...")
            jobs = _run_reddit(
                fetch_text=mock_fetch_text,
                timeout_s=10,
                retries=2,
                backoff_s=1.0
            )
            
            print("✓ Reddit adapter completed")
            print(f"  Jobs found: {len(jobs)}")
            
            # Print job details
            if jobs:
                job = jobs[0]
                print(f"  Sample job title: {job.get('title', 'N/A')}")
                print(f"  Sample job company: {job.get('company', 'N/A')}")
                print(f"  Sample job link: {job.get('jobLink', 'N/A')}")
            
            # Check diagnostics
            if "social_reddit" in SOURCE_DIAGNOSTICS:
                details = SOURCE_DIAGNOSTICS["social_reddit"]["details"]
                print(f"  Diagnostic entries: {len(details)}")
                
                for detail in details:
                    print(f"    Source: {detail.get('name')} - Status: {detail.get('status')}")
                    if detail.get('error'):
                        print(f"      Error: {detail.get('error')}")
            
        finally:
            # Restore original config
            register_module._SOCIAL_CONFIG = original_social_config
        
        print("\n🎉 Reddit adapter test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_reddit_adapter_direct()
    sys.exit(0 if success else 1)