#!/usr/bin/env python3
"""
Social Sources Integration Test
Tests that the social sources are properly integrated and can be enabled in the pipeline.
"""

import sys
import os
import json
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_social_sources_integration():
    """Test that social sources are properly integrated."""
    print("Testing Social Sources Integration...")
    print("=" * 50)
    
    try:
        # Test 1: Check social config file exists
        print("Test 1: Checking social sources configuration...")
        config_path = Path("data/social-sources-config.json")
        if not config_path.exists():
            print(f"❌ Social config file not found: {config_path}")
            return False
        
        with open(config_path) as f:
            config = json.load(f)
        
        assert config.get("enabled") == True, "Social sources should be enabled"
        assert "reddit" in config, "Reddit configuration missing"
        assert "x" in config, "X configuration missing"
        assert "mastodon" in config, "Mastodon configuration missing"
        
        # Check Reddit subreddits
        reddit_subreddits = config["reddit"].get("subreddits", [])
        expected_subreddits = ["gamedev", "gameDevClassifieds", "gamedevjobs", "INAT", "gamejobs", "indiegaming"]
        assert len(reddit_subreddits) == 6, f"Expected 6 subreddits, got {len(reddit_subreddits)}"
        assert reddit_subreddits == expected_subreddits, f"Subreddits don't match expected: {expected_subreddits}"
        
        print("✓ Social sources configuration is correct")
        
        # Test 2: Check pipeline can load social config
        print("Test 2: Testing pipeline social config loading...")
        from src.jobs.registry import load_social_config
        
        loaded_config = load_social_config(enabled=True)
        assert loaded_config.get("enabled") == True, "Loaded config should be enabled"
        assert len(loaded_config.get("reddit", {}).get("subreddits", [])) == 6, "Should load 6 subreddits"
        
        print("✓ Pipeline can load social configuration")
        
        # Test 3: Check social adapters are registered
        print("Test 3: Testing social adapter registration...")
        from src.jobs.adapters.plugins.social.register import ensure_registered
        
        ensure_registered(social_config=loaded_config)
        print("✓ Social adapters registered successfully")
        
        # Test 4: Check pipeline arguments work
        print("Test 4: Testing pipeline argument parsing...")
        from src.jobs.pipeline import parse_args
        from src.jobs.common.config import DEFAULT_SOCIAL_CONFIG_PATH
        
        # Test with social enabled
        import sys
        original_argv = sys.argv
        try:
            sys.argv = ["test", "--social-enabled", "--output-dir", "test_output"]
            args = parse_args()
            assert args.social_enabled == True, "Social should be enabled"
            print(f"  Default config path from code: {DEFAULT_SOCIAL_CONFIG_PATH}")
            print(f"  Parsed config path: {args.social_config_path}")
            # Check if the paths are equivalent
            assert str(DEFAULT_SOCIAL_CONFIG_PATH) in str(args.social_config_path), "Config path should match default"
        finally:
            sys.argv = original_argv
        
        print("✓ Pipeline argument parsing works correctly")
        
        # Test 5: Check source loaders include social sources
        print("Test 5: Testing social source loaders...")
        from src.jobs.adapters import default_source_loaders
        
        # Mock the social config for this test
        import src.jobs.adapters.plugins.social.register as register_module
        original_social_config = register_module._SOCIAL_CONFIG
        register_module._SOCIAL_CONFIG = loaded_config
        
        try:
            loaders = default_source_loaders(social_enabled=True, social_config=loaded_config)
            loader_names = [name for name, _ in loaders]
            
            assert "social_reddit" in loader_names, "social_reddit loader should be available"
            assert "social_x" in loader_names, "social_x loader should be available"
            assert "social_mastodon" in loader_names, "social_mastodon loader should be available"
            
            print("✓ Social source loaders are available")
            
        finally:
            # Restore original config
            register_module._SOCIAL_CONFIG = original_social_config
        
        print("\n🎉 All social sources integration tests passed!")
        print("📊 Social sources are ready for pipeline integration:")
        print(f"  ✅ Reddit: {len(loaded_config.get('reddit', {}).get('subreddits', []))} subreddits")
        print(f"  ✅ X (Twitter): {len(loaded_config.get('x', {}).get('queries', []))} queries")
        print(f"  ✅ Mastodon: {len(loaded_config.get('mastodon', {}).get('instances', []))} instances")
        print("🔧 Pipeline ready to run with --social-enabled flag")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_social_sources_integration()
    sys.exit(0 if success else 1)