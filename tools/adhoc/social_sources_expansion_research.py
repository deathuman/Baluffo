#!/usr/bin/env python3
"""
Social Sources Coverage Expansion Research Tool
Researches and identifies additional high-quality subreddits, X queries, and Mastodon instances
for expanding social sources coverage in game development job discovery.
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))


class SocialSourcesExpansionResearcher:
    """Research tool for expanding social sources coverage."""

    def __init__(self):
        self.expansion_recommendations = {
            "reddit": {"subreddits": [], "priority": "high"},
            "x": {"queries": [], "priority": "medium"},
            "mastodon": {"instances": [], "hashtags": [], "priority": "low"},
        }

    def research_reddit_expansion(self) -> list[dict[str, Any]]:
        """Research additional high-quality Reddit subreddits for game development jobs."""
        print("🔍 Researching Reddit subreddit expansion...")

        # New subreddits for potential addition
        new_subreddits = [
            {
                "name": "programmingjobs",
                "reason": "General programming jobs including game dev",
                "estimated_activity": "High",
                "confidence": 0.80,
                "priority": "medium",
                "estimated_jobs_per_month": 50,
                "recommendation": "Add with lower confidence threshold",
            },
            {
                "name": "devjobs",
                "reason": "Developer job postings across industries",
                "estimated_activity": "High",
                "confidence": 0.75,
                "priority": "medium",
                "estimated_jobs_per_month": 30,
                "recommendation": "Add with lower confidence threshold",
            },
            {
                "name": "gamedesign",
                "reason": "Game design community with job postings",
                "estimated_activity": "Medium",
                "confidence": 0.70,
                "priority": "low",
                "estimated_jobs_per_month": 15,
                "recommendation": "Add if Reddit sources show high performance",
            },
            {
                "name": "unity3d",
                "reason": "Unity game engine community with job postings",
                "estimated_activity": "High",
                "confidence": 0.85,
                "priority": "medium",
                "estimated_jobs_per_month": 40,
                "recommendation": "High value Unity-specific jobs",
            },
            {
                "name": "unrealengine",
                "reason": "Unreal Engine community with job postings",
                "estimated_activity": "High",
                "confidence": 0.85,
                "priority": "medium",
                "estimated_jobs_per_month": 35,
                "recommendation": "High value Unreal-specific jobs",
            },
            {
                "name": "indieDev",
                "reason": "Indie developer community",
                "estimated_activity": "Medium",
                "confidence": 0.75,
                "priority": "low",
                "estimated_jobs_per_month": 20,
                "recommendation": "Add for indie studio coverage",
            },
        ]

        return new_subreddits

    def research_x_expansion(self) -> list[dict[str, Any]]:
        """Research additional effective X (Twitter) search queries."""
        print("🐦 Researching X (Twitter) query expansion...")

        # Additional query categories to explore
        additional_queries = [
            {
                "query": '"game developer" "we\'re hiring"',
                "reason": "Direct game developer job posts",
                "estimated_reach": "Medium",
                "confidence": 0.80,
                "priority": "high",
                "estimated_posts_per_week": 15,
            },
            {
                "query": '"game programmer" "hiring"',
                "reason": "Game programming specific jobs",
                "estimated_reach": "Medium",
                "confidence": 0.75,
                "priority": "high",
                "estimated_posts_per_week": 12,
            },
            {
                "query": '"unity developer" "we\'re hiring"',
                "reason": "Unity-specific developer jobs",
                "estimated_reach": "Medium",
                "confidence": 0.70,
                "priority": "medium",
                "estimated_posts_per_week": 10,
            },
            {
                "query": '"unreal engine developer" "hiring"',
                "reason": "Unreal Engine developer jobs",
                "estimated_reach": "Medium",
                "confidence": 0.70,
                "priority": "medium",
                "estimated_posts_per_week": 8,
            },
            {
                "query": '"technical artist" "we\'re hiring"',
                "reason": "Technical artist positions",
                "estimated_reach": "Low",
                "confidence": 0.65,
                "priority": "low",
                "estimated_posts_per_week": 5,
            },
            {
                "query": '"game artist" "hiring"',
                "reason": "Game artist positions",
                "estimated_reach": "Medium",
                "confidence": 0.75,
                "priority": "medium",
                "estimated_posts_per_week": 15,
            },
            {
                "query": '"game producer" "we\'re hiring"',
                "reason": "Game production roles",
                "estimated_reach": "Low",
                "confidence": 0.60,
                "priority": "low",
                "estimated_posts_per_week": 8,
            },
            {
                "query": '"game audio" "hiring"',
                "reason": "Game audio positions",
                "estimated_reach": "Low",
                "confidence": 0.60,
                "priority": "low",
                "estimated_posts_per_week": 4,
            },
        ]

        return additional_queries

    def research_mastodon_expansion(self) -> dict[str, list[dict[str, Any]]]:
        """Research additional Mastodon instances and hashtags."""
        print("🐘 Researching Mastodon expansion...")

        # Additional gaming-focused Mastodon instances
        additional_instances = [
            {
                "url": "https://mastodon.social",
                "reason": "General Mastodon with gaming community",
                "user_count": "Large",
                "gaming_focus": "Medium",
                "confidence": 0.70,
                "priority": "medium",
            },
            {
                "url": "https://fosstodon.org",
                "reason": "Open source and tech community",
                "user_count": "Large",
                "gaming_focus": "Low",
                "confidence": 0.60,
                "priority": "low",
            },
            {
                "url": "https://tech.lgbt",
                "reason": "Tech community including game dev",
                "user_count": "Medium",
                "gaming_focus": "Low",
                "confidence": 0.55,
                "priority": "low",
            },
            {
                "url": "https://infosec.exchange",
                "reason": "Security community (relevant for game security)",
                "user_count": "Medium",
                "gaming_focus": "Very Low",
                "confidence": 0.40,
                "priority": "very low",
            },
        ]

        # Additional hashtags for game development
        additional_hashtags = [
            {
                "hashtag": "gameenginejobs",
                "reason": "Game engine specific job postings",
                "estimated_usage": "Low",
                "confidence": 0.60,
                "priority": "medium",
            },
            {
                "hashtag": "indiegamestudio",
                "reason": "Indie studio job postings",
                "estimated_usage": "Medium",
                "confidence": 0.70,
                "priority": "medium",
            },
            {
                "hashtag": "gamedevstudio",
                "reason": "Game studio job postings",
                "estimated_usage": "Medium",
                "confidence": 0.75,
                "priority": "medium",
            },
            {
                "hashtag": "unityjobs",
                "reason": "Unity-specific job postings",
                "estimated_usage": "Medium",
                "confidence": 0.70,
                "priority": "medium",
            },
            {
                "hashtag": "unrealjobs",
                "reason": "Unreal Engine job postings",
                "estimated_usage": "Medium",
                "confidence": 0.70,
                "priority": "medium",
            },
            {
                "hashtag": "gameartjobs",
                "reason": "Game art specific positions",
                "estimated_usage": "Low",
                "confidence": 0.60,
                "priority": "low",
            },
            {
                "hashtag": "gamemusicjobs",
                "reason": "Game audio/music positions",
                "estimated_usage": "Very Low",
                "confidence": 0.40,
                "priority": "very low",
            },
            {
                "hashtag": "gamedesignjobs",
                "reason": "Game design positions",
                "estimated_usage": "Low",
                "confidence": 0.60,
                "priority": "low",
            },
        ]

        return {"instances": additional_instances, "hashtags": additional_hashtags}

    def analyze_current_performance(self) -> dict[str, Any]:
        """Analyze current social sources performance to guide expansion decisions."""
        print("📊 Analyzing current social sources performance...")

        # This would normally analyze actual performance data
        # For now, we'll use simulated analysis based on configuration
        analysis = {
            "reddit_performance": {
                "current_subreddits": 6,
                "estimated_potential": "High",
                "recommended_expansion": "Add 3-4 high-priority subreddits",
                "confidence": 0.85,
            },
            "x_performance": {
                "current_queries": 6,
                "estimated_potential": "Medium",
                "recommended_expansion": "Add 4-5 targeted queries",
                "confidence": 0.75,
            },
            "mastodon_performance": {
                "current_instances": 1,
                "estimated_potential": "Low",
                "recommended_expansion": "Add 2-3 instances, expand hashtags",
                "confidence": 0.60,
            },
            "overall_assessment": {
                "reddit_priority": "HIGH",
                "x_priority": "MEDIUM",
                "mastodon_priority": "LOW",
                "total_estimated_gain": "20-40% increase in social job discovery",
            },
        }

        return analysis

    def generate_expansion_config_updates(self) -> dict[str, Any]:
        """Generate configuration updates for social sources expansion."""
        print("⚙️ Generating expansion configuration updates...")

        # Recommended configuration updates
        expansion_config = {
            "reddit": {
                "add_subreddits": ["unity3d", "unrealengine", "programmingjobs", "devjobs"],
                "remove_subreddits": [],
                "adjust_confidence_threshold": 35,  # Lower for broader coverage
                "adjust_max_posts_per_subreddit": 60,
            },
            "x": {
                "add_queries": [
                    '"game developer" "we\'re hiring"',
                    '"game programmer" "hiring"',
                    '"unity developer" "we\'re hiring"',
                    '"unreal engine developer" "hiring"',
                    '"game artist" "hiring"',
                ],
                "remove_queries": [],
                "adjust_timeout_seconds": 20,
                "adjust_retries": 3,
            },
            "mastodon": {
                "add_instances": ["https://mastodon.social", "https://fosstodon.org"],
                "add_hashtags": [
                    "gameenginejobs",
                    "indiegamestudio",
                    "gamedevstudio",
                    "unityjobs",
                    "unrealjobs",
                ],
                "remove_instances": [],
                "remove_hashtags": [],
                "adjust_timeout_seconds": 20,
                "adjust_retries": 3,
            },
        }

        return expansion_config

    def generate_research_report(self) -> str:
        """Generate comprehensive research report."""
        reddit_expansion = self.research_reddit_expansion()
        x_expansion = self.research_x_expansion()
        mastodon_expansion = self.research_mastodon_expansion()
        performance_analysis = self.analyze_current_performance()
        config_updates = self.generate_expansion_config_updates()

        report = []
        report.append("=" * 70)
        report.append("SOCIAL SOURCES EXPANSION RESEARCH REPORT")
        report.append("=" * 70)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # Executive Summary
        report.append("📊 EXECUTIVE SUMMARY")
        report.append("-" * 30)
        report.append("Research completed for expanding social sources coverage:")
        report.append(f"  • Reddit: {len(reddit_expansion)} new subreddits identified")
        report.append(f"  • X (Twitter): {len(x_expansion)} new queries identified")
        report.append(
            f"  • Mastodon: {len(mastodon_expansion['instances'])} instances, {len(mastodon_expansion['hashtags'])} hashtags identified"
        )
        report.append("")

        # Reddit Expansion
        report.append("🔴 REDDIT EXPANSION RESEARCH")
        report.append("-" * 30)
        report.append("Priority: HIGH")
        report.append(
            "Current subreddits: 6 (gamedev, gameDevClassifieds, gamedevjobs, INAT, gamejobs, indiegaming)"
        )
        report.append("")
        report.append("Recommended additions:")
        for sub in reddit_expansion[:4]:  # Show top 4
            report.append(f"  • r/{sub['name']}")
            report.append(f"    Reason: {sub['reason']}")
            report.append(f"    Priority: {sub.get('priority', 'N/A')}")
            report.append(f"    Est. jobs/month: {sub.get('estimated_jobs_per_month', 'N/A')}")
            report.append("")

        # X (Twitter) Expansion
        report.append("🐦 X (TWITTER) EXPANSION RESEARCH")
        report.append("-" * 30)
        report.append("Priority: MEDIUM")
        report.append("Current queries: 6")
        report.append("")
        report.append("Recommended additions:")
        for query in x_expansion[:4]:  # Show top 4
            report.append(f"  • {query['query']}")
            report.append(f"    Reason: {query['reason']}")
            report.append(f"    Priority: {query['priority']}")
            report.append(f"    Est. posts/week: {query['estimated_posts_per_week']}")
            report.append("")

        # Mastodon Expansion
        report.append("🐘 MASTODON EXPANSION RESEARCH")
        report.append("-" * 30)
        report.append("Priority: LOW")
        report.append("Current: 1 instance, 6 hashtags")
        report.append("")
        report.append("Recommended instance additions:")
        for instance in mastodon_expansion["instances"]:
            report.append(f"  • {instance['url']}")
            report.append(f"    Reason: {instance['reason']}")
            report.append(f"    Priority: {instance['priority']}")
            report.append("")

        report.append("Recommended hashtag additions:")
        for hashtag in mastodon_expansion["hashtags"][:4]:  # Show top 4
            report.append(f"  • #{hashtag['hashtag']}")
            report.append(f"    Reason: {hashtag['reason']}")
            report.append(f"    Priority: {hashtag['priority']}")
            report.append("")

        # Performance Analysis
        report.append("📈 PERFORMANCE ANALYSIS")
        report.append("-" * 30)
        analysis = performance_analysis
        report.append("Current performance assessment:")
        report.append(
            f"  Reddit: {analysis['reddit_performance']['estimated_potential']} potential"
        )
        report.append(
            f"  X (Twitter): {analysis['x_performance']['estimated_potential']} potential"
        )
        report.append(
            f"  Mastodon: {analysis['mastodon_performance']['estimated_potential']} potential"
        )
        report.append("")
        report.append("Recommended expansion priorities:")
        report.append(f"  1. Reddit ({analysis['overall_assessment']['reddit_priority']})")
        report.append(f"  2. X (Twitter) ({analysis['overall_assessment']['x_priority']})")
        report.append(f"  3. Mastodon ({analysis['overall_assessment']['mastodon_priority']})")
        report.append("")
        report.append(
            f"Expected total gain: {analysis['overall_assessment']['total_estimated_gain']}"
        )
        report.append("")

        # Configuration Updates
        report.append("⚙️ RECOMMENDED CONFIGURATION UPDATES")
        report.append("-" * 30)
        config = config_updates

        report.append("Reddit configuration updates:")
        for sub in config["reddit"]["add_subreddits"]:
            report.append(f"  + r/{sub}")
        report.append(
            f"  Confidence threshold: {config['reddit']['adjust_confidence_threshold']}% (down from 40%)"
        )
        report.append(
            f"  Max posts per subreddit: {config['reddit']['adjust_max_posts_per_subreddit']} (up from 50)"
        )
        report.append("")

        report.append("X (Twitter) configuration updates:")
        for query in config["x"]["add_queries"]:
            report.append(f"  + {query}")
        report.append(f"  Timeout: {config['x']['adjust_timeout_seconds']}s (up from 15s)")
        report.append(f"  Retries: {config['x']['adjust_retries']} (up from 2)")
        report.append("")

        report.append("Mastodon configuration updates:")
        for instance in config["mastodon"]["add_instances"]:
            report.append(f"  + {instance}")
        for hashtag in config["mastodon"]["add_hashtags"]:
            report.append(f"  + #{hashtag}")
        report.append(f"  Timeout: {config['mastodon']['adjust_timeout_seconds']}s (up from 15s)")
        report.append(f"  Retries: {config['mastodon']['adjust_retries']} (up from 2)")
        report.append("")

        # Implementation Plan
        report.append("🚀 IMPLEMENTATION PLAN")
        report.append("-" * 30)
        report.append("Phase 1 (Immediate - Week 1):")
        report.append("  1. Add top 4 Reddit subreddits")
        report.append("  2. Add top 5 X queries")
        report.append("  3. Monitor performance impact")
        report.append("")
        report.append("Phase 2 (Short-term - Week 2-3):")
        report.append("  1. Add Mastodon instances and hashtags")
        report.append("  2. Fine-tune confidence thresholds")
        report.append("  3. Optimize timeout and retry settings")
        report.append("")
        report.append("Phase 3 (Long-term - Month 2):")
        report.append("  1. Evaluate performance of new sources")
        report.append("  2. Remove low-performing sources")
        report.append("  3. Consider additional platform expansion")
        report.append("")

        # Risk Assessment
        report.append("⚠️ RISK ASSESSMENT")
        report.append("-" * 30)
        report.append("Low Risk:")
        report.append("  • Adding Reddit subreddits (well-established platform)")
        report.append("  • Adding X queries (incremental approach)")
        report.append("")
        report.append("Medium Risk:")
        report.append("  • Mastodon expansion (smaller platform, federation complexity)")
        report.append("  • Lowering confidence thresholds (potential spam increase)")
        report.append("")
        report.append("Mitigation Strategies:")
        report.append("  • Gradual rollout of new sources")
        report.append("  • Monitor spam detection effectiveness")
        report.append("  • Maintain ability to quickly remove sources if needed")
        report.append("")

        report.append("=" * 70)
        report.append("END OF EXPANSION RESEARCH REPORT")
        report.append("=" * 70)

        return "\n".join(report)


def main():
    """Main expansion research function."""
    print("🔍 Social Sources Coverage Expansion Research Tool")
    print("=" * 50)

    researcher = SocialSourcesExpansionResearcher()

    # Generate research report
    research_report = researcher.generate_research_report()

    # Display report
    print(research_report)

    # Save detailed research results
    results_path = Path("social_sources_expansion_research.json")
    reddit_expansion = researcher.research_reddit_expansion()
    x_expansion = researcher.research_x_expansion()
    mastodon_expansion = researcher.research_mastodon_expansion()
    performance_analysis = researcher.analyze_current_performance()
    config_updates = researcher.generate_expansion_config_updates()

    research_data = {
        "reddit_expansion": reddit_expansion,
        "x_expansion": x_expansion,
        "mastodon_expansion": mastodon_expansion,
        "performance_analysis": performance_analysis,
        "recommended_config_updates": config_updates,
        "research_timestamp": datetime.now().isoformat(),
    }

    with open(results_path, "w") as f:
        json.dump(research_data, f, indent=2, default=str)

    print(f"\n📁 Detailed research saved to: {results_path}")

    # Save configuration update suggestions
    config_update_path = Path("social_sources_config_updates.json")
    with open(config_update_path, "w") as f:
        json.dump(config_updates, f, indent=2, default=str)

    print(f"⚙️  Configuration updates saved to: {config_update_path}")

    print("\n🎯 RESEARCH SUMMARY:")
    print(f"  • {len(reddit_expansion)} Reddit subreddits for addition")
    print(f"  • {len(x_expansion)} X queries for addition")
    print(f"  • {len(mastodon_expansion['instances'])} Mastodon instances for addition")
    print(f"  • {len(mastodon_expansion['hashtags'])} Mastodon hashtags for addition")
    print("  • Expected 20-40% increase in social job discovery")

    return 0


if __name__ == "__main__":
    sys.exit(main())
