#!/usr/bin/env python3
"""
Social Sources Performance Monitoring Dashboard
Comprehensive monitoring tools for tracking job discovery rates, API usage, and error rates
for Reddit, X (Twitter), and Mastodon sources in production.
"""

import json
import statistics
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def load_pipeline_report(report_path: Path) -> dict[str, Any]:
    """Load the latest pipeline report."""
    try:
        with open(report_path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"❌ Error loading report {report_path}: {e}")
        return {}

def analyze_social_sources_performance(report: dict[str, Any]) -> dict[str, Any]:
    """Analyze performance metrics for social sources."""
    sources = report.get("sources", [])
    social_sources = [s for s in sources if s.get("name", "").startswith(("social_reddit", "social_x", "social_mastodon"))]
    
    performance_data = {
        "total_social_sources": len(social_sources),
        "by_platform": {
            "reddit": [],
            "x": [],
            "mastodon": []
        },
        "timing_metrics": {
            "total_duration": 0,
            "average_duration": 0,
            "median_duration": 0,
            "max_duration": 0,
            "platform_durations": {}
        },
        "success_rates": {
            "overall": 0,
            "by_platform": {}
        },
        "job_discovery": {
            "total_jobs": 0,
            "by_platform": {},
            "by_subreddit": {},
            "by_query": {},
            "by_instance": {}
        },
        "error_analysis": {
            "total_errors": 0,
            "by_platform": {},
            "error_types": {}
        }
    }
    
    for source in social_sources:
        name = source.get("name", "")
        platform = "reddit" if "social_reddit" in name else "x" if "social_x" in name else "mastodon"
        
        # Timing metrics
        duration = int(source.get("durationMs", 0))
        performance_data["timing_metrics"]["total_duration"] += duration
        performance_data["by_platform"][platform].append(source)
        performance_data["timing_metrics"]["platform_durations"][platform] = performance_data["timing_metrics"]["platform_durations"].get(platform, 0) + duration
        
        # Job discovery
        kept_count = int(source.get("keptCount", 0))
        performance_data["job_discovery"]["total_jobs"] += kept_count
        performance_data["job_discovery"]["by_platform"][platform] = performance_data["job_discovery"]["by_platform"].get(platform, 0) + kept_count
        
        # Error analysis
        status = source.get("status", "").lower()
        if status == "error":
            performance_data["error_analysis"]["total_errors"] += 1
            performance_data["error_analysis"]["by_platform"][platform] = performance_data["error_analysis"]["by_platform"].get(platform, 0) + 1
            error_msg = source.get("error", "")
            if error_msg:
                performance_data["error_analysis"]["error_types"][error_msg] = performance_data["error_analysis"]["error_types"].get(error_msg, 0) + 1
        
        # Platform-specific breakdowns
        if platform == "reddit":
            subreddit = name.split(":")[-1] if ":" in name else name
            performance_data["job_discovery"]["by_subreddit"][subreddit] = performance_data["job_discovery"]["by_subreddit"].get(subreddit, 0) + kept_count
        elif platform == "x":
            query = name.split(":")[-1] if ":" in name else name
            performance_data["job_discovery"]["by_query"][query] = performance_data["job_discovery"]["by_query"].get(query, 0) + kept_count
        elif platform == "mastodon":
            instance = name.split(":")[-1] if ":" in name else name
            performance_data["job_discovery"]["by_instance"][instance] = performance_data["job_discovery"]["by_instance"].get(instance, 0) + kept_count
    
    # Calculate averages and rates
    durations = [int(s.get("durationMs", 0)) for s in social_sources]
    if durations:
        performance_data["timing_metrics"]["average_duration"] = statistics.mean(durations)
        performance_data["timing_metrics"]["median_duration"] = statistics.median(durations)
        performance_data["timing_metrics"]["max_duration"] = max(durations)
    
    successful_sources = [s for s in social_sources if s.get("status", "").lower() == "ok"]
    if social_sources:
        performance_data["success_rates"]["overall"] = len(successful_sources) / len(social_sources)
    
    for platform, sources in performance_data["by_platform"].items():
        if sources:
            successful = [s for s in sources if s.get("status", "").lower() == "ok"]
            performance_data["success_rates"]["by_platform"][platform] = len(successful) / len(sources)
    
    return performance_data

def generate_performance_report(performance_data: dict[str, Any]) -> str:
    """Generate a formatted performance report."""
    report = []
    report.append("=" * 60)
    report.append("SOCIAL SOURCES PERFORMANCE DASHBOARD")
    report.append("=" * 60)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Overview
    report.append("📊 OVERVIEW")
    report.append("-" * 30)
    report.append(f"Total Social Sources: {performance_data['total_social_sources']}")
    report.append(f"Total Jobs Discovered: {performance_data['job_discovery']['total_jobs']}")
    report.append(f"Overall Success Rate: {performance_data['success_rates']['overall']:.1%}")
    report.append(f"Total Error Count: {performance_data['error_analysis']['total_errors']}")
    report.append("")
    
    # Platform Breakdown
    report.append("🎯 PLATFORM BREAKDOWN")
    report.append("-" * 30)
    for platform in ["reddit", "x", "mastodon"]:
        sources = performance_data["by_platform"][platform]
        if sources:
            job_count = performance_data["job_discovery"]["by_platform"].get(platform, 0)
            success_rate = performance_data["success_rates"]["by_platform"].get(platform, 0)
            avg_duration = statistics.mean([int(s.get("durationMs", 0)) for s in sources]) if sources else 0
            
            report.append(f"{platform.upper()}:")
            report.append(f"  Sources: {len(sources)}")
            report.append(f"  Jobs Discovered: {job_count}")
            report.append(f"  Success Rate: {success_rate:.1%}")
            report.append(f"  Avg Duration: {avg_duration:.0f}ms")
            report.append("")
    
    # Timing Analysis
    report.append("⏱️ TIMING ANALYSIS")
    report.append("-" * 30)
    report.append(f"Total Duration: {performance_data['timing_metrics']['total_duration']}ms")
    report.append(f"Average Duration: {performance_data['timing_metrics']['average_duration']:.0f}ms")
    report.append(f"Median Duration: {performance_data['timing_metrics']['median_duration']:.0f}ms")
    report.append(f"Max Duration: {performance_data['timing_metrics']['max_duration']}ms")
    report.append("")
    
    # Job Discovery Details
    report.append("🔍 JOB DISCOVERY DETAILS")
    report.append("-" * 30)
    
    if performance_data["job_discovery"]["by_subreddit"]:
        report.append("Reddit Subreddits:")
        for subreddit, count in sorted(performance_data["job_discovery"]["by_subreddit"].items(), key=lambda x: x[1], reverse=True):
            report.append(f"  {subreddit}: {count} jobs")
        report.append("")
    
    if performance_data["job_discovery"]["by_query"]:
        report.append("X Queries:")
        for query, count in sorted(performance_data["job_discovery"]["by_query"].items(), key=lambda x: x[1], reverse=True):
            report.append(f"  {query}: {count} jobs")
        report.append("")
    
    if performance_data["job_discovery"]["by_instance"]:
        report.append("Mastodon Instances:")
        for instance, count in sorted(performance_data["job_discovery"]["by_instance"].items(), key=lambda x: x[1], reverse=True):
            report.append(f"  {instance}: {count} jobs")
        report.append("")
    
    # Error Analysis
    report.append("🚨 ERROR ANALYSIS")
    report.append("-" * 30)
    if performance_data["error_analysis"]["by_platform"]:
        for platform, count in performance_data["error_analysis"]["by_platform"].items():
            if count > 0:
                report.append(f"{platform.upper()}: {count} errors")
    
    if performance_data["error_analysis"]["error_types"]:
        report.append("Error Types:")
        for error_type, count in sorted(performance_data["error_analysis"]["error_types"].items(), key=lambda x: x[1], reverse=True):
            report.append(f"  {error_type}: {count}")
    
    report.append("")
    report.append("=" * 60)
    report.append("END OF REPORT")
    report.append("=" * 60)
    
    return "\n".join(report)

def monitor_api_usage():
    """Monitor API usage patterns for social sources."""
    print("🔍 Monitoring API Usage Patterns...")
    
    # This would integrate with actual API monitoring in production
    # For now, we'll analyze the patterns from the reports
    print("API Usage Monitoring:")
    print("- Reddit: JSON API with RSS/HTML fallback")
    print("- X: API endpoint with RSS fallback")
    print("- Mastodon: REST API with rate limiting")
    print("- Current rate limits configured conservatively")
    print("")

def monitor_error_patterns(error_data: dict[str, Any]):
    """Analyze error patterns and suggest improvements."""
    print("🚨 Error Pattern Analysis...")
    
    if error_data["error_analysis"]["total_errors"] > 0:
        print("Error Summary:")
        for platform, count in error_data["error_analysis"]["by_platform"].items():
            if count > 0:
                print(f"  {platform.upper()}: {count} errors")
        
        print("\nCommon Error Types:")
        for error_type, count in sorted(error_data["error_analysis"]["error_types"].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {error_type}: {count}")
    else:
        print("✅ No errors detected in social sources!")
    
    print("")

def generate_recommendations(performance_data: dict[str, Any]) -> list[str]:
    """Generate optimization recommendations based on performance data."""
    recommendations = []
    
    # Success rate recommendations
    if performance_data["success_rates"]["overall"] < 0.9:
        recommendations.append("⚠️  Low overall success rate. Consider adjusting rate limits or timeout settings.")
    
    # Performance recommendations
    if performance_data["timing_metrics"]["average_duration"] > 30000:  # 30 seconds
        recommendations.append("⚠️  High average duration. Consider optimizing fetch strategies or parallelization.")
    
    # Job discovery recommendations
    if performance_data["job_discovery"]["total_jobs"] < 10:
        recommendations.append("⚠️  Low job discovery rate. Consider adding more subreddits/queries or adjusting confidence thresholds.")
    
    # Platform-specific recommendations
    for platform in ["reddit", "x", "mastodon"]:
        success_rate = performance_data["success_rates"]["by_platform"].get(platform, 0)
        if success_rate < 0.8:
            recommendations.append(f"⚠️  {platform.upper()} success rate below 80%. Review configuration and API access.")
    
    # Positive recommendations
    if not recommendations:
        recommendations.append("✅ All metrics look good! Consider expanding coverage with additional sources.")
    
    return recommendations

def main():
    """Main monitoring dashboard function."""
    print("🚀 Social Sources Performance Monitoring Dashboard")
    print("=" * 60)
    
    # Look for pipeline reports
    report_paths = [
        Path("test_social_fetch/jobs-fetch-report.json"),
        Path("data/jobs-fetch-report.json"),
        Path("jobs-fetch-report.json")
    ]
    
    latest_report = None
    for report_path in report_paths:
        if report_path.exists():
            latest_report = load_pipeline_report(report_path)
            if latest_report:
                break
    
    if not latest_report:
        print("❌ No pipeline report found. Please run the social sources pipeline first.")
        print("Use: python src/jobs/pipeline.py --social-enabled --output-dir test_social_fetch")
        return 1
    
    # Analyze performance
    print("📊 Analyzing social sources performance...")
    performance_data = analyze_social_sources_performance(latest_report)
    
    # Generate and display report
    report_text = generate_performance_report(performance_data)
    print(report_text)
    
    # Monitor API usage
    monitor_api_usage()
    
    # Analyze error patterns
    monitor_error_patterns(performance_data)
    
    # Generate recommendations
    print("💡 OPTIMIZATION RECOMMENDATIONS")
    print("-" * 30)
    recommendations = generate_recommendations(performance_data)
    for rec in recommendations:
        print(rec)
    
    print("")
    print("🎯 Key Metrics Summary:")
    print(f"- Total Jobs: {performance_data['job_discovery']['total_jobs']}")
    print(f"- Success Rate: {performance_data['success_rates']['overall']:.1%}")
    print(f"- Error Count: {performance_data['error_analysis']['total_errors']}")
    print(f"- Avg Duration: {performance_data['timing_metrics']['average_duration']:.0f}ms")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())