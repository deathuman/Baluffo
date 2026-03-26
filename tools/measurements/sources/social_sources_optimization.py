#!/usr/bin/env python3
"""
Social Sources Configuration Optimization Tool
Fine-tunes rate limits, confidence thresholds, and performance settings based on real data
from production monitoring and analysis.
"""

import json
import statistics
import sys
from pathlib import Path
from typing import Any

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))


def load_current_config(config_path: Path) -> dict[str, Any]:
    """Load the current social sources configuration."""
    try:
        with open(config_path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"❌ Error loading config {config_path}: {e}")
        return {}


def analyze_performance_data(report_path: Path) -> dict[str, Any]:
    """Analyze performance data from pipeline reports."""
    try:
        with open(report_path) as f:
            report = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"❌ Error loading report {report_path}: {e}")
        return {}

    sources = report.get("sources", [])
    social_sources = [
        s
        for s in sources
        if s.get("name", "").startswith(("social_reddit", "social_x", "social_mastodon"))
    ]

    analysis = {
        "platform_metrics": {
            "reddit": {"durations": [], "success_rates": [], "job_counts": []},
            "x": {"durations": [], "success_rates": [], "job_counts": []},
            "mastodon": {"durations": [], "success_rates": [], "job_counts": []},
        },
        "overall_metrics": {"avg_duration": 0, "success_rate": 0, "total_jobs": 0, "error_rate": 0},
    }

    successful_sources = 0
    total_duration = 0
    total_jobs = 0

    for source in social_sources:
        name = source.get("name", "")
        platform = (
            "reddit" if "social_reddit" in name else "x" if "social_x" in name else "mastodon"
        )

        duration = int(source.get("durationMs", 0))
        kept_count = int(source.get("keptCount", 0))
        status = source.get("status", "").lower()

        # Add to platform metrics
        analysis["platform_metrics"][platform]["durations"].append(duration)
        analysis["platform_metrics"][platform]["job_counts"].append(kept_count)
        analysis["platform_metrics"][platform]["success_rates"].append(1 if status == "ok" else 0)

        # Update overall metrics
        total_duration += duration
        total_jobs += kept_count
        if status == "ok":
            successful_sources += 1

    # Calculate overall metrics
    total_sources = len(social_sources)
    if total_sources > 0:
        analysis["overall_metrics"]["avg_duration"] = total_duration / total_sources
        analysis["overall_metrics"]["success_rate"] = successful_sources / total_sources
        analysis["overall_metrics"]["total_jobs"] = total_jobs
        analysis["overall_metrics"]["error_rate"] = 1 - analysis["overall_metrics"]["success_rate"]

    # Calculate platform-specific statistics
    for platform in ["reddit", "x", "mastodon"]:
        metrics = analysis["platform_metrics"][platform]
        if metrics["durations"]:
            metrics["avg_duration"] = statistics.mean(metrics["durations"])
            metrics["median_duration"] = statistics.median(metrics["durations"])
            metrics["max_duration"] = max(metrics["durations"])
            metrics["success_rate"] = statistics.mean(metrics["success_rates"])
            metrics["avg_jobs"] = statistics.mean(metrics["job_counts"])
        else:
            metrics["avg_duration"] = 0
            metrics["median_duration"] = 0
            metrics["max_duration"] = 0
            metrics["success_rate"] = 0
            metrics["avg_jobs"] = 0

    return analysis


def generate_optimization_recommendations(
    current_config: dict[str, Any], performance_data: dict[str, Any]
) -> dict[str, Any]:
    """Generate optimization recommendations based on performance analysis."""
    recommendations = {
        "rate_limit_optimizations": [],
        "confidence_threshold_adjustments": [],
        "timeout_optimizations": [],
        "retry_optimizations": [],
        "overall_suggestions": [],
    }

    # Analyze overall performance
    overall = performance_data.get("overall_metrics", {})
    avg_duration = overall.get("avg_duration", 0)
    success_rate = overall.get("success_rate", 0)
    error_rate = overall.get("error_rate", 0)

    # Rate limit optimizations
    if success_rate < 0.8:
        recommendations["rate_limit_optimizations"].append(
            {
                "platform": "all",
                "current": "Current rate limits may be too aggressive",
                "recommended": "Increase rate limit delays by 50%",
                "reason": f"Low success rate ({success_rate:.1%}) suggests API throttling",
            }
        )
    elif avg_duration > 30000:  # 30 seconds
        recommendations["rate_limit_optimizations"].append(
            {
                "platform": "all",
                "current": "Current rate limits may be too conservative",
                "recommended": "Decrease rate limit delays by 25%",
                "reason": f"High average duration ({avg_duration / 1000:.1f}s) suggests overly conservative limits",
            }
        )

    # Platform-specific rate limit optimizations
    platform_metrics = performance_data.get("platform_metrics", {})
    for platform, metrics in platform_metrics.items():
        if not metrics.get("durations"):
            continue

        success_rate = metrics.get("success_rate", 0)
        avg_duration = metrics.get("avg_duration", 0)
        max_duration = metrics.get("max_duration", 0)

        # Reddit-specific optimizations
        if platform == "reddit":
            current_delay = current_config.get("reddit", {}).get("rateLimitDelay", 2.0)
            if success_rate < 0.8:
                recommendations["rate_limit_optimizations"].append(
                    {
                        "platform": "reddit",
                        "current": f"Rate limit delay: {current_delay}s",
                        "recommended": f"Increase to {current_delay * 1.5:.1f}s",
                        "reason": f"Reddit success rate ({success_rate:.1%}) too low",
                    }
                )
            elif avg_duration < 5000 and success_rate > 0.95:  # Less than 5 seconds
                recommendations["rate_limit_optimizations"].append(
                    {
                        "platform": "reddit",
                        "current": f"Rate limit delay: {current_delay}s",
                        "recommended": f"Decrease to {max(0.5, current_delay * 0.75):.1f}s",
                        "reason": f"Reddit is fast ({avg_duration / 1000:.1f}s avg) and reliable ({success_rate:.1%})",
                    }
                )

        # X-specific optimizations
        elif platform == "x":
            current_timeout = current_config.get("x", {}).get("timeoutSeconds", 15)
            if max_duration > current_timeout * 1000 * 0.8:  # 80% of timeout
                recommendations["timeout_optimizations"].append(
                    {
                        "platform": "x",
                        "current": f"Timeout: {current_timeout}s",
                        "recommended": f"Increase to {current_timeout + 5}s",
                        "reason": f"X queries frequently hit timeout (max: {max_duration / 1000:.1f}s)",
                    }
                )

        # Mastodon-specific optimizations
        elif platform == "mastodon":
            current_retries = current_config.get("mastodon", {}).get("retries", 2)
            if success_rate < 0.7:
                recommendations["retry_optimizations"].append(
                    {
                        "platform": "mastodon",
                        "current": f"Retries: {current_retries}",
                        "recommended": f"Increase to {current_retries + 1}",
                        "reason": f"Mastodon success rate ({success_rate:.1%}) too low",
                    }
                )

    # Confidence threshold optimizations
    total_jobs = overall.get("total_jobs", 0)
    if total_jobs < 20:  # Low job discovery
        current_min_conf = current_config.get("minConfidence", 40)
        recommendations["confidence_threshold_adjustments"].append(
            {
                "current": f"Min confidence: {current_min_conf}%",
                "recommended": f"Decrease to {max(10, current_min_conf - 10)}%",
                "reason": f"Low job discovery ({total_jobs} jobs) suggests confidence threshold too high",
            }
        )
    elif total_jobs > 100:  # High job discovery but potential spam
        current_min_conf = current_config.get("minConfidence", 40)
        recommendations["confidence_threshold_adjustments"].append(
            {
                "current": f"Min confidence: {current_min_conf}%",
                "recommended": f"Increase to {min(80, current_min_conf + 10)}%",
                "reason": f"High job discovery ({total_jobs} jobs) may include spam",
            }
        )

    # Overall suggestions
    if success_rate < 0.9:
        recommendations["overall_suggestions"].append(
            f"Overall success rate is low ({success_rate:.1%}). Focus on rate limiting and error handling."
        )
    if error_rate > 0.1:
        recommendations["overall_suggestions"].append(
            f"High error rate ({error_rate:.1%}). Review API access and network stability."
        )
    if avg_duration > 20000:  # 20 seconds
        recommendations["overall_suggestions"].append(
            f"High average duration ({avg_duration / 1000:.1f}s). Consider parallelization or timeout optimization."
        )

    return recommendations


def apply_optimizations(
    current_config: dict[str, Any], recommendations: dict[str, Any]
) -> dict[str, Any]:
    """Apply optimization recommendations to create new configuration."""
    optimized_config = current_config.copy()

    # Apply rate limit optimizations
    for opt in recommendations.get("rate_limit_optimizations", []):
        platform = opt["platform"]
        if platform == "reddit" and "reddit" in optimized_config:
            if "Increase to" in opt["recommended"]:
                new_delay = float(opt["recommended"].split("to ")[1].replace("s", ""))
                optimized_config["reddit"]["rateLimitDelay"] = new_delay
            elif "Decrease to" in opt["recommended"]:
                new_delay = float(opt["recommended"].split("to ")[1].replace("s", ""))
                optimized_config["reddit"]["rateLimitDelay"] = new_delay
        elif platform == "x" and "x" in optimized_config:
            if "Increase to" in opt["recommended"]:
                new_timeout = int(opt["recommended"].split("to ")[1].replace("s", ""))
                optimized_config["x"]["timeoutSeconds"] = new_timeout
        elif platform == "mastodon" and "mastodon" in optimized_config:
            if "Increase to" in opt["recommended"]:
                new_retries = int(opt["recommended"].split("to ")[1])
                optimized_config["mastodon"]["retries"] = new_retries

    # Apply confidence threshold adjustments
    for opt in recommendations.get("confidence_threshold_adjustments", []):
        if "Decrease to" in opt["recommended"]:
            new_confidence = int(opt["recommended"].split("to ")[1].replace("%", ""))
            optimized_config["minConfidence"] = new_confidence
        elif "Increase to" in opt["recommended"]:
            new_confidence = int(opt["recommended"].split("to ")[1].replace("%", ""))
            optimized_config["minConfidence"] = new_confidence

    return optimized_config


def generate_optimization_report(
    current_config: dict[str, Any],
    optimized_config: dict[str, Any],
    recommendations: dict[str, Any],
    performance_data: dict[str, Any],
) -> str:
    """Generate a comprehensive optimization report."""
    report = []
    report.append("=" * 60)
    report.append("SOCIAL SOURCES CONFIGURATION OPTIMIZATION REPORT")
    report.append("=" * 60)
    report.append(f"Generated: {performance_data.get('timestamp', 'N/A')}")
    report.append("")

    # Performance Summary
    overall = performance_data.get("overall_metrics", {})
    report.append("📊 PERFORMANCE SUMMARY")
    report.append("-" * 30)
    report.append(f"Average Duration: {overall.get('avg_duration', 0) / 1000:.1f}s")
    report.append(f"Success Rate: {overall.get('success_rate', 0):.1%}")
    report.append(f"Error Rate: {overall.get('error_rate', 0):.1%}")
    report.append(f"Total Jobs Discovered: {overall.get('total_jobs', 0)}")
    report.append("")

    # Platform Analysis
    report.append("🎯 PLATFORM-SPECIFIC ANALYSIS")
    report.append("-" * 30)
    platform_metrics = performance_data.get("platform_metrics", {})
    for platform in ["reddit", "x", "mastodon"]:
        metrics = platform_metrics.get(platform, {})
        if metrics.get("durations"):
            report.append(f"{platform.upper()}:")
            report.append(f"  Success Rate: {metrics.get('success_rate', 0):.1%}")
            report.append(f"  Avg Duration: {metrics.get('avg_duration', 0) / 1000:.1f}s")
            report.append(f"  Avg Jobs: {metrics.get('avg_jobs', 0):.1f}")
            report.append(f"  Median Duration: {metrics.get('median_duration', 0) / 1000:.1f}s")
            report.append("")

    # Rate Limit Optimizations
    if recommendations.get("rate_limit_optimizations"):
        report.append("⏱️ RATE LIMIT OPTIMIZATIONS")
        report.append("-" * 30)
        for opt in recommendations["rate_limit_optimizations"]:
            report.append(f"{opt['platform'].upper()}:")
            report.append(f"  Current: {opt['current']}")
            report.append(f"  Recommended: {opt['recommended']}")
            report.append(f"  Reason: {opt['reason']}")
            report.append("")

    # Confidence Threshold Adjustments
    if recommendations.get("confidence_threshold_adjustments"):
        report.append("🎯 CONFIDENCE THRESHOLD ADJUSTMENTS")
        report.append("-" * 30)
        for opt in recommendations["confidence_threshold_adjustments"]:
            report.append(f"Current: {opt['current']}")
            report.append(f"Recommended: {opt['recommended']}")
            report.append(f"Reason: {opt['reason']}")
            report.append("")

    # Timeout and Retry Optimizations
    if recommendations.get("timeout_optimizations"):
        report.append("⏰ TIMEOUT OPTIMIZATIONS")
        report.append("-" * 30)
        for opt in recommendations["timeout_optimizations"]:
            report.append(f"{opt['platform'].upper()}:")
            report.append(f"  Current: {opt['current']}")
            report.append(f"  Recommended: {opt['recommended']}")
            report.append(f"  Reason: {opt['reason']}")
            report.append("")

    if recommendations.get("retry_optimizations"):
        report.append("🔄 RETRY OPTIMIZATIONS")
        report.append("-" * 30)
        for opt in recommendations["retry_optimizations"]:
            report.append(f"{opt['platform'].upper()}:")
            report.append(f"  Current: {opt['current']}")
            report.append(f"  Recommended: {opt['recommended']}")
            report.append(f"  Reason: {opt['reason']}")
            report.append("")

    # Configuration Changes Summary
    report.append("⚙️ CONFIGURATION CHANGES")
    report.append("-" * 30)
    if current_config.get("minConfidence") != optimized_config.get("minConfidence"):
        report.append(
            f"minConfidence: {current_config.get('minConfidence')}% → {optimized_config.get('minConfidence')}%"
        )
    if current_config.get("reddit", {}).get("rateLimitDelay") != optimized_config.get(
        "reddit", {}
    ).get("rateLimitDelay"):
        old_delay = current_config.get("reddit", {}).get("rateLimitDelay", "N/A")
        new_delay = optimized_config.get("reddit", {}).get("rateLimitDelay", "N/A")
        report.append(f"reddit.rateLimitDelay: {old_delay}s → {new_delay}s")
    if current_config.get("x", {}).get("timeoutSeconds") != optimized_config.get("x", {}).get(
        "timeoutSeconds"
    ):
        old_timeout = current_config.get("x", {}).get("timeoutSeconds", "N/A")
        new_timeout = optimized_config.get("x", {}).get("timeoutSeconds", "N/A")
        report.append(f"x.timeoutSeconds: {old_timeout}s → {new_timeout}s")
    if current_config.get("mastodon", {}).get("retries") != optimized_config.get(
        "mastodon", {}
    ).get("retries"):
        old_retries = current_config.get("mastodon", {}).get("retries", "N/A")
        new_retries = optimized_config.get("mastodon", {}).get("retries", "N/A")
        report.append(f"mastodon.retries: {old_retries} → {new_retries}")

    # Overall Suggestions
    if recommendations.get("overall_suggestions"):
        report.append("💡 OVERALL SUGGESTIONS")
        report.append("-" * 30)
        for suggestion in recommendations["overall_suggestions"]:
            report.append(f"• {suggestion}")
        report.append("")

    report.append("=" * 60)
    report.append("END OF OPTIMIZATION REPORT")
    report.append("=" * 60)

    return "\n".join(report)


def main():
    """Main optimization function."""
    print("🔧 Social Sources Configuration Optimization Tool")
    print("=" * 60)

    # Load current configuration
    config_path = Path("data/social-sources-config.json")
    current_config = load_current_config(config_path)

    if not current_config:
        print(
            "❌ No social sources configuration found. Please create data/social-sources-config.json first."
        )
        return 1

    # Load performance data
    report_paths = [
        Path("test_social_fetch/jobs-fetch-report.json"),
        Path("data/jobs-fetch-report.json"),
        Path("jobs-fetch-report.json"),
    ]

    performance_data = None
    for report_path in report_paths:
        if report_path.exists():
            analysis = analyze_performance_data(report_path)
            if analysis:
                performance_data = analysis
                break

    if not performance_data:
        print(
            "❌ No performance data found. Please run the social sources pipeline first to gather performance metrics."
        )
        print("Use: python src/jobs/pipeline.py --social-enabled --output-dir test_social_fetch")
        return 1

    # Generate optimization recommendations
    print("📊 Analyzing performance data...")
    recommendations = generate_optimization_recommendations(current_config, performance_data)

    # Apply optimizations
    print("⚙️ Applying optimizations...")
    optimized_config = apply_optimizations(current_config, recommendations)

    # Generate report
    print("📄 Generating optimization report...")
    optimization_report = generate_optimization_report(
        current_config, optimized_config, recommendations, performance_data
    )

    # Display report
    print(optimization_report)

    # Save optimized configuration
    optimized_path = Path("data/social-sources-config-optimized.json")
    with open(optimized_path, "w") as f:
        json.dump(optimized_config, f, indent=2)

    print(f"✅ Optimized configuration saved to: {optimized_path}")
    print("🔧 Apply the optimized configuration with:")
    print(f"cp {optimized_path} {config_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
