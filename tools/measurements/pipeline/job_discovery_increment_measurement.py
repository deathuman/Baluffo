#!/usr/bin/env python3
"""
Job Discovery Increment Measurement Tool
Compares baseline vs social-enabled pipeline runs to quantify job discovery gains
from Reddit, X (Twitter), and Mastodon integration.
"""

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))


def run_baseline_pipeline(output_dir: Path, timeout: int = 300) -> dict[str, Any]:
    """Run pipeline without social sources for baseline measurement."""
    print("📊 Running BASELINE pipeline (without social sources)...")
    print("-" * 50)

    cmd = [
        sys.executable,
        "src/jobs/pipeline.py",
        "--output-dir",
        str(output_dir),
        "--timeout",
        str(timeout),
        "--retries",
        "2",
        "--backoff",
        "1.5",
        "--max-workers",
        "6",
        "--quiet",
    ]

    print(f"Command: {' '.join(cmd)}")
    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 60,  # Add buffer for startup
        )

        duration = time.time() - start_time
        print(f"✅ Baseline pipeline completed in {duration:.1f}s")
        print(f"Exit code: {result.returncode}")

        # Try to load the report
        report_path = output_dir / "jobs-fetch-report.json"
        if report_path.exists():
            with open(report_path) as f:
                report = json.load(f)

            return {
                "success": True,
                "duration": duration,
                "report": report,
                "output_count": int(report.get("summary", {}).get("outputCount", 0)),
                "source_count": len(report.get("sources", [])),
                "error_count": int(report.get("summary", {}).get("failedSources", 0)),
            }
        else:
            print("❌ No report file generated")
            return {
                "success": False,
                "duration": duration,
                "error": "No report file generated",
                "output_count": 0,
                "source_count": 0,
                "error_count": 0,
            }

    except subprocess.TimeoutExpired:
        print("❌ Baseline pipeline timed out")
        return {
            "success": False,
            "duration": timeout,
            "error": "Pipeline timed out",
            "output_count": 0,
            "source_count": 0,
            "error_count": 0,
        }
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, TypeError, ValueError) as e:
        print(f"❌ Error running baseline pipeline: {e}")
        return {
            "success": False,
            "duration": time.time() - start_time,
            "error": str(e),
            "output_count": 0,
            "source_count": 0,
            "error_count": 0,
        }


def run_social_pipeline(output_dir: Path, timeout: int = 300) -> dict[str, Any]:
    """Run pipeline with social sources enabled for comparison."""
    print("🎯 Running SOCIAL SOURCES pipeline (with Reddit/X/Mastodon)...")
    print("-" * 50)

    cmd = [
        sys.executable,
        "src/jobs/pipeline.py",
        "--social-enabled",
        "--output-dir",
        str(output_dir),
        "--timeout",
        str(timeout),
        "--retries",
        "2",
        "--backoff",
        "1.5",
        "--max-workers",
        "6",
        "--quiet",
    ]

    print(f"Command: {' '.join(cmd)}")
    start_time = time.time()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 60,  # Add buffer for startup
        )

        duration = time.time() - start_time
        print(f"✅ Social pipeline completed in {duration:.1f}s")
        print(f"Exit code: {result.returncode}")

        # Try to load the report
        report_path = output_dir / "jobs-fetch-report.json"
        if report_path.exists():
            with open(report_path) as f:
                report = json.load(f)

            # Analyze social sources specifically
            social_sources = [
                s
                for s in report.get("sources", [])
                if s.get("name", "").startswith(("social_reddit", "social_x", "social_mastodon"))
            ]
            social_output_count = sum(int(s.get("keptCount", 0)) for s in social_sources)

            return {
                "success": True,
                "duration": duration,
                "report": report,
                "output_count": int(report.get("summary", {}).get("outputCount", 0)),
                "social_output_count": social_output_count,
                "social_sources": len(social_sources),
                "source_count": len(report.get("sources", [])),
                "error_count": int(report.get("summary", {}).get("failedSources", 0)),
                "social_sources_details": social_sources,
            }
        else:
            print("❌ No report file generated")
            return {
                "success": False,
                "duration": duration,
                "error": "No report file generated",
                "output_count": 0,
                "social_output_count": 0,
                "social_sources": 0,
                "source_count": 0,
                "error_count": 0,
            }

    except subprocess.TimeoutExpired:
        print("❌ Social pipeline timed out")
        return {
            "success": False,
            "duration": timeout,
            "error": "Pipeline timed out",
            "output_count": 0,
            "social_output_count": 0,
            "social_sources": 0,
            "source_count": 0,
            "error_count": 0,
        }
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, TypeError, ValueError) as e:
        print(f"❌ Error running social pipeline: {e}")
        return {
            "success": False,
            "duration": time.time() - start_time,
            "error": str(e),
            "output_count": 0,
            "social_output_count": 0,
            "social_sources": 0,
            "source_count": 0,
            "error_count": 0,
        }


def calculate_incremental_gains(baseline: dict[str, Any], social: dict[str, Any]) -> dict[str, Any]:
    """Calculate incremental gains from social sources."""
    baseline_jobs = baseline.get("output_count", 0)
    social_jobs = social.get("output_count", 0)
    social_only_jobs = social.get("social_output_count", 0)

    # Calculate gains
    absolute_gain = social_jobs - baseline_jobs
    percentage_gain = ((social_jobs - baseline_jobs) / max(1, baseline_jobs)) * 100

    # Performance metrics
    baseline_duration = baseline.get("duration", 0)
    social_duration = social.get("duration", 0)
    duration_increase = social_duration - baseline_duration
    duration_percentage = ((social_duration - baseline_duration) / max(1, baseline_duration)) * 100

    # Error rates
    baseline_errors = baseline.get("error_count", 0)
    social_errors = social.get("error_count", 0)
    error_increase = social_errors - baseline_errors

    # Social source breakdown
    social_sources = social.get("social_sources", 0)
    social_sources_details = social.get("social_sources_details", [])

    # Platform breakdown
    reddit_sources = [s for s in social_sources_details if "social_reddit" in s.get("name", "")]
    x_sources = [s for s in social_sources_details if "social_x" in s.get("name", "")]
    mastodon_sources = [s for s in social_sources_details if "social_mastodon" in s.get("name", "")]

    reddit_jobs = sum(int(s.get("keptCount", 0)) for s in reddit_sources)
    x_jobs = sum(int(s.get("keptCount", 0)) for s in x_sources)
    mastodon_jobs = sum(int(s.get("keptCount", 0)) for s in mastodon_sources)

    return {
        "baseline": {
            "jobs": baseline_jobs,
            "duration": baseline_duration,
            "errors": baseline_errors,
            "sources": baseline.get("source_count", 0),
        },
        "social_enabled": {
            "jobs": social_jobs,
            "duration": social_duration,
            "errors": social_errors,
            "sources": social.get("source_count", 0),
            "social_sources": social_sources,
        },
        "incremental_gains": {
            "absolute_job_gain": absolute_gain,
            "percentage_job_gain": percentage_gain,
            "social_only_jobs": social_only_jobs,
            "duration_increase": duration_increase,
            "duration_percentage_increase": duration_percentage,
            "error_increase": error_increase,
        },
        "social_breakdown": {
            "reddit": {
                "sources": len(reddit_sources),
                "jobs": reddit_jobs,
                "percentage": (reddit_jobs / max(1, social_only_jobs)) * 100,
            },
            "x": {
                "sources": len(x_sources),
                "jobs": x_jobs,
                "percentage": (x_jobs / max(1, social_only_jobs)) * 100,
            },
            "mastodon": {
                "sources": len(mastodon_sources),
                "jobs": mastodon_jobs,
                "percentage": (mastodon_jobs / max(1, social_only_jobs)) * 100,
            },
        },
    }


def generate_increment_report(gains: dict[str, Any]) -> str:
    """Generate comprehensive increment report."""
    report = []
    report.append("=" * 70)
    report.append("JOB DISCOVERY INCREMENT MEASUREMENT REPORT")
    report.append("=" * 70)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")

    # Executive Summary
    report.append("📊 EXECUTIVE SUMMARY")
    report.append("-" * 30)
    baseline_jobs = gains["baseline"]["jobs"]
    social_jobs = gains["social_enabled"]["jobs"]
    absolute_gain = gains["incremental_gains"]["absolute_job_gain"]
    percentage_gain = gains["incremental_gains"]["percentage_job_gain"]

    report.append(f"Baseline Job Discovery: {baseline_jobs:,} jobs")
    report.append(f"Social-Enabled Job Discovery: {social_jobs:,} jobs")
    report.append(f"Absolute Increment: +{absolute_gain:,} jobs")
    report.append(f"Percentage Increment: {percentage_gain:+.1f}%")
    report.append("")

    # Performance Impact
    report.append("⏱️ PERFORMANCE IMPACT")
    report.append("-" * 30)
    baseline_duration = gains["baseline"]["duration"]
    social_duration = gains["social_enabled"]["duration"]
    duration_increase = gains["incremental_gains"]["duration_increase"]
    duration_percentage = gains["incremental_gains"]["duration_percentage_increase"]

    report.append(f"Baseline Duration: {baseline_duration:.1f}s")
    report.append(f"Social-Enabled Duration: {social_duration:.1f}s")
    report.append(f"Duration Increase: +{duration_increase:.1f}s ({duration_percentage:+.1f}%)")
    report.append("")

    # Error Analysis
    report.append("🚨 ERROR ANALYSIS")
    report.append("-" * 30)
    baseline_errors = gains["baseline"]["errors"]
    social_errors = gains["social_enabled"]["errors"]
    error_increase = gains["incremental_gains"]["error_increase"]

    report.append(f"Baseline Errors: {baseline_errors}")
    report.append(f"Social-Enabled Errors: {social_errors}")
    report.append(f"Error Increase: {error_increase:+}")
    report.append("")

    # Social Sources Breakdown
    report.append("🎯 SOCIAL SOURCES BREAKDOWN")
    report.append("-" * 30)
    social_only_jobs = gains["incremental_gains"]["social_only_jobs"]
    report.append(f"Social Sources Contribution: {social_only_jobs:,} jobs")
    report.append("")

    # Platform-specific breakdown
    reddit = gains["social_breakdown"]["reddit"]
    x = gains["social_breakdown"]["x"]
    mastodon = gains["social_breakdown"]["mastodon"]

    report.append("Platform Contributions:")
    report.append(f"  Reddit: {reddit['jobs']:,} jobs ({reddit['percentage']:.1f}%)")
    report.append(f"  X (Twitter): {x['jobs']:,} jobs ({x['percentage']:.1f}%)")
    report.append(f"  Mastodon: {mastodon['jobs']:,} jobs ({mastodon['percentage']:.1f}%)")
    report.append("")

    report.append("Platform Sources:")
    report.append(f"  Reddit: {reddit['sources']} subreddits")
    report.append(f"  X (Twitter): {x['sources']} queries")
    report.append(f"  Mastodon: {mastodon['sources']} instances")
    report.append("")

    # ROI Analysis
    report.append("💰 ROI ANALYSIS")
    report.append("-" * 30)
    jobs_per_second_baseline = baseline_jobs / max(1, baseline_duration)
    jobs_per_second_social = social_jobs / max(1, social_duration)

    report.append(f"Jobs/Second (Baseline): {jobs_per_second_baseline:.2f}")
    report.append(f"Jobs/Second (Social): {jobs_per_second_social:.2f}")
    report.append(
        f"Efficiency Change: {(jobs_per_second_social - jobs_per_second_baseline):+.2f} jobs/s"
    )
    report.append("")

    # Recommendations
    report.append("💡 RECOMMENDATIONS")
    report.append("-" * 30)

    if percentage_gain > 10:
        report.append("✅ HIGH IMPACT: Social sources provide significant job discovery gains")
        report.append("   → Recommended: Enable social sources in production")
    elif percentage_gain > 5:
        report.append("✅ MODERATE IMPACT: Social sources provide moderate gains")
        report.append("   → Recommended: Enable with monitoring")
    else:
        report.append("⚠️  LOW IMPACT: Social sources provide minimal gains")
        report.append("   → Recommended: Review configuration or consider alternatives")

    if duration_percentage > 50:
        report.append("⚠️  PERFORMANCE WARNING: Significant duration increase detected")
        report.append("   → Recommended: Optimize social source configuration")

    if error_increase > 5:
        report.append("⚠️  ERROR WARNING: High error rate increase")
        report.append("   → Recommended: Review error handling and rate limits")

    report.append("")
    report.append("🎯 NEXT STEPS:")
    if percentage_gain > 0:
        report.append("1. Enable social sources in production pipeline")
        report.append("2. Monitor job quality and error rates")
        report.append("3. Consider expanding to additional platforms")
    else:
        report.append("1. Review social source configuration")
        report.append("2. Consider alternative job discovery methods")
        report.append("3. Monitor for platform changes")

    report.append("")
    report.append("=" * 70)
    report.append("END OF INCREMENT MEASUREMENT REPORT")
    report.append("=" * 70)

    return "\n".join(report)


def main():
    """Main increment measurement function."""
    print("🚀 Job Discovery Increment Measurement Tool")
    print("=" * 50)

    # Create test directories
    baseline_dir = Path("test_baseline_measurement")
    social_dir = Path("test_social_measurement")

    baseline_dir.mkdir(exist_ok=True)
    social_dir.mkdir(exist_ok=True)

    print("Starting baseline measurement...")
    print("This may take several minutes...")
    print("")

    # Run baseline pipeline
    baseline_result = run_baseline_pipeline(baseline_dir, timeout=300)

    if not baseline_result["success"]:
        print("❌ Failed to run baseline pipeline")
        print(f"Error: {baseline_result.get('error', 'Unknown error')}")
        return 1

    print("\n" + "=" * 50)
    print("Starting social sources measurement...")
    print("This may take several minutes...")
    print("")

    # Run social pipeline
    social_result = run_social_pipeline(social_dir, timeout=300)

    if not social_result["success"]:
        print("❌ Failed to run social pipeline")
        print(f"Error: {social_result.get('error', 'Unknown error')}")
        return 1

    print("\n" + "=" * 50)
    print("Calculating incremental gains...")

    # Calculate gains
    gains = calculate_incremental_gains(baseline_result, social_result)

    # Generate report
    increment_report = generate_increment_report(gains)

    # Display results
    print(increment_report)

    # Save detailed results
    results_path = Path("job_discovery_increment_results.json")
    with open(results_path, "w") as f:
        json.dump(gains, f, indent=2, default=str)

    print(f"\n📁 Detailed results saved to: {results_path}")

    # Summary statistics
    print("\n" + "=" * 50)
    print("📈 MEASUREMENT SUMMARY")
    print("=" * 50)
    print(f"Job Discovery Increase: {gains['incremental_gains']['absolute_job_gain']:,} jobs")
    print(f"Percentage Increase: {gains['incremental_gains']['percentage_job_gain']:+.1f}%")
    print(f"Social Sources Contribution: {gains['incremental_gains']['social_only_jobs']:,} jobs")
    print(
        f"Platform Breakdown: Reddit {gains['social_breakdown']['reddit']['jobs']:,}, X {gains['social_breakdown']['x']['jobs']:,}, Mastodon {gains['social_breakdown']['mastodon']['jobs']:,}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
