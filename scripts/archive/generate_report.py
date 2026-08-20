#!/usr/bin/env python3
"""Generate readiness report from analysis results.

This script reads the JSON analysis from analyze_repo.py and generates
a formatted report with scores per pillar and maturity level.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PILLAR_NAMES = {
    "style_validation": "Style & Validation",
    "build_system": "Build System",
    "testing": "Testing",
    "documentation": "Documentation",
    "dev_environment": "Dev Environment",
    "debugging": "Debugging",
    "versioning": "Versioning & Releases",
    "security": "Security & Reliability",
    "onboarding": "Onboarding",
}

PILLAR_CRITERIA_COUNT = {
    "style_validation": 10,
    "build_system": 8,
    "testing": 12,
    "documentation": 12,
    "dev_environment": 8,
    "debugging": 8,
    "versioning": 8,
    "security": 8,
    "onboarding": 7,
}


def calculate_pillar_score(pillar_data: dict[str, Any]) -> float:
    """Calculate percentage score for a pillar."""
    met = len(pillar_data.get("met", []))
    # Use the actual count from criteria count
    total = PILLAR_CRITERIA_COUNT.get("style_validation", 10)  # default

    # Find which pillar this is
    for pillar_name, count in PILLAR_CRITERIA_COUNT.items():
        if pillar_data == {}:  # placeholder
            continue

    return met


def get_maturity_level(score: float) -> str:
    """Determine maturity level based on score."""
    if score >= 81:
        return "Level 5: Optimized"
    elif score >= 61:
        return "Level 4: Established"
    elif score >= 41:
        return "Level 3: Functional"
    elif score >= 21:
        return "Level 2: Developing"
    else:
        return "Level 1: Initial"


def generate_report(analysis: dict[str, Any]) -> str:
    """Generate formatted report from analysis."""
    lines = []

    lines.append("=" * 70)
    lines.append("AI REPOSITORY READINESS REPORT")
    lines.append("=" * 70)
    lines.append("")

    # Repository info
    repo = analysis.get("repository", "Unknown")
    lines.append(f"Repository: {repo}")
    lines.append("")

    # Pillar scores
    lines.append("-" * 70)
    lines.append("PILLAR SCORES")
    lines.append("-" * 70)

    total_score = 0
    pillar_count = 0

    for pillar_key, pillar_name in PILLAR_NAMES.items():
        pillar_data = analysis.get("pillars", {}).get(pillar_key, {})
        met = len(pillar_data.get("met", []))
        total_criteria = PILLAR_CRITERIA_COUNT.get(pillar_key, 10)

        score = (met / total_criteria * 100) if total_criteria > 0 else 0
        total_score += score
        pillar_count += 1

        # Bar visualization
        bar_length = int(score / 5)
        bar = "█" * bar_length + "░" * (20 - bar_length)

        lines.append(f"{pillar_name:25} {bar} {score:5.1f}% ({met}/{total_criteria})")

    lines.append("")

    # Overall score
    overall_score = total_score / pillar_count if pillar_count > 0 else 0
    maturity_level = get_maturity_level(overall_score)

    lines.append("-" * 70)
    lines.append("OVERALL")
    lines.append("-" * 70)

    bar_length = int(overall_score / 5)
    bar = "█" * bar_length + "░" * (20 - bar_length)

    lines.append(f"Maturity Level: {maturity_level}")
    lines.append(f"Overall Score:   {bar} {overall_score:.1f}%")
    lines.append("")

    # Detailed results
    lines.append("-" * 70)
    lines.append("DETAILED RESULTS")
    lines.append("-" * 70)

    for pillar_key, pillar_name in PILLAR_NAMES.items():
        pillar_data = analysis.get("pillars", {}).get(pillar_key, {})
        met = pillar_data.get("met", [])
        unmet = pillar_data.get("unmet", [])

        if met or unmet:
            lines.append(f"\n### {pillar_name}")

            if met:
                lines.append("  Met criteria:")
                for c in met:
                    lines.append(f"    ✓ {c}")

            if unmet:
                lines.append("  Unmet criteria:")
                for c in unmet[:5]:  # Show first 5
                    lines.append(f"    ✗ {c}")
                if len(unmet) > 5:
                    lines.append(f"    ... and {len(unmet) - 5} more")

    lines.append("")
    lines.append("=" * 70)
    lines.append("END OF REPORT")
    lines.append("=" * 70)

    return "\n".join(lines)


def main() -> int:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate readiness report")
    parser.add_argument("input", nargs="?", help="Input JSON file (from analyze_repo.py)")
    parser.add_argument("-o", "--output", help="Output report file")
    parser.add_argument("-j", "--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    # Read analysis data
    if args.input:
        analysis = json.loads(Path(args.input).read_text())
    else:
        # Try to read from stdin
        analysis = json.load(sys.stdin)

    if args.json:
        # Output as JSON with calculated scores
        output = {}
        total_score = 0
        pillar_count = 0

        for pillar_key, pillar_name in PILLAR_NAMES.items():
            pillar_data = analysis.get("pillars", {}).get(pillar_key, {})
            met = len(pillar_data.get("met", []))
            total_criteria = PILLAR_CRITERIA_COUNT.get(pillar_key, 10)
            score = (met / total_criteria * 100) if total_criteria > 0 else 0
            total_score += score
            pillar_count += 1
            output[pillar_key] = {
                "name": pillar_name,
                "score": score,
                "met": met,
                "total": total_criteria,
            }

        output["overall"] = {
            "score": total_score / pillar_count if pillar_count > 0 else 0,
            "level": get_maturity_level(total_score / pillar_count if pillar_count > 0 else 0),
        }

        print(json.dumps(output, indent=2))
    else:
        # Generate text report
        report = generate_report(analysis)

        if args.output:
            Path(args.output).write_text(report)
            print(f"Report saved to {args.output}")
        else:
            print(report)

    return 0


if __name__ == "__main__":
    sys.exit(main())
