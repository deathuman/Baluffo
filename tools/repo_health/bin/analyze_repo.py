#!/usr/bin/env python3
"""Repository maturity analyzer for Baluffo.

Config-driven analysis with four-state evaluation:
- met: Criterion satisfied with evidence
- unmet: Criterion not satisfied
- not_applicable: Criterion doesn't apply to this project
- unknown: Cannot determine without verification

Evidence levels:
- present: File/config exists
- enforced: CI/pre-commit runs it
- passes: Command actually succeeds
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


class MaturityAnalyzer:
    """Config-driven repository maturity analyzer."""

    STATE_MET = "met"
    STATE_UNMET = "unmet"
    STATE_NOT_APPLICABLE = "not_applicable"
    STATE_UNKNOWN = "unknown"

    def __init__(self, root_path: str = ".", config_path: str | None = None):
        self.root = Path(root_path).resolve()

        if config_path:
            self.config_path = Path(config_path)
        else:
            self.config_path = (
                self.root
                / "tools"
                / "repo_health"
                / "profiles"
                / "baluffo"
                / "readiness"
                / "maturity-criteria.yaml"
            )

        self.config = self._load_config()
        self.results: dict[str, Any] = {}
        self.verify_mode = False
        self._command_cache: dict[str, bool] = {}

    def _load_config(self) -> dict[str, Any]:
        """Load criteria from YAML config."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config not found: {self.config_path}")

        with open(self.config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _check_file_exists(self, paths: list[str]) -> bool:
        """Check if any of the given paths exist."""
        for path in paths:
            if (self.root / path).exists():
                return True
        return False

    def _check_file_exists_single(self, path: str) -> bool:
        """Check if a single path exists."""
        return (self.root / path).exists()

    def _check_directory_exists(self, path: str) -> bool:
        """Check if directory exists."""
        return (self.root / path).is_dir()

    def _check_script_in_package_json(self, script: str) -> bool:
        """Check if a script exists in package.json."""
        pkg_json = self.root / "package.json"
        if not pkg_json.exists():
            return False

        try:
            with open(pkg_json, encoding="utf-8") as f:
                pkg = json.load(f)
            return "scripts" in pkg and script in pkg["scripts"]
        except (OSError, json.JSONDecodeError):
            return False

    def _check_command_passes(self, command: str) -> bool:
        """Check if a command passes (runs without error)."""
        # Use cache to avoid running same command twice
        if command in self._command_cache:
            return self._command_cache[command]

        try:
            result = subprocess.run(
                command,
                shell=True,
                cwd=self.root,
                capture_output=True,
                timeout=120,
                env={"PYTHONPATH": str(self.root)},
            )
            passed = result.returncode == 0
            self._command_cache[command] = passed
            return passed
        except (subprocess.TimeoutExpired, OSError):
            self._command_cache[command] = False
            return False

    def _check_code_contains_pattern(
        self, pattern: str, paths_glob: str, min_matches: int = 1
    ) -> bool:
        """Check if code files contain a regex pattern."""
        matches = 0
        for file_path in self.root.glob(paths_glob):
            if not file_path.is_file():
                continue
            try:
                content = file_path.read_text(encoding="utf-8")
                if re.search(pattern, content):
                    matches += 1
                    if matches >= min_matches:
                        return True
            except (OSError, UnicodeDecodeError):
                continue
        return matches >= min_matches

    def _check_readme_has_sections(self, sections: list[str]) -> bool:
        """Check if README contains required sections."""
        readme_paths = ["README.md", "README.rst", "README.txt"]

        for readme_name in readme_paths:
            readme = self.root / readme_name
            if not readme.exists():
                continue

            try:
                content = readme.read_text(encoding="utf-8").lower()
                if all(section.lower() in content for section in sections):
                    return True
            except OSError:
                continue

        return False

    def _check_no_secrets_in_code(self) -> bool:
        """Check for hardcoded secrets (basic heuristic)."""
        # Common patterns for secrets
        secret_patterns = [
            r'api_key\s*=\s*["\'][^"\']+["\']',
            r'secret_key\s*=\s*["\'][^"\']+["\']',
            r'password\s*=\s*["\'][^"\']+["\']',
            r'token\s*=\s*["\'][A-Za-z0-9_\-]{20,}["\']',
        ]

        for py_file in self.root.glob("src/**/*.py"):
            if not py_file.is_file():
                continue
            try:
                content = py_file.read_text(encoding="utf-8")
                for pattern in secret_patterns:
                    if re.search(pattern, content, re.IGNORECASE):
                        return False  # Found potential secret
            except (OSError, UnicodeDecodeError):
                continue

        return True

    def _check_not_applicable(self, criterion: dict[str, Any]) -> bool:
        """Check if criterion has not_applicable condition."""
        if "not_applicable_if" in criterion:
            for condition in criterion["not_applicable_if"]:
                if condition == "true":
                    return True
        return False

    def _evaluate_criterion(self, criterion_id: str, criterion: dict[str, Any]) -> str:
        """Evaluate a single criterion and return its state."""
        # Check if not applicable
        if self._check_not_applicable(criterion):
            return self.STATE_NOT_APPLICABLE

        check_type = criterion.get("check", "")

        # Handle different check types
        try:
            if check_type == "file_exists_any":
                paths = criterion.get("paths", [])
                if self._check_file_exists(paths):
                    return self.STATE_MET
                return self.STATE_UNMET

            elif check_type == "file_exists":
                path = criterion.get("path", "")
                if self._check_file_exists_single(path):
                    return self.STATE_MET
                return self.STATE_UNMET

            elif check_type == "directory_exists":
                path = criterion.get("path", "")
                if self._check_directory_exists(path):
                    return self.STATE_MET
                return self.STATE_UNMET

            elif check_type == "script_in_package_json":
                script = criterion.get("script", "")
                if self._check_script_in_package_json(script):
                    return self.STATE_MET
                return self.STATE_UNMET

            elif check_type == "command_passes":
                command = criterion.get("command", "")
                mode = criterion.get("mode", "scan")

                # In scan mode, command checks return unknown
                if mode == "verify" and self.verify_mode:
                    if self._check_command_passes(command):
                        return self.STATE_MET
                    return self.STATE_UNMET
                else:
                    # In scan mode, we can't verify commands
                    return self.STATE_UNKNOWN

            elif check_type == "code_contains_pattern":
                pattern = criterion.get("pattern", "")
                paths = criterion.get("paths", ["src/**/*.py"])
                min_matches = criterion.get("min_matches", 1)

                # Glob returns relative paths, handle both
                glob_path = paths[0] if isinstance(paths, list) else paths
                if self._check_code_contains_pattern(pattern, glob_path, min_matches):
                    return self.STATE_MET
                return self.STATE_UNMET

            elif check_type == "readme_has_sections":
                sections = criterion.get("sections", [])
                if self._check_readme_has_sections(sections):
                    return self.STATE_MET
                return self.STATE_UNMET

            elif check_type == "no_secrets_in_code":
                if self._check_no_secrets_in_code():
                    return self.STATE_MET
                return self.STATE_UNMET

            else:
                return self.STATE_UNKNOWN

        except (
            AttributeError,
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            re.error,
            subprocess.SubprocessError,
            TypeError,
            ValueError,
        ):
            # Any error means we can't determine
            return self.STATE_UNKNOWN

    def _calculate_pillar_score(self, pillar_id: str, pillar: dict[str, Any]) -> dict[str, Any]:
        """Calculate score for a pillar."""
        criteria = pillar.get("criteria", {})
        weight = pillar.get("weight", 1.0)

        results = {
            "met": [],
            "unmet": [],
            "not_applicable": [],
            "unknown": [],
        }

        total_applicable_weight = 0.0
        met_weight = 0.0

        for criterion_id, criterion in criteria.items():
            criterion_weight = criterion.get("weight", 1.0)
            state = self._evaluate_criterion(criterion_id, criterion)
            results[state].append(criterion_id)

            if state != self.STATE_NOT_APPLICABLE:
                total_applicable_weight += criterion_weight
                if state == self.STATE_MET:
                    met_weight += criterion_weight

        # Calculate pillar score (percentage of applicable criteria met)
        if total_applicable_weight > 0:
            score = (met_weight / total_applicable_weight) * 100
        else:
            score = 0.0

        # Calculate confidence (100% if no unknowns, reduced otherwise)
        total_criteria = len(criteria)
        unknown_count = len(results[self.STATE_UNKNOWN])
        if total_criteria > 0:
            confidence = 1.0 - (unknown_count / total_criteria)
        else:
            confidence = 1.0

        return {
            "score": round(score, 1),
            "weight": weight,
            "confidence": round(confidence, 2),
            "results": results,
        }

    def analyze(self, verify: bool = False) -> dict[str, Any]:
        """Run full analysis."""
        self.verify_mode = verify

        pillars_config = self.config.get("pillars", {})
        suggestions_config = self.config.get("suggestions", {})

        pillar_scores = {}
        total_weight = 0.0
        weighted_sum = 0.0

        all_results = {
            "met": [],
            "unmet": [],
            "not_applicable": [],
            "unknown": [],
        }

        for pillar_id, pillar in pillars_config.items():
            pillar_result = self._calculate_pillar_score(pillar_id, pillar)
            pillar_scores[pillar_id] = pillar_result

            weight = pillar.get("weight", 1.0)
            total_weight += weight
            weighted_sum += pillar_result["score"] * weight

            # Collect all results
            for state in [
                self.STATE_MET,
                self.STATE_UNMET,
                self.STATE_NOT_APPLICABLE,
                self.STATE_UNKNOWN,
            ]:
                for criterion_id in pillar_result["results"][state]:
                    all_results[state].append(f"{pillar_id}.{criterion_id}")

        # Calculate overall score
        overall_score = (weighted_sum / total_weight) if total_weight > 0 else 0.0

        # Calculate overall confidence
        all_criteria_count = sum(len(v) for v in all_results.values())
        unknown_count = len(all_results[self.STATE_UNKNOWN])
        overall_confidence = (
            1.0 - (unknown_count / all_criteria_count) if all_criteria_count > 0 else 1.0
        )

        # Determine maturity level
        status = self._get_maturity_status(overall_score)

        # Generate suggested next steps
        suggested_next = self._generate_suggestions(pillar_scores, suggestions_config)

        self.results = {
            "repository": str(self.root),
            "score": round(overall_score, 1),
            "status": status,
            "confidence": round(overall_confidence, 2),
            "verify_mode": verify,
            "pillars": pillar_scores,
            "met": all_results[self.STATE_MET],
            "unmet": all_results[self.STATE_UNMET],
            "not_applicable": all_results[self.STATE_NOT_APPLICABLE],
            "unknown": all_results[self.STATE_UNKNOWN],
            "suggested_next_step": suggested_next,
        }

        return self.results

    def _get_maturity_status(self, score: float) -> str:
        """Determine maturity status from score."""
        if score >= 81:
            return "Optimized"
        elif score >= 61:
            return "Established"
        elif score >= 41:
            return "Functional"
        elif score >= 21:
            return "Developing"
        else:
            return "Initial"

    def _generate_suggestions(
        self, pillar_scores: dict[str, Any], suggestions_config: dict[str, Any]
    ) -> str:
        """Generate suggested next step based on lowest-scoring pillar."""
        # Find pillar with lowest score
        lowest_pillar = None
        lowest_score = float("inf")

        for pillar_id, pillar_data in pillar_scores.items():
            score = pillar_data["score"]
            if score < lowest_score:
                lowest_score = score
                lowest_pillar = pillar_id

        if lowest_pillar and lowest_pillar in suggestions_config:
            suggestions = suggestions_config[lowest_pillar]
            if suggestions:
                return suggestions[0]

        return "Continue improving documentation and testing coverage"


def main() -> int:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Analyze repository maturity for Baluffo")
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="Repository path (default: current directory)",
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Path to maturity-criteria.yaml config",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output JSON file",
    )
    parser.add_argument(
        "-v",
        "--verify",
        action="store_true",
        help="Verify mode: run commands and check results (slower)",
    )
    parser.add_argument(
        "--scan",
        action="store_true",
        help="Scan mode (default): static repo inspection only",
    )

    args = parser.parse_args()

    try:
        analyzer = MaturityAnalyzer(args.path, args.config)
        results = analyzer.analyze(verify=args.verify)

        json_output = json.dumps(results, indent=2)

        if args.output:
            Path(args.output).write_text(json_output, encoding="utf-8")
            print(f"Analysis saved to {args.output}")
        else:
            print(json_output)

        # Print summary
        print(f"\n{'=' * 50}")
        print(f"Maturity Score: {results['score']}%")
        print(f"Status: {results['status']}")
        print(f"Confidence: {results['confidence']:.0%}")
        if results.get("verify_mode"):
            print("Mode: Verified (commands executed)")
        else:
            print("Mode: Scan (static analysis)")
        print(f"\nSuggested: {results['suggested_next_step']}")
        print(f"{'=' * 50}")

        return 0

    except (
        FileNotFoundError,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
        yaml.YAMLError,
    ) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
