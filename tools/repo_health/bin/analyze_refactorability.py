#!/usr/bin/env python3
"""AI Refactorability Analyzer for Baluffo.

This tool answers three questions:
1. Where are the AI-dangerous boundary violations?
2. Which files are the biggest refactor hotspots?
3. What existing docs, contracts, and tests make those hotspots safer or riskier?

It measures AI-oriented refactorability, not generic code quality.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

COMPOSITION_ROOTS = [
    "src.jobs",
    "src.admin_bridge",
    "src.bridge",
]

CANONICAL_DOCS = [
    "AGENTS.md",
    "docs/architecture-ai-map.md",
    "docs/testing.md",
    "docs/admin-bridge-api.md",
    "docs/DATA_CONTRACT.md",
    "docs/RELEASE.md",
]

CANONICAL_REGISTRIES = [
    "frontend/shared/ui/selectors.js",
]

CONTRACT_DOCS = [
    "docs/DATA_CONTRACT.md",
    "docs/admin-bridge-api.md",
    "docs/RELEASE.md",
    "docs/fetcher-runtime-contracts.md",
]

SENSITIVE_LEAF_DIRS = [
    "scripts/",
    "src/ship/",
    "src/jobs/adapters/plugins/",
]

SIZE_WARNING_THRESHOLD = 500
SIZE_HIGH_RISK_THRESHOLD = 900

HOTSPOT_NAME_PATTERNS = [
    "runtime",
    "app",
    "domain",
    "orchestrator",
    "bridge",
    "pipeline",
    "main",
]


class RefactorabilityAnalyzer:
    """Analyzer for AI-oriented refactorability."""

    def __init__(self, root_path: str = "."):
        self.root = Path(root_path).resolve()
        self.package_json = self.root / "package.json"
        self.results: dict[str, Any] = {}

    def analyze(self) -> dict[str, Any]:
        """Run full analysis."""
        self.results = {
            "repository": str(self.root),
            "canonical_docs": self._check_canonical_docs(),
            "command_discoverability": self._check_command_discoverability(),
            "boundary_violations": self._check_boundary_violations(),
            "hotspots": self._check_hotspots(),
            "change_locality": self._check_change_locality(),
            "contracts": self._check_contracts(),
            "test_routing": self._check_test_routing(),
            "config_drift": self._check_config_drift(),
            "registries": self._check_registries(),
        }

        self._calculate_scores()
        self._generate_recommendations()

        return self.results

    def _check_canonical_docs(self) -> dict[str, Any]:
        """Check for presence of canonical AI navigation docs."""
        found = []
        missing = []

        for doc in CANONICAL_DOCS:
            if (self.root / doc).exists():
                found.append(doc)
            else:
                missing.append(doc)

        total = len(CANONICAL_DOCS)
        score = int((len(found) / total) * 100) if total > 0 else 0

        return {
            "found": found,
            "missing": missing,
            "score": score,
            "confidence": min(1.0, len(found) / 3) if len(found) >= 3 else 0.5,
        }

    def _check_command_discoverability(self) -> dict[str, Any]:
        """Check package.json for command discoverability."""
        expected_commands = {
            "dev": "Development server",
            "build": "Build command",
            "verify": "Verification/validation",
            "test:py": "Python tests",
            "test:unit": "Frontend unit tests",
            "test:smoke": "Smoke tests",
            "lint:py": "Python linting",
        }

        found = {}
        missing = []

        if self.package_json.exists():
            try:
                with open(self.package_json, encoding="utf-8") as f:
                    pkg = json.load(f)
                scripts = pkg.get("scripts", {})

                for cmd, desc in expected_commands.items():
                    if cmd in scripts:
                        found[cmd] = desc
                    else:
                        missing.append(cmd)
            except (json.JSONDecodeError, OSError):
                missing = list(expected_commands.keys())
        else:
            missing = list(expected_commands.keys())

        cheap_path = "test:unit" in found or "lint:py" in found
        high_risk_path = "test:smoke" in found or "verify" in found

        return {
            "found": found,
            "missing": missing,
            "cheap_verification_path": cheap_path,
            "high_risk_verification_path": high_risk_path,
            "score": int((len(found) / len(expected_commands)) * 100),
        }

    def _check_boundary_violations(self) -> dict[str, Any]:
        """Scan for composition-root imports from leaf/build/package code."""
        violations = []

        for py_file in self.root.glob("scripts/**/*.py"):
            violations.extend(self._scan_file_for_violations(py_file))

        for py_file in self.root.glob("src/ship/**/*.py"):
            violations.extend(self._scan_file_for_violations(py_file))

        for py_file in self.root.glob("src/jobs/adapters/plugins/**/*.py"):
            violations.extend(self._scan_file_for_violations(py_file))

        score = max(0, 100 - len(violations) * 15)

        return {
            "violations": violations,
            "count": len(violations),
            "score": score,
        }

    def _scan_file_for_violations(self, file_path: Path) -> list[dict[str, Any]]:
        """Scan a single file for composition-root imports."""
        violations = []

        try:
            content = file_path.read_text(encoding="utf-8")
            for root in COMPOSITION_ROOTS:
                pattern = rf"^from\s+{re.escape(root)}\s+import\b|^import\s+{re.escape(root)}\s*$"
                matches = re.findall(pattern, content, re.MULTILINE)
                if matches:
                    violations.append(
                        {
                            "file": str(file_path.relative_to(self.root)),
                            "offending_import": root,
                            "why_risky": f"Leaf code should not import broad composition root '{root}'",
                            "suggested_import": self._suggest_leaf_import(root),
                        }
                    )
        except (OSError, UnicodeDecodeError):
            pass

        return violations

    def _suggest_leaf_import(self, root: str) -> str | None:
        """Suggest a lower-level import target."""
        suggestions = {
            "src.jobs": "src/jobs/common/, src/jobs/adapters/plugins/, or direct module",
            "src.admin_bridge": "src/bridge/api.py, src/bridge/config.py",
            "src.bridge": "src/bridge/api.py, src/bridge/config.py",
        }
        return suggestions.get(root)

    def _check_hotspots(self) -> dict[str, Any]:
        """Find oversized files and runtime hotspots."""
        hotspots = []

        for py_file in self.root.glob("src/**/*.py"):
            if not py_file.is_file():
                continue

            try:
                lines = py_file.read_text(encoding="utf-8").splitlines()
                loc = len(lines)

                if loc < SIZE_WARNING_THRESHOLD:
                    continue

                reasons = []
                if loc >= SIZE_HIGH_RISK_THRESHOLD:
                    reasons.append("high-risk-size")
                elif loc >= SIZE_WARNING_THRESHOLD:
                    reasons.append("oversized")

                name = py_file.name.lower()
                if any(p in name for p in HOTSPOT_NAME_PATTERNS):
                    reasons.append("runtime-hotspot")

                if reasons:
                    hotspots.append(
                        {
                            "file": str(py_file.relative_to(self.root)),
                            "loc": loc,
                            "reasons": reasons,
                            "hotspot_score": self._calculate_hotspot_score(loc, reasons),
                        }
                    )
            except (OSError, UnicodeDecodeError):
                continue

        hotspots.sort(key=lambda x: x["hotspot_score"], reverse=True)

        return {
            "hotspots": hotspots[:10],
            "count": len(hotspots),
            "score": max(0, 100 - len(hotspots) * 8),
        }

    def _calculate_hotspot_score(self, loc: int, reasons: list[str]) -> int:
        """Calculate numeric hotspot score."""
        score = 0
        if loc >= SIZE_HIGH_RISK_THRESHOLD:
            score += 50
        elif loc >= SIZE_WARNING_THRESHOLD:
            score += 25

        if "runtime-hotspot" in reasons:
            score += 30
        if "multi-role" in reasons:
            score += 20

        return score

    def _check_change_locality(self) -> dict[str, Any]:
        """Estimate whether changes tend to stay local."""
        locality_issues = []

        for py_file in self.root.glob("src/**/*.py"):
            if not py_file.is_file():
                continue

            try:
                content = py_file.read_text(encoding="utf-8")

                import_count = len(re.findall(r"^from\s+|^import\s+", content, re.MULTILINE))

                if import_count > 20:
                    locality_issues.append(
                        {
                            "file": str(py_file.relative_to(self.root)),
                            "imports": import_count,
                            "locality": "low",
                            "reason": f"Imports {import_count} modules - likely crosses many subsystems",
                        }
                    )
                elif import_count > 12:
                    locality_issues.append(
                        {
                            "file": str(py_file.relative_to(self.root)),
                            "imports": import_count,
                            "locality": "medium",
                            "reason": f"Imports {import_count} modules - moderate subsystem coupling",
                        }
                    )
            except (OSError, UnicodeDecodeError):
                continue

        locality_issues.sort(key=lambda x: x["imports"], reverse=True)

        low_count = sum(1 for i in locality_issues if i["locality"] == "low")
        score = max(0, 100 - low_count * 20 - (len(locality_issues) - low_count) * 10)

        return {
            "issues": locality_issues[:10],
            "low_locality_count": low_count,
            "score": score,
        }

    def _check_contracts(self) -> dict[str, Any]:
        """Check for explicit contract documentation."""
        found = []
        missing = []

        for doc in CONTRACT_DOCS:
            if (self.root / doc).exists():
                found.append(doc)
            else:
                missing.append(doc)

        subsystems_with_contracts = {
            "bridge": "docs/admin-bridge-api.md" in found,
            "data": "docs/DATA_CONTRACT.md" in found,
            "release": "docs/RELEASE.md" in found,
            "fetcher": "docs/fetcher-runtime-contracts.md" in found,
        }

        score = int((len(found) / len(CONTRACT_DOCS)) * 100) if CONTRACT_DOCS else 0

        return {
            "found": found,
            "missing": missing,
            "subsystems": subsystems_with_contracts,
            "score": score,
        }

    def _check_test_routing(self) -> dict[str, Any]:
        """Check test discoverability and routing."""
        layers = []
        guidance_present = False

        if (self.root / "docs/testing.md").exists():
            layers.append("testing_docs")
            guidance_present = True

        if (self.root / "tests").is_dir():
            layers.append("python_tests")

        if (self.root / "tests/frontend").is_dir() or (self.root / "frontend").is_dir():
            layers.append("frontend_tests")

        if self.package_json.exists():
            try:
                with open(self.package_json, encoding="utf-8") as f:
                    pkg = json.load(f)
                scripts = pkg.get("scripts", {})
                if any("test" in s for s in scripts):
                    layers.append("test_scripts")
            except (json.JSONDecodeError, OSError):
                pass

        if (self.root / "src/packaged_desktop_smoke.py").exists():
            layers.append("desktop_smoke")

        score = min(100, len(layers) * 20)

        return {
            "layers": layers,
            "guidance_present": guidance_present,
            "score": score,
        }

    def _check_config_drift(self) -> dict[str, Any]:
        """Detect duplicated config resolution logic."""
        config_patterns = [
            r"os\.getenv",
            r"os\.environ",
            r"argparse",
            r"ArgumentParser",
            r"config\.(?:get|load)",
            r"load.*config",
        ]

        files_with_config = {}

        for py_file in self.root.glob("scripts/**/*.py"):
            self._scan_config_patterns(py_file, files_with_config, config_patterns)

        for py_file in self.root.glob("src/**/*.py"):
            if "ship" in str(py_file) or "bridge" in str(py_file):
                self._scan_config_patterns(py_file, files_with_config, config_patterns)

        score = max(0, 100 - len(files_with_config) * 15)

        return {
            "files": list(files_with_config.keys())[:15],
            "count": len(files_with_config),
            "confidence": "low"
            if len(files_with_config) < 3
            else "medium"
            if len(files_with_config) < 6
            else "high",
            "score": score,
        }

    def _scan_config_patterns(self, file_path: Path, files_with_config: dict, patterns: list[str]):
        """Scan file for config patterns."""
        try:
            content = file_path.read_text(encoding="utf-8")
            matches = 0
            for pattern in patterns:
                matches += len(re.findall(pattern, content))

            if matches >= 2:
                files_with_config[str(file_path.relative_to(self.root))] = matches
        except (OSError, UnicodeDecodeError):
            pass

    def _check_registries(self) -> dict[str, Any]:
        """Check for centralized registries."""
        found = []
        missing = []
        referenced = []

        for registry in CANONICAL_REGISTRIES:
            if (self.root / registry).exists():
                found.append(registry)
                if self._is_registry_referenced(registry):
                    referenced.append(registry)
            else:
                missing.append(registry)

        score = int((len(found) / len(CANONICAL_REGISTRIES)) * 100) if CANONICAL_REGISTRIES else 0

        return {
            "found": found,
            "missing": missing,
            "referenced_in_docs": referenced,
            "score": score,
        }

    def _is_registry_referenced(self, registry: str) -> bool:
        """Check if registry is referenced in docs."""
        registry_name = Path(registry).stem

        for doc in CANONICAL_DOCS:
            doc_path = self.root / doc
            if doc_path.exists():
                try:
                    content = doc_path.read_text(encoding="utf-8")
                    if registry_name in content:
                        return True
                except (OSError, UnicodeDecodeError):
                    pass

        return False

    def _calculate_scores(self):
        """Calculate overall scores and level."""
        pillars = self.results

        weights = {
            "boundary_violations": 1.3,
            "change_locality": 1.2,
            "contracts": 1.0,
            "test_routing": 1.0,
            "config_drift": 0.9,
            "registries": 1.0,
            "hotspots": 1.0,
            "canonical_docs": 0.8,
            "command_discoverability": 0.9,
        }

        total_weight = sum(weights.values())
        weighted_sum = 0.0

        for pillar_name, weight in weights.items():
            pillar_score = pillars.get(pillar_name, {}).get("score", 0)
            weighted_sum += pillar_score * weight

        overall_score = int(weighted_sum / total_weight)

        self.results["overall_score"] = overall_score
        self.results["level"] = self._get_level(overall_score)

    def _get_level(self, score: int) -> str:
        """Determine refactorability level."""
        if score >= 81:
            return "Refactor-Ready"
        elif score >= 61:
            return "Reliable"
        elif score >= 41:
            return "Workable"
        elif score >= 21:
            return "Emerging"
        else:
            return "Fragile"

    def _generate_recommendations(self):
        """Generate ranked improvement recommendations."""
        recommendations = []

        bv = self.results.get("boundary_violations", {})
        if bv.get("count", 0) > 0:
            for v in bv.get("violations", [])[:3]:
                recommendations.append(
                    {
                        "priority": "high",
                        "action": "fix_boundary_violation",
                        "target": v["file"],
                        "recommendation": f"Replace broad import '{v['offending_import']}' with narrower import",
                    }
                )

        hs = self.results.get("hotspots", {})
        if hs.get("count", 0) > 0:
            for h in hs.get("hotspots", [])[:3]:
                recommendations.append(
                    {
                        "priority": "medium",
                        "action": "split_hotspot",
                        "target": h["file"],
                        "recommendation": f"Split oversized file ({h['loc']} LOC) by responsibility",
                    }
                )

        cd = self.results.get("canonical_docs", {})
        if cd.get("missing"):
            for doc in cd.get("missing", [])[:2]:
                recommendations.append(
                    {
                        "priority": "medium",
                        "action": "add_canonical_doc",
                        "target": doc,
                        "recommendation": f"Add missing canonical doc: {doc}",
                    }
                )

        ct = self.results.get("contracts", {})
        if ct.get("missing"):
            recommendations.append(
                {
                    "priority": "medium",
                    "action": "document_contract",
                    "target": ct["missing"][0] if ct["missing"] else None,
                    "recommendation": "Add contract doc for under-documented subsystem",
                }
            )

        tr = self.results.get("test_routing", {})
        if not tr.get("guidance_present"):
            recommendations.append(
                {
                    "priority": "medium",
                    "action": "improve_test_routing",
                    "target": "docs/testing.md",
                    "recommendation": "Add test routing guidance to testing docs",
                }
            )

        self.results["recommendations"] = recommendations[:10]

        self.results["top_hotspots"] = [h["file"] for h in hs.get("hotspots", [])[:5]]

        self.results["top_violations"] = [
            f"{v['file']}: {v['offending_import']}" for v in bv.get("violations", [])[:5]
        ]


def main() -> int:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Analyze AI refactorability of Baluffo")
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="Repository path (default: current directory)",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output JSON file",
    )

    args = parser.parse_args()

    try:
        analyzer = RefactorabilityAnalyzer(args.path)
        results = analyzer.analyze()

        json_output = json.dumps(results, indent=2)

        if args.output:
            Path(args.output).write_text(json_output, encoding="utf-8")
            print(f"Analysis saved to {args.output}")
        else:
            print(json_output)

        print(f"\n{'=' * 60}")
        print(f"AI Refactorability Score: {results['overall_score']}%")
        print(f"Level: {results['level']}")
        print(f"{'=' * 60}")

        print(
            f"\nTop Boundary Violations: {len(results.get('boundary_violations', {}).get('violations', []))}"
        )
        for v in results.get("top_violations", [])[:3]:
            print(f"  - {v}")

        print(f"\nTop Hotspots: {len(results.get('hotspots', {}).get('hotspots', []))}")
        for h in results.get("top_hotspots", [])[:3]:
            print(f"  - {h}")

        print("\nTop Recommendations:")
        for r in results.get("recommendations", [])[:3]:
            print(f"  [{r['priority']}] {r['recommendation']}")

        print(f"{'=' * 60}")

        return 0

    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
