#!/usr/bin/env python3
"""Enforce the checked-in Ruff C901 source-complexity baseline."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = ROOT / "scripts" / "complexity_baseline.json"
RUFF_RULE = "C901"
RUFF_THRESHOLD = 10
RUFF_SCOPE = ("src",)

RUFF_MESSAGE_RE = re.compile(r"^`(?P<symbol>.+)` is too complex \((?P<complexity>\d+) > \d+\)$")


class ComplexityBaselineError(ValueError):
    """Raised when the baseline or current findings violate the gate contract."""


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ComplexityBaselineError(f"Duplicate JSON key in baseline: {key}")
        result[key] = value
    return result


def load_baseline(path: Path = BASELINE_PATH) -> dict[str, Any]:
    try:
        return json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_json_keys
        )
    except json.JSONDecodeError as exc:
        raise ComplexityBaselineError(f"Invalid JSON in {path}: {exc}") from exc


def _ruff_version() -> str:
    completed = subprocess.run(
        [sys.executable, "-m", "ruff", "--version"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip() or "no ruff output"
        raise ComplexityBaselineError(f"Unable to determine Ruff version: {details}")
    output = completed.stdout.strip()
    prefix = "ruff "
    if not output.startswith(prefix):
        raise ComplexityBaselineError(f"Unexpected Ruff version output: {output}")
    return output.removeprefix(prefix)


def validate_baseline(baseline: dict[str, Any], *, ruff_version: str) -> dict[str, int]:
    expected_metadata = {
        "ruff_version": ruff_version,
        "rule": RUFF_RULE,
        "threshold": RUFF_THRESHOLD,
        "scope": list(RUFF_SCOPE),
    }
    for key, expected in expected_metadata.items():
        actual = baseline.get(key)
        if actual != expected:
            raise ComplexityBaselineError(f"Baseline {key} must be {expected!r}; found {actual!r}.")

    entries = baseline.get("entries")
    if not isinstance(entries, dict):
        raise ComplexityBaselineError("Baseline entries must be an object keyed by path::symbol.")

    normalized: dict[str, int] = {}
    for key, value in entries.items():
        if not isinstance(key, str) or "::" not in key:
            raise ComplexityBaselineError(f"Invalid baseline entry key: {key!r}")
        if not isinstance(value, int) or value <= RUFF_THRESHOLD:
            raise ComplexityBaselineError(
                f"Baseline entry {key} must be an integer above {RUFF_THRESHOLD}."
            )
        normalized[key] = value
    return normalized


def _relative_posix_path(filename: str) -> str:
    path = Path(filename)
    try:
        relative = path.resolve().relative_to(ROOT)
    except ValueError:
        relative = path
    return relative.as_posix()


def parse_ruff_finding(item: dict[str, Any]) -> tuple[str, int] | None:
    if item.get("code") != RUFF_RULE:
        return None
    message = item.get("message")
    filename = item.get("filename")
    if not isinstance(message, str) or not isinstance(filename, str):
        raise ComplexityBaselineError(f"Malformed Ruff finding: {item!r}")
    match = RUFF_MESSAGE_RE.match(message)
    if not match:
        raise ComplexityBaselineError(f"Unexpected Ruff C901 message: {message}")
    path = _relative_posix_path(filename)
    symbol = match.group("symbol")
    complexity = int(match.group("complexity"))
    return f"{path}::{symbol}", complexity


def findings_by_key(items: Iterable[dict[str, Any]]) -> dict[str, int]:
    findings: dict[str, int] = {}
    for item in items:
        parsed = parse_ruff_finding(item)
        if parsed is None:
            continue
        key, complexity = parsed
        if key in findings:
            raise ComplexityBaselineError(f"Duplicate Ruff complexity finding: {key}")
        findings[key] = complexity
    return findings


def compare_findings(
    baseline_entries: dict[str, int], current_findings: dict[str, int]
) -> list[str]:
    failures: list[str] = []
    for key, complexity in sorted(current_findings.items()):
        allowed = baseline_entries.get(key)
        if allowed is None:
            failures.append(f"New complexity hotspot: {key} is {complexity} > {RUFF_THRESHOLD}")
        elif complexity > allowed:
            failures.append(
                f"Complexity increased: {key} is {complexity}; baseline allows {allowed}"
            )
    return failures


def collect_current_findings() -> dict[str, int]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--select",
            RUFF_RULE,
            "--output-format",
            "json",
            *RUFF_SCOPE,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode not in (0, 1):
        details = completed.stderr.strip() or completed.stdout.strip() or "no ruff output"
        raise ComplexityBaselineError(f"Ruff complexity check failed: {details}")
    try:
        items = json.loads(completed.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise ComplexityBaselineError(f"Ruff emitted invalid JSON: {exc}") from exc
    if not isinstance(items, list):
        raise ComplexityBaselineError("Ruff complexity output must be a JSON list.")
    return findings_by_key(items)


def main() -> int:
    try:
        ruff_version = _ruff_version()
        baseline = load_baseline()
        baseline_entries = validate_baseline(baseline, ruff_version=ruff_version)
        failures = compare_findings(baseline_entries, collect_current_findings())
    except ComplexityBaselineError as exc:
        print(f"Complexity baseline check failed: {exc}", file=sys.stderr)
        return 1

    if failures:
        print("Complexity baseline check failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print("Complexity baseline check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
