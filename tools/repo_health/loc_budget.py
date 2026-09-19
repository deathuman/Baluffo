"""Tracked source line-count budget (anti-regrowth ratchet).

The simplification program removes tens of thousands of lines. Without a gate,
that reduction silently grows back: a helper is re-inlined, a wrapper is
re-added, a split leaf re-accretes. This module makes the reduction monotonic by
comparing the current tracked line count per top-level area against a checked-in
baseline.

Two invariants are enforced together, so the baseline cannot rot:

* **No growth** -- an area may not exceed its baselined line count. A new feature
  legitimately adds lines; that is allowed, but only by editing the baseline in
  the same change, which makes the cost visible in review.
* **No stale entry** -- an area may not fall *below* its baselined count either.
  A reduction that is not ratcheted down would let the removed lines return
  unnoticed, so the baseline must be lowered in the same change that lowers the
  count.

Deliberate scope decisions:

* **Tracked files only.** Discovery goes through ``git ls-files`` rather than a
  filesystem walk. This checkout contains untracked duplicate trees (``tmp/``,
  ``_out/portable-build-cache/``) holding full copies of ``src/`` and
  ``frontend/``; walking the tree would count them and the gate would be
  meaningless. When git is unavailable the check reports that it cannot measure
  rather than silently measuring something else.
* **Areas, not files.** A per-file baseline would fight every legitimate split
  and rename. Per-area tolerates churn inside an area while still blocking net
  growth, which is the property that matters.
* **Source extensions only.** ``docs/`` and ``data/`` are excluded: prose and
  runtime artifacts are not code, and ``data/`` churns on every run.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE_RELATIVE_PATH = "tools/repo_health/loc_baseline.json"

# Extensions counted as source. JSON/YAML/Markdown are excluded: they are data,
# config, or prose, and counting them makes the budget move for non-code reasons.
SOURCE_SUFFIXES = frozenset({".py", ".js", ".mjs", ".cjs", ".ts", ".css", ".html"})

# Top-level prefixes excluded from measurement.
EXCLUDED_PREFIXES = ("docs/", "data/")

_BASELINE_METADATA_KEYS = ("measured", "source_suffixes", "excluded_prefixes")


class LocBudgetError(ValueError):
    """Raised when the baseline file violates the gate contract."""


def _area_of(rel_path: str) -> str:
    """Top-level area owning a repo-relative posix path."""
    head, separator, _ = rel_path.partition("/")
    return head if separator else "<root>"


def measure_areas(repo_root: Path) -> dict[str, int]:
    """Tracked source line count per top-level area.

    Raises ``LocBudgetError`` when the tree is not a git work tree, because any
    fallback would measure untracked build output instead of the real source.
    """
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip() or "no git output"
        raise LocBudgetError(f"unable to list tracked files: {details}")

    areas: dict[str, int] = {}
    for rel_path in completed.stdout.split():
        if rel_path.startswith(EXCLUDED_PREFIXES):
            continue
        if Path(rel_path).suffix not in SOURCE_SUFFIXES:
            continue
        absolute = repo_root / rel_path
        try:
            text = absolute.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        areas[_area_of(rel_path)] = areas.get(_area_of(rel_path), 0) + len(text.splitlines())
    return dict(sorted(areas.items()))


def load_baseline(repo_root: Path) -> dict[str, int]:
    """Baselined line count per area, validated against the gate contract."""
    path = repo_root / BASELINE_RELATIVE_PATH
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise LocBudgetError(f"missing baseline: {BASELINE_RELATIVE_PATH}") from exc
    except json.JSONDecodeError as exc:
        raise LocBudgetError(f"invalid JSON in {BASELINE_RELATIVE_PATH}: {exc}") from exc
    if not isinstance(payload, dict):
        raise LocBudgetError("baseline must be a JSON object")

    entries = payload.get("areas")
    if not isinstance(entries, dict) or not entries:
        raise LocBudgetError("baseline 'areas' must be a non-empty object")
    baseline: dict[str, int] = {}
    for area, count in entries.items():
        if not isinstance(area, str) or not area:
            raise LocBudgetError(f"invalid area name: {area!r}")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise LocBudgetError(f"area {area!r} must be a non-negative integer")
        baseline[area] = count
    return baseline


def compare_areas(baseline: Mapping[str, int], current: Mapping[str, int]) -> list[str]:
    """Failures for growth, for a missing area, and for an un-ratcheted reduction."""
    failures: list[str] = []
    for area in sorted(set(baseline) | set(current)):
        allowed = baseline.get(area)
        actual = current.get(area)
        if allowed is None:
            failures.append(f"new area {area!r} is not baselined ({actual} lines)")
        elif actual is None:
            failures.append(
                f"area {area!r} is baselined but no longer measured; prune it from the baseline"
            )
        elif actual > allowed:
            failures.append(
                f"growth: {area} is {actual} lines; baseline allows {allowed}. "
                f"Justify it and set {area!r} to {actual} in {BASELINE_RELATIVE_PATH}."
            )
        elif actual < allowed:
            failures.append(
                f"stale baseline: {area} is {actual} lines; baseline says {allowed}. "
                f"Ratchet {area!r} down to {actual} in {BASELINE_RELATIVE_PATH} "
                f"(run this tool with --update)."
            )
    return failures


def check_loc_budget(repo_root: Path | None = None) -> list[str]:
    """Fail on area growth and on an un-ratcheted reduction."""
    root = ROOT if repo_root is None else repo_root
    return compare_areas(load_baseline(root), measure_areas(root))


def render_report(current: Mapping[str, int], baseline: Mapping[str, int] | None) -> str:
    """Human-readable per-area table, with baseline delta when available."""
    lines = [f"{'area':<18}{'lines':>10}{'baseline':>10}{'delta':>10}"]
    total = 0
    total_baseline = 0
    for area, count in sorted(current.items(), key=lambda item: -item[1]):
        total += count
        if baseline is None:
            lines.append(f"{area:<18}{count:>10}{'-':>10}{'-':>10}")
            continue
        allowed = baseline.get(area)
        total_baseline += allowed or 0
        if allowed is None:
            lines.append(f"{area:<18}{count:>10}{'?':>10}{'?':>10}")
        else:
            lines.append(f"{area:<18}{count:>10}{allowed:>10}{count - allowed:>+10}")
    lines.append(f"{'TOTAL':<18}{total:>10}")
    if baseline is not None:
        lines[-1] = f"{'TOTAL':<18}{total:>10}{total_baseline:>10}{total - total_baseline:>+10}"
    return "\n".join(lines)


def write_baseline(repo_root: Path, areas: Mapping[str, int]) -> Path:
    """Rewrite the baseline to the measured counts, preserving metadata keys."""
    path = repo_root / BASELINE_RELATIVE_PATH
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}
    if not isinstance(existing, dict):
        existing = {}
    payload: dict[str, object] = {
        key: existing[key] for key in _BASELINE_METADATA_KEYS if key in existing
    }
    payload["_comment"] = (
        "Tracked source line budget. An area may not grow above its count, and a "
        "reduction must be ratcheted down here in the same change. "
        "Regenerate with: python tools/repo_health/loc_budget.py --update"
    )
    payload["areas"] = dict(sorted(areas.items()))
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", action="store_true", help="Print the area table and exit 0.")
    parser.add_argument("--update", action="store_true", help="Rewrite the baseline from the tree.")
    args = parser.parse_args(argv)

    try:
        current = measure_areas(ROOT)
    except LocBudgetError as exc:
        print(f"LOC budget check failed: {exc}", file=sys.stderr)
        return 1

    if args.update:
        path = write_baseline(ROOT, current)
        print(render_report(current, current))
        print(f"\nbaseline written: {path.relative_to(ROOT).as_posix()}")
        return 0

    if args.report:
        try:
            baseline: dict[str, int] | None = load_baseline(ROOT)
        except LocBudgetError:
            baseline = None
        print(render_report(current, baseline))
        return 0

    try:
        failures = check_loc_budget(ROOT)
    except LocBudgetError as exc:
        print(f"LOC budget check failed: {exc}", file=sys.stderr)
        return 1
    if failures:
        print("LOC budget check failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("LOC budget check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
