"""Duplicate function-body guardrail.

Repo guardrail that reports functions whose bodies are byte-identical after
AST normalisation, so copy-pasted helpers are caught at commit time instead of
accumulating silently.

Why AST normalisation rather than a line diff: it ignores formatting, comments
and docstring-free whitespace, so two copies of the same logic are recognised
even when indentation or comments drifted. Docstrings *are* part of the body,
so a re-documented copy counts as a new pattern -- which is the intended
behaviour, because the fix is to reuse the original, not to re-word it.

Two invariants are enforced together, so the baseline cannot rot:

* **No uncovered pattern** -- every qualifying group of identical bodies must be
  baselined. A genuinely new duplication fails immediately.
* **No stale baseline entry** -- every baselined hash must still describe a
  qualifying group. Once a pattern is deduplicated the entry must be pruned in
  the same change; keeping it would let the pattern silently return.

Deliberate scope: only ``src/``, ``scripts/`` and ``tools/`` are scanned (tests
are excluded -- fixtures and stubs are duplicated by design), and two classes of
false positive are filtered before a group can qualify:

* **Trivial bodies** -- a group must average at least ``MIN_AVERAGE_LINES``
  lines, because one-line accessors and ``return dict(value)`` shims are not
  worth coupling across packages.
* **Declarations** -- Protocol/ABC members whose body is ``...``, ``pass`` or
  ``raise NotImplementedError``. These are interface contracts that *must* be
  restated per implementer; the largest such group in this repo has 270 members.
"""

from __future__ import annotations

import ast
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

SCANNED_ROOTS = ("src", "scripts", "tools")

# A group must have at least this many identical copies to qualify.
MIN_COPIES = 3

# ...and must average at least this many lines, to skip one-line shims.
MIN_AVERAGE_LINES = 4

BASELINE_RELATIVE_PATH = "tools/repo_health/duplicate_bodies_baseline.json"

_STUB_BODIES = {"...", "pass"}


@dataclass(frozen=True)
class DuplicatePattern:
    """One group of identical function bodies."""

    digest: str
    locations: tuple[str, ...]
    average_lines: float
    sample_body: str

    @property
    def copy_count(self) -> int:
        return len(self.locations)


def _normalized_body(node: ast.FunctionDef) -> str:
    return ast.dump(ast.Module(body=node.body, type_ignores=[]))


def _body_lines(node: ast.FunctionDef, source_lines: list[str]) -> list[str]:
    start = node.lineno - 1
    end = node.end_lineno or node.lineno
    return source_lines[start:end]


def _is_declaration(sample: str) -> bool:
    """True for Protocol/ABC members, which are restated per implementer."""
    stripped = [
        line.strip()
        for line in sample.splitlines()[1:]
        if line.strip() and not line.strip().startswith("#")
    ]
    if not stripped:
        return True
    if stripped[0] in _STUB_BODIES:
        return True
    if stripped[0].startswith("raise NotImplementedError"):
        return True
    # Docstring-only body.
    return len(stripped) == 1 and stripped[0].startswith(('"""', "'''"))


def collect_duplicate_patterns(repo_root: Path) -> list[DuplicatePattern]:
    """Return every qualifying group of identical function bodies."""
    grouped: dict[str, list[tuple[str, ast.FunctionDef, list[str]]]] = defaultdict(list)

    for root_name in SCANNED_ROOTS:
        root = repo_root / root_name
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.py")):
            try:
                source = path.read_text(encoding="utf-8")
                tree = ast.parse(source)
            except (SyntaxError, UnicodeDecodeError):
                continue
            source_lines = source.splitlines()
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef):
                    continue
                digest = hashlib.sha256(_normalized_body(node).encode()).hexdigest()[:12]
                grouped[digest].append((path.relative_to(repo_root).as_posix(), node, source_lines))

    patterns: list[DuplicatePattern] = []
    for digest, members in grouped.items():
        if len(members) < MIN_COPIES:
            continue
        line_counts = [
            (node.end_lineno or node.lineno) - node.lineno + 1 for _rel, node, _lines in members
        ]
        average = sum(line_counts) / len(line_counts)
        if average < MIN_AVERAGE_LINES:
            continue
        sample = "\n".join(_body_lines(members[0][1], members[0][2]))
        if _is_declaration(sample):
            continue
        patterns.append(
            DuplicatePattern(
                digest=digest,
                locations=tuple(sorted(rel for rel, _node, _lines in members)),
                average_lines=average,
                sample_body=sample,
            )
        )

    return sorted(patterns, key=lambda pattern: (-pattern.copy_count, pattern.digest))


def load_baseline(repo_root: Path) -> set[str]:
    """Return the baselined pattern digests.

    A missing or malformed baseline is reported as an empty set rather than
    raising, so a caller running with a different ``repo_root`` (as the
    guardrail tests do) gets ordinary failure messages instead of a traceback.
    ``check_baseline_file`` surfaces the underlying problem.
    """
    path = repo_root / BASELINE_RELATIVE_PATH
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    patterns = payload.get("patterns") if isinstance(payload, dict) else None
    if not isinstance(patterns, list):
        return set()
    return {str(entry) for entry in patterns}


def check_baseline_file(repo_root: Path) -> list[str]:
    """Fail when the baseline file is missing, unreadable or malformed."""
    path = repo_root / BASELINE_RELATIVE_PATH
    if not path.is_file():
        return [f"{BASELINE_RELATIVE_PATH} is missing; run the duplication gate to seed it."]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{BASELINE_RELATIVE_PATH} is not valid JSON: {exc}"]
    except OSError as exc:
        return [f"{BASELINE_RELATIVE_PATH} could not be read: {exc}"]
    patterns = payload.get("patterns") if isinstance(payload, dict) else None
    if not isinstance(patterns, list):
        return [f"{BASELINE_RELATIVE_PATH} must be an object with a `patterns` list."]
    return []


def list_uncovered_patterns(repo_root: Path) -> list[str]:
    """Qualifying patterns that are not baselined."""
    baselined = load_baseline(repo_root)
    messages: list[str] = []
    for pattern in collect_duplicate_patterns(repo_root):
        if pattern.digest in baselined:
            continue
        locations = "\n".join(f"      {location}" for location in pattern.locations)
        messages.append(
            f"{pattern.digest}: {pattern.copy_count} identical copies "
            f"(avg {pattern.average_lines:.1f} lines)\n{locations}"
        )
    return messages


def list_stale_baseline_entries(repo_root: Path) -> list[str]:
    """Baselined digests that no longer describe a qualifying pattern."""
    baselined = load_baseline(repo_root)
    current = {pattern.digest for pattern in collect_duplicate_patterns(repo_root)}
    return [
        f"{digest} is baselined but no longer duplicated" for digest in sorted(baselined - current)
    ]


def check_duplicate_function_bodies(repo_root: Path) -> list[str]:
    """Fail on new duplicate bodies."""
    return list_uncovered_patterns(repo_root)


def check_duplicate_bodies_stale_baseline(repo_root: Path) -> list[str]:
    """Fail when the baseline is unusable or lists a no-longer-duplicated pattern."""
    return check_baseline_file(repo_root) or list_stale_baseline_entries(repo_root)
