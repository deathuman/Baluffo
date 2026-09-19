"""Dead-branch and leaked-loop-binding policy for repo source.

Two defect classes that no existing gate catches. Verified against the current
toolchain before writing this: ``ruff`` (E, F, I, B, UP, and a trial of SIM, RET,
PIE, PLR), ``vulture --min-confidence=60``, ``mypy``, and ``eslint`` all report
nothing for either class.

1. ``check_noop_branches`` — an ``if`` whose body and the statement that follows
   it return the identical expression, so the condition cannot change the
   outcome. The condition is computed and discarded. Python and JavaScript.

2. ``check_leaked_loop_bindings`` — a name first bound inside a ``for`` body and
   read after that loop ends, with nothing rebinding it in between. The read then
   observes whichever iteration happened to run last (or raises ``NameError`` if
   none ran). This is the shape of the ``source_sync_shard`` stale-``output`` bug,
   where the loop and the leaking read sit at *different* nesting levels.

Both checks are deliberately narrow. A gate that fires on legitimate code gets
disabled, which is worse than no gate:

- No-op branches require **no ``elif``/``else``** and an identical trailing
  return, so a real two-branch dispatch never matches.
- Leaked bindings only fire when the name's *first* assignment in the scope is
  inside the loop. ``result = None`` before a loop that reassigns it is the
  ordinary "last value wins" idiom and never matches.
- Nested function bodies are separate scopes and are analysed on their own.
  Lambda and comprehension parameters are recorded as bindings at their own line,
  so ``lambda row: row[...]`` shadows rather than leaks — that shadowing was the
  source of every false positive in the first attempt at this check.

``ruff``'s F841 already covers the related "assigned but never read" class, so
this module deliberately does not duplicate it.

AI boundary owns: dead-branch and leaked-loop-binding detection only.
AI boundary implement in: this leaf; group wiring in ``repo_guardrails.py``.
AI boundary search before contracts: ``repo_guardrails.py`` GROUPS and the
existing policy modules (``duplicate_body_policy.py``, ``loc_budget.py``).
AI boundary verify: ``python tools/repo_health/repo_guardrails.py --group dead-code``.
"""

from __future__ import annotations

import ast
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

PY_SCANNED_ROOTS = ("src", "scripts", "tools")
JS_SCANNED_ROOTS = ("frontend",)
JS_SKIP_PARTS = ("node_modules",)

BASELINE_FILENAME = "dead_branch_baseline.json"


@dataclass(frozen=True)
class DeadCodeFinding:
    """One detected defect, addressable by ``path:line``."""

    path: str
    line: int
    kind: str
    detail: str

    def key(self) -> str:
        return f"{self.path}:{self.line}"


def _iter_files(root: Path, roots: tuple[str, ...], suffix: str) -> list[Path]:
    found: list[Path] = []
    for name in roots:
        base = root / name
        if not base.is_dir():
            continue
        found.extend(
            path
            for path in sorted(base.rglob(f"*{suffix}"))
            if path.is_file() and not any(part in path.parts for part in JS_SKIP_PARTS)
        )
    return found


# --------------------------------------------------------------------------- #
# Python: no-op branches
# --------------------------------------------------------------------------- #


def _return_expr(stmt: ast.stmt) -> str | None:
    """Normalized source of ``return <expr>``, or None when not a bare return."""
    if isinstance(stmt, ast.Return) and stmt.value is not None:
        try:
            return ast.unparse(stmt.value)
        except Exception:  # pragma: no cover - unparse is total for parsed ASTs
            return None
    return None


def _python_noop_branches(tree: ast.AST) -> list[tuple[int, str]]:
    """Find ``if C: return X`` immediately followed by ``return X``."""
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list):
            continue
        for index, stmt in enumerate(body[:-1]):
            if not isinstance(stmt, ast.If) or stmt.orelse or len(stmt.body) != 1:
                continue
            inside = _return_expr(stmt.body[0])
            following = _return_expr(body[index + 1])
            if inside is not None and inside == following:
                hits.append((stmt.lineno, inside))
    return hits


# --------------------------------------------------------------------------- #
# Python: leaked loop bindings
# --------------------------------------------------------------------------- #


class _ScopeCollector(ast.NodeVisitor):
    """Bindings and reads for exactly one scope; nested scopes are not entered.

    Lambda and comprehension parameters are recorded as bindings at their own
    line without descending, which is what makes ``lambda row: row[...]`` count
    as a shadowing rebind instead of a leaked read.
    """

    def __init__(self) -> None:
        self.assignments: dict[str, list[int]] = defaultdict(list)
        self.reads: dict[str, list[int]] = defaultdict(list)
        self.loops: list[ast.For] = []

    def _bind(self, name: str, lineno: int) -> None:
        self.assignments[name].append(lineno)

    @staticmethod
    def _bind_args(args: ast.arguments, lineno: int) -> list[str]:
        names = [arg.arg for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs)]
        for extra in (args.vararg, args.kwarg):
            if extra is not None:
                names.append(extra.arg)
        return names

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Store):
            self._bind(node.id, node.lineno)
        elif isinstance(node.ctx, ast.Load):
            self.reads[node.id].append(node.lineno)

    def visit_arg(self, node: ast.arg) -> None:
        self._bind(node.arg, node.lineno)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._bind((alias.asname or alias.name).split(".")[0], node.lineno)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            self._bind((alias.asname or alias.name).split(".")[0], node.lineno)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name:
            self._bind(node.name, node.lineno)
        for stmt in node.body:
            self.visit(stmt)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        for name in self._bind_args(node.args, node.lineno):
            self._bind(name, node.lineno)

    def _visit_comprehension(self, node: ast.AST) -> None:
        for generator in getattr(node, "generators", ()):
            for child in ast.walk(generator.target):
                if isinstance(child, ast.Name):
                    self._bind(child.id, node.lineno)

    visit_ListComp = _visit_comprehension
    visit_SetComp = _visit_comprehension
    visit_DictComp = _visit_comprehension
    visit_GeneratorExp = _visit_comprehension

    def _visit_nested(self, node: ast.AST) -> None:
        """Record a nested scope's name without analysing its body here."""
        name = getattr(node, "name", None)
        if name:
            self._bind(name, node.lineno)

    visit_FunctionDef = _visit_nested
    visit_AsyncFunctionDef = _visit_nested
    visit_ClassDef = _visit_nested

    def visit_For(self, node: ast.For) -> None:
        self.loops.append(node)
        self.generic_visit(node)


def _scope_parameters(scope: ast.AST) -> list[ast.arg]:
    """Every parameter bound by a function scope, in declaration order."""
    args = getattr(scope, "args", None)
    if not isinstance(args, ast.arguments):
        return []
    parameters = [*args.posonlyargs, *args.args, *args.kwonlyargs]
    for extra in (args.vararg, args.kwarg):
        if extra is not None:
            parameters.append(extra)
    return parameters


def _leaked_bindings_in_scope(scope: ast.AST) -> list[tuple[int, str]]:
    """Leaked loop bindings for one scope, reported at the leaking read."""
    collector = _ScopeCollector()
    # A function's own parameters bind before any statement in its body, so seed
    # them first. Without this, ``last_error`` in ``_playwright_static_probe``
    # (a parameter reassigned in a loop and read afterwards -- the deliberate
    # "last value wins" accumulator) looks like a name the loop introduced.
    for arg in _scope_parameters(scope):
        collector._bind(arg.arg, arg.lineno)
    for stmt in getattr(scope, "body", []):
        collector.visit(stmt)

    findings: dict[int, str] = {}
    for loop in collector.loops:
        end = loop.end_lineno
        if end is None:
            continue
        for name, lines in collector.assignments.items():
            first = min(lines)
            # Only names the loop itself introduces: a binding that already
            # existed before the loop is the ordinary "last value wins" idiom.
            if not loop.lineno <= first <= end:
                continue
            later = [line for line in collector.reads.get(name, ()) if line > end]
            if not later:
                continue
            read = min(later)
            if any(first < line <= read for line in lines):
                continue
            findings[read] = name
    return sorted((line, name) for line, name in findings.items())


def _walk_scopes(node: ast.AST) -> list[ast.AST]:
    """Every module and function scope in the tree, outermost first."""
    scopes: list[ast.AST] = [node]
    for child in ast.walk(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scopes.append(child)
    return scopes


def _python_leaked_bindings(tree: ast.AST) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for scope in _walk_scopes(tree):
        hits.extend(_leaked_bindings_in_scope(scope))
    return hits


# --------------------------------------------------------------------------- #
# JavaScript: no-op branches
# --------------------------------------------------------------------------- #

_JS_IF_BLOCK = re.compile(r"^\s*if\s*\((?P<cond>.+)\)\s*\{\s*$")
_JS_IF_INLINE = re.compile(r"^\s*if\s*\((?P<cond>.+)\)\s*return\s+(?P<value>.+?);\s*$")
_JS_RETURN = re.compile(r"^\s*return\s+(?P<value>.+?);\s*$")
_JS_CLOSE = re.compile(r"^\s*\}\s*$")


def _js_code(line: str) -> str:
    """Strip a trailing line comment so regex matching sees only code."""
    return line.split("//", 1)[0]


def _js_noop_branches(path: Path) -> list[tuple[int, str]]:
    """Find ``if (C) { return X } return X;`` and its single-line form.

    The block form may put its closing brace between the inner and the trailing
    ``return``, so the two returns are not necessarily adjacent lines -- the
    shape ``if (C) {\\n return X;\\n}\\nreturn X;`` is the common formatting.
    """
    lines = [
        _js_code(line) for line in path.read_text(encoding="utf-8", errors="ignore").splitlines()
    ]
    hits: list[tuple[int, str]] = []
    for index, line in enumerate(lines):
        inline = _JS_IF_INLINE.match(line)
        if inline and index + 1 < len(lines):
            following = _JS_RETURN.match(lines[index + 1])
            if following and following.group("value").strip() == inline.group("value").strip():
                hits.append((index + 1, inline.group("value").strip()))
            continue
        if not _JS_IF_BLOCK.match(line):
            continue
        inner = _JS_RETURN.match(lines[index + 1]) if index + 1 < len(lines) else None
        if not inner:
            continue
        # Step over the branch's closing brace when the body is on its own line.
        after = index + 2
        if after < len(lines) and _JS_CLOSE.match(lines[after]):
            after += 1
        if after >= len(lines):
            continue
        following = _JS_RETURN.match(lines[after])
        if following and following.group("value").strip() == inner.group("value").strip():
            hits.append((index + 1, inner.group("value").strip()))
    return hits


# --------------------------------------------------------------------------- #
# Public checks
# --------------------------------------------------------------------------- #


def collect_findings(repo_root: Path) -> list[DeadCodeFinding]:
    """Every dead-branch and leaked-loop-binding finding in the repo."""
    findings: list[DeadCodeFinding] = []
    for path in _iter_files(repo_root, PY_SCANNED_ROOTS, ".py"):
        relative = path.relative_to(repo_root).as_posix()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError, OSError):
            continue
        for line, expr in _python_noop_branches(tree):
            findings.append(DeadCodeFinding(relative, line, "noop-branch", f"returns {expr!r}"))
        for line, name in _python_leaked_bindings(tree):
            findings.append(
                DeadCodeFinding(relative, line, "leaked-loop-binding", f"'{name}' read after loop")
            )
    for path in _iter_files(repo_root, JS_SCANNED_ROOTS, ".js"):
        relative = path.relative_to(repo_root).as_posix()
        for line, expr in _js_noop_branches(path):
            findings.append(DeadCodeFinding(relative, line, "noop-branch", f"returns {expr!r}"))
    unique = {(finding.path, finding.line): finding for finding in findings}
    return [unique[key] for key in sorted(unique)]


def baseline_path() -> Path:
    return Path(__file__).resolve().parent / BASELINE_FILENAME


def load_baseline() -> set[str] | None:
    """Recorded findings, or None when the baseline is absent or unusable."""
    path = baseline_path()
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    entries = payload.get("findings")
    if not isinstance(entries, list):
        return None
    return {str(entry) for entry in entries}


def write_baseline(repo_root: Path) -> int:
    """Record every current finding. Returns the number recorded."""
    findings = collect_findings(repo_root)
    payload = {
        "findings": [finding.key() for finding in findings],
        "detail": {finding.key(): f"{finding.kind}: {finding.detail}" for finding in findings},
    }
    baseline_path().write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return len(findings)


def check_dead_branches(repo_root: Path) -> list[str]:
    """Fail on any finding not recorded in the baseline."""
    baseline = load_baseline()
    if baseline is None:
        return [
            f"{BASELINE_FILENAME} is missing or unusable; "
            "run `python tools/repo_health/dead_branch_policy.py --update`"
        ]
    return [
        f"{finding.key()}: {finding.kind} ({finding.detail})"
        for finding in collect_findings(repo_root)
        if finding.key() not in baseline
    ]


def check_dead_branch_stale_baseline(repo_root: Path) -> list[str]:
    """Fail when the baseline is unusable or lists a no-longer-detected finding."""
    baseline = load_baseline()
    if baseline is None:
        return [
            f"{BASELINE_FILENAME} is missing or unusable; "
            "run `python tools/repo_health/dead_branch_policy.py --update`"
        ]
    present = {finding.key() for finding in collect_findings(repo_root)}
    return [
        f"{key} is in {BASELINE_FILENAME} but no longer detected; prune it"
        for key in sorted(baseline - present)
    ]


def main(argv: list[str] | None = None) -> int:
    """Standalone entry point: report findings, or ``--update`` the baseline."""
    import sys

    repo_root = Path(__file__).resolve().parents[2]
    args = list(sys.argv[1:] if argv is None else argv)
    if "--update" in args:
        print(f"dead-code baseline: recorded {write_baseline(repo_root)} finding(s)")
        return 0
    findings = collect_findings(repo_root)
    if not findings:
        print("dead-code policy: no findings")
        return 0
    for finding in findings:
        print(f"{finding.key()}: {finding.kind} ({finding.detail})")
    print(f"dead-code policy: {len(findings)} finding(s)")
    return 1


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
