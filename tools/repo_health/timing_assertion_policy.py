"""Wall-clock assertion policy for the Python test suite.

A test that subtracts two clock readings and compares the difference to a literal
is measuring the machine that ran it: the assertion passes on an idle laptop, fails
inside a full suite run, and its failure says nothing about the code. The structural
replacements already exist (``tests/helpers/timing.py``: ``ScriptedClock``,
``install_time_module``, ``EventLog``, ``forbid``); this gate is what makes new
timing tests start from them instead of from a stopwatch.

What counts as a finding
------------------------
An ``assert`` whose ``Compare`` puts a clock-derived expression on one side and a
numeric literal on the other, in the direction that can flake:

- upper bounds -- ``elapsed < 4.0``, ``4.0 > elapsed`` -- the class that fails
  under host load;
- equality -- ``elapsed == 0`` -- never true for a real clock;
- chained forms -- ``assert 0 <= elapsed < 5`` -- judged pair by pair.

Lower bounds (``elapsed >= 0``, ``elapsed > 0``) are deliberately not flagged: a
monotonic value only grows, so load cannot make them flake.

Clock-derived is a small dataflow, not a regex: values that come from
``time.perf_counter`` / ``monotonic`` / ``time`` / ``process_time`` (through
aliases, ``from time import ...`` names, arithmetic, and propagating calls such as
``int``/``round``/``sum``/``max``/``statistics.mean``) are followed into the places
a test puts them: name assignments, literal dict keys and payload fields
(``payload["elapsed"] = ...``, ``result.elapsed = ...``), list builds
(``.append``/``.extend``/``.insert``), dict mutation (``.update``/
``.setdefault``/subscript assignment), and ``.get``/``.values`` reads back out. An
average such as ``sum(samples) / len(samples)`` is therefore still the clock read it
was computed from. Attribute values such as ``ScriptedClock().now`` are not clock
calls, so a scripted-clock assertion with an exact expected number is never flagged
-- that is the shape we want tests to move toward.

The flow crosses module-local helpers as well. Every ``def`` the file calls by name
is summarised as "returns a clock reading when these parameters are clock" (or, for a
returned payload, "these keys carry one when these parameters do"), computed to a
fixed point so a helper that calls another helper still resolves. A call site is
then tainted when its arguments supply the clock, so ``elapsed = measure(t0)`` and
``assert elapsed < 1`` in another function is caught, along with
``assert measure(t0) < 1`` and ``assert payload()["elapsed"] < 1``.

The summary stops at an import boundary -- a helper defined in another module is
not resolved -- and a key that is not a literal is not followed, so those cases
still need the marker or the structural rewrite.

Escape hatches, in order of preference
--------------------------------------
1. Rewrite the assertion structurally (see ``tests/helpers/timing.py``).
2. Mark a deliberate deadline with ``# timing-ok: <reason>``, either on the
   ``assert``'s own lines or on a comment line directly above it. The reason is
   required -- a bare marker does not count -- and it is the review trail for why
   this bound is temporal. A marker on a neighbouring statement's line never
   covers the next ``assert``.
3. Baseline it with ``--update`` only for pre-existing debt that a change is not
   there to fix. The baseline is multiset-exact, so a bound edited to a new number
   is a new finding and needs a deliberate baseline bump, and a removed entry
   fails as stale instead of lingering.

AI boundary owns: wall-clock assertion detection in ``tests/**/*.py`` only.
AI boundary implement in: this leaf; group wiring in ``repo_guardrails.py``.
AI boundary search before contracts: ``repo_guardrails.py`` GROUPS and
``dead_branch_policy.py`` (the baseline idiom this copies).
AI boundary verify: ``python tools/repo_health/repo_guardrails.py --group timing``.
"""

from __future__ import annotations

import ast
import json
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

SCANNED_ROOT = "tests"
SKIP_DIR_NAMES = ("fixtures", "__pycache__")
BASELINE_FILENAME = "timing_assertion_baseline.json"

CLOCK_ATTRS = frozenset({"perf_counter", "monotonic", "time", "process_time"})
PROPAGATING_FUNCS = frozenset(
    {"int", "float", "round", "abs", "max", "min", "sum", "mean", "fmean", "median", "average"}
)
MARKER_RE = re.compile(r"#\s*timing-ok:\s*\S")
# A read the analyser can see always spells one of these in the source: a clock
# attribute (``perf_counter``, ``monotonic``, ``process_time``, ``time``) or the
# import that gives the module its name. Files without one cannot produce a
# finding, so they skip parsing entirely -- 828 of 888 test files in practice.
CLOCK_TOKENS = (
    "perf_counter",
    "monotonic",
    "process_time",
    ".time",
    "from time import",
    "import time",
)


@dataclass(frozen=True)
class TimingAssertion:
    """One wall-clock assertion, addressable by file:line."""

    path: str
    line: int
    source: str

    def key(self) -> str:
        """Baseline key: stable across line moves, sensitive to the compared value."""
        return f"{self.path}::{self.source}"


def _iter_test_files(root: Path) -> list[Path]:
    base = root / SCANNED_ROOT
    if not base.is_dir():
        return []
    return [
        path
        for path in sorted(base.rglob("*.py"))
        if path.is_file() and not any(part in SKIP_DIR_NAMES for part in path.parts)
    ]


def _clock_bindings(tree: ast.AST) -> tuple[set[str], set[str]]:
    """Module names and directly-imported call names that read a clock."""
    modules = {"time"}
    calls: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "time":
                    modules.add(alias.asname or "time")
        elif isinstance(node, ast.ImportFrom) and node.module == "time":
            for alias in node.names:
                if alias.name in CLOCK_ATTRS:
                    calls.add(alias.asname or alias.name)
    return modules, calls


def _literal_key(node: ast.expr) -> str | None:
    """A constant subscript or dict key, spelled the way paths spell it."""
    if isinstance(node, ast.Constant) and isinstance(node.value, (str, int)):
        if not isinstance(node.value, bool):
            return repr(node.value)
    return None


def _path_of(node: ast.expr) -> str | None:
    """Canonical access path for a name, attribute chain, or literal-key subscript."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _path_of(node.value)
        return f"{base}.{node.attr}" if base else None
    if isinstance(node, ast.Subscript):
        base = _path_of(node.value)
        key = _literal_key(node.slice)
        return f"{base}[{key}]" if base and key is not None else None
    return None


def _tainted(path: str, tainted: set[str]) -> bool:
    """True when ``path`` was written from a clock, or sits inside a tainted container."""
    if path in tainted:
        return True
    return any(path.startswith((f"{base}.", f"{base}[")) for base in tainted)


def _scope_nodes(body: list[ast.stmt]) -> list[ast.AST]:
    """Nodes belonging to one scope; a nested function or class body has its own."""
    nodes: list[ast.AST] = []
    stack: list[ast.AST] = list(body)
    while stack:
        node = stack.pop()
        nodes.append(node)
        stack.extend(
            child
            for child in ast.iter_child_nodes(node)
            if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        )
    return nodes


def _param_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[str, ...]:
    """Positional and keyword-only parameter names, in call order."""
    args = node.args
    return tuple(arg.arg for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs))


def _mutation_bindings(call: ast.Call) -> list[tuple[str, ast.expr]]:
    """Container writes done through a method: append/extend/insert/update/setdefault."""
    func = call.func
    if not isinstance(func, ast.Attribute):
        return []
    base = _path_of(func.value)
    if base is None:
        return []
    args = call.args
    if func.attr in {"append", "extend", "insert"}:
        return [(base, args[-1])] if args else []
    if func.attr == "setdefault":
        key = _literal_key(args[0]) if args else None
        return [(f"{base}[{key}]", args[1])] if key and len(args) > 1 else []
    if func.attr == "update":
        bindings = [(base, args[0])] if args else []
        bindings.extend(
            (f"{base}[{repr(keyword.arg)}]", keyword.value)
            for keyword in call.keywords
            if keyword.arg
        )
        return bindings
    return []


def _assigned_bindings(nodes: Iterable[ast.AST]) -> list[tuple[str, ast.expr]]:
    """Every ``path <- value`` write among ``nodes``, in walk order.

    Collected raw: splitting a written dict into its keys needs the taint state,
    so it happens in ``_expanded`` during the fixed point. Module-wide rather than
    per-scope on purpose -- the rule only fires when the value is compared to a
    numeric literal, so a name reused across two tests cannot produce a finding by
    itself, and a payload field written in a helper resolves where it is read.
    """
    bindings: list[tuple[str, ast.expr]] = []
    for node in nodes:
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                path = _path_of(target)
                if path is not None:
                    bindings.append((path, node.value))
        elif isinstance(node, ast.AugAssign):
            path = _path_of(node.target)
            if path is not None:
                bindings.append((path, node.value))
        elif isinstance(node, ast.Call):
            bindings.extend(_mutation_bindings(node))
    return bindings


@dataclass(frozen=True)
class _Summary:
    """What a module-local helper returns, as far as the clock reaches.

    ``scalar`` is ``None`` when no return carries a clock reading; otherwise it
    names the parameters whose taint is enough to make one carry it (empty means
    the return is a clock reading whatever the arguments). ``keys`` does the same
    per returned dict key, so a payload helper does not taint its counters.
    """

    params: tuple[str, ...] = ()
    scalar: frozenset[str] | None = None
    keys: tuple[tuple[str, frozenset[str]], ...] = ()


@dataclass
class _Flow:
    """One file's clock state: readers, tainted paths, and helper summaries."""

    modules: set[str]
    imported_calls: set[str]
    tainted: set[str] = field(default_factory=set)
    summaries: dict[str, _Summary] = field(default_factory=dict)

    def trial(self, seed: set[str]) -> _Flow:
        """A copy whose taint starts with ``seed``, sharing the helper summaries."""
        return _Flow(
            modules=self.modules,
            imported_calls=self.imported_calls,
            tainted=self.tainted | seed,
            summaries=self.summaries,
        )


def _call_summary(call: ast.Call, flow: _Flow) -> _Summary | None:
    """The summary of the module-local helper this call names, if there is one."""
    func = call.func
    return flow.summaries.get(func.id) if isinstance(func, ast.Name) else None


def _deps_met(deps: frozenset[str], call: ast.Call, summary: _Summary, flow: _Flow) -> bool:
    """Whether the call's arguments supply the clock a helper's return depends on."""
    if not deps:
        return True
    if any(isinstance(arg, ast.Starred) for arg in call.args):
        return False
    bound: dict[str, ast.expr] = dict(zip(summary.params, call.args, strict=False))
    bound.update({keyword.arg: keyword.value for keyword in call.keywords if keyword.arg})
    return all(name in bound and _is_clock_expr(bound[name], flow) for name in deps)


def _expanded(path: str, value: ast.expr, flow: _Flow) -> list[tuple[str, ast.expr | None]]:
    """Bindings one write stands for, with ``None`` meaning "the clock already".

    A literal dict splits into per-key bindings so a sibling field is not tainted;
    a helper that returns a payload splits the same way from its summary, while a
    helper returning a duration taints the whole target.
    """
    if isinstance(value, ast.Call):
        summary = _call_summary(value, flow)
        if summary is not None:
            returned: list[tuple[str, ast.expr | None]] = [
                (f"{path}[{key}]", None)
                for key, deps in summary.keys
                if _deps_met(deps, value, summary, flow)
            ]
            if summary.scalar is not None and _deps_met(summary.scalar, value, summary, flow):
                returned.insert(0, (path, None))
            if returned:
                return returned
        func = value.func
        if isinstance(func, ast.Name) and func.id == "dict" and value.keywords:
            return [
                (f"{path}[{repr(keyword.arg)}]", keyword.value)
                for keyword in value.keywords
                if keyword.arg
            ]
    if not isinstance(value, ast.Dict):
        return [(path, value)]
    bindings: list[tuple[str, ast.expr | None]] = []
    for key, item in zip(value.keys, value.values, strict=True):
        literal = _literal_key(key) if key is not None else None
        if literal is None:
            return [(path, value)]
        bindings.append((f"{path}[{literal}]", item))
    return bindings


def _call_reads_clock(node: ast.Call, flow: _Flow) -> bool:
    """Clock readers, payload lookups, helper returns, and reducers over clock values."""
    summary = _call_summary(node, flow)
    if summary is not None:
        return summary.scalar is not None and _deps_met(summary.scalar, node, summary, flow)
    func = node.func
    tainted = flow.tainted

    def prop(expr: ast.expr) -> bool:
        return _is_clock_expr(expr, flow)

    if isinstance(func, ast.Attribute):
        if isinstance(func.value, ast.Name) and func.value.id in flow.modules:
            return func.attr in CLOCK_ATTRS
        base = _path_of(func.value)
        if func.attr in PROPAGATING_FUNCS:
            # A reducer over a tainted container (``samples.mean()``) needs no args.
            if base is not None and _tainted(base, tainted):
                return True
            return any(prop(arg) for arg in node.args)
        if base is None:
            return False
        key = _literal_key(node.args[0]) if node.args else None
        if func.attr == "get" and key is not None:
            return _tainted(f"{base}[{key}]", tainted)
        return func.attr == "values" and _tainted(base, tainted)
    if isinstance(func, ast.Name):
        if func.id in flow.imported_calls:
            return True
        if func.id in PROPAGATING_FUNCS:
            return any(prop(arg) for arg in node.args)
    return False


def _is_clock_expr(node: ast.expr, flow: _Flow) -> bool:
    path = _path_of(node)
    if path is not None and _tainted(path, flow.tainted):
        return True
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Call):
        key = _literal_key(node.slice)
        summary = _call_summary(node.value, flow)
        if key is not None and summary is not None:
            if any(
                returned == key and _deps_met(deps, node.value, summary, flow)
                for returned, deps in summary.keys
            ):
                return True
    if isinstance(node, (ast.Attribute, ast.Subscript)):
        base = _path_of(node.value)
        return base is not None and _tainted(base, flow.tainted)
    if isinstance(node, ast.Call):
        return _call_reads_clock(node, flow)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return any(_is_clock_expr(element, flow) for element in node.elts)
    if isinstance(node, ast.BinOp):
        return _is_clock_expr(node.left, flow) or _is_clock_expr(node.right, flow)
    if isinstance(node, ast.UnaryOp):
        return _is_clock_expr(node.operand, flow)
    if isinstance(node, ast.IfExp):
        return _is_clock_expr(node.body, flow) or _is_clock_expr(node.orelse, flow)
    return False


def _is_numeric_expr(node: ast.expr, constants: dict[str, float]) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return not isinstance(node.value, bool)
    if isinstance(node, ast.Name):
        return node.id in constants
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        return _is_numeric_expr(node.operand, constants)
    if isinstance(node, ast.BinOp):
        return _is_numeric_expr(node.left, constants) and _is_numeric_expr(node.right, constants)
    return False


def _function_summary(node: ast.FunctionDef | ast.AsyncFunctionDef, flow: _Flow) -> _Summary:
    """Which of a helper's returns carry a clock, and from which parameters.

    Each parameter is trialled on its own: taint is an OR over sub-expressions, so
    a return carries a clock when it is unconditional or some one parameter
    supplied it, and the union of those parameters is the summary's dependency set.
    """
    nodes = _scope_nodes(node.body)
    bindings = _assigned_bindings(nodes)
    returns: list[ast.expr] = []
    for child in nodes:
        if isinstance(child, ast.Return) and child.value is not None:
            returns.append(child.value)
    params = _param_names(node)
    scalar: set[str] | None = None
    keys: dict[str, set[str]] = {}
    for seed in (None, *params):
        trial = flow.trial({seed} if seed else set())
        _tainted_paths(bindings, trial)
        deps = {seed} if seed else set()
        for value in returns:
            if isinstance(value, ast.Dict):
                for key, item in zip(value.keys, value.values, strict=True):
                    literal = _literal_key(key) if key is not None else None
                    if literal is not None and _is_clock_expr(item, trial):
                        keys.setdefault(literal, set()).update(deps)
            elif _is_clock_expr(value, trial):
                if scalar is None:
                    scalar = set()
                scalar.update(deps)
    return _Summary(
        params=params,
        scalar=None if scalar is None else frozenset(scalar),
        keys=tuple(sorted((key, frozenset(deps)) for key, deps in keys.items())),
    )


def _summarize(tree: ast.Module, flow: _Flow) -> None:
    """Fixed point over the module's helpers, with module-scope clock constants as base."""
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    functions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in called
    ]
    module_bindings = _assigned_bindings(_scope_nodes(tree.body))
    for _ in range(8):
        _tainted_paths(module_bindings, flow)
        before = dict(flow.summaries)
        for function in functions:
            flow.summaries[function.name] = _function_summary(function, flow)
        if flow.summaries == before:
            break


def _tainted_paths(bindings: list[tuple[str, ast.expr]], flow: _Flow) -> set[str]:
    """Fixed point over the module's writes: which paths hold a clock reading."""
    changed = True
    while changed:
        changed = False
        for path, value in bindings:
            for target, source in _expanded(path, value, flow):
                if target in flow.tainted:
                    continue
                if source is None or _is_clock_expr(source, flow):
                    flow.tainted.add(target)
                    changed = True
    return flow.tainted


def _constant_names(bindings: list[tuple[str, ast.expr]]) -> dict[str, float]:
    """Bare names whose every binding is a numeric literal expression."""
    values: dict[str, list[ast.expr]] = {}
    for path, value in bindings:
        if path.isidentifier():
            values.setdefault(path, []).append(value)
    constants: dict[str, float] = {}
    changed = True
    while changed:
        changed = False
        for name, exprs in values.items():
            if name in constants:
                continue
            if exprs and all(_is_numeric_expr(expr, constants) for expr in exprs):
                constants[name] = 0.0
                changed = True
    return constants


def _flakes(compare: ast.Compare, *, clock, numeric) -> bool:
    """True when a pair in the comparison can flake under host load."""
    sides = [compare.left, *compare.comparators]
    for index, op in enumerate(compare.ops):
        left, right = sides[index], sides[index + 1]
        if numeric(left) and numeric(right):
            continue  # two literals cannot flake
        if clock(left) and numeric(right) and isinstance(op, (ast.Lt, ast.LtE, ast.Eq, ast.NotEq)):
            return True
        if numeric(left) and clock(right) and isinstance(op, (ast.Gt, ast.GtE, ast.Eq, ast.NotEq)):
            return True
    return False


def _has_marker(lines: list[str], node: ast.Assert) -> bool:
    """A `# timing-ok: <reason>` marker on the assert, or on a comment above it.

    A marker on a *neighbouring statement's* line must not leak: the preceding
    line only counts when it is itself a comment, not when it is another assert
    that happens to carry a marker.
    """
    end = node.end_lineno or node.lineno
    for index in range(node.lineno, min(len(lines), end) + 1):
        if MARKER_RE.search(lines[index - 1]):
            return True
    previous = node.lineno - 1
    if previous >= 1 and lines[previous - 1].lstrip().startswith("#"):
        return MARKER_RE.search(lines[previous - 1]) is not None
    return False


def _findings_in_file(path: Path, relative: str) -> list[TimingAssertion]:
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    if not any(token in text for token in CLOCK_TOKENS):
        return []
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    modules, imported_calls = _clock_bindings(tree)
    flow = _Flow(modules, imported_calls)
    bindings = _assigned_bindings(ast.walk(tree))
    _summarize(tree, flow)
    if not _tainted_paths(bindings, flow):
        return []
    constants = _constant_names(bindings)
    lines = text.splitlines()

    def clock(node: ast.expr) -> bool:
        return _is_clock_expr(node, flow)

    def numeric(node: ast.expr) -> bool:
        return _is_numeric_expr(node, constants)

    findings: list[TimingAssertion] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assert) or not isinstance(node.test, ast.Compare):
            continue
        if _has_marker(lines, node):
            continue
        if not _flakes(node.test, clock=clock, numeric=numeric):
            continue
        try:
            source = " ".join(ast.unparse(node).split())
        except Exception:  # pragma: no cover - unparse is total for parsed ASTs
            source = "assert <unparseable>"
        findings.append(TimingAssertion(relative, node.lineno, source))
    return findings


def collect_findings(repo_root: Path) -> list[TimingAssertion]:
    """Every unmarked wall-clock assertion in the Python test suite."""
    findings: list[TimingAssertion] = []
    for path in _iter_test_files(repo_root):
        relative = path.relative_to(repo_root).as_posix()
        findings.extend(_findings_in_file(path, relative))
    return sorted(findings, key=lambda finding: (finding.path, finding.line))


def baseline_path() -> Path:
    return Path(__file__).resolve().parent / BASELINE_FILENAME


def load_baseline() -> list[str] | None:
    """Recorded finding keys (duplicates kept), or None when unusable."""
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
    return [str(entry) for entry in entries]


def write_baseline(repo_root: Path) -> int:
    """Record every current finding. Returns the number recorded."""
    findings = collect_findings(repo_root)
    payload = {
        "_comment": (
            "Wall-clock assertions baselined when the gate landed. This is debt, not "
            "policy: a new bound must be structural or carry `# timing-ok: <reason>`. "
            "Regenerate with: python tools/repo_health/timing_assertion_policy.py --update"
        ),
        "findings": sorted(finding.key() for finding in findings),
    }
    baseline_path().write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8", newline="\n")
    return len(findings)


def _baseline_missing() -> list[str]:
    return [
        f"{BASELINE_FILENAME} is missing or unusable; "
        "run `python tools/repo_health/timing_assertion_policy.py --update`"
    ]


def check_timing_assertions(
    repo_root: Path, findings: list[TimingAssertion] | None = None
) -> list[str]:
    """Fail on any wall-clock assertion not recorded in the baseline.

    ``findings`` lets a caller that already scanned the suite reuse one parse for
    the new-and-stale pair; the guardrail group runs both checks per commit.
    """
    baseline = load_baseline()
    if baseline is None:
        return _baseline_missing()
    allowed = Counter(baseline)
    if findings is None:
        findings = collect_findings(repo_root)
    surplus = Counter(finding.key() for finding in findings) - allowed
    messages: list[str] = []
    for finding in findings:
        key = finding.key()
        if surplus[key] <= 0:
            continue
        surplus[key] -= 1
        messages.append(f"{finding.path}:{finding.line}: {finding.source}")
    return messages


def check_timing_assertion_stale_baseline(
    repo_root: Path, findings: list[TimingAssertion] | None = None
) -> list[str]:
    """Fail when the baseline lists a finding that no longer exists."""
    baseline = load_baseline()
    if baseline is None:
        return _baseline_missing()
    if findings is None:
        findings = collect_findings(repo_root)
    found = Counter(finding.key() for finding in findings)
    stale = Counter(baseline) - found
    return [
        f"{key} is in {BASELINE_FILENAME} but no longer detected; prune it with --update"
        for key in sorted(stale)
    ]


def main(argv: list[str] | None = None) -> int:
    """Standalone entry point: report findings, or ``--update`` the baseline."""
    import sys

    repo_root = Path(__file__).resolve().parents[2]
    args = list(sys.argv[1:] if argv is None else argv)
    if "--update" in args:
        print(f"timing baseline: recorded {write_baseline(repo_root)} finding(s)")
        return 0
    findings = collect_findings(repo_root)
    for finding in findings:
        print(f"{finding.path}:{finding.line}: {finding.source}")
    print(f"timing policy: {len(findings)} wall-clock assertion(s)")
    return 1 if findings else 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
