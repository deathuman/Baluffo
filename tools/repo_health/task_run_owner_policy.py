"""Task-run owner-fields guardrail.

Repo guardrail that fails when a ``start_run`` call site creates an active
lifecycle row without ownership metadata, so the owner-less zombie rows that
once blocked every task launch cannot be reintroduced by new call sites.

Background: rows in the task_runs projection with ``status='running'`` and no
``ownerKind``/``ownerPid`` could never be reaped by the pid/owner checks in
``src.bridge.lifecycle_cleanup``, and once their heartbeat went cold they
blocked all subsequent task launches (observed live: two orphaned sync stubs
disabled the Update-jobs button for hours). Every production ``start_run``
caller now passes owner fields; this guardrail keeps it that way.

Rule: each ``.start_run(...)`` / ``start_lifecycle_run(...)`` call in ``src/``
must pass ``owner_kind=`` or ``owner_pid=`` (either one satisfies the check;
``owner_kind="pipeline"`` rows are reaped by owner-kind, pid rows by the pid
check). Terminal writers (``finish_run``/``fail_run``/``cancel_run``/
``orphan_run``/``heartbeat_run``) update existing rows and are out of scope.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Facade/entrypoint names whose keyword arguments are forwarded verbatim to
# TaskLifecycleService.start_run. Both shapes appear in src/:
#   .start_run(run_id=..., task_type=..., ...)          (service + facade)
#   start_lifecycle_run(run_id=..., task_type=..., ...) (bound dependency)
START_RUN_CALLEE_NAMES = {"start_run", "start_lifecycle_run"}

OWNER_KEYWORDS = ("owner_kind", "owner_pid")

# Call expressions whose receiver/qualifier basename is one of these are
# pass-through delegations or data builders, not real starts:
# - AdminTaskLifecycle.start_run(**kwargs) forwards to TaskLifecycleService
#   verbatim; ownership is enforced at the actual call sites.
# - dict/typing constructors and payload builders that merely *name* the kwarg.
QUALIFIER_ALLOWLIST = frozenset(
    {
        "self",
        # dict/typing constructors and payload builders that merely *name* the
        # kwarg (e.g. pytest fixtures, spec dicts)
        "dict",
        "build_live_task_payload",
    }
)


def _call_callee_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _call_qualifier_name(node: ast.Call) -> str:
    """Return the receiver basename for attribute calls, '' otherwise."""
    func = node.func
    if isinstance(func, ast.Attribute):
        value = func.value
        if isinstance(value, ast.Name):
            return value.id
        if isinstance(value, ast.Attribute):
            return value.attr
    return ""


def _is_kwargs_forwarder(node: ast.Call, tree: ast.AST) -> bool:
    """True when the call is exactly ``...callee(**kwargs)`` inside a function
    that binds ``**kwargs``.

    Delegation shims (e.g. AdminTaskLifecycle.start_run forwarding verbatim to
    TaskLifecycleService.start_run) do not create rows themselves; ownership
    is enforced at the call sites this guardrail already scans. A call whose
    single argument is a bare ``**kwargs`` spread inside a kwargs-binding
    function is a forwarder by construction.
    """
    if not (len(node.args) == 0 and len(node.keywords) == 1):
        return False
    only_kwarg = node.keywords[0]
    if only_kwarg.arg is not None or not isinstance(only_kwarg.value, ast.Name):
        return False
    if only_kwarg.value.id != "kwargs":
        return False
    for item in ast.walk(tree):
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for child in ast.walk(item):
                if child is node:
                    return item.args.kwarg is not None
    return False


def _passed_owner_keyword(node: ast.Call) -> str | None:
    for keyword in node.keywords:
        if keyword.arg in OWNER_KEYWORDS:
            return keyword.arg
    return None


def find_ownerless_start_run_calls(source: str, filename: str) -> list[str]:
    """Return human-readable failures for owner-less start_run calls in source."""
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as exc:  # pragma: no cover - parse errors surface elsewhere
        return [f"{filename}: could not parse: {exc}"]

    failures: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = _call_callee_name(node)
        if callee not in START_RUN_CALLEE_NAMES:
            continue
        if _call_qualifier_name(node) in QUALIFIER_ALLOWLIST:
            continue
        if _is_kwargs_forwarder(node, tree):
            continue
        if _passed_owner_keyword(node) is None:
            failures.append(
                f"{filename}:{node.lineno}: {callee}(...) omits owner_kind/owner_pid; "
                "an active row without ownership metadata can never be reaped by "
                "lifecycle_cleanup and will block task launches once its heartbeat "
                "goes cold. Pass owner_kind (e.g. 'process' + owner_pid for spawned "
                "children, 'bridge_thread' for in-process workers, 'pipeline' for "
                "pipeline-owned rows) or owner_pid."
            )
    return failures


def check_task_run_start_owner_fields(repo_root: Path | None = None) -> list[str]:
    """Scan src/ for start_run call sites missing owner_kind/owner_pid."""
    root = repo_root or ROOT
    src_root = root / "src"
    if not src_root.is_dir():
        return [f"src directory is missing: {src_root}"]

    failures: list[str] = []
    for path in sorted(src_root.rglob("*.py")):
        rel_path = path.relative_to(root).as_posix()
        try:
            source = path.read_text(encoding="utf-8")
        except OSError as exc:
            failures.append(f"{rel_path}: could not read: {exc}")
            continue
        if "start_run" not in source and "start_lifecycle_run" not in source:
            continue
        failures.extend(find_ownerless_start_run_calls(source, rel_path))
    return failures


__all__ = [
    "START_RUN_CALLEE_NAMES",
    "check_task_run_start_owner_fields",
    "find_ownerless_start_run_calls",
]
