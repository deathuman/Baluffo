"""Guardrail: every policy check must be reachable from a guardrail group.

A ``test_*`` function in one of these modules is not a test until a
``run_*_group`` list in ``repo_guardrails.py`` actually names it. The runner
passes ``repo_root`` and nothing else, so a function that is merely *defined*
never executes -- and nothing in the suite says so.

That is not hypothetical. Three functions went unexecuted this way and were
found by hand, and the hand-fix then had to be done twice: once for
``release_docs_policy`` (``3c2c7e8f``) and again for ``workflow_policy``
(``03bb67c3``). One of the three was not merely unlisted but unlistable: it took
``tmp_path``, which the runner cannot supply, so it had to be deleted rather than
registered. Two of the unlisted ones were also *failing*, so registering them
surfaced real defects.

This module closes that class. It reads the group lists out of
``repo_guardrails.py`` by AST, so it cannot drift from the thing it audits, and
asserts three properties:

1. no defined ``test_*`` is unregistered,
2. no registered name lacks a definition,
3. every registered ``test_*`` is callable with ``repo_root`` alone.

Property 3 is the one that turns "unlisted" into "un-wireable" -- a signature the
runner cannot satisfy is dead on arrival, and the fix is to change the function,
not the list.

Two registration shapes exist in ``repo_guardrails.py`` and both are handled:

- **Explicit lists.** ``("module", name) for name in (...)`` comprehensions, and
  plain ``("module", "name")`` pairs.
- **Discovery.** ``run_compat_group`` enumerates a module with
  ``inspect.getmembers`` and subtracts an ``excluded`` set, so everything except
  the exclusions is registered by construction. The excluded names are then
  registered explicitly by ``run_test_shape_group``. Treating discovery as "not
  listed" would produce a wall of false failures, so it is modelled as what it
  is.

Deliberately plain functions with no pytest import: the CI ``Lint`` lane runs
the guardrail groups without pytest installed, and a module-level
``import pytest`` there turns the whole lane red.
"""

from __future__ import annotations

import ast
from pathlib import Path

POLICY_SUFFIX = "_policy.py"
# The importable module name drops the extension: "workflow_policy.py" is
# imported as "workflow_policy", and the runner reports failures by that name.
_MODULE_SUFFIX = POLICY_SUFFIX.removesuffix(".py")
GROUP_FUNCTION_PREFIX = "run_"
GROUP_FUNCTION_SUFFIX = "_group"


def _string_constants(node: ast.AST) -> list[str] | None:
    """Every element of a tuple/list literal as a string, or None if not all strings."""
    if not isinstance(node, (ast.Tuple, ast.List)):
        return None
    values: list[str] = []
    for element in node.elts:
        if not isinstance(element, ast.Constant) or not isinstance(element.value, str):
            return None
        values.append(element.value)
    return values


def _is_check_name(value: str) -> bool:
    return value.startswith("test_")


def _parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    parents: dict[ast.AST, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _comprehension_registrations(
    node: ast.comprehension, parents: dict[ast.AST, ast.AST]
) -> tuple[str, list[str]] | None:
    """Read one ``("module", name) for name in (...)`` registration.

    The element is a *mixed* tuple -- a string module plus a ``Name`` bound by
    the comprehension -- so it cannot be read with the all-strings helper.

    Returns ``(module, names)``, or None when the comprehension is not that shape.
    """
    names = _string_constants(node.iter)
    if names is None or not names or not all(_is_check_name(n) for n in names):
        return None
    if not isinstance(node.target, ast.Name):
        return None
    target = node.target.id
    parent = parents.get(node)
    if not isinstance(parent, (ast.ListComp, ast.GeneratorExp)):
        return None
    element = parent.elt
    if not isinstance(element, ast.Tuple) or len(element.elts) != 2:
        return None
    module_node, reference_node = element.elts
    if not isinstance(module_node, ast.Constant) or not isinstance(module_node.value, str):
        return None
    if not isinstance(reference_node, ast.Name) or reference_node.id != target:
        return None
    module = module_node.value
    if not module.endswith(_MODULE_SUFFIX):
        return None
    return module, names


def _pair_registrations(node: ast.AST) -> tuple[str, str] | None:
    """Read a literal ``("module", "test_name")`` pair."""
    values = _string_constants(node)
    if values is None or len(values) != 2:
        return None
    module, name = values
    if not module.endswith(_MODULE_SUFFIX):
        return None
    if not _is_check_name(name):
        return None
    return module, name


def _assigned_string_set(node: ast.AST) -> set[str] | None:
    """Read a ``{...}`` set literal whose elements are all strings."""
    if not isinstance(node, ast.Set):
        return None
    values: list[str] = []
    for element in node.elts:
        if not isinstance(element, ast.Constant) or not isinstance(element.value, str):
            return None
        values.append(element.value)
    return set(values)


def _collect_registrations(tree: ast.AST) -> tuple[dict[str, set[str]], set[str]]:
    """Map module -> explicitly registered names, and the discovery modules.

    Discovery modules are registered by construction for every name they define
    except their ``excluded`` set, so they are returned separately and the caller
    fills them in from the definitions.
    """
    parents = _parent_map(tree)
    explicit: dict[str, set[str]] = {}
    discovery: set[str] = set()
    excluded: set[str] = set()

    for function in tree.body:
        if not isinstance(function, ast.FunctionDef):
            continue
        if not function.name.startswith(GROUP_FUNCTION_PREFIX):
            continue
        if not function.name.endswith(GROUP_FUNCTION_SUFFIX):
            continue

        for node in ast.walk(function):
            if isinstance(node, ast.comprehension):
                registration = _comprehension_registrations(node, parents)
                if registration is not None:
                    module, names = registration
                    explicit.setdefault(module, set()).update(names)
            elif isinstance(node, (ast.Tuple, ast.List)):
                pair = _pair_registrations(node)
                if pair is not None:
                    module, name = pair
                    explicit.setdefault(module, set()).add(name)
                elif isinstance(node, ast.Set):
                    assigned = _assigned_string_set(node)
                    if assigned and any(_is_check_name(n) for n in assigned):
                        excluded.update(n for n in assigned if _is_check_name(n))
            elif isinstance(node, ast.Call):
                func = node.func
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr == "import_module"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                ):
                    module = node.args[0].value
                    if module.endswith(POLICY_SUFFIX.removesuffix(".py")):
                        discovery.add(module)

    return explicit, discovery | excluded


def _guardrail_tree(repo_root: Path) -> ast.AST:
    return ast.parse(
        (repo_root / "tools" / "repo_health" / "repo_guardrails.py").read_text(encoding="utf-8")
    )


def _defined_checks(module_path: Path) -> set[str]:
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_check_name(node.name)
    }


def _policy_modules(repo_root: Path) -> dict[str, set[str]]:
    health = repo_root / "tools" / "repo_health"
    return {path.name: _defined_checks(path) for path in sorted(health.glob(f"*{POLICY_SUFFIX}"))}


def _module_name(filename: str) -> str:
    """Importable module name for a policy file: strip the extension, keep ``_policy``.

    ``release_docs_policy.py`` is imported as ``release_docs_policy``, and the
    runner reports failures under that name.
    """
    return filename.removesuffix(".py")


def test_every_policy_check_is_registered_in_a_group(repo_root: Path) -> None:
    """No defined policy check may be unreachable from a guardrail group."""
    explicit, discovery = _collect_registrations(_guardrail_tree(repo_root))
    defined = _policy_modules(repo_root)

    unregistered: list[str] = []
    for filename, names in defined.items():
        module = _module_name(filename)
        if module in discovery:
            # Registered by construction via inspect.getmembers.
            continue
        for name in sorted(names - explicit.get(module, set())):
            unregistered.append(f"{filename}::{name}")

    assert not unregistered, (
        "Policy checks are defined but never registered in a run_*_group list, so they "
        "never execute: "
        + ", ".join(unregistered)
        + ". Add the name to the matching group in tools/repo_health/repo_guardrails.py, "
        "or delete the function if it duplicates a real pytest test."
    )


def test_registered_policy_checks_all_exist(repo_root: Path) -> None:
    """A registered name must resolve to a defined function."""
    explicit, _ = _collect_registrations(_guardrail_tree(repo_root))
    defined = _policy_modules(repo_root)

    dangling: list[str] = []
    for module, names in explicit.items():
        available = defined.get(f"{module}.py", set())
        for name in sorted(names - available):
            dangling.append(f"{module}::{name}")

    assert not dangling, (
        "repo_guardrails.py registers policy checks that do not exist: "
        + ", ".join(dangling)
        + ". A renamed or deleted check leaves a group silently weaker."
    )


def _required_parameters(function: ast.FunctionDef) -> list[str]:
    """Parameter names the caller must supply, ignoring ``self``/``cls``.

    Mirrors ``inspect.signature``: parameters with a default are optional, and
    ``*args`` absorbs anything. Positional defaults bind to the *trailing*
    parameters, so the required ones are the leading slice.
    """
    args = function.args
    positional = [*args.posonlyargs, *args.args]
    required_positional = positional[: len(positional) - len(args.defaults)]
    required_kwonly = [
        arg.arg
        for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=False)
        if default is None
    ]
    names = [arg.arg for arg in required_positional]
    names.extend(required_kwonly)
    return [name for name in names if name not in ("self", "cls")]


def test_registered_policy_checks_are_callable_by_the_runner(repo_root: Path) -> None:
    """Every registered check must be callable the way the runner calls it.

    ``_run_python_check`` does ``check(repo_root=ROOT)`` when the signature has a
    ``repo_root`` parameter and ``check()`` otherwise. A check that *requires* any
    other parameter can never be registered, which is how one dead copy ended up
    duplicated rather than wired: it took ``tmp_path``. Optional parameters are
    fine; only unsatisfiable ones make a check dead on arrival.
    """
    explicit, _ = _collect_registrations(_guardrail_tree(repo_root))
    health = repo_root / "tools" / "repo_health"

    unsatisfiable: list[str] = []
    for module, names in explicit.items():
        module_path = health / f"{module}.py"
        if not module_path.is_file():
            continue
        module_tree = ast.parse(module_path.read_text(encoding="utf-8"))
        functions = {
            node.name: node
            for node in module_tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for name in sorted(names):
            function = functions.get(name)
            if function is None:
                continue
            required = [param for param in _required_parameters(function) if param != "repo_root"]
            if required:
                unsatisfiable.append(f"{module}::{name} (requires {required})")

    assert not unsatisfiable, (
        "Registered policy checks require parameters the guardrail runner cannot "
        "supply; _run_python_check only passes repo_root: "
        + ", ".join(unsatisfiable)
        + ". Take repo_root alone and default the rest, or delete the function."
    )
