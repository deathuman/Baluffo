"""Shared helpers for the ``tools/repo_health`` AST inventory tools.

The bridge-API, desktop-update, updater and update-manager inventory tools all
parse Python sources, walk the repo's Python paths, print a JSON inventory, and
project their row dataclasses to JSON. Those helpers were byte-identical copies
before this module existed; they live here now so the tools keep only their own
scanning logic.

Import style: the tools are imported both ways in this repo — ``repo_guardrails``
adds this directory to ``sys.path`` and imports them flat, while tests import them
as ``tools.repo_health.<tool>``. Each tool therefore tries the qualified import
first and falls back to the flat one, so one module identity serves both.
"""

from __future__ import annotations

import argparse
import ast
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def parse_python(path: Path) -> ast.Module:
    """Parse one Python file, attributing syntax errors to its real path."""
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def iter_python_paths(repo_root: Path) -> list[Path]:
    """Every ``*.py`` under the scanned source roots, sorted and deduplicated."""
    roots = (
        repo_root / "src",
        repo_root / "tests",
        repo_root / "tools",
        repo_root / "scripts",
    )
    paths: list[Path] = []
    for root in roots:
        if root.exists():
            paths.extend(sorted(root.rglob("*.py")))
    return sorted(paths)


def relative_posix(path: Path, repo_root: Path) -> str:
    """Repo-relative posix path, so reports read the same on every platform."""
    return path.relative_to(repo_root).as_posix()


def print_inventory(inventory: tuple[Any, ...]) -> None:
    """Print a row inventory as the stable sorted-key JSON contract."""
    print(json.dumps([row.as_json() for row in inventory], indent=2, sort_keys=True))


class NameCategoriesReferencesRow:
    """JSON projection shared by rows keyed by name/categories/references.

    Mixin only: the concrete dataclasses keep their own fields, so their
    constructor, equality and hash behaviour is unchanged.
    """

    def as_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "categories": list(self.categories),
            "references": list(self.references),
        }


class PathModuleLineCategoriesRow:
    """JSON projection shared by rows keyed by path/module/line/categories."""

    def as_json(self) -> dict[str, object]:
        return {
            "path": self.path,
            "module": self.module,
            "line": self.line,
            "categories": list(self.categories),
        }


# --------------------------------------------------------------------------
# Shared inventory orchestration
#
# Five tools in this directory share one shape: scan some Python sources, match
# a syntactic pattern, project rows, then assert four or five properties over
# them. Only the *pattern* is genuinely per-tool, so only the pattern stays
# per-tool; the orchestration lives here once.
#
# Every spec is built inside the calling function from that module's globals,
# never at import time. The tests monkeypatch `EXPECTED_*`, `CLASSIFIED_IMPORTS`,
# `DEPENDENCY_CATEGORIES` and the allowlists on the wrapper modules, so a spec
# captured at import would silently ignore every one of those patches.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ImportInventorySpec:
    """One ``(path, module, line, categories)`` import inventory.

    ``count_hint`` is the sentence that follows "expected N." and is per-tool
    because the tests assert these messages verbatim. ``extra_row_check`` exists
    for the one tool whose per-row rule is not the shared allowlist: the runtime
    facade inventory fails on *every* row, because any import of the facade from
    ``src/ship`` is the defect it exists to catch.
    """

    row_type: type[Any]
    label: str
    entity: str
    count_hint: str
    expected_count: int
    classified: dict[str, set[str]]
    known_categories: frozenset[str]
    allowlist: frozenset[str]
    allowlist_message: str
    include_categories: bool
    iter_paths: Callable[[Path], list[Path]] = iter_python_paths
    # The common case is "match these modules", which the core can build itself.
    # A tool whose pattern is genuinely different passes `detect` instead; the
    # runtime facade inventory does, because it must also catch bare and
    # relative imports.
    facade_targets: frozenset[str] = frozenset()
    facade_parent: str = ""
    detect: Callable[[Path], list[tuple[str, int]]] | None = None
    enforce_allowlist: bool = True
    extra_row_check: Callable[[Any], list[str]] | None = None

    def detector(self) -> Callable[[Path], list[tuple[str, int]]]:
        if self.detect is not None:
            return self.detect
        targets = self.facade_targets
        parent = self.facade_parent
        return lambda path: facade_import_references(path, targets, parent)


def collect_import_inventory(spec: ImportInventorySpec, repo_root: Path) -> tuple[Any, ...]:
    """Project every detected import as a row, sorted by path/line/module."""
    rows: list[Any] = []
    for path in spec.iter_paths(repo_root):
        relative = relative_posix(path, repo_root)
        categories = tuple(sorted(spec.classified.get(relative, set())))
        for module, line in spec.detector()(path):
            fields: dict[str, object] = {"path": relative, "module": module, "line": line}
            if spec.include_categories:
                fields["categories"] = categories
            rows.append(spec.row_type(**fields))
    return tuple(sorted(rows, key=lambda row: (row.path, row.line, row.module)))


def check_import_inventory(spec: ImportInventorySpec, repo_root: Path) -> list[str]:
    """Assert count, category vocabulary, classification coverage and allowlist.

    The message wording is per-tool and is carried in the spec rather than
    generated, because these strings are asserted verbatim by the tests and are
    the user-facing output of ``--check``.
    """
    inventory = collect_import_inventory(spec, repo_root)
    failures: list[str] = []

    if len(inventory) != spec.expected_count:
        failures.append(
            f"{spec.label} has {len(inventory)} import records; "
            f"expected {spec.expected_count}. {spec.count_hint}"
        )

    unknown_categories = {
        category
        for categories in spec.classified.values()
        for category in categories
        if category not in spec.known_categories
    }
    if unknown_categories:
        failures.append(f"{spec.label} has unknown categories: {sorted(unknown_categories)}.")

    discovered_paths = {row.path for row in inventory}
    for missing in sorted(set(spec.classified) - discovered_paths):
        failures.append(f"{spec.entity} classification for {missing} has no matching import.")

    for row in inventory:
        if spec.include_categories and not row.categories:
            failures.append(
                f"{spec.entity} import is unclassified: {row.path}:{row.line} imports {row.module}."
            )
        if not spec.enforce_allowlist:
            if spec.extra_row_check is not None:
                failures.extend(spec.extra_row_check(row))
        elif row.path.startswith("src/") and row.path not in spec.allowlist:
            failures.append(
                f"{spec.allowlist_message}: {row.path}:{row.line} imports {row.module}."
            )
    return failures


@dataclass(frozen=True)
class RootDependencySpec:
    """One ``(name, categories, references)`` root-binding dependency inventory."""

    row_type: type[Any]
    label: str
    entity: str
    dependency_name: str
    tracked_modules: tuple[str, ...]
    expected_dependency_count: int
    expected_reference_count: int
    categories: dict[str, set[str]]
    known_categories: frozenset[str]
    count_hint: str
    reference_hint: str
    # The updater tool sees bindings two extra ways: a dynamic
    # ``getattr(module, "name")`` read, and a name that only a test monkeypatches
    # onto the facade. Both are expressed as hooks rather than by forking the
    # whole collection loop.
    extra_references: Callable[[Path], list[tuple[str, str]]] | None = None
    extra_category_names: Callable[[Path], set[str]] | None = None
    extra_category: str = ""


def collect_root_dependency_inventory(spec: RootDependencySpec, repo_root: Path) -> tuple[Any, ...]:
    """Group ``receiver.<name>`` references by name across the tracked modules."""
    references_by_name: dict[str, list[str]] = {}
    for relative in spec.tracked_modules:
        path = repo_root / relative
        for name, reference in iter_root_binding_references(path, repo_root, spec.dependency_name):
            references_by_name.setdefault(name, []).append(reference)

    if spec.extra_references is not None:
        for name, reference in spec.extra_references(repo_root):
            references_by_name.setdefault(name, []).append(reference)

    extra_names = (
        spec.extra_category_names(repo_root) if spec.extra_category_names is not None else set()
    )
    rows: list[Any] = []
    for name, references in sorted(references_by_name.items()):
        found = set(spec.categories.get(name, set()))
        if name in extra_names:
            found.add(spec.extra_category)
        rows.append(
            spec.row_type(
                name=name,
                categories=tuple(sorted(found)),
                references=tuple(sorted(references)),
            )
        )
    return tuple(rows)


def check_root_dependency_inventory(spec: RootDependencySpec, repo_root: Path) -> list[str]:
    """Assert dependency count, reference count, vocabulary and classification."""
    inventory = collect_root_dependency_inventory(spec, repo_root)
    failures: list[str] = []

    dependency_count = len(inventory)
    reference_count = sum(len(row.references) for row in inventory)
    if dependency_count != spec.expected_dependency_count:
        failures.append(
            f"{spec.label} has {dependency_count} dependencies; "
            f"expected {spec.expected_dependency_count}. {spec.count_hint}"
        )
    if reference_count != spec.expected_reference_count:
        failures.append(
            f"{spec.label} has {reference_count} references; "
            f"expected {spec.expected_reference_count}. {spec.reference_hint}"
        )

    unknown_categories = {
        category
        for categories in spec.categories.values()
        for category in categories
        if category not in spec.known_categories
    }
    if unknown_categories:
        failures.append(f"{spec.label} has unknown categories: {sorted(unknown_categories)}.")

    discovered_names = {row.name for row in inventory}
    for missing in sorted(set(spec.categories) - discovered_names):
        failures.append(
            f"{spec.entity} classification for {missing} has no matching "
            f"{spec.dependency_name}.<name> reference."
        )

    for row in inventory:
        if not row.categories:
            failures.append(
                f"{spec.entity} is unclassified: {row.name} "
                f"referenced at {', '.join(row.references)}."
            )
    return failures


def iter_root_binding_references(
    path: Path, repo_root: Path, receiver: str
) -> list[tuple[str, str]]:
    """Every ``<receiver>.<name>`` attribute access in one file, sorted."""
    tree = parse_python(path)
    relative = relative_posix(path, repo_root)
    references = [
        (node.attr, f"{relative}:{node.lineno}")
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
        if node.value.id == receiver
    ]
    return sorted(references, key=lambda item: (item[0], item[1]))


def facade_import_references(
    path: Path, targets: frozenset[str], parent: str
) -> list[tuple[str, int]]:
    """Absolute and parent-relative imports of any module in ``targets``.

    Handles ``import a.b``, ``from a.b import x`` and ``from <parent> import
    name`` where the joined module is a target. A one-element ``targets`` makes
    this behave identically to equality testing, which is what the single-module
    callers relied on.
    """
    tree = parse_python(path)
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in targets:
                    imports.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.module in targets:
                imports.append((node.module, node.lineno))
            elif node.module == parent:
                for alias in node.names:
                    module = f"{parent}.{alias.name}"
                    if module in targets:
                        imports.append((module, node.lineno))
    return imports


def run_inventory_main(
    *,
    description: str,
    check_flag_help: str,
    collect: Callable[[], tuple[Any, ...]],
    check: Callable[[], list[str]],
    root: Path,
) -> int:
    """Shared ``--check`` CLI body for the five inventory tools.

    The five ``--check`` surfaces are load-bearing: ``compat`` and six
    ``test_repo_guardrails_compat_group_runs_*`` tests drive them. They stay five
    separate commands rather than one flag-driven entrypoint.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--check", action="store_true", help=check_flag_help)
    args = parser.parse_args()

    inventory = collect()
    failures = check() if args.check else []
    if failures:
        for failure in failures:
            print(failure)
        return 1
    print_inventory(inventory)
    return 0
