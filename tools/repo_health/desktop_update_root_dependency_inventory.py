from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path

try:
    from tools.repo_health.inventory_common import (
        NameCategoriesReferencesRow,
        parse_python,
        relative_posix,
    )
    from tools.repo_health.inventory_common import (
        print_inventory as _print_inventory,
    )
except ImportError:  # direct script execution puts this directory on sys.path
    from inventory_common import (
        NameCategoriesReferencesRow,
        parse_python,
        relative_posix,
    )
    from inventory_common import (
        print_inventory as _print_inventory,
    )

ROOT = Path(__file__).resolve().parents[2]

TRACKED_MODULES = (
    "src/ship/desktop_update_shared.py",
    "src/ship/desktop_update_state.py",
    "src/ship/desktop_update_service.py",
)

EXPECTED_DEPENDENCY_COUNT = 0
EXPECTED_REFERENCE_COUNT = 0

CATEGORIES = {
    "constant",
    "stdlib-binding",
    "shared-helper",
    "state-helper",
    "service-helper",
    "external-adapter",
    "crypto-binding",
    "mutable-compat-hook",
    "runtime-path",
}

CONSTANTS: set[str] = set()

STDLIB_BINDINGS: set[str] = set()

SHARED_HELPERS: set[str] = set()

STATE_HELPERS = {}

EXTERNAL_ADAPTERS: set[str] = set()

CRYPTO_BINDINGS: set[str] = set()

RUNTIME_PATH_DEPENDENCIES: set[str] = set()

MUTABLE_COMPAT_HOOKS = STDLIB_BINDINGS | EXTERNAL_ADAPTERS | CRYPTO_BINDINGS | set()

DEPENDENCY_CATEGORIES: dict[str, set[str]] = {}
for _name in CONSTANTS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("constant")
for _name in STDLIB_BINDINGS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("stdlib-binding")
for _name in SHARED_HELPERS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("shared-helper")
for _name in STATE_HELPERS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("state-helper")
for _name in EXTERNAL_ADAPTERS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("external-adapter")
for _name in CRYPTO_BINDINGS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("crypto-binding")
for _name in RUNTIME_PATH_DEPENDENCIES:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("runtime-path")
for _name in MUTABLE_COMPAT_HOOKS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("mutable-compat-hook")


@dataclass(frozen=True)
class DesktopUpdateRootDependency(NameCategoriesReferencesRow):
    name: str
    categories: tuple[str, ...]
    references: tuple[str, ...]


def _iter_dependency_references(path: Path, repo_root: Path) -> list[tuple[str, str]]:
    tree = parse_python(path)
    references: list[tuple[str, str]] = []
    relative = relative_posix(path, repo_root)
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id == "deps":
                references.append((node.attr, f"{relative}:{node.lineno}"))
    return sorted(references, key=lambda item: (item[0], item[1]))


def collect_desktop_update_root_dependency_inventory(
    repo_root: Path = ROOT,
) -> tuple[DesktopUpdateRootDependency, ...]:
    references_by_name: dict[str, list[str]] = {}
    for relative in TRACKED_MODULES:
        path = repo_root / relative
        for name, reference in _iter_dependency_references(path, repo_root):
            references_by_name.setdefault(name, []).append(reference)

    rows: list[DesktopUpdateRootDependency] = []
    for name, references in sorted(references_by_name.items()):
        rows.append(
            DesktopUpdateRootDependency(
                name=name,
                categories=tuple(sorted(DEPENDENCY_CATEGORIES.get(name, set()))),
                references=tuple(sorted(references)),
            )
        )
    return tuple(rows)


def check_desktop_update_root_dependency_inventory(
    repo_root: Path | None = None,
) -> list[str]:
    root = repo_root or ROOT
    inventory = collect_desktop_update_root_dependency_inventory(root)
    failures: list[str] = []
    dependency_count = len(inventory)
    reference_count = sum(len(row.references) for row in inventory)
    if dependency_count != EXPECTED_DEPENDENCY_COUNT:
        failures.append(
            f"Desktop update root dependency inventory has {dependency_count} dependencies; "
            f"expected {EXPECTED_DEPENDENCY_COUNT}. Update the classification inventory "
            "after reviewing updater root-binding compatibility."
        )
    if reference_count != EXPECTED_REFERENCE_COUNT:
        failures.append(
            f"Desktop update root dependency inventory has {reference_count} references; "
            f"expected {EXPECTED_REFERENCE_COUNT}. Review new or removed deps.<name> usages."
        )
    unknown_categories = {
        category
        for categories in DEPENDENCY_CATEGORIES.values()
        for category in categories
        if category not in CATEGORIES
    }
    if unknown_categories:
        failures.append(
            "Desktop update root dependency inventory has unknown categories: "
            f"{sorted(unknown_categories)}."
        )
    discovered_names = {row.name for row in inventory}
    configured_names = set(DEPENDENCY_CATEGORIES)
    for missing in sorted(configured_names - discovered_names):
        failures.append(
            f"Desktop update root dependency classification for {missing} has no matching "
            "deps.<name> reference."
        )
    for row in inventory:
        if not row.categories:
            failures.append(
                f"Desktop update root dependency is unclassified: {row.name} "
                f"referenced at {', '.join(row.references)}."
            )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inventory desktop update leaf dependencies on facade root bindings."
    )
    parser.add_argument(
        "--check", action="store_true", help="Fail if dependency inventory drifted."
    )
    args = parser.parse_args()

    inventory = collect_desktop_update_root_dependency_inventory(ROOT)
    failures = check_desktop_update_root_dependency_inventory(ROOT) if args.check else []
    if failures:
        for failure in failures:
            print(failure)
        return 1
    _print_inventory(inventory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
