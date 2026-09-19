from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path

try:
    from tools.repo_health.inventory_common import (
        PathModuleLineCategoriesRow,
        iter_python_paths,
        parse_python,
        relative_posix,
    )
    from tools.repo_health.inventory_common import (
        print_inventory as _print_inventory,
    )
except ImportError:  # direct script execution puts this directory on sys.path
    from inventory_common import (
        PathModuleLineCategoriesRow,
        iter_python_paths,
        parse_python,
        relative_posix,
    )
    from inventory_common import (
        print_inventory as _print_inventory,
    )

ROOT = Path(__file__).resolve().parents[2]
EXPECTED_FACADE_IMPORT_COUNT = 2

FACADE_MODULES = {
    "src.ship.desktop_update",
    "src.ship.desktop_updater",
}

CATEGORIES = {
    "compatibility-root",
    "root-binding-owner",
    "packaged-smoke-runtime",
    "updater-helper-consumer",
    "test-compat",
    "packaging-build",
    "tooling",
}

CLASSIFIED_IMPORTS: dict[str, set[str]] = {
    "tests/test_desktop_updater_entrypoint.py": {"test-compat"},
    "tests/test_desktop_updater_exception_ratchet.py": {"test-compat"},
}

LEAF_FACADE_IMPORT_ALLOWLIST: set[str] = set()


@dataclass(frozen=True)
class DesktopUpdateFacadeImport(PathModuleLineCategoriesRow):
    path: str
    module: str
    line: int
    categories: tuple[str, ...]


def _imported_facade_modules(path: Path) -> list[tuple[str, int]]:
    tree = parse_python(path)
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in FACADE_MODULES:
                    imports.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.module in FACADE_MODULES:
                imports.append((node.module, node.lineno))
            elif node.module == "src.ship":
                for alias in node.names:
                    module = f"src.ship.{alias.name}"
                    if module in FACADE_MODULES:
                        imports.append((module, node.lineno))
    return imports


def collect_desktop_update_facade_inventory(
    repo_root: Path = ROOT,
) -> tuple[DesktopUpdateFacadeImport, ...]:
    rows: list[DesktopUpdateFacadeImport] = []
    for path in iter_python_paths(repo_root):
        relative = relative_posix(path, repo_root)
        categories = tuple(sorted(CLASSIFIED_IMPORTS.get(relative, set())))
        for module, line in _imported_facade_modules(path):
            rows.append(
                DesktopUpdateFacadeImport(
                    path=relative,
                    module=module,
                    line=line,
                    categories=categories,
                )
            )
    return tuple(sorted(rows, key=lambda row: (row.path, row.line, row.module)))


def check_desktop_update_facade_inventory(repo_root: Path | None = None) -> list[str]:
    root = repo_root or ROOT
    inventory = collect_desktop_update_facade_inventory(root)
    failures: list[str] = []
    if len(inventory) != EXPECTED_FACADE_IMPORT_COUNT:
        failures.append(
            f"Desktop update facade inventory has {len(inventory)} import records; "
            f"expected {EXPECTED_FACADE_IMPORT_COUNT}. Update "
            "EXPECTED_FACADE_IMPORT_COUNT and review facade consumer classifications."
        )
    unknown_categories = {
        category
        for categories in CLASSIFIED_IMPORTS.values()
        for category in categories
        if category not in CATEGORIES
    }
    if unknown_categories:
        failures.append(
            f"Desktop update facade inventory has unknown categories: {sorted(unknown_categories)}."
        )
    discovered_paths = {row.path for row in inventory}
    configured_paths = set(CLASSIFIED_IMPORTS)
    for missing in sorted(configured_paths - discovered_paths):
        failures.append(
            f"Desktop update facade classification for {missing} has no matching import."
        )
    for row in inventory:
        if not row.categories:
            failures.append(
                f"Desktop update facade import is unclassified: {row.path}:{row.line} "
                f"imports {row.module}."
            )
        if row.path.startswith("src/") and row.path not in LEAF_FACADE_IMPORT_ALLOWLIST:
            failures.append(
                f"Leaf Python module imports desktop update facade outside allowlist: "
                f"{row.path}:{row.line} imports {row.module}."
            )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Inventory desktop update facade imports.")
    parser.add_argument("--check", action="store_true", help="Fail if facade inventory drifted.")
    args = parser.parse_args()

    inventory = collect_desktop_update_facade_inventory(ROOT)
    failures = check_desktop_update_facade_inventory(ROOT) if args.check else []
    if failures:
        for failure in failures:
            print(failure)
        return 1
    _print_inventory(inventory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
