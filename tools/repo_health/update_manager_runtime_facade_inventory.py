from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path

try:
    from tools.repo_health.inventory_common import (
        PathModuleLineCategoriesRow,
        parse_python,
        relative_posix,
    )
    from tools.repo_health.inventory_common import (
        print_inventory as _print_inventory,
    )
except ImportError:  # direct script execution puts this directory on sys.path
    from inventory_common import (
        PathModuleLineCategoriesRow,
        parse_python,
        relative_posix,
    )
    from inventory_common import (
        print_inventory as _print_inventory,
    )

ROOT = Path(__file__).resolve().parents[2]
FACADE_MODULE = "src.ship.update_manager"
EXPECTED_RUNTIME_FACADE_IMPORT_COUNT = 0


@dataclass(frozen=True)
class UpdateManagerRuntimeFacadeImport(PathModuleLineCategoriesRow):
    path: str
    module: str
    line: int


def _iter_runtime_python_paths(repo_root: Path) -> list[Path]:
    ship_root = repo_root / "src" / "ship"
    if not ship_root.exists():
        return []
    return sorted(
        path
        for path in ship_root.rglob("*.py")
        if relative_posix(path, repo_root) != "src/ship/update_manager.py"
    )


def _imported_facade_modules(path: Path) -> list[tuple[str, int]]:
    tree = parse_python(path)
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in {FACADE_MODULE, "update_manager"}:
                    imports.append((FACADE_MODULE, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.module == FACADE_MODULE:
                imports.append((FACADE_MODULE, node.lineno))
            elif node.module == "src.ship":
                for alias in node.names:
                    if alias.name == "update_manager":
                        imports.append((FACADE_MODULE, node.lineno))
            elif node.level == 1 and node.module == "update_manager":
                imports.append((FACADE_MODULE, node.lineno))
            elif node.level == 1 and node.module is None:
                for alias in node.names:
                    if alias.name == "update_manager":
                        imports.append((FACADE_MODULE, node.lineno))
    return imports


def collect_update_manager_runtime_facade_inventory(
    repo_root: Path = ROOT,
) -> tuple[UpdateManagerRuntimeFacadeImport, ...]:
    rows: list[UpdateManagerRuntimeFacadeImport] = []
    for path in _iter_runtime_python_paths(repo_root):
        relative = relative_posix(path, repo_root)
        for module, line in _imported_facade_modules(path):
            rows.append(
                UpdateManagerRuntimeFacadeImport(
                    path=relative,
                    module=module,
                    line=line,
                )
            )
    return tuple(sorted(rows, key=lambda row: (row.path, row.line, row.module)))


def check_update_manager_runtime_facade_inventory(
    repo_root: Path | None = None,
) -> list[str]:
    root = repo_root or ROOT
    inventory = collect_update_manager_runtime_facade_inventory(root)
    failures: list[str] = []
    if len(inventory) != EXPECTED_RUNTIME_FACADE_IMPORT_COUNT:
        failures.append(
            f"Update-manager runtime facade inventory has {len(inventory)} import records; "
            f"expected {EXPECTED_RUNTIME_FACADE_IMPORT_COUNT}. Runtime src/ship Python "
            "should import update-manager leaves instead of src.ship.update_manager."
        )
    for row in inventory:
        failures.append(
            f"Runtime src/ship Python imports update-manager facade: "
            f"{row.path}:{row.line} imports {row.module}."
        )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inventory runtime src/ship imports of the update_manager facade."
    )
    parser.add_argument("--check", action="store_true", help="Fail if runtime imports drifted.")
    args = parser.parse_args()

    inventory = collect_update_manager_runtime_facade_inventory(ROOT)
    failures = check_update_manager_runtime_facade_inventory(ROOT) if args.check else []
    if failures:
        for failure in failures:
            print(failure)
        return 1
    _print_inventory(inventory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
