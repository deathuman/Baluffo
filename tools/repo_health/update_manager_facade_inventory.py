from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXPECTED_FACADE_IMPORT_COUNT = 1

FACADE_MODULE = "src.ship.update_manager"

CATEGORIES = {
    "test-compat",
    "test-api",
    "tooling",
}

CLASSIFIED_IMPORTS: dict[str, set[str]] = {
    "tests/test_ship_update_manager.py": {"test-api"},
}

RUNTIME_IMPORT_ALLOWLIST: set[str] = set()


@dataclass(frozen=True)
class UpdateManagerFacadeImport:
    path: str
    module: str
    line: int
    categories: tuple[str, ...]

    def as_json(self) -> dict[str, object]:
        return {
            "path": self.path,
            "module": self.module,
            "line": self.line,
            "categories": list(self.categories),
        }


def _parse_python(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _iter_python_paths(repo_root: Path) -> list[Path]:
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


def _relative(path: Path, repo_root: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def _imported_facade_modules(path: Path) -> list[tuple[str, int]]:
    tree = _parse_python(path)
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == FACADE_MODULE:
                    imports.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.module == FACADE_MODULE:
                imports.append((node.module, node.lineno))
            elif node.module == "src.ship":
                for alias in node.names:
                    module = f"src.ship.{alias.name}"
                    if module == FACADE_MODULE:
                        imports.append((module, node.lineno))
    return imports


def collect_update_manager_facade_inventory(
    repo_root: Path = ROOT,
) -> tuple[UpdateManagerFacadeImport, ...]:
    rows: list[UpdateManagerFacadeImport] = []
    for path in _iter_python_paths(repo_root):
        relative = _relative(path, repo_root)
        categories = tuple(sorted(CLASSIFIED_IMPORTS.get(relative, set())))
        for module, line in _imported_facade_modules(path):
            rows.append(
                UpdateManagerFacadeImport(
                    path=relative,
                    module=module,
                    line=line,
                    categories=categories,
                )
            )
    return tuple(sorted(rows, key=lambda row: (row.path, row.line, row.module)))


def check_update_manager_facade_inventory(repo_root: Path | None = None) -> list[str]:
    root = repo_root or ROOT
    inventory = collect_update_manager_facade_inventory(root)
    failures: list[str] = []
    if len(inventory) != EXPECTED_FACADE_IMPORT_COUNT:
        failures.append(
            f"Update manager facade inventory has {len(inventory)} import records; "
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
            f"Update manager facade inventory has unknown categories: {sorted(unknown_categories)}."
        )
    discovered_paths = {row.path for row in inventory}
    configured_paths = set(CLASSIFIED_IMPORTS)
    for missing in sorted(configured_paths - discovered_paths):
        failures.append(
            f"Update manager facade classification for {missing} has no matching import."
        )
    for row in inventory:
        if not row.categories:
            failures.append(
                f"Update manager facade import is unclassified: {row.path}:{row.line} "
                f"imports {row.module}."
            )
        if row.path.startswith("src/") and row.path not in RUNTIME_IMPORT_ALLOWLIST:
            failures.append(
                f"Runtime Python module imports update manager facade outside allowlist: "
                f"{row.path}:{row.line} imports {row.module}."
            )
    return failures


def _print_inventory(inventory: tuple[UpdateManagerFacadeImport, ...]) -> None:
    print(json.dumps([row.as_json() for row in inventory], indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Inventory update manager facade imports.")
    parser.add_argument("--check", action="store_true", help="Fail if facade inventory drifted.")
    args = parser.parse_args()

    inventory = collect_update_manager_facade_inventory(ROOT)
    failures = check_update_manager_facade_inventory(ROOT) if args.check else []
    if failures:
        for failure in failures:
            print(failure)
        return 1
    _print_inventory(inventory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
