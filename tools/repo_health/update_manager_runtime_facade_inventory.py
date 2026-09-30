from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

try:
    from tools.repo_health.inventory_common import (
        ImportInventorySpec,
        PathModuleLineCategoriesRow,
        check_import_inventory,
        collect_import_inventory,
        parse_python,
        relative_posix,
        run_inventory_main,
    )
except ImportError:  # direct script execution puts this directory on sys.path
    from inventory_common import (
        ImportInventorySpec,
        PathModuleLineCategoriesRow,
        check_import_inventory,
        collect_import_inventory,
        parse_python,
        relative_posix,
        run_inventory_main,
    )

ROOT = Path(__file__).resolve().parents[2]
FACADE_MODULE = "src.ship.update_manager"
EXPECTED_RUNTIME_FACADE_IMPORT_COUNT = 0

_LABEL = "Update-manager runtime facade inventory"
_COUNT_HINT = (
    "Runtime src/ship Python should import update-manager leaves instead of "
    "src.ship.update_manager."
)
_ROW_MESSAGE = "Runtime src/ship Python imports update-manager facade"


@dataclass(frozen=True)
class UpdateManagerRuntimeFacadeImport(PathModuleLineCategoriesRow):
    """One ``src.ship.update_manager`` import from inside ``src/ship``."""

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
    """Absolute, bare, parent-relative and module-relative facade imports.

    Wider than the other facade detectors: this one also has to catch
    ``import update_manager`` and ``from . import update_manager``, because a
    ``src/ship`` module can reach the facade without ever naming the package.
    """
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


def _spec(repo_root: Path) -> ImportInventorySpec:
    """Build the spec from this module's globals on every call, so the drift
    tests' ``monkeypatch.setattr`` calls on the module constants still bite."""
    return ImportInventorySpec(
        row_type=UpdateManagerRuntimeFacadeImport,
        label=_LABEL,
        entity="Update-manager runtime facade",
        count_hint=_COUNT_HINT,
        expected_count=EXPECTED_RUNTIME_FACADE_IMPORT_COUNT,
        classified={},
        known_categories=frozenset(),
        allowlist=frozenset(),
        allowlist_message="",
        include_categories=False,
        detect=_imported_facade_modules,
        iter_paths=_iter_runtime_python_paths,
        # Every row is the defect: any src/ship module importing the facade is
        # what this inventory exists to report, so the shared src/ allowlist rule
        # does not apply and there is no classification to enforce.
        enforce_allowlist=False,
        extra_row_check=lambda row: [
            f"{_ROW_MESSAGE}: {row.path}:{row.line} imports {row.module}."
        ],
    )


def collect_update_manager_runtime_facade_inventory(
    repo_root: Path = ROOT,
) -> tuple[UpdateManagerRuntimeFacadeImport, ...]:
    return collect_import_inventory(_spec(repo_root), repo_root)


def check_update_manager_runtime_facade_inventory(
    repo_root: Path | None = None,
) -> list[str]:
    return check_import_inventory(_spec(repo_root or ROOT), repo_root or ROOT)


def main() -> int:
    return run_inventory_main(
        description="Inventory runtime src/ship imports of the update_manager facade.",
        check_flag_help="Fail if runtime imports drifted.",
        collect=lambda: collect_update_manager_runtime_facade_inventory(ROOT),
        check=lambda: check_update_manager_runtime_facade_inventory(ROOT),
        root=ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
