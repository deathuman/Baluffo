from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

try:
    from tools.repo_health.inventory_common import (
        ImportInventorySpec,
        PathModuleLineCategoriesRow,
        check_import_inventory,
        collect_import_inventory,
        run_inventory_main,
    )
except ImportError:  # direct script execution puts this directory on sys.path
    from inventory_common import (
        ImportInventorySpec,
        PathModuleLineCategoriesRow,
        check_import_inventory,
        collect_import_inventory,
        run_inventory_main,
    )

ROOT = Path(__file__).resolve().parents[2]
EXPECTED_FACADE_IMPORT_COUNT = 1

FACADE_MODULE = "src.ship.update_manager"

CATEGORIES = {
    "test-compat",
    "test-api",
    "tooling",
}

CLASSIFIED_IMPORTS: dict[str, set[str]] = {
    "tests/test_ship_update_manager_facade.py": {"test-compat"},
}

RUNTIME_IMPORT_ALLOWLIST: set[str] = set()

# Message wording is asserted verbatim by the tests and is the output of
# `--check`, so it lives here as data rather than being generated in the core.
_LABEL = "Update manager facade inventory"
_ENTITY = "Update manager facade"
_ALLOWLIST_MESSAGE = "Runtime Python module imports update manager facade outside allowlist"


@dataclass(frozen=True)
class UpdateManagerFacadeImport(PathModuleLineCategoriesRow):
    """One ``src.ship.update_manager`` import, with its consumer classification."""

    path: str
    module: str
    line: int
    categories: tuple[str, ...]


def _spec(repo_root: Path) -> ImportInventorySpec:
    """Build the spec from this module's globals, on every call.

    Reading the module-level constants here rather than at import time is what
    keeps ``monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 3)``
    effective: a spec captured at import would ignore every such patch.
    """
    return ImportInventorySpec(
        row_type=UpdateManagerFacadeImport,
        label=_LABEL,
        entity=_ENTITY,
        count_hint=(
            "Update EXPECTED_FACADE_IMPORT_COUNT and review facade consumer classifications."
        ),
        expected_count=EXPECTED_FACADE_IMPORT_COUNT,
        classified=CLASSIFIED_IMPORTS,
        known_categories=frozenset(CATEGORIES),
        allowlist=frozenset(RUNTIME_IMPORT_ALLOWLIST),
        allowlist_message=_ALLOWLIST_MESSAGE,
        include_categories=True,
        facade_targets=frozenset({FACADE_MODULE}),
        facade_parent="src.ship",
    )


def collect_update_manager_facade_inventory(
    repo_root: Path = ROOT,
) -> tuple[UpdateManagerFacadeImport, ...]:
    return collect_import_inventory(_spec(repo_root), repo_root)


def check_update_manager_facade_inventory(repo_root: Path | None = None) -> list[str]:
    return check_import_inventory(_spec(repo_root or ROOT), repo_root or ROOT)


def main() -> int:
    return run_inventory_main(
        description="Inventory update manager facade imports.",
        check_flag_help="Fail if facade inventory drifted.",
        collect=lambda: collect_update_manager_facade_inventory(ROOT),
        check=lambda: check_update_manager_facade_inventory(ROOT),
        root=ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
