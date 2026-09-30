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

_LABEL = "Desktop update facade inventory"
_ENTITY = "Desktop update facade"
_ALLOWLIST_MESSAGE = "Leaf Python module imports desktop update facade outside allowlist"


@dataclass(frozen=True)
class DesktopUpdateFacadeImport(PathModuleLineCategoriesRow):
    """One desktop-update facade import, with its consumer classification."""

    path: str
    module: str
    line: int
    categories: tuple[str, ...]


def _spec(repo_root: Path) -> ImportInventorySpec:
    """Build the spec from this module's globals on every call, so the drift
    tests' ``monkeypatch.setattr`` calls on the module constants still bite."""
    return ImportInventorySpec(
        row_type=DesktopUpdateFacadeImport,
        label=_LABEL,
        entity=_ENTITY,
        count_hint=(
            "Update EXPECTED_FACADE_IMPORT_COUNT and review facade consumer classifications."
        ),
        expected_count=EXPECTED_FACADE_IMPORT_COUNT,
        classified=CLASSIFIED_IMPORTS,
        known_categories=frozenset(CATEGORIES),
        allowlist=frozenset(LEAF_FACADE_IMPORT_ALLOWLIST),
        allowlist_message=_ALLOWLIST_MESSAGE,
        include_categories=True,
        facade_targets=frozenset(FACADE_MODULES),
        facade_parent="src.ship",
    )


def collect_desktop_update_facade_inventory(
    repo_root: Path = ROOT,
) -> tuple[DesktopUpdateFacadeImport, ...]:
    return collect_import_inventory(_spec(repo_root), repo_root)


def check_desktop_update_facade_inventory(repo_root: Path | None = None) -> list[str]:
    return check_import_inventory(_spec(repo_root or ROOT), repo_root or ROOT)


def main() -> int:
    return run_inventory_main(
        description="Inventory desktop update facade imports.",
        check_flag_help="Fail if facade inventory drifted.",
        collect=lambda: collect_desktop_update_facade_inventory(ROOT),
        check=lambda: check_desktop_update_facade_inventory(ROOT),
        root=ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
