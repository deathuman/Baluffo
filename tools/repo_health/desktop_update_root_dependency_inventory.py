from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

try:
    from tools.repo_health.inventory_common import (
        NameCategoriesReferencesRow,
        RootDependencySpec,
        check_root_dependency_inventory,
        collect_root_dependency_inventory,
        run_inventory_main,
    )
except ImportError:  # direct script execution puts this directory on sys.path
    from inventory_common import (
        NameCategoriesReferencesRow,
        RootDependencySpec,
        check_root_dependency_inventory,
        collect_root_dependency_inventory,
        run_inventory_main,
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

# The receiver whose attributes are the root bindings under audit.
RECEIVER = "deps"
_LABEL = "Desktop update root dependency inventory"
_ENTITY = "Desktop update root dependency"
_COUNT_HINT = (
    "Update the classification inventory after reviewing updater root-binding compatibility."
)
_REFERENCE_HINT = "Review new or removed deps.<name> usages."


@dataclass(frozen=True)
class DesktopUpdateRootDependency(NameCategoriesReferencesRow):
    """One §deps.<name>§ root binding and where it is referenced."""

    name: str
    categories: tuple[str, ...]
    references: tuple[str, ...]


def _spec(repo_root: Path) -> RootDependencySpec:
    """Build the spec from this module's globals on every call, so the drift
    tests' ``monkeypatch.setattr`` calls on the module constants still bite."""
    return RootDependencySpec(
        row_type=DesktopUpdateRootDependency,
        label=_LABEL,
        entity=_ENTITY,
        dependency_name=RECEIVER,
        tracked_modules=TRACKED_MODULES,
        expected_dependency_count=EXPECTED_DEPENDENCY_COUNT,
        expected_reference_count=EXPECTED_REFERENCE_COUNT,
        categories=DEPENDENCY_CATEGORIES,
        known_categories=frozenset(CATEGORIES),
        count_hint=_COUNT_HINT,
        reference_hint=_REFERENCE_HINT,
    )


def collect_desktop_update_root_dependency_inventory(
    repo_root: Path = ROOT,
) -> tuple[DesktopUpdateRootDependency, ...]:
    return collect_root_dependency_inventory(_spec(repo_root), repo_root)


def check_desktop_update_root_dependency_inventory(
    repo_root: Path | None = None,
) -> list[str]:
    return check_root_dependency_inventory(_spec(repo_root or ROOT), repo_root or ROOT)


def main() -> int:
    return run_inventory_main(
        description="Inventory desktop update leaf dependencies on facade root bindings.",
        check_flag_help="Fail if dependency inventory drifted.",
        collect=lambda: collect_desktop_update_root_dependency_inventory(ROOT),
        check=lambda: check_desktop_update_root_dependency_inventory(ROOT),
        root=ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
