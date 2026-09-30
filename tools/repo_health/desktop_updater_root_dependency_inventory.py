from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

try:
    from tools.repo_health.inventory_common import (
        NameCategoriesReferencesRow,
        RootDependencySpec,
        check_root_dependency_inventory,
        collect_root_dependency_inventory,
        parse_python,
        relative_posix,
        run_inventory_main,
    )
except ImportError:  # direct script execution puts this directory on sys.path
    from inventory_common import (
        NameCategoriesReferencesRow,
        RootDependencySpec,
        check_root_dependency_inventory,
        collect_root_dependency_inventory,
        parse_python,
        relative_posix,
        run_inventory_main,
    )

ROOT = Path(__file__).resolve().parents[2]

TRACKED_MODULES = (
    "src/ship/desktop_updater_ui.py",
    "src/ship/desktop_updater_release.py",
    "src/ship/desktop_updater_install.py",
)
TRACKED_IMPORT_MODULES = {
    "desktop_updater",
    "desktop_updater_ui",
    "desktop_updater_release",
    "desktop_updater_install",
}

EXPECTED_DEPENDENCY_COUNT = 0
EXPECTED_REFERENCE_COUNT = 0

CATEGORIES = {
    "constant",
    "stdlib-binding",
    "ui-helper",
    "release-helper",
    "install-helper",
    "shared-helper",
    "state-helper",
    "update-manager-compat",
    "mutable-compat-hook",
    "facade-monkeypatch-compat",
}

CONSTANTS: set[str] = set()

STDLIB_BINDINGS: set[str] = set()

UI_HELPERS: set[str] = set()

RELEASE_HELPERS: set[str] = set()

INSTALL_HELPERS: set[str] = set()

SHARED_HELPERS: set[str] = set()

STATE_HELPERS: set[str] = set()

UPDATE_MANAGER_COMPAT: set[str] = set()

MUTABLE_COMPAT_HOOKS = (
    STDLIB_BINDINGS
    | UI_HELPERS
    | RELEASE_HELPERS
    | INSTALL_HELPERS
    | SHARED_HELPERS
    | STATE_HELPERS
    | UPDATE_MANAGER_COMPAT
)

DEPENDENCY_CATEGORIES: dict[str, set[str]] = {}
for _name in CONSTANTS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("constant")
for _name in STDLIB_BINDINGS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("stdlib-binding")
for _name in UI_HELPERS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("ui-helper")
for _name in RELEASE_HELPERS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("release-helper")
for _name in INSTALL_HELPERS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("install-helper")
for _name in SHARED_HELPERS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("shared-helper")
for _name in STATE_HELPERS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("state-helper")
for _name in UPDATE_MANAGER_COMPAT:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("update-manager-compat")
for _name in MUTABLE_COMPAT_HOOKS:
    DEPENDENCY_CATEGORIES.setdefault(_name, set()).add("mutable-compat-hook")

# The receiver whose attributes are the root bindings under audit, and the one
# dynamic form this tool must also catch: `getattr(module, "name")`.
RECEIVER = "module"
MONKEYPATCH_CATEGORY = "facade-monkeypatch-compat"
_LABEL = "Desktop updater root dependency inventory"
_ENTITY = "Desktop updater root dependency"
_COUNT_HINT = (
    "Update the classification inventory after reviewing updater helper root-binding compatibility."
)
_REFERENCE_HINT = "Review new or removed module.<name> usages."


@dataclass(frozen=True)
class DesktopUpdaterRootDependency(NameCategoriesReferencesRow):
    """One §module.<name>§ root binding and where it is referenced."""

    name: str
    categories: tuple[str, ...]
    references: tuple[str, ...]


def _iter_getattr_references(path: Path, repo_root: Path) -> list[tuple[str, str]]:
    """`getattr(module, "name")` reads the same binding as `module.name` does.

    A dynamic read is invisible to the attribute walk, so without this a binding
    could be used and the inventory would report it unused.
    """
    tree = parse_python(path)
    relative = relative_posix(path, repo_root)
    references = [
        (node.args[1].value, f"{relative}:{node.lineno}")
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == RECEIVER
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
    ]
    return sorted(references, key=lambda item: (item[0], item[1]))


def _imports_desktop_updater_as_updater(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "src.ship":
            for alias in node.names:
                if alias.name in TRACKED_IMPORT_MODULES and alias.asname == "updater":
                    return True
        if isinstance(node, ast.Import):
            for alias in node.names:
                if (
                    alias.name.removeprefix("src.ship.") in TRACKED_IMPORT_MODULES
                    and alias.asname == "updater"
                ):
                    return True
    return False


def _iter_facade_monkeypatch_names(repo_root: Path) -> set[str]:
    tests_root = repo_root / "tests"
    if not tests_root.is_dir():
        return set()
    monkeypatched: set[str] = set()
    for path in tests_root.rglob("*.py"):
        try:
            tree = parse_python(path)
        except OSError:
            continue
        if not _imports_desktop_updater_as_updater(tree):
            continue
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setattr"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "monkeypatch"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id == "updater"
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
            ):
                monkeypatched.add(node.args[1].value)
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setattr"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "monkeypatch"
                and node.args
                and isinstance(node.args[0], ast.Attribute)
                and isinstance(node.args[0].value, ast.Name)
                and node.args[0].value.id == "updater"
            ):
                monkeypatched.add(node.args[0].attr)
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "object"
                and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "patch"
                and isinstance(node.func.value.value, ast.Name)
                and node.func.value.value.id == "mock"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id == "updater"
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
            ):
                monkeypatched.add(node.args[1].value)
    return monkeypatched


def _extra_references(repo_root: Path) -> list[tuple[str, str]]:
    """The dynamic `getattr(module, ...)` reads across the tracked modules."""
    references: list[tuple[str, str]] = []
    for relative in TRACKED_MODULES:
        references.extend(_iter_getattr_references(repo_root / relative, repo_root))
    return references


def _spec(repo_root: Path) -> RootDependencySpec:
    """Build the spec from this module's globals on every call, so the drift
    tests' ``monkeypatch.setattr`` calls on the module constants still bite."""
    return RootDependencySpec(
        row_type=DesktopUpdaterRootDependency,
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
        extra_references=_extra_references,
        extra_category_names=_iter_facade_monkeypatch_names,
        extra_category=MONKEYPATCH_CATEGORY,
    )


def collect_desktop_updater_root_dependency_inventory(
    repo_root: Path = ROOT,
) -> tuple[DesktopUpdaterRootDependency, ...]:
    return collect_root_dependency_inventory(_spec(repo_root), repo_root)


def check_desktop_updater_root_dependency_inventory(
    repo_root: Path | None = None,
) -> list[str]:
    return check_root_dependency_inventory(_spec(repo_root or ROOT), repo_root or ROOT)


def main() -> int:
    return run_inventory_main(
        description="Inventory desktop updater helper dependencies on facade root bindings.",
        check_flag_help="Fail if dependency inventory drifted.",
        collect=lambda: collect_desktop_updater_root_dependency_inventory(ROOT),
        check=lambda: check_desktop_updater_root_dependency_inventory(ROOT),
        root=ROOT,
    )


if __name__ == "__main__":
    raise SystemExit(main())
