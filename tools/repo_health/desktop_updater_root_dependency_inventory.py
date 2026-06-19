from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

TRACKED_MODULES = (
    "src/ship/desktop_updater_ui.py",
    "src/ship/desktop_updater_release.py",
    "src/ship/desktop_updater_install.py",
)

EXPECTED_DEPENDENCY_COUNT = 30
EXPECTED_REFERENCE_COUNT = 44

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

UI_HELPERS = {
    "NullProgressWindow",
    "_drain_helper_queue",
    "_helper_failure_dialog_enabled",
    "_helper_relaunch_verify_timeout_s",
    "_helper_window_layout",
    "_launch_executable",
}

RELEASE_HELPERS = {
    "_classify_install_failure",
    "_ensure_verified_zip_for_install",
    "_find_release_for_target_version",
    "_recover_manifest_for_install",
}

INSTALL_HELPERS = {
    "_copy_install_snapshot",
    "_finalize_success",
    "_recover_interrupted_install",
    "_restore_install_snapshot",
    "_sync_extract_to_install",
    "_verify_target_startup",
    "_wait_for_launcher_exit",
}

SHARED_HELPERS = {
    "compute_sha256",
    "desktop_update_public_key_candidate_paths",
    "download_file",
    "fetch_json",
    "iso_now",
    "load_desktop_update_public_keys",
    "resolve_github_api_base",
    "resolve_release_repo",
    "validate_desktop_manifest",
    "verify_manifest_signature",
    "write_json_atomic",
}

STATE_HELPERS = {
    "read_cached_manifest",
}

UPDATE_MANAGER_COMPAT = {"update_manager"}

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


@dataclass(frozen=True)
class DesktopUpdaterRootDependency:
    name: str
    categories: tuple[str, ...]
    references: tuple[str, ...]

    def as_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "categories": list(self.categories),
            "references": list(self.references),
        }


def _parse_python(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _relative(path: Path, repo_root: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def _iter_dependency_references(path: Path, repo_root: Path) -> list[tuple[str, str]]:
    tree = _parse_python(path)
    references: list[tuple[str, str]] = []
    relative = _relative(path, repo_root)
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id == "module":
                references.append((node.attr, f"{relative}:{node.lineno}"))
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "module"
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            references.append((node.args[1].value, f"{relative}:{node.lineno}"))
    return sorted(references, key=lambda item: (item[0], item[1]))


def _imports_desktop_updater_as_updater(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "src.ship":
            for alias in node.names:
                if alias.name == "desktop_updater" and alias.asname == "updater":
                    return True
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "src.ship.desktop_updater" and alias.asname == "updater":
                    return True
    return False


def _iter_facade_monkeypatch_names(repo_root: Path) -> set[str]:
    tests_root = repo_root / "tests"
    if not tests_root.is_dir():
        return set()
    monkeypatched: set[str] = set()
    for path in tests_root.rglob("*.py"):
        try:
            tree = _parse_python(path)
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
    return monkeypatched


def collect_desktop_updater_root_dependency_inventory(
    repo_root: Path = ROOT,
) -> tuple[DesktopUpdaterRootDependency, ...]:
    references_by_name: dict[str, list[str]] = {}
    for relative in TRACKED_MODULES:
        path = repo_root / relative
        for name, reference in _iter_dependency_references(path, repo_root):
            references_by_name.setdefault(name, []).append(reference)

    facade_monkeypatch_names = _iter_facade_monkeypatch_names(repo_root)
    rows: list[DesktopUpdaterRootDependency] = []
    for name, references in sorted(references_by_name.items()):
        categories = set(DEPENDENCY_CATEGORIES.get(name, set()))
        if name in facade_monkeypatch_names:
            categories.add("facade-monkeypatch-compat")
        rows.append(
            DesktopUpdaterRootDependency(
                name=name,
                categories=tuple(sorted(categories)),
                references=tuple(sorted(references)),
            )
        )
    return tuple(rows)


def check_desktop_updater_root_dependency_inventory(
    repo_root: Path | None = None,
) -> list[str]:
    root = repo_root or ROOT
    inventory = collect_desktop_updater_root_dependency_inventory(root)
    failures: list[str] = []
    dependency_count = len(inventory)
    reference_count = sum(len(row.references) for row in inventory)
    if dependency_count != EXPECTED_DEPENDENCY_COUNT:
        failures.append(
            f"Desktop updater root dependency inventory has {dependency_count} dependencies; "
            f"expected {EXPECTED_DEPENDENCY_COUNT}. Update the classification inventory "
            "after reviewing updater helper root-binding compatibility."
        )
    if reference_count != EXPECTED_REFERENCE_COUNT:
        failures.append(
            f"Desktop updater root dependency inventory has {reference_count} references; "
            f"expected {EXPECTED_REFERENCE_COUNT}. Review new or removed module.<name> usages."
        )
    unknown_categories = {
        category
        for categories in DEPENDENCY_CATEGORIES.values()
        for category in categories
        if category not in CATEGORIES
    }
    if unknown_categories:
        failures.append(
            "Desktop updater root dependency inventory has unknown categories: "
            f"{sorted(unknown_categories)}."
        )
    discovered_names = {row.name for row in inventory}
    configured_names = set(DEPENDENCY_CATEGORIES)
    for missing in sorted(configured_names - discovered_names):
        failures.append(
            f"Desktop updater root dependency classification for {missing} has no matching "
            "module.<name> reference."
        )
    for row in inventory:
        if not row.categories:
            failures.append(
                f"Desktop updater root dependency is unclassified: {row.name} "
                f"referenced at {', '.join(row.references)}."
            )
    return failures


def _print_inventory(inventory: tuple[DesktopUpdaterRootDependency, ...]) -> None:
    print(json.dumps([row.as_json() for row in inventory], indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inventory desktop updater helper dependencies on facade root bindings."
    )
    parser.add_argument(
        "--check", action="store_true", help="Fail if dependency inventory drifted."
    )
    args = parser.parse_args()

    inventory = collect_desktop_updater_root_dependency_inventory(ROOT)
    failures = check_desktop_updater_root_dependency_inventory(ROOT) if args.check else []
    if failures:
        for failure in failures:
            print(failure)
        return 1
    _print_inventory(inventory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
