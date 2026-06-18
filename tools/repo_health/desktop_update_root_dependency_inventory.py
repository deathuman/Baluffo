from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

TRACKED_MODULES = (
    "src/ship/desktop_update_shared.py",
    "src/ship/desktop_update_state.py",
    "src/ship/desktop_update_service.py",
)

EXPECTED_DEPENDENCY_COUNT = 74
EXPECTED_REFERENCE_COUNT = 199

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

STDLIB_BINDINGS = {
    "Request",
    "URLError",
    "UTC",
    "contextlib",
    "datetime",
    "os",
    "shutil",
    "ssl",
    "subprocess",
    "tempfile",
    "threading",
    "time",
    "urlopen",
    "uuid",
}

SHARED_HELPERS = {
    "DesktopUpdatePaths",
    "_build_desktop_update_ssl_context",
    "_decode_public_keys_payload",
    "_json_headers",
    "_looks_like_windows_absolute_path",
    "_replace_with_retry",
    "_resolve_desktop_session_root_fallback",
    "_resolve_runtime_path",
    "_resolve_ship_current_version",
    "_runtime_session_root_candidate_fallback",
    "_uses_github_https",
    "_write_atomic",
    "compare_versions",
    "compute_sha256",
    "desktop_update_public_key_candidate_paths",
    "download_file",
    "fetch_json",
    "install_stage_label",
    "iso_now",
    "load_desktop_update_public_keys",
    "normalize_install_stage",
    "pid_is_running",
    "read_desktop_session_state",
    "read_json",
    "resolve_desktop_session_root",
    "resolve_github_api_base",
    "resolve_release_repo",
    "validate_desktop_manifest",
    "verify_manifest_signature",
    "write_json_atomic",
}

STATE_HELPERS = {
    "_apply_credible_handoff_status",
    "_cached_release_notes",
    "_cached_release_notes_history",
    "_failure_result",
    "_handoff_status_pending",
    "_load_credible_handoff_install_plan",
    "_manifest_to_status",
    "_normalize_installed_status",
    "_normalize_release_notes_entry",
    "_normalize_release_notes_history",
    "_normalize_release_notes_payload",
    "_portable_artifact_name",
    "_reconcile_downloaded_artifact_status",
    "_reconcile_handoff_status",
    "_retryable_install_status",
    "_stale_download_failed_status",
    "clear_handoff_diagnostics",
    "clear_handoff_request",
    "clear_install_plan",
    "clear_staged_helper",
    "clear_success_marker",
    "default_status_payload",
    "helper_runtime_tmpdir",
    "load_status",
    "read_cached_manifest",
    "save_status",
    "validate_install_plan",
    "write_handoff_diagnostics",
}

EXTERNAL_ADAPTERS = {
    "psutil",
}

CRYPTO_BINDINGS: set[str] = set()

RUNTIME_PATH_DEPENDENCIES = {
    "DesktopUpdatePaths",
    "_RUNTIME_SESSION_ROOT_FALLBACK",
    "_looks_like_windows_absolute_path",
    "_resolve_desktop_session_root_fallback",
    "_resolve_runtime_path",
    "_resolve_ship_current_version",
    "_runtime_session_root_candidate_fallback",
    "read_desktop_session_state",
    "resolve_desktop_session_root",
}

MUTABLE_COMPAT_HOOKS = (
    STDLIB_BINDINGS
    | EXTERNAL_ADAPTERS
    | CRYPTO_BINDINGS
    | {
        "_RUNTIME_SESSION_ROOT_FALLBACK",
        "DesktopUpdatePaths",
        "load_status",
        "read_json",
        "save_status",
        "write_json_atomic",
    }
)

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
class DesktopUpdateRootDependency:
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


def _print_inventory(inventory: tuple[DesktopUpdateRootDependency, ...]) -> None:
    print(json.dumps([row.as_json() for row in inventory], indent=2, sort_keys=True))


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
