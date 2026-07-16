from __future__ import annotations

import argparse
import ast
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BRIDGE_API_PATH = Path("src/bridge/api.py")
EXPECTED_BRIDGE_API_FIELD_COUNT = 94

CATEGORIES = {
    "runtime-path",
    "service-handle",
    "bootstrap-injected",
    "service-wired",
    "route-used",
    "post-route-used",
    "helper-used",
    "test-overridden",
    "default-only",
}

RUNTIME_PATH_FIELDS = {
    "runtime_config",
    "DISCOVERY_REPORT_PATH",
    "JOBS_FETCH_REPORT_PATH",
    "APPROVAL_STATE_PATH",
    "DISCOVERY_LOG_PATH",
    "FETCHER_LOG_PATH",
    "STARTUP_METRICS_PATH",
    "SOURCE_POLICY_RECOMMENDATIONS_PATH",
    "SOURCE_POLICY_REVIEW_STATE_PATH",
    "DEDUP_REVIEW_STATE_PATH",
    "DISCOVERY_CANDIDATES_PATH",
    "DESKTOP_UPDATE_STATE_PATH",
    "DESKTOP_SESSION_ACTIVITY_AT",
    "app_version",
}

SERVICE_HANDLE_FIELDS = {"registry", "sync", "pipeline", "discovery", "availability"}
BOOTSTRAP_FILES = {
    Path("src/bridge/bootstrap.py"),
    Path("src/bridge/admin_entrypoint_api.py"),
}
PRODUCTION_SCAN_ROOTS = (
    Path("src/bridge"),
    Path("src/ship"),
)
PRODUCTION_SCAN_FILES = (Path("src/admin_bridge.py"),)


@dataclass(frozen=True)
class BridgeApiField:
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


def _bridge_api_class(tree: ast.Module) -> ast.ClassDef:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "BridgeApi":
            return node
    raise AssertionError("BridgeApi class not found in src/bridge/api.py")


def bridge_api_field_names(repo_root: Path = ROOT) -> list[str]:
    tree = _parse_python(repo_root / BRIDGE_API_PATH)
    bridge_api = _bridge_api_class(tree)
    fields: list[str] = []
    for node in bridge_api.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            fields.append(node.target.id)
    return fields


def _bridge_api_service_wired_fields(repo_root: Path, field_names: set[str]) -> set[str]:
    tree = _parse_python(repo_root / BRIDGE_API_PATH)
    bridge_api = _bridge_api_class(tree)
    wired: set[str] = set()
    for node in bridge_api.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name not in {"__post_init__", "_wire_registry_defaults"}:
            continue
        for child in ast.walk(node):
            if isinstance(child, ast.Constant) and isinstance(child.value, str):
                if child.value in field_names:
                    wired.add(child.value)
    return wired


def _call_keyword_fields(path: Path, field_names: set[str]) -> set[str]:
    tree = _parse_python(path)
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg in field_names:
                    found.add(keyword.arg)
    return found


def _api_attribute_refs(path: Path, field_names: set[str]) -> set[str]:
    tree = _parse_python(path)
    refs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            if node.attr not in field_names:
                continue
            if isinstance(node.value, ast.Name) and node.value.id == "api":
                refs.add(node.attr)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "api"
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
            and node.args[1].value in field_names
        ):
            refs.add(node.args[1].value)
    return refs


def _test_override_refs(path: Path, field_names: set[str]) -> set[str]:
    tree = _parse_python(path)
    refs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in field_names:
            if isinstance(node.ctx, ast.Store):
                refs.add(node.attr)
        elif isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg in field_names:
                    refs.add(keyword.arg)
            if (
                isinstance(node.func, ast.Name)
                and node.func.id == "setattr"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)
                and node.args[1].value in field_names
            ):
                refs.add(node.args[1].value)
    return refs


def _production_python_paths(repo_root: Path) -> list[Path]:
    paths: set[Path] = set()
    for root in PRODUCTION_SCAN_ROOTS:
        paths.update((repo_root / root).rglob("*.py"))
    for rel_path in PRODUCTION_SCAN_FILES:
        path = repo_root / rel_path
        if path.exists():
            paths.add(path)
    return sorted(paths)


def _test_python_paths(repo_root: Path) -> list[Path]:
    tests_root = repo_root / "tests"
    if not tests_root.exists():
        return []
    return sorted(tests_root.rglob("*.py"))


def _generic_production_refs(repo_root: Path, field_names: set[str]) -> dict[str, set[str]]:
    refs: dict[str, set[str]] = defaultdict(set)
    for path in _production_python_paths(repo_root):
        rel_path = path.relative_to(repo_root).as_posix()
        if rel_path == BRIDGE_API_PATH.as_posix():
            continue
        text = path.read_text(encoding="utf-8")
        for field_name in field_names:
            if field_name in text:
                refs[field_name].add(rel_path)
    return refs


def collect_bridge_api_field_inventory(repo_root: Path = ROOT) -> tuple[BridgeApiField, ...]:
    field_names = bridge_api_field_names(repo_root)
    field_name_set = set(field_names)
    categories: dict[str, set[str]] = {field_name: set() for field_name in field_names}
    references: dict[str, set[str]] = {field_name: set() for field_name in field_names}

    for field_name in sorted(RUNTIME_PATH_FIELDS & field_name_set):
        categories[field_name].add("runtime-path")
    for field_name in sorted(SERVICE_HANDLE_FIELDS & field_name_set):
        categories[field_name].add("service-handle")

    for rel_path in BOOTSTRAP_FILES:
        path = repo_root / rel_path
        if not path.exists():
            continue
        for field_name in _call_keyword_fields(path, field_name_set):
            categories[field_name].add("bootstrap-injected")
            references[field_name].add(f"{rel_path.as_posix()}:bootstrap")

    for field_name in _bridge_api_service_wired_fields(repo_root, field_name_set):
        categories[field_name].add("service-wired")
        references[field_name].add(f"{BRIDGE_API_PATH.as_posix()}:service-wiring")

    for path in _production_python_paths(repo_root):
        rel_path = path.relative_to(repo_root).as_posix()
        if rel_path == BRIDGE_API_PATH.as_posix():
            continue
        attrs = _api_attribute_refs(path, field_name_set)
        for field_name in attrs:
            if rel_path.startswith("src/bridge/routes/post_routes"):
                categories[field_name].add("post-route-used")
            elif rel_path.startswith("src/bridge/routes/"):
                categories[field_name].add("route-used")
            else:
                categories[field_name].add("helper-used")
            references[field_name].add(rel_path)

    for path in _test_python_paths(repo_root):
        rel_path = path.relative_to(repo_root).as_posix()
        for field_name in _test_override_refs(path, field_name_set):
            categories[field_name].add("test-overridden")
            references[field_name].add(rel_path)

    for field_name in field_names:
        if not categories[field_name]:
            categories[field_name].add("default-only")

    return tuple(
        BridgeApiField(
            name=field_name,
            categories=tuple(sorted(categories[field_name])),
            references=tuple(sorted(references[field_name])),
        )
        for field_name in field_names
    )


def check_bridge_api_field_inventory(repo_root: Path | None = None) -> list[str]:
    root = repo_root or ROOT
    inventory = collect_bridge_api_field_inventory(root)
    failures: list[str] = []

    if len(inventory) != EXPECTED_BRIDGE_API_FIELD_COUNT:
        failures.append(
            "BridgeApi has "
            f"{len(inventory)} dataclass fields; expected {EXPECTED_BRIDGE_API_FIELD_COUNT}. "
            "Update EXPECTED_BRIDGE_API_FIELD_COUNT and review field classification evidence."
        )

    field_names = {field.name for field in inventory}
    for configured in sorted((RUNTIME_PATH_FIELDS | SERVICE_HANDLE_FIELDS) - field_names):
        failures.append(f"BridgeApi field classification references missing field {configured!r}.")

    generic_refs = _generic_production_refs(root, field_names)
    for field in inventory:
        unknown = set(field.categories) - CATEGORIES
        if unknown:
            failures.append(
                f"BridgeApi field {field.name} has unknown categories: {sorted(unknown)}."
            )
        if not field.categories:
            failures.append(f"BridgeApi field {field.name} is unclassified.")
        if "default-only" in field.categories and generic_refs.get(field.name):
            refs = ", ".join(sorted(generic_refs[field.name]))
            failures.append(
                f"BridgeApi field {field.name} is default-only but referenced by production code: {refs}."
            )

    return failures


def _print_text(inventory: tuple[BridgeApiField, ...]) -> None:
    for field in inventory:
        categories = ", ".join(field.categories)
        references = ", ".join(field.references)
        suffix = f" [{references}]" if references else ""
        print(f"{field.name}: {categories}{suffix}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Classify BridgeApi dataclass field usage.")
    parser.add_argument(
        "--json", action="store_true", help="Print machine-readable inventory JSON."
    )
    parser.add_argument("--check", action="store_true", help="Validate the inventory.")
    args = parser.parse_args(argv)

    inventory = collect_bridge_api_field_inventory(ROOT)
    if args.json:
        print(json.dumps([field.as_json() for field in inventory], indent=2, sort_keys=True))
    elif not args.check:
        _print_text(inventory)

    failures = check_bridge_api_field_inventory(ROOT) if args.check else []
    if failures:
        for failure in failures:
            print(failure)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
