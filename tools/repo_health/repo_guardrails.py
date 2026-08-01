from __future__ import annotations

import argparse
import ast
import fnmatch
import importlib
import inspect
import json
import re
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TOOLS_ROOT = Path(__file__).resolve().parent
BASELINE_PATH = TOOLS_ROOT / "test_line_budget_baseline.json"
DEFERRED_SOURCE_BUDGET_PATH = TOOLS_ROOT / "deferred_source_line_budget.json"
FRONTEND_GUARDRAILS = TOOLS_ROOT / "frontend_structure_guardrails.mjs"
FIXTURE_REFERENCE_ALLOWLIST_PATH = TOOLS_ROOT / "fixture_reference_allowlist.json"
SOURCE_SUPPRESSION_BUDGET_PATH = TOOLS_ROOT / "source_suppression_budget.json"
BRIDGE_API_COMPOSITION_MODULES = {
    "src/bridge/admin_entrypoint_api.py",
    "src/bridge/api.py",
    "src/bridge/bootstrap.py",
}

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from bridge_api_field_inventory import check_bridge_api_field_inventory
from bridge_route_inventory import check_bridge_route_inventory
from desktop_update_facade_inventory import check_desktop_update_facade_inventory
from desktop_update_root_dependency_inventory import (
    check_desktop_update_root_dependency_inventory,
)
from desktop_updater_root_dependency_inventory import (
    check_desktop_updater_root_dependency_inventory,
)
from update_manager_facade_inventory import check_update_manager_facade_inventory
from update_manager_runtime_facade_inventory import (
    check_update_manager_runtime_facade_inventory,
)

GROUPS = (
    "docs",
    "workflow",
    "compat",
    "routes",
    "frontend",
    "repo-root",
    "test-shape",
    "fixtures",
    "line-budget",
)

MARKDOWN_LINK_RE = re.compile(r"(?<!\!)\[[^\]]+\]\(([^)]+)\)")


@dataclass(frozen=True)
class GuardFailure:
    group: str
    name: str
    message: str


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    next_heading = text.find("\n## ", start + len(heading))
    return text[start:] if next_heading == -1 else text[start:next_heading]


def _load_function(module_name: str, function_name: str) -> Callable[..., object]:
    return getattr(importlib.import_module(module_name), function_name)


def _run_python_check(group: str, module_name: str, function_name: str) -> GuardFailure | None:
    check = _load_function(module_name, function_name)
    try:
        parameters = inspect.signature(check).parameters
        if "repo_root" in parameters:
            check(repo_root=ROOT)
        else:
            check()
    except Exception as exc:  # noqa: BLE001 - guardrails should report all assertion/config failures.
        return GuardFailure(group, function_name, str(exc))
    return None


def _run_python_checks(group: str, checks: Iterable[tuple[str, str]]) -> list[GuardFailure]:
    failures: list[GuardFailure] = []
    for module_name, function_name in checks:
        failure = _run_python_check(group, module_name, function_name)
        if failure is not None:
            failures.append(failure)
    return failures


def _iter_doc_paths() -> list[Path]:
    docs = sorted((ROOT / "docs").rglob("*.md"))
    tools = sorted((ROOT / "tools").rglob("*.md"))
    return [ROOT / "README.md", ROOT / "CONTRIBUTING.md", *docs, *tools]


def _local_markdown_target(raw: str) -> str | None:
    target = raw.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    if not target or target.startswith("#"):
        return None
    if target.startswith(("http://", "https://", "mailto:")):
        return None
    return target.split("#", 1)[0]


def check_markdown_links() -> list[str]:
    missing: list[str] = []
    for doc_path in _iter_doc_paths():
        text = doc_path.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK_RE.findall(text):
            target = _local_markdown_target(raw_target)
            if target is None:
                continue
            resolved = (doc_path.parent / target).resolve()
            if not resolved.exists():
                missing.append(f"{doc_path.relative_to(ROOT)} -> {target}")
    return missing


def check_repo_root_structure() -> list[str]:
    failures: list[str] = []
    required_root_files = (
        "index.html",
        "jobs.html",
        "saved.html",
        "admin.html",
        "theme.js",
        "frontend-runtime-config.js",
    )
    moved_root_support_files = (
        "admin-config.js",
        "app-local-data-client.js",
        "local-data-client.js",
        "desktop-local-data-client.js",
        "jobs-state.js",
        "jobs-parsing-utils.js",
        "saved-zip-utils.js",
    )
    required_frontend_owners = (
        "frontend/shared/config/admin-config.js",
        "frontend/shared/local-data/app-client.js",
        "frontend/shared/local-data/browser-client.js",
        "frontend/shared/local-data/desktop-client.js",
        "frontend/jobs/state.js",
        "frontend/jobs/parsing-utils.js",
        "frontend/saved/zip-utils.js",
    )

    for rel_path in required_root_files:
        if not (ROOT / rel_path).exists():
            failures.append(f"required root asset missing: {rel_path}")
    if not (ROOT / "styles").is_dir():
        failures.append("required root styles directory missing: styles")
    for rel_path in moved_root_support_files:
        if (ROOT / rel_path).exists():
            failures.append(f"support module drifted back to repo root: {rel_path}")
    for rel_path in required_frontend_owners:
        if not (ROOT / rel_path).exists():
            failures.append(f"relocated support module missing: {rel_path}")

    owner_expectations = {
        "scripts/run_startup_probe_pair.py": 'PAIR_ARTIFACT_ROOT = ROOT / ".tmp" / "packaged-desktop-smoke-pair"',
        "tests/helpers/temp_paths.py": '.tmp" / "pytest',
        "src/packaged_desktop_smoke.py": '.tmp" / "packaged-desktop-smoke',
        "probes/packaged_desktop_double_launch_probe.py": '.tmp" / "probes" / "double-launch',
        "tests/frontend/packaged-desktop-smoke.mjs": ".tmp/packaged-desktop-smoke/",
        "tests/frontend/packaged-desktop-smoke.first-run-jobs.mjs": ".tmp/packaged-desktop-smoke/",
        "tests/frontend/packaged-desktop-smoke.jobs-pipeline.mjs": ".tmp/packaged-desktop-smoke/",
        "playwright.config.js": ".tmp/playwright/test-results",
        ".github/workflows/build-portable-exe.yml": ".tmp/packaged-desktop-smoke",
    }
    forbidden_tokens = (".codex-tmp/", ".codex-tmp\\", "test-results/")
    for rel_path, expected_token in owner_expectations.items():
        text = (ROOT / rel_path).read_text(encoding="utf-8")
        if expected_token not in text:
            failures.append(f"{rel_path} should reference the .tmp-owned path")
        for forbidden in forbidden_tokens:
            if forbidden in text:
                failures.append(f"{rel_path} should not reference stale temp root `{forbidden}`")
    return failures


def check_runtime_facade_usage() -> list[str]:
    jobs_root = ROOT / "src" / "jobs"
    runtime_module = jobs_root / "adapters" / "_runtime.py"
    failures: list[str] = []
    if runtime_module.exists():
        failures.append("src/jobs/adapters/_runtime.py should not return.")
    offenders = []
    for path in jobs_root.rglob("*.py"):
        if "_runtime.facade(" in path.read_text(encoding="utf-8"):
            offenders.append(str(path.relative_to(ROOT)))
    if offenders:
        failures.append("Found retired `_runtime.facade()` usage:\n- " + "\n- ".join(offenders))
    return failures


def _bridge_api_import_failures(
    paths: Iterable[Path], *, import_scope: str, reference_scope: str
) -> list[str]:
    failures: list[str] = []
    for path in sorted(paths):
        rel_path = path.relative_to(ROOT).as_posix()

        text = path.read_text(encoding="utf-8")
        try:
            tree = ast.parse(text, filename=rel_path)
        except SyntaxError as exc:
            failures.append(f"{rel_path} could not be parsed: {exc}")
            continue

        bridge_api_import_lines: list[int] = []
        bridge_api_reference_lines: list[int] = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "src.bridge.api"
                and any(alias.name == "BridgeApi" for alias in node.names)
            ):
                bridge_api_import_lines.append(node.lineno)
            elif isinstance(node, ast.Import) and any(
                alias.name == "src.bridge.api" for alias in node.names
            ):
                bridge_api_import_lines.append(node.lineno)
            elif (
                isinstance(node, ast.ImportFrom)
                and node.module == "src.bridge"
                and any(alias.name == "api" for alias in node.names)
            ):
                bridge_api_import_lines.append(node.lineno)
            elif isinstance(node, ast.Name) and node.id == "BridgeApi":
                bridge_api_reference_lines.append(node.lineno)
            elif isinstance(node, ast.Attribute) and node.attr == "BridgeApi":
                bridge_api_reference_lines.append(node.lineno)

        if bridge_api_import_lines:
            failures.append(
                f"{rel_path} imports BridgeApi at lines {sorted(set(bridge_api_import_lines))}; "
                f"{import_scope} must depend on narrow capability protocols."
            )
        if bridge_api_reference_lines:
            failures.append(
                f"{rel_path} references BridgeApi at lines "
                f"{sorted(set(bridge_api_reference_lines))}; {reference_scope} must type against "
                "narrow capability protocols."
            )
    return failures


def check_bridge_route_bridge_api_imports() -> list[str]:
    route_root = ROOT / "src" / "bridge" / "routes"
    if not route_root.is_dir():
        return [f"bridge route directory is missing: {route_root.relative_to(ROOT)}"]
    return _bridge_api_import_failures(
        route_root.glob("*.py"),
        import_scope="route modules",
        reference_scope="route modules",
    )


def check_bridge_server_bridge_api_imports() -> list[str]:
    server_root = ROOT / "src" / "bridge" / "server"
    if not server_root.is_dir():
        return [f"bridge server directory is missing: {server_root.relative_to(ROOT)}"]
    return _bridge_api_import_failures(
        server_root.glob("*.py"),
        import_scope="bridge server modules",
        reference_scope="bridge server modules",
    )


def check_bridge_production_bridge_api_imports() -> list[str]:
    source_root = ROOT / "src"
    if not source_root.is_dir():
        return [f"source directory is missing: {source_root.relative_to(ROOT)}"]
    paths = [
        path
        for path in source_root.rglob("*.py")
        if path.relative_to(ROOT).as_posix() not in BRIDGE_API_COMPOSITION_MODULES
    ]
    return _bridge_api_import_failures(
        paths,
        import_scope="production modules outside bridge composition",
        reference_scope="production modules outside bridge composition",
    )


def _load_line_budget_baseline() -> dict[str, object]:
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def _matches_any(rel_path: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatch(rel_path, pattern) for pattern in patterns)


def _line_count(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").splitlines())


def check_line_budget() -> list[str]:
    baseline = _load_line_budget_baseline()
    default_budget = int(baseline["default_budget"])
    integration_budget = int(baseline["integration_budget"])
    excluded_globs = tuple(baseline["excluded_globs"])
    integration_globs = tuple(baseline["integration_globs"])
    baselined_files = dict(baseline["files"])
    failures: list[str] = []

    for path in sorted((ROOT / "tests").rglob("*")):
        if not path.is_file() or path.suffix not in {".py", ".mjs"}:
            continue
        rel_path = path.relative_to(ROOT).as_posix()
        if _matches_any(rel_path, excluded_globs):
            continue

        budget = integration_budget if _matches_any(rel_path, integration_globs) else default_budget
        allowed = int(baselined_files.get(rel_path, budget))
        line_count = _line_count(path)
        if line_count > allowed:
            if rel_path in baselined_files:
                failures.append(f"{rel_path} has {line_count} lines; baseline is {allowed}.")
            else:
                failures.append(f"{rel_path} has {line_count} lines; budget is {budget}.")

    return failures


def _load_deferred_source_budget() -> dict[str, object]:
    return json.loads(DEFERRED_SOURCE_BUDGET_PATH.read_text(encoding="utf-8"))


def check_deferred_source_line_budget() -> list[str]:
    failures: list[str] = []
    try:
        payload = _load_deferred_source_budget()
    except OSError as exc:
        return [f"deferred source budget file is missing: {exc}"]
    except json.JSONDecodeError as exc:
        return [f"deferred source budget file is not valid JSON: {exc}"]

    entries = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return ["deferred source budget must be an object with a `files` list."]

    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            failures.append(f"deferred source budget entry {index} must be an object.")
            continue

        rel_path = str(entry.get("path") or "").strip().replace("\\", "/")
        max_lines = entry.get("max_lines")
        rationale = str(entry.get("rationale") or "").strip()

        if not rel_path:
            failures.append(f"deferred source budget entry {index} must include a path.")
            continue
        if Path(rel_path).is_absolute() or ".." in Path(rel_path).parts:
            failures.append(
                f"deferred source budget entry {index} path must be repo-relative: {rel_path}"
            )
            continue
        if not isinstance(max_lines, int) or max_lines <= 0:
            failures.append(
                f"deferred source budget entry {index} for {rel_path} must include a positive integer max_lines."
            )
            continue
        if not rationale:
            failures.append(
                f"deferred source budget entry {index} for {rel_path} must include a non-empty rationale."
            )
            continue

        path = ROOT / rel_path
        if not path.is_file():
            failures.append(f"{rel_path} is listed in deferred source budget but does not exist.")
            continue

        line_count = _line_count(path)
        if line_count > max_lines:
            failures.append(
                f"{rel_path} has {line_count} lines; deferred source budget is {max_lines}."
            )

    return failures


def _suppression_codes(line: str) -> list[str]:
    if "# noqa:" in line:
        raw_codes = line.split("# noqa:", 1)[1].strip().split()[0]
        return [code.strip() for code in raw_codes.split(",") if code.strip()]
    if "# noqa" in line:
        return ["noqa-unspecified"]
    if "# type: ignore" in line:
        return ["type-ignore"]
    if "# pyright:" in line:
        return ["pyright"]
    if "# mypy:" in line:
        return ["mypy"]
    return []


def _count_source_suppressions() -> tuple[int, Counter[str], Counter[tuple[str, str]]]:
    total = 0
    by_code: Counter[str] = Counter()
    by_code_file: Counter[tuple[str, str]] = Counter()
    for path in sorted((ROOT / "src").rglob("*.py")):
        rel_path = path.relative_to(ROOT).as_posix()
        for line in path.read_text(encoding="utf-8").splitlines():
            codes = _suppression_codes(line)
            if not codes:
                continue
            total += 1
            by_code.update(codes)
            by_code_file.update((code, rel_path) for code in codes)
    return total, by_code, by_code_file


def check_source_suppression_budget() -> list[str]:
    failures: list[str] = []
    try:
        payload = json.loads(SOURCE_SUPPRESSION_BUDGET_PATH.read_text(encoding="utf-8"))
    except OSError as exc:
        return [f"source suppression budget file is missing: {exc}"]
    except json.JSONDecodeError as exc:
        return [f"source suppression budget file is not valid JSON: {exc}"]

    if not isinstance(payload, dict):
        return ["source suppression budget must be a JSON object."]
    if str(payload.get("scope") or "") != "src":
        failures.append("source suppression budget scope must be `src`.")
    max_total = payload.get("max_total_comments")
    if not isinstance(max_total, int) or max_total < 0:
        failures.append("source suppression budget must include non-negative max_total_comments.")
        max_total = 0
    max_by_code = payload.get("max_by_code")
    if not isinstance(max_by_code, dict) or not max_by_code:
        failures.append("source suppression budget must include a non-empty max_by_code object.")
        max_by_code = {}
    for code, budget in max_by_code.items():
        if not isinstance(code, str) or not code.strip():
            failures.append("source suppression budget codes must be non-empty strings.")
        if not isinstance(budget, int) or budget < 0:
            failures.append(f"source suppression budget for {code!r} must be non-negative.")
    allowed_by_code_file = payload.get("allowed_by_code_file", {})
    if not isinstance(allowed_by_code_file, dict):
        failures.append("source suppression budget allowed_by_code_file must be an object.")
        allowed_by_code_file = {}
    for code, paths in allowed_by_code_file.items():
        if not isinstance(code, str) or not code.strip():
            failures.append(
                "source suppression budget allowed_by_code_file codes must be non-empty strings."
            )
            continue
        if not isinstance(paths, list) or not paths:
            failures.append(
                f"source suppression budget allowed files for {code!r} must be a non-empty list."
            )
            continue
        for path in paths:
            if not isinstance(path, str) or not path.startswith("src/"):
                failures.append(
                    f"source suppression budget allowed file for {code!r} must be a repo-relative src path."
                )
    if not str(payload.get("rationale") or "").strip():
        failures.append("source suppression budget must include a non-empty rationale.")
    if failures:
        return failures

    total, by_code, by_code_file = _count_source_suppressions()
    if total > max_total:
        failures.append(f"src has {total} suppression comments; budget is {max_total}.")
    for code, count in sorted(by_code.items()):
        budget = int(max_by_code.get(code, -1))
        if budget < 0:
            failures.append(f"src has unbudgeted suppression code {code}: {count}.")
        elif count > budget:
            failures.append(f"src has {count} {code} suppressions; budget is {budget}.")
    for code, paths in sorted(allowed_by_code_file.items()):
        allowed_paths = set(paths)
        for observed_code, rel_path in sorted(by_code_file):
            if observed_code == code and rel_path not in allowed_paths:
                failures.append(
                    f"src has {code} suppression in {rel_path}; allowed files are {sorted(allowed_paths)}."
                )
    return failures


def _load_fixture_reference_allowlist() -> tuple[set[str], list[str]]:
    if not FIXTURE_REFERENCE_ALLOWLIST_PATH.exists():
        return set(), []
    try:
        entries = json.loads(FIXTURE_REFERENCE_ALLOWLIST_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return set(), [f"fixture allowlist is not valid JSON: {exc}"]
    if not isinstance(entries, list):
        return set(), ["fixture allowlist must be a JSON list."]

    allowed: set[str] = set()
    failures: list[str] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            failures.append(f"fixture allowlist entry {index} must be an object.")
            continue
        path = str(entry.get("path") or "").strip().replace("\\", "/")
        reason = str(entry.get("reason") or "").strip()
        if not path or not reason:
            failures.append(
                f"fixture allowlist entry {index} must include non-empty path and reason."
            )
            continue
        allowed.add(path)
    return allowed, failures


def _iter_fixture_reference_sources() -> Iterable[Path]:
    source_roots = ("tests", "tools", "src", "frontend", "scripts")
    suffixes = {".py", ".mjs", ".js", ".json", ".md", ".yml", ".yaml", ".toml"}
    excluded_parts = {".git", ".tmp", "node_modules", "__pycache__", "dist", "_out"}
    for root_name in source_roots:
        root = ROOT / root_name
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in suffixes:
                continue
            rel_parts = set(path.relative_to(ROOT).parts)
            if rel_parts & excluded_parts:
                continue
            if path.is_relative_to(ROOT / "tests" / "fixtures"):
                continue
            yield path


def check_fixture_references() -> list[str]:
    allowed, failures = _load_fixture_reference_allowlist()
    reference_texts = [
        path.read_text(encoding="utf-8", errors="ignore")
        for path in _iter_fixture_reference_sources()
    ]
    combined_text = "\n".join(reference_texts)

    for fixture_path in sorted((ROOT / "tests" / "fixtures").rglob("*")):
        if not fixture_path.is_file():
            continue
        rel_path = fixture_path.relative_to(ROOT).as_posix()
        if rel_path in allowed:
            continue
        rel_from_fixtures = fixture_path.relative_to(ROOT / "tests" / "fixtures").as_posix()
        tokens = {rel_path, rel_path.replace("/", "\\"), rel_from_fixtures, fixture_path.name}
        if not any(token in combined_text for token in tokens):
            failures.append(f"{rel_path} is not referenced by any test, helper, or source file.")

    return failures


def _failure_from_messages(group: str, name: str, messages: list[str]) -> GuardFailure | None:
    if messages:
        return GuardFailure(group, name, "\n".join(messages))
    return None


def run_docs_group() -> list[GuardFailure]:
    checks = [
        ("release_docs_policy", name)
        for name in (
            "test_release_guide_is_canonical_single_source",
            "test_release_docs_cover_the_current_public_release_line",
            "test_local_setup_points_to_canonical_commands_and_docs",
            "test_ai_bootstrap_sequence_is_single_path",
            "test_docs_workflow_is_indexed_and_linked_for_contributors",
            "test_runtime_and_tool_configs_keep_separate_ownership",
            "test_serena_tooling_is_first_class_for_codex_and_opencode",
            "test_docs_avoid_stale_archive_and_generated_artifact_links",
            "test_ai_docs_classify_compatibility_surfaces",
            "test_ai_docs_use_exact_frontend_syntax_example",
            "test_readme_is_product_overview_not_ai_entrypoint",
            "test_release_and_setup_docs_use_canonical_packaged_smoke_commands",
            "test_testing_doc_owns_verification_matrix",
            "test_release_guide_uses_canonical_release_preflight",
            "test_active_docs_avoid_stale_runtime_and_test_guidance",
            "test_index_routes_current_process_docs_only",
            "test_contributing_points_startup_perf_changes_to_canonical_architecture_doc",
        )
    ]
    failures = _run_python_checks("docs", checks)
    markdown_failure = _failure_from_messages(
        "docs", "check_markdown_links", check_markdown_links()
    )
    if markdown_failure:
        failures.append(markdown_failure)
    return failures


def run_workflow_group() -> list[GuardFailure]:
    checks = [
        ("workflow_policy", name)
        for name in (
            "test_release_workflow_uses_canonical_test_entrypoints",
            "test_lint_workflow_uses_canonical_precommit_entrypoints",
            "test_github_workflows_use_project_node_runtime_and_playwright_bridge_owner",
            "test_lint_workflow_enforces_ruff_import_sorting",
            "test_lint_workflow_enforces_source_complexity_baseline",
            "test_package_json_exposes_repo_guardrails_entrypoint",
            "test_package_json_exposes_python_security_audit_entrypoint",
            "test_pre_push_hook_uses_timed_lint_default_and_explicit_full_ci_mode",
            "test_pre_commit_hook_runs_lint_gate",
            "test_package_json_dev_pipeline_uses_module_entrypoint",
            "test_package_json_exposes_refactor_changed_entrypoint",
            "test_package_json_packaged_smoke_scripts_use_direct_dist_by_default",
            "test_package_json_perf_scripts_reuse_existing_perf_entrypoints",
        )
    ]
    return _run_python_checks("workflow", checks)


def run_compat_group() -> list[GuardFailure]:
    excluded = {
        "test_discovered_python_test_files_define_real_tests",
        "test_frontend_test_patterns_disallow_generated_manifest_aggregators",
    }
    module = importlib.import_module("suite_contract_policy")
    checks = [
        ("suite_contract_policy", name)
        for name, value in inspect.getmembers(module, inspect.isfunction)
        if name.startswith("test_") and name not in excluded
    ]
    failures = _run_python_checks("compat", checks)
    bridge_api_failure = _failure_from_messages(
        "compat", "check_bridge_api_field_inventory", check_bridge_api_field_inventory()
    )
    if bridge_api_failure:
        failures.append(bridge_api_failure)
    bridge_production_failure = _failure_from_messages(
        "compat",
        "check_bridge_production_bridge_api_imports",
        check_bridge_production_bridge_api_imports(),
    )
    if bridge_production_failure:
        failures.append(bridge_production_failure)
    bridge_server_failure = _failure_from_messages(
        "compat",
        "check_bridge_server_bridge_api_imports",
        check_bridge_server_bridge_api_imports(),
    )
    if bridge_server_failure:
        failures.append(bridge_server_failure)
    desktop_update_failure = _failure_from_messages(
        "compat",
        "check_desktop_update_facade_inventory",
        check_desktop_update_facade_inventory(),
    )
    if desktop_update_failure:
        failures.append(desktop_update_failure)
    desktop_update_root_failure = _failure_from_messages(
        "compat",
        "check_desktop_update_root_dependency_inventory",
        check_desktop_update_root_dependency_inventory(),
    )
    if desktop_update_root_failure:
        failures.append(desktop_update_root_failure)
    desktop_updater_root_failure = _failure_from_messages(
        "compat",
        "check_desktop_updater_root_dependency_inventory",
        check_desktop_updater_root_dependency_inventory(),
    )
    if desktop_updater_root_failure:
        failures.append(desktop_updater_root_failure)
    update_manager_failure = _failure_from_messages(
        "compat",
        "check_update_manager_facade_inventory",
        check_update_manager_facade_inventory(),
    )
    if update_manager_failure:
        failures.append(update_manager_failure)
    update_manager_runtime_failure = _failure_from_messages(
        "compat",
        "check_update_manager_runtime_facade_inventory",
        check_update_manager_runtime_facade_inventory(),
    )
    if update_manager_runtime_failure:
        failures.append(update_manager_runtime_failure)
    return failures


def run_routes_group() -> list[GuardFailure]:
    failures: list[GuardFailure] = []
    for name, messages in (
        ("check_bridge_route_inventory", check_bridge_route_inventory()),
        (
            "check_bridge_route_bridge_api_imports",
            check_bridge_route_bridge_api_imports(),
        ),
    ):
        failure = _failure_from_messages("routes", name, messages)
        if failure:
            failures.append(failure)
    return failures


def run_frontend_group() -> list[GuardFailure]:
    completed = subprocess.run(
        ["node", "--test", "--test-reporter=dot", str(FRONTEND_GUARDRAILS)],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode == 0:
        return []
    output = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
    return [GuardFailure("frontend", "frontend_structure_guardrails", output.strip())]


def run_repo_root_group() -> list[GuardFailure]:
    failures: list[GuardFailure] = []
    for name, messages in (
        ("check_repo_root_structure", check_repo_root_structure()),
        ("check_runtime_facade_usage", check_runtime_facade_usage()),
    ):
        failure = _failure_from_messages("repo-root", name, messages)
        if failure:
            failures.append(failure)
    return failures


def run_test_shape_group() -> list[GuardFailure]:
    checks = [
        ("suite_contract_policy", "test_discovered_python_test_files_define_real_tests"),
        (
            "suite_contract_policy",
            "test_frontend_test_patterns_disallow_generated_manifest_aggregators",
        ),
    ]
    return _run_python_checks("test-shape", checks)


def run_fixtures_group() -> list[GuardFailure]:
    failure = _failure_from_messages(
        "fixtures", "check_fixture_references", check_fixture_references()
    )
    return [failure] if failure else []


def run_line_budget_group() -> list[GuardFailure]:
    failures: list[GuardFailure] = []
    for name, messages in (
        ("check_line_budget", check_line_budget()),
        ("check_deferred_source_line_budget", check_deferred_source_line_budget()),
        ("check_source_suppression_budget", check_source_suppression_budget()),
    ):
        failure = _failure_from_messages("line-budget", name, messages)
        if failure:
            failures.append(failure)
    return failures


GROUP_RUNNERS: dict[str, Callable[[], list[GuardFailure]]] = {
    "docs": run_docs_group,
    "workflow": run_workflow_group,
    "compat": run_compat_group,
    "routes": run_routes_group,
    "frontend": run_frontend_group,
    "repo-root": run_repo_root_group,
    "test-shape": run_test_shape_group,
    "fixtures": run_fixtures_group,
    "line-budget": run_line_budget_group,
}


def run_groups(groups: Iterable[str]) -> list[GuardFailure]:
    failures: list[GuardFailure] = []
    for group in groups:
        group_failures = GROUP_RUNNERS[group]()
        if group_failures:
            failures.extend(group_failures)
            continue
        print(f"repo guardrails: {group} passed")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Baluffo repository policy guardrails.")
    parser.add_argument(
        "--group",
        action="append",
        choices=GROUPS,
        help="Run one guardrail group. Repeat for multiple groups. Defaults to all groups.",
    )
    args = parser.parse_args(argv)
    groups = args.group or list(GROUPS)
    failures = run_groups(groups)
    if failures:
        for failure in failures:
            print(f"[{failure.group}] {failure.name}\n{failure.message}\n", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
