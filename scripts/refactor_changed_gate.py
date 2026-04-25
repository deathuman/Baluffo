#!/usr/bin/env python3
"""Run a path-aware refactor verification lane for changed files."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from shutil import which

try:
    from scripts import precommit_gate
except ImportError:  # pragma: no cover - direct script execution path
    import precommit_gate  # type: ignore[no-redef]

PYTHON = sys.executable
DIFF_FILTER = "--diff-filter=ACMRTUXB"
NPM = which("npm") or "npm"
DOCS_COMMAND = [PYTHON, "tools/repo_health/repo_guardrails.py", "--group", "docs"]
WORKFLOW_COMMAND = [
    PYTHON,
    "tools/repo_health/repo_guardrails.py",
    "--group",
    "workflow",
]
EXTENDED_COMMAND = [NPM, "run", "test:py:extended"]
COMPAT_COMMAND = [PYTHON, "tools/repo_health/repo_guardrails.py", "--group", "compat"]


def _resolve_diff_base() -> str | None:
    candidate_refs: list[str] = []
    try:
        upstream = precommit_gate._git_lines(
            "rev-parse",
            "--abbrev-ref",
            "--symbolic-full-name",
            "@{upstream}",
        )
        if upstream:
            candidate_refs.extend(upstream)
    except RuntimeError:
        pass

    candidate_refs.append("origin/main")
    seen: set[str] = set()
    for candidate in candidate_refs:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            merge_base = precommit_gate._git_lines("merge-base", "HEAD", candidate)
        except RuntimeError:
            continue
        if merge_base:
            return merge_base[0]

    try:
        head_parent = precommit_gate._git_lines("rev-parse", "HEAD~1")
    except RuntimeError:
        return None
    return head_parent[0] if head_parent else None


def _collect_committed_changed_files(base_ref: str | None = None) -> list[str]:
    base = base_ref or _resolve_diff_base()
    if not base:
        return []

    files: list[str] = []
    seen: set[str] = set()
    for rel_path in precommit_gate._git_lines("diff", "--name-only", DIFF_FILTER, f"{base}..HEAD"):
        if rel_path in seen or precommit_gate._is_excluded_root(rel_path):
            continue
        abs_path = precommit_gate.ROOT / rel_path
        if not abs_path.exists() or abs_path.is_dir():
            continue
        seen.add(rel_path)
        files.append(rel_path)
    return files


def collect_refactor_changed_files(base_ref: str | None = None) -> list[str]:
    files = precommit_gate.collect_changed_files()
    if files:
        return files
    return _collect_committed_changed_files(base_ref)


def _matches_any(rel_path: str, prefixes: tuple[str, ...], exact: tuple[str, ...] = ()) -> bool:
    return rel_path in exact or any(rel_path.startswith(prefix) for prefix in prefixes)


def _is_docs_change(rel_path: str) -> bool:
    return rel_path.startswith("docs/")


def _is_workflow_change(rel_path: str) -> bool:
    return rel_path in {
        "package.json",
        "scripts/install_git_hooks.py",
        "scripts/precommit_gate.py",
        "scripts/refactor_changed_gate.py",
        ".githooks/pre-commit",
        ".githooks/pre-push",
    }


def _is_desktop_app_surface(rel_path: str) -> bool:
    return _matches_any(rel_path, ("src/ship/desktop_app/",))


def _is_desktop_updater_surface(rel_path: str) -> bool:
    return _matches_any(
        rel_path,
        ("src/ship/desktop_updater_",),
        exact=("src/ship/desktop_updater.py",),
    )


def _is_desktop_update_surface(rel_path: str) -> bool:
    return _matches_any(
        rel_path,
        ("src/ship/desktop_update_",),
        exact=("src/ship/desktop_update.py",),
    )


def _is_packaged_smoke_root(rel_path: str) -> bool:
    return rel_path == "src/packaged_desktop_smoke.py"


def _is_discovery_compat_surface(rel_path: str) -> bool:
    return rel_path in {
        "src/source_discovery.py",
        "src/source_discovery/gamesmap.py",
        "src/source_discovery/reporting.py",
        "src/source_discovery/web_search.py",
    }


def _is_source_sync_surface(rel_path: str) -> bool:
    return rel_path == "src/source_sync.py"


def _is_admin_bridge_surface(rel_path: str) -> bool:
    return rel_path == "src/admin_bridge.py"


def _is_jobs_compat_surface(rel_path: str) -> bool:
    return rel_path in {
        "src/jobs_fetcher.py",
        "src/jobs/pipeline.py",
        "src/jobs/state.py",
        "src/jobs/reporting.py",
        "src/jobs/common/contracts.py",
    }


def _is_packaging_or_release_change(rel_path: str) -> bool:
    return _matches_any(
        rel_path,
        (
            "src/ship/packaged_smoke/",
            "scripts/build_",
        ),
        exact=(
            "src/ship/runtime_launcher.py",
            "src/ship/update_manager.py",
            "scripts/orchestrator.py",
        ),
    )


GROUP_COMMANDS: dict[str, list[list[str]]] = {
    "workflow": [WORKFLOW_COMMAND],
    "desktop_app": [
        COMPAT_COMMAND,
        [PYTHON, "-m", "pytest", "tests/desktop_app", "-q"],
    ],
    "desktop_updater": [
        COMPAT_COMMAND,
        [PYTHON, "-m", "pytest", "tests/test_desktop_updater.py", "-q"],
    ],
    "desktop_update": [
        COMPAT_COMMAND,
        [PYTHON, "-m", "pytest", "tests/test_desktop_update.py", "-q"],
    ],
    "packaged_smoke": [
        COMPAT_COMMAND,
        [PYTHON, "-m", "pytest", "tests/packaged_desktop", "-q"],
    ],
    "discovery": [
        COMPAT_COMMAND,
        [PYTHON, "-m", "pytest", "tests/source_discovery", "-q"],
    ],
    "source_sync": [
        COMPAT_COMMAND,
        [PYTHON, "-m", "pytest", "tests/test_source_sync.py", "-q"],
    ],
    "admin_bridge": [
        COMPAT_COMMAND,
        [PYTHON, "-m", "pytest", "tests/admin", "-q"],
    ],
    "jobs": [
        COMPAT_COMMAND,
        [
            PYTHON,
            "-m",
            "pytest",
            "tests/test_jobs_fetcher.py",
            "tests/test_jobs_fetcher_google_sheets.py",
            "tests/test_jobs_fetcher_parsing.py",
            "tests/test_jobs_fetcher_pipeline.py",
            "tests/test_jobs_fetcher_providers.py",
            "tests/test_jobs_fetcher_quality.py",
            "tests/test_jobs_package.py",
            "tests/test_jobs_pipeline_guard.py",
            "-q",
        ],
    ],
}

GROUP_MATCHERS: tuple[tuple[str, Callable[[str], bool]], ...] = (
    ("workflow", _is_workflow_change),
    ("desktop_app", _is_desktop_app_surface),
    ("desktop_updater", _is_desktop_updater_surface),
    ("desktop_update", _is_desktop_update_surface),
    ("packaged_smoke", _is_packaged_smoke_root),
    ("discovery", _is_discovery_compat_surface),
    ("source_sync", _is_source_sync_surface),
    ("admin_bridge", _is_admin_bridge_surface),
    ("jobs", _is_jobs_compat_surface),
)


def _selected_groups(files: list[str]) -> list[str]:
    groups: list[str] = []
    for name, matcher in GROUP_MATCHERS:
        if any(matcher(path) for path in files):
            groups.append(name)
    return groups


def build_verification_commands(files: list[str]) -> list[list[str]]:
    if not files:
        return []

    if any(_is_packaging_or_release_change(path) for path in files):
        return [EXTENDED_COMMAND]

    groups = _selected_groups(files)
    non_workflow_groups = [name for name in groups if name != "workflow"]
    if len(non_workflow_groups) > 1:
        return [EXTENDED_COMMAND]

    commands: list[list[str]] = []
    if any(_is_docs_change(path) for path in files):
        commands.append(DOCS_COMMAND)
    for name in groups:
        commands.extend(GROUP_COMMANDS[name])
    return commands


def _run_command(command: list[str]) -> int:
    completed = subprocess.run(command, cwd=precommit_gate.ROOT, check=False)
    return completed.returncode


def run_changed(base_ref: str | None = None) -> int:
    files = collect_refactor_changed_files(base_ref)
    if not files:
        print("No changed files found for refactor guard; skipping.")
        return 0

    commands = build_verification_commands(files)
    if not commands:
        print("No refactor-sensitive changes detected; skipping.")
        return 0

    print("Running refactor-sensitive verification for:")
    for rel_path in files:
        print(f" - {rel_path}")

    for command in commands:
        print(f"-> {' '.join(command)}")
        return_code = _run_command(command)
        if return_code != 0:
            return return_code
    return 0


if __name__ == "__main__":
    raise SystemExit(run_changed())
