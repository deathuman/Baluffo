"""Tests for the ship-bundle manifest completeness guardrail.

Ensures every top-level ``src/*.py|*.json`` module either ships in the desktop bundle
(``APP_RUNTIME_SCRIPTS``) or is explicitly declared dev/container tooling, and that no
manifest entry is stale. This is what catches a new top-level runtime module forgotten
from the bundle manifest (e.g. the earlier ``source_registry_data.py`` miss).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.repo_health import repo_guardrails
from tools.repo_health.repo_guardrails import check_bundle_manifest_completeness


def _make_src(root: Path, *filenames: str) -> Path:
    src_root = root / "src"
    src_root.mkdir(parents=True, exist_ok=True)
    for name in filenames:
        src_root.joinpath(name).write_text("x = 1\n", encoding="utf-8")
    return src_root


def test_clean_shipping_and_allowlisted_modules_pass(tmp_path: Path) -> None:
    _make_src(tmp_path, "runtime_module.py", "dev_tool.py", "__init__.py")
    failures = check_bundle_manifest_completeness(
        tmp_path,
        ship_filenames=["__init__.py", "runtime_module.py"],
        non_shipping_filenames=["dev_tool.py"],
    )
    assert failures == []


def test_unlisted_module_fails_with_guidance(tmp_path: Path) -> None:
    _make_src(tmp_path, "__init__.py", "forgotten_runtime.py")
    failures = check_bundle_manifest_completeness(
        tmp_path,
        ship_filenames=["__init__.py"],
        non_shipping_filenames=[],
    )
    assert len(failures) == 1
    assert "forgotten_runtime.py" in failures[0]
    assert "APP_RUNTIME_SCRIPTS" in failures[0]
    assert "NON_SHIPPING_TOP_LEVEL_MODULES" in failures[0]


def test_allowlisted_dev_tool_does_not_require_manifest_entry(tmp_path: Path) -> None:
    _make_src(tmp_path, "__init__.py", "dev_tool.py")
    failures = check_bundle_manifest_completeness(
        tmp_path,
        ship_filenames=["__init__.py"],
        non_shipping_filenames=["dev_tool.py"],
    )
    assert failures == []


def test_stale_manifest_entry_fails(tmp_path: Path) -> None:
    _make_src(tmp_path, "__init__.py")
    failures = check_bundle_manifest_completeness(
        tmp_path,
        ship_filenames=["__init__.py", "removed_module.py"],
        non_shipping_filenames=[],
    )
    assert len(failures) == 1
    assert "removed_module.py" in failures[0]
    assert "does not exist" in failures[0]


def test_json_data_file_counts_as_ship_relevant(tmp_path: Path) -> None:
    _make_src(tmp_path, "__init__.py", "runtime.json")
    failures = check_bundle_manifest_completeness(
        tmp_path,
        ship_filenames=["__init__.py", "runtime.json"],
        non_shipping_filenames=[],
    )
    assert failures == []


def test_real_repo_is_manifest_complete() -> None:
    assert check_bundle_manifest_completeness() == []


def test_bundle_group_is_wired_and_reports_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "bundle" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["bundle"] is repo_guardrails.run_bundle_group

    monkeypatch.setattr(
        repo_guardrails,
        "check_bundle_manifest_completeness",
        lambda repo_root=None: ["forgotten_runtime.py missing from manifest"],
    )
    assert repo_guardrails.run_bundle_group() == [
        repo_guardrails.GuardFailure(
            "bundle",
            "check_bundle_manifest_completeness",
            "forgotten_runtime.py missing from manifest",
        )
    ]
