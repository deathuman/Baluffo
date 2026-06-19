from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from tools.repo_health import desktop_updater_root_dependency_inventory as inventory
from tools.repo_health import repo_guardrails


def _write(tmp_path: Path, rel_path: str, source: str) -> Path:
    path = tmp_path / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(source).strip() + "\n", encoding="utf-8")
    return path


def _write_tracked_modules(
    tmp_path: Path,
    *,
    ui: str = "value = 1",
    release: str = "value = 1",
    install: str = "value = 1",
) -> None:
    _write(tmp_path, "src/ship/desktop_updater_ui.py", ui)
    _write(tmp_path, "src/ship/desktop_updater_release.py", release)
    _write(tmp_path, "src/ship/desktop_updater_install.py", install)


def test_current_desktop_updater_root_dependency_inventory_is_complete() -> None:
    rows = inventory.collect_desktop_updater_root_dependency_inventory()
    by_name = {row.name: row for row in rows}

    assert len(rows) == inventory.EXPECTED_DEPENDENCY_COUNT
    assert sum(len(row.references) for row in rows) == inventory.EXPECTED_REFERENCE_COUNT
    assert inventory.check_desktop_updater_root_dependency_inventory() == []
    assert {
        "DESKTOP_UPDATE_MANIFEST_ASSET",
        "DESKTOP_UPDATER_NO_DIALOG_ENV",
        "DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV",
        "os",
        "subprocess",
        "time",
        "zipfile",
        "_restore_data_backup_if_needed",
        "_save_install_stage_status",
        "DesktopUpdatePaths",
        "clear_handoff_request",
        "clear_success_marker",
        "install_stage_label",
        "load_status",
        "pid_is_running",
        "save_status",
        "validate_install_plan",
    }.isdisjoint(by_name)
    assert by_name["update_manager"].categories == (
        "mutable-compat-hook",
        "update-manager-compat",
    )
    assert by_name["fetch_json"].categories == (
        "facade-monkeypatch-compat",
        "mutable-compat-hook",
        "shared-helper",
    )
    assert by_name["compute_sha256"].categories == (
        "facade-monkeypatch-compat",
        "mutable-compat-hook",
        "shared-helper",
    )
    assert by_name["_sync_extract_to_install"].categories == (
        "facade-monkeypatch-compat",
        "install-helper",
        "mutable-compat-hook",
    )
    assert by_name["_recover_manifest_for_install"].categories == (
        "mutable-compat-hook",
        "release-helper",
    )


def test_inventory_collects_module_and_getattr_references(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_tracked_modules(
        tmp_path,
        ui="""
        def path(module):
            return module.time.monotonic() + getattr(module, "os", object()).name
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 2)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 2)
    monkeypatch.setattr(
        inventory,
        "DEPENDENCY_CATEGORIES",
        {
            "os": {"stdlib-binding", "mutable-compat-hook"},
            "time": {"stdlib-binding", "mutable-compat-hook"},
        },
    )

    rows = inventory.collect_desktop_updater_root_dependency_inventory(tmp_path)

    assert [(row.name, row.references) for row in rows] == [
        ("os", ("src/ship/desktop_updater_ui.py:2",)),
        ("time", ("src/ship/desktop_updater_ui.py:2",)),
    ]
    assert inventory.check_desktop_updater_root_dependency_inventory(tmp_path) == []


def test_inventory_classifies_facade_monkeypatch_compatibility(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_tracked_modules(
        tmp_path,
        release="""
        def path(module):
            return module.fetch_json("https://example.invalid")
        """,
    )
    _write(
        tmp_path,
        "tests/test_desktop_updater.py",
        """
        from src.ship import desktop_updater as updater

        def test_patch(monkeypatch):
            monkeypatch.setattr(updater, "fetch_json", lambda url: {})
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 1)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 1)
    monkeypatch.setattr(
        inventory,
        "DEPENDENCY_CATEGORIES",
        {"fetch_json": {"shared-helper", "mutable-compat-hook"}},
    )

    rows = inventory.collect_desktop_updater_root_dependency_inventory(tmp_path)

    assert rows[0].categories == (
        "facade-monkeypatch-compat",
        "mutable-compat-hook",
        "shared-helper",
    )
    assert inventory.check_desktop_updater_root_dependency_inventory(tmp_path) == []


def test_inventory_ignores_unrelated_updater_monkeypatches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_tracked_modules(
        tmp_path,
        release="""
        def path(module):
            return module.fetch_json("https://example.invalid")
        """,
    )
    _write(
        tmp_path,
        "tests/test_unrelated.py",
        """
        updater = object()

        def test_patch(monkeypatch):
            monkeypatch.setattr(updater, "fetch_json", lambda url: {})
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 1)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 1)
    monkeypatch.setattr(
        inventory,
        "DEPENDENCY_CATEGORIES",
        {"fetch_json": {"shared-helper", "mutable-compat-hook"}},
    )

    rows = inventory.collect_desktop_updater_root_dependency_inventory(tmp_path)

    assert rows[0].categories == ("mutable-compat-hook", "shared-helper")
    assert inventory.check_desktop_updater_root_dependency_inventory(tmp_path) == []


def test_inventory_reports_dependency_count_drift(tmp_path: Path, monkeypatch) -> None:
    _write_tracked_modules(
        tmp_path,
        install="""
        def path(module):
            return module.time.monotonic()
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 2)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 1)
    monkeypatch.setattr(
        inventory,
        "DEPENDENCY_CATEGORIES",
        {"time": {"stdlib-binding", "mutable-compat-hook"}},
    )

    failures = inventory.check_desktop_updater_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop updater root dependency inventory has 1 dependencies; expected 2. "
        "Update the classification inventory after reviewing updater helper root-binding "
        "compatibility."
    ]


def test_inventory_reports_reference_count_drift(tmp_path: Path, monkeypatch) -> None:
    _write_tracked_modules(
        tmp_path,
        install="""
        def path(module):
            return module.time.monotonic()
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 1)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 2)
    monkeypatch.setattr(
        inventory,
        "DEPENDENCY_CATEGORIES",
        {"time": {"stdlib-binding", "mutable-compat-hook"}},
    )

    failures = inventory.check_desktop_updater_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop updater root dependency inventory has 1 references; expected 2. "
        "Review new or removed module.<name> usages."
    ]


def test_inventory_rejects_unclassified_dependency(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_tracked_modules(
        tmp_path,
        release="""
        def path(module):
            return module.new_helper()
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 1)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 1)
    monkeypatch.setattr(inventory, "DEPENDENCY_CATEGORIES", {})

    failures = inventory.check_desktop_updater_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop updater root dependency is unclassified: new_helper referenced at "
        "src/ship/desktop_updater_release.py:2."
    ]


def test_inventory_rejects_stale_classification_without_reference(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_tracked_modules(tmp_path)
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 0)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 0)
    monkeypatch.setattr(
        inventory,
        "DEPENDENCY_CATEGORIES",
        {"time": {"stdlib-binding", "mutable-compat-hook"}},
    )

    failures = inventory.check_desktop_updater_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop updater root dependency classification for time has no matching "
        "module.<name> reference."
    ]


def test_inventory_rejects_unknown_category(tmp_path: Path, monkeypatch) -> None:
    _write_tracked_modules(
        tmp_path,
        install="""
        def path(module):
            return module.time.monotonic()
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 1)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 1)
    monkeypatch.setattr(inventory, "DEPENDENCY_CATEGORIES", {"time": {"root-free"}})

    failures = inventory.check_desktop_updater_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop updater root dependency inventory has unknown categories: ['root-free']."
    ]


def test_repo_guardrails_compat_group_runs_desktop_updater_root_dependency_inventory(
    monkeypatch,
) -> None:
    monkeypatch.setattr(repo_guardrails, "_run_python_checks", lambda _group, _checks: [])
    monkeypatch.setattr(repo_guardrails, "check_bridge_api_field_inventory", lambda: [])
    monkeypatch.setattr(repo_guardrails, "check_desktop_update_facade_inventory", lambda: [])
    monkeypatch.setattr(
        repo_guardrails,
        "check_desktop_update_root_dependency_inventory",
        lambda: [],
    )
    monkeypatch.setattr(
        repo_guardrails,
        "check_desktop_updater_root_dependency_inventory",
        lambda: ["updater root dependency drift"],
    )

    assert "compat" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["compat"] is repo_guardrails.run_compat_group
    assert repo_guardrails.run_compat_group() == [
        repo_guardrails.GuardFailure(
            "compat",
            "check_desktop_updater_root_dependency_inventory",
            "updater root dependency drift",
        )
    ]
