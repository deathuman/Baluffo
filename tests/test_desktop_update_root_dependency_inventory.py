from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from tools.repo_health import desktop_update_root_dependency_inventory as inventory
from tools.repo_health import repo_guardrails


def _write(tmp_path: Path, rel_path: str, source: str) -> Path:
    path = tmp_path / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(source).strip() + "\n", encoding="utf-8")
    return path


def _write_tracked_modules(
    tmp_path: Path,
    *,
    shared: str = "value = 1",
    state: str = "value = 1",
    service: str = "value = 1",
) -> None:
    _write(tmp_path, "src/ship/desktop_update_shared.py", shared)
    _write(tmp_path, "src/ship/desktop_update_state.py", state)
    _write(tmp_path, "src/ship/desktop_update_service.py", service)


def test_current_desktop_update_root_dependency_inventory_is_complete() -> None:
    rows = inventory.collect_desktop_update_root_dependency_inventory()
    by_name = {row.name: row for row in rows}

    assert len(rows) == inventory.EXPECTED_DEPENDENCY_COUNT
    assert sum(len(row.references) for row in rows) == inventory.EXPECTED_REFERENCE_COUNT
    assert inventory.check_desktop_update_root_dependency_inventory() == []
    assert rows == ()
    assert "DesktopUpdatePaths" not in by_name
    assert "read_desktop_session_state" not in by_name
    assert "resolve_desktop_session_root" not in by_name
    assert "write_json_atomic" not in by_name
    assert "load_status" not in by_name
    assert "save_status" not in by_name
    assert "_RUNTIME_SESSION_ROOT_FALLBACK" not in by_name
    assert "psutil" not in by_name


def test_inventory_collects_direct_deps_attribute_references(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_tracked_modules(
        tmp_path,
        shared="""
        def path(deps):
            return deps.DesktopUpdatePaths.from_data_dir(deps.os.environ["DATA_DIR"])
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 2)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 2)
    monkeypatch.setattr(
        inventory,
        "DEPENDENCY_CATEGORIES",
        {
            "DesktopUpdatePaths": {"shared-helper", "runtime-path"},
            "os": {"stdlib-binding", "mutable-compat-hook"},
        },
    )

    rows = inventory.collect_desktop_update_root_dependency_inventory(tmp_path)

    assert [(row.name, row.references) for row in rows] == [
        ("DesktopUpdatePaths", ("src/ship/desktop_update_shared.py:2",)),
        ("os", ("src/ship/desktop_update_shared.py:2",)),
    ]
    assert inventory.check_desktop_update_root_dependency_inventory(tmp_path) == []


def test_inventory_reports_dependency_count_drift(tmp_path: Path, monkeypatch) -> None:
    _write_tracked_modules(
        tmp_path,
        shared="""
        def path(deps):
            return deps.os.name
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 2)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 1)
    monkeypatch.setattr(inventory, "DEPENDENCY_CATEGORIES", {"os": {"stdlib-binding"}})

    failures = inventory.check_desktop_update_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop update root dependency inventory has 1 dependencies; expected 2. "
        "Update the classification inventory after reviewing updater root-binding compatibility."
    ]


def test_inventory_reports_reference_count_drift(tmp_path: Path, monkeypatch) -> None:
    _write_tracked_modules(
        tmp_path,
        shared="""
        def path(deps):
            return deps.os.name
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 1)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 2)
    monkeypatch.setattr(inventory, "DEPENDENCY_CATEGORIES", {"os": {"stdlib-binding"}})

    failures = inventory.check_desktop_update_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop update root dependency inventory has 1 references; expected 2. "
        "Review new or removed deps.<name> usages."
    ]


def test_inventory_rejects_unclassified_dependency(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_tracked_modules(
        tmp_path,
        shared="""
        def path(deps):
            return deps.new_helper()
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 1)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 1)
    monkeypatch.setattr(inventory, "DEPENDENCY_CATEGORIES", {})

    failures = inventory.check_desktop_update_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop update root dependency is unclassified: new_helper referenced at "
        "src/ship/desktop_update_shared.py:2."
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
        {"os": {"stdlib-binding"}},
    )

    failures = inventory.check_desktop_update_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop update root dependency classification for os has no matching "
        "deps.<name> reference."
    ]


def test_inventory_rejects_unknown_category(tmp_path: Path, monkeypatch) -> None:
    _write_tracked_modules(
        tmp_path,
        shared="""
        def path(deps):
            return deps.os.name
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_DEPENDENCY_COUNT", 1)
    monkeypatch.setattr(inventory, "EXPECTED_REFERENCE_COUNT", 1)
    monkeypatch.setattr(inventory, "DEPENDENCY_CATEGORIES", {"os": {"root-free"}})

    failures = inventory.check_desktop_update_root_dependency_inventory(tmp_path)

    assert failures == [
        "Desktop update root dependency inventory has unknown categories: ['root-free']."
    ]


def test_repo_guardrails_compat_group_runs_desktop_update_root_dependency_inventory(
    monkeypatch,
) -> None:
    monkeypatch.setattr(repo_guardrails, "_run_python_checks", lambda _group, _checks: [])
    monkeypatch.setattr(repo_guardrails, "check_bridge_api_field_inventory", lambda: [])
    monkeypatch.setattr(repo_guardrails, "check_desktop_update_facade_inventory", lambda: [])
    monkeypatch.setattr(
        repo_guardrails,
        "check_desktop_update_root_dependency_inventory",
        lambda: ["root dependency drift"],
    )

    assert "compat" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["compat"] is repo_guardrails.run_compat_group
    assert repo_guardrails.run_compat_group() == [
        repo_guardrails.GuardFailure(
            "compat",
            "check_desktop_update_root_dependency_inventory",
            "root dependency drift",
        )
    ]
