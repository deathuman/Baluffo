from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from tools.repo_health import repo_guardrails
from tools.repo_health import update_manager_facade_inventory as inventory


def _write(tmp_path: Path, rel_path: str, source: str) -> Path:
    path = tmp_path / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(source).strip() + "\n", encoding="utf-8")
    return path


def test_current_update_manager_facade_inventory_is_complete() -> None:
    rows = inventory.collect_update_manager_facade_inventory()
    by_path = {row.path: row for row in rows}

    assert len(rows) == inventory.EXPECTED_FACADE_IMPORT_COUNT
    assert inventory.check_update_manager_facade_inventory() == []
    assert all(row.path.startswith("tests/") for row in rows)
    assert "tests/test_ship_update_manager.py" not in by_path
    assert "tests/test_ship_update_manager_facade.py" in by_path
    assert "tests/test_ship_update_manager_apply_exception_ratchet.py" not in by_path
    assert all(row.categories for row in rows)


def test_inventory_collects_supported_update_manager_facade_import_forms(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "tests/test_update_manager_facade.py",
        """
        from src.ship import update_manager as update_manager_mod
        from src.ship.update_manager import ShipPaths
        import src.ship.update_manager
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 3)
    monkeypatch.setattr(
        inventory,
        "CLASSIFIED_IMPORTS",
        {"tests/test_update_manager_facade.py": {"test-compat"}},
    )

    rows = inventory.collect_update_manager_facade_inventory(tmp_path)

    assert [(row.module, row.line) for row in rows] == [
        ("src.ship.update_manager", 1),
        ("src.ship.update_manager", 2),
        ("src.ship.update_manager", 3),
    ]
    assert {row.categories for row in rows} == {("test-compat",)}
    assert inventory.check_update_manager_facade_inventory(tmp_path) == []


def test_inventory_reports_import_count_drift(tmp_path: Path, monkeypatch) -> None:
    _write(
        tmp_path,
        "tests/test_update_manager_facade.py",
        """
        from src.ship import update_manager as update_manager_mod
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 2)
    monkeypatch.setattr(
        inventory,
        "CLASSIFIED_IMPORTS",
        {"tests/test_update_manager_facade.py": {"test-compat"}},
    )

    failures = inventory.check_update_manager_facade_inventory(tmp_path)

    assert failures == [
        "Update manager facade inventory has 1 import records; expected 2. "
        "Update EXPECTED_FACADE_IMPORT_COUNT and review facade consumer classifications."
    ]


def test_inventory_rejects_unclassified_facade_import(tmp_path: Path, monkeypatch) -> None:
    _write(
        tmp_path,
        "tests/test_update_manager_facade.py",
        """
        from src.ship import update_manager
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 1)
    monkeypatch.setattr(inventory, "CLASSIFIED_IMPORTS", {})

    failures = inventory.check_update_manager_facade_inventory(tmp_path)

    assert failures == [
        "Update manager facade import is unclassified: tests/test_update_manager_facade.py:1 "
        "imports src.ship.update_manager."
    ]


def test_inventory_rejects_stale_classification_without_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "tests/test_update_manager_facade.py",
        """
        value = 1
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 0)
    monkeypatch.setattr(
        inventory,
        "CLASSIFIED_IMPORTS",
        {"tests/test_update_manager_facade.py": {"test-compat"}},
    )

    failures = inventory.check_update_manager_facade_inventory(tmp_path)

    assert failures == [
        "Update manager facade classification for tests/test_update_manager_facade.py "
        "has no matching import."
    ]


def test_inventory_rejects_runtime_import_outside_allowlist(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "src/ship/runtime_leaf.py",
        """
        from src.ship import update_manager
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 1)
    monkeypatch.setattr(
        inventory,
        "CLASSIFIED_IMPORTS",
        {"src/ship/runtime_leaf.py": {"tooling"}},
    )
    monkeypatch.setattr(inventory, "RUNTIME_IMPORT_ALLOWLIST", set())

    failures = inventory.check_update_manager_facade_inventory(tmp_path)

    assert failures == [
        "Runtime Python module imports update manager facade outside allowlist: "
        "src/ship/runtime_leaf.py:1 imports src.ship.update_manager."
    ]


def test_repo_guardrails_compat_group_runs_update_manager_facade_inventory(
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
        "check_update_manager_facade_inventory",
        lambda: ["facade drift"],
    )
    monkeypatch.setattr(
        repo_guardrails,
        "check_update_manager_runtime_facade_inventory",
        lambda: [],
    )

    assert "compat" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["compat"] is repo_guardrails.run_compat_group
    assert repo_guardrails.run_compat_group() == [
        repo_guardrails.GuardFailure(
            "compat",
            "check_update_manager_facade_inventory",
            "facade drift",
        )
    ]
