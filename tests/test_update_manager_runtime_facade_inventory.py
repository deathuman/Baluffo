from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from tools.repo_health import repo_guardrails
from tools.repo_health import update_manager_runtime_facade_inventory as inventory


def _write(tmp_path: Path, rel_path: str, source: str) -> Path:
    path = tmp_path / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(source).strip() + "\n", encoding="utf-8")
    return path


def test_current_update_manager_runtime_facade_inventory_is_empty() -> None:
    assert inventory.collect_update_manager_runtime_facade_inventory() == ()
    assert inventory.check_update_manager_runtime_facade_inventory() == []


def test_inventory_rejects_supported_runtime_facade_import_forms(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/ship/runtime_a.py",
        """
        from src.ship import update_manager
        """,
    )
    _write(
        tmp_path,
        "src/ship/runtime_b.py",
        """
        import src.ship.update_manager
        """,
    )
    _write(
        tmp_path,
        "src/ship/runtime_c.py",
        """
        from src.ship.update_manager import ShipPaths
        """,
    )
    _write(
        tmp_path,
        "src/ship/runtime_d.py",
        """
        from . import update_manager
        """,
    )
    _write(
        tmp_path,
        "src/ship/runtime_e.py",
        """
        from .update_manager import ShipPaths
        """,
    )

    rows = inventory.collect_update_manager_runtime_facade_inventory(tmp_path)
    failures = inventory.check_update_manager_runtime_facade_inventory(tmp_path)

    assert [(row.path, row.module, row.line) for row in rows] == [
        ("src/ship/runtime_a.py", "src.ship.update_manager", 1),
        ("src/ship/runtime_b.py", "src.ship.update_manager", 1),
        ("src/ship/runtime_c.py", "src.ship.update_manager", 1),
        ("src/ship/runtime_d.py", "src.ship.update_manager", 1),
        ("src/ship/runtime_e.py", "src.ship.update_manager", 1),
    ]
    assert failures == [
        "Update-manager runtime facade inventory has 5 import records; expected 0. "
        "Runtime src/ship Python should import update-manager leaves instead of "
        "src.ship.update_manager.",
        "Runtime src/ship Python imports update-manager facade: "
        "src/ship/runtime_a.py:1 imports src.ship.update_manager.",
        "Runtime src/ship Python imports update-manager facade: "
        "src/ship/runtime_b.py:1 imports src.ship.update_manager.",
        "Runtime src/ship Python imports update-manager facade: "
        "src/ship/runtime_c.py:1 imports src.ship.update_manager.",
        "Runtime src/ship Python imports update-manager facade: "
        "src/ship/runtime_d.py:1 imports src.ship.update_manager.",
        "Runtime src/ship Python imports update-manager facade: "
        "src/ship/runtime_e.py:1 imports src.ship.update_manager.",
    ]


def test_inventory_ignores_scripts_tests_and_facade_module(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/ship/update_manager.py",
        """
        from src.ship import update_manager
        """,
    )
    _write(
        tmp_path,
        "scripts/build.py",
        """
        from src.ship.update_manager import ShipPaths
        """,
    )
    _write(
        tmp_path,
        "tests/test_update_manager.py",
        """
        from src.ship import update_manager
        """,
    )

    assert inventory.collect_update_manager_runtime_facade_inventory(tmp_path) == ()
    assert inventory.check_update_manager_runtime_facade_inventory(tmp_path) == []


def test_repo_guardrails_compat_group_runs_update_manager_runtime_inventory(
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
        "check_update_manager_runtime_facade_inventory",
        lambda: ["runtime facade drift"],
    )

    assert "compat" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["compat"] is repo_guardrails.run_compat_group
    assert repo_guardrails.run_compat_group() == [
        repo_guardrails.GuardFailure(
            "compat",
            "check_update_manager_runtime_facade_inventory",
            "runtime facade drift",
        )
    ]
