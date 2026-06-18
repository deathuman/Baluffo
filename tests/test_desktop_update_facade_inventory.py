from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from tools.repo_health import desktop_update_facade_inventory as inventory
from tools.repo_health import repo_guardrails


def _write(tmp_path: Path, rel_path: str, source: str) -> Path:
    path = tmp_path / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(source).strip() + "\n", encoding="utf-8")
    return path


def test_current_desktop_update_facade_inventory_is_complete() -> None:
    rows = inventory.collect_desktop_update_facade_inventory()
    by_path = {row.path: row for row in rows}

    assert len(rows) == inventory.EXPECTED_FACADE_IMPORT_COUNT
    assert inventory.check_desktop_update_facade_inventory() == []
    assert {
        "src/packaged_desktop_smoke.py",
    } <= set(by_path)
    assert "src/admin_bridge.py" not in by_path
    assert "src/ship/desktop_app/__init__.py" not in by_path
    assert "src/ship/desktop_updater.py" not in by_path
    assert "scripts/build_desktop_update_release.py" not in by_path
    assert sum(1 for row in rows if row.module == "src.ship.desktop_updater") == 2
    assert all(row.categories for row in rows)


def test_inventory_collects_supported_facade_import_forms(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "src/admin_bridge.py",
        """
        from src.ship import desktop_update as desktop_update_mod
        from src.ship.desktop_updater import main
        import src.ship.desktop_update
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 3)
    monkeypatch.setattr(
        inventory,
        "CLASSIFIED_IMPORTS",
        {"src/admin_bridge.py": {"compatibility-root"}},
    )
    monkeypatch.setattr(
        inventory,
        "LEAF_FACADE_IMPORT_ALLOWLIST",
        {"src/admin_bridge.py"},
    )

    rows = inventory.collect_desktop_update_facade_inventory(tmp_path)

    assert [(row.module, row.line) for row in rows] == [
        ("src.ship.desktop_update", 1),
        ("src.ship.desktop_updater", 2),
        ("src.ship.desktop_update", 3),
    ]
    assert {row.categories for row in rows} == {("compatibility-root",)}
    assert inventory.check_desktop_update_facade_inventory(tmp_path) == []


def test_inventory_reports_import_count_drift(tmp_path: Path, monkeypatch) -> None:
    _write(
        tmp_path,
        "src/admin_bridge.py",
        """
        from src.ship import desktop_update as desktop_update_mod
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 2)
    monkeypatch.setattr(
        inventory,
        "CLASSIFIED_IMPORTS",
        {"src/admin_bridge.py": {"compatibility-root"}},
    )
    monkeypatch.setattr(
        inventory,
        "LEAF_FACADE_IMPORT_ALLOWLIST",
        {"src/admin_bridge.py"},
    )

    failures = inventory.check_desktop_update_facade_inventory(tmp_path)

    assert failures == [
        "Desktop update facade inventory has 1 import records; expected 2. "
        "Update EXPECTED_FACADE_IMPORT_COUNT and review facade consumer classifications."
    ]


def test_inventory_rejects_unclassified_facade_import(tmp_path: Path, monkeypatch) -> None:
    _write(
        tmp_path,
        "src/bridge/helper.py",
        """
        from src.ship import desktop_update
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 1)
    monkeypatch.setattr(inventory, "CLASSIFIED_IMPORTS", {})
    monkeypatch.setattr(inventory, "LEAF_FACADE_IMPORT_ALLOWLIST", set())

    failures = inventory.check_desktop_update_facade_inventory(tmp_path)

    assert failures == [
        "Desktop update facade import is unclassified: src/bridge/helper.py:1 "
        "imports src.ship.desktop_update.",
        "Leaf Python module imports desktop update facade outside allowlist: "
        "src/bridge/helper.py:1 imports src.ship.desktop_update.",
    ]


def test_inventory_rejects_stale_classification_without_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "src/admin_bridge.py",
        """
        value = 1
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 0)
    monkeypatch.setattr(
        inventory,
        "CLASSIFIED_IMPORTS",
        {"src/admin_bridge.py": {"compatibility-root"}},
    )

    failures = inventory.check_desktop_update_facade_inventory(tmp_path)

    assert failures == [
        "Desktop update facade classification for src/admin_bridge.py has no matching import."
    ]


def test_inventory_rejects_src_leaf_import_outside_allowlist(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "src/ship/leaf.py",
        """
        from src.ship import desktop_update
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_FACADE_IMPORT_COUNT", 1)
    monkeypatch.setattr(
        inventory,
        "CLASSIFIED_IMPORTS",
        {"src/ship/leaf.py": {"packaged-smoke-runtime"}},
    )
    monkeypatch.setattr(inventory, "LEAF_FACADE_IMPORT_ALLOWLIST", set())

    failures = inventory.check_desktop_update_facade_inventory(tmp_path)

    assert failures == [
        "Leaf Python module imports desktop update facade outside allowlist: "
        "src/ship/leaf.py:1 imports src.ship.desktop_update."
    ]


def test_repo_guardrails_compat_group_runs_desktop_update_facade_inventory(
    monkeypatch,
) -> None:
    monkeypatch.setattr(repo_guardrails, "_run_python_checks", lambda _group, _checks: [])
    monkeypatch.setattr(repo_guardrails, "check_bridge_api_field_inventory", lambda: [])
    monkeypatch.setattr(
        repo_guardrails,
        "check_desktop_update_facade_inventory",
        lambda: ["facade drift"],
    )

    assert "compat" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["compat"] is repo_guardrails.run_compat_group
    assert repo_guardrails.run_compat_group() == [
        repo_guardrails.GuardFailure(
            "compat",
            "check_desktop_update_facade_inventory",
            "facade drift",
        )
    ]
