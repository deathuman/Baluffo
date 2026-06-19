from __future__ import annotations

from pathlib import Path

from src.ship import desktop_updater_install as updater_install


def test_desktop_updater_install_uses_update_manager_leaves() -> None:
    source = Path(updater_install.__file__).read_text(encoding="utf-8")

    assert "from src.ship import update_manager" not in source
    assert "from src.ship.update_manager_apply import" in source
    assert "from src.ship.update_manager_paths import ShipPaths" in source


def test_desktop_updater_install_preserves_update_manager_patch_namespace() -> None:
    assert hasattr(updater_install.update_manager, "ShipPaths")
    assert hasattr(updater_install.update_manager, "create_data_backup")
    assert hasattr(updater_install.update_manager, "restore_data_backup")
    assert hasattr(updater_install.update_manager, "run_migrations")
