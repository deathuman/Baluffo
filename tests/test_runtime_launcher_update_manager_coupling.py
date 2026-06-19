from __future__ import annotations

from pathlib import Path

from src.ship import runtime_launcher as launcher


def test_runtime_launcher_uses_update_manager_leaves() -> None:
    source = Path(launcher.__file__).read_text(encoding="utf-8")

    assert "from src.ship import update_manager" not in source
    assert "from src.ship.update_manager_bootstrap import" in source
    assert "from src.ship.update_manager_paths import" in source
    assert "from src.ship.update_manager_recovery import" in source
    assert "from src.ship.update_manager_state import" in source
    assert "from src.ship.update_manager_validation import" in source


def test_runtime_launcher_preserves_update_manager_patch_namespace() -> None:
    assert hasattr(launcher.update_manager, "ShipPaths")
    assert hasattr(launcher.update_manager, "REQUIRED_VERSION_FILES")
    assert hasattr(launcher.update_manager, "ensure_state")
    assert hasattr(launcher.update_manager, "startup_check")
    assert hasattr(launcher.update_manager, "repair_version_from_runtime_bootstrap")
    assert hasattr(launcher.update_manager, "health_check_version")
