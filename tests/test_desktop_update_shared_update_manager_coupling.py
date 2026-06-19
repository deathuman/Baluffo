from __future__ import annotations

from pathlib import Path

from src.ship import desktop_update_shared as du_shared


def test_desktop_update_shared_uses_update_manager_leaves_for_current_version() -> None:
    source = Path(du_shared.__file__).read_text(encoding="utf-8")

    assert "from src.ship import update_manager" not in source
    assert "from src.ship.update_manager_paths import ShipPaths" in source
    assert "from src.ship.update_manager_state import ensure_state" in source
