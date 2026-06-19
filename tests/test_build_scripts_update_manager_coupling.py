from __future__ import annotations

from pathlib import Path

import pytest

from scripts import build_portable_exe, build_ship_bundle

pytestmark = pytest.mark.packaging


def test_build_ship_bundle_uses_update_manager_leaves() -> None:
    source = Path(build_ship_bundle.__file__).read_text(encoding="utf-8")

    assert "from src.ship.update_manager import" not in source
    assert "from src.ship.update_manager_bootstrap import refresh_runtime_bootstrap" in source
    assert "from src.ship.update_manager_paths import ShipPaths" in source


def test_build_portable_exe_uses_update_manager_leaves() -> None:
    source = Path(build_portable_exe.__file__).read_text(encoding="utf-8")

    assert "from src.ship.update_manager import" not in source
    assert "from src.ship.update_manager_paths import REQUIRED_VERSION_FILES" in source
