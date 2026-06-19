from __future__ import annotations

from pathlib import Path

import pytest

from scripts import build_portable_exe, build_ship_bundle

pytestmark = pytest.mark.packaging

ROOT = Path(__file__).resolve().parents[1]


def test_build_ship_bundle_uses_update_manager_leaves() -> None:
    source = Path(build_ship_bundle.__file__).read_text(encoding="utf-8")

    assert "from src.ship.update_manager import" not in source
    assert "from src.ship.update_manager_bootstrap import refresh_runtime_bootstrap" in source
    assert "from src.ship.update_manager_paths import ShipPaths" in source


def test_build_portable_exe_uses_update_manager_leaves() -> None:
    source = Path(build_portable_exe.__file__).read_text(encoding="utf-8")

    assert "from src.ship.update_manager import" not in source
    assert "from src.ship.update_manager_paths import REQUIRED_VERSION_FILES" in source


def test_ship_update_scripts_use_update_manager_cli_leaf() -> None:
    script_names = (
        "apply-update.sh",
        "apply-update.ps1",
        "create-support-bundle.sh",
        "create-support-bundle.ps1",
        "recover-previous.sh",
        "recover-previous.ps1",
        "run-bridge.sh",
        "run-bridge.ps1",
    )

    for script_name in script_names:
        source = (ROOT / "src" / "ship" / script_name).read_text(encoding="utf-8")

        assert "src.ship.update_manager_cli" in source, script_name
        assert "src.ship.update_manager " not in source, script_name
        assert "update_manager.py" not in source, script_name


def test_update_manager_cli_leaf_is_module_executable() -> None:
    source = (ROOT / "src" / "ship" / "update_manager_cli.py").read_text(encoding="utf-8")

    assert 'if __name__ == "__main__":' in source
    assert "raise SystemExit(main())" in source
