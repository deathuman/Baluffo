import importlib

from src.ship import desktop_update_shared as desktop_update_shared_mod
from src.ship import desktop_update_state as desktop_update_state_mod
from src.ship import desktop_updater as updater
from src.ship.desktop_update_manifest import DESKTOP_UPDATER_VERSION


def test_helper_entrypoint_does_not_rebind_leaf_desktop_update_dependencies(monkeypatch) -> None:
    monkeypatch.setattr(desktop_update_shared_mod, "root", None)
    monkeypatch.setattr(desktop_update_state_mod, "root", None)

    reloaded = importlib.reload(updater)

    assert desktop_update_shared_mod.root is None
    assert desktop_update_state_mod.root is None
    assert reloaded.install_stage_label("handoff_requested", "preparing") == "Preparing update"
    assert (
        desktop_update_state_mod.default_status_payload(current_version="1.4.0")["helperVersion"]
        == DESKTOP_UPDATER_VERSION
    )
