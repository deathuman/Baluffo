import importlib

from src.ship import desktop_update as du
from src.ship import desktop_update_shared as desktop_update_shared_mod
from src.ship import desktop_update_state as desktop_update_state_mod
from src.ship import desktop_updater as updater


def test_helper_entrypoint_rebinds_shared_desktop_update_dependencies(monkeypatch) -> None:
    monkeypatch.setattr(desktop_update_shared_mod, "root", None)
    monkeypatch.setattr(desktop_update_state_mod, "root", None)

    reloaded = importlib.reload(updater)

    assert desktop_update_shared_mod.root is du
    assert desktop_update_state_mod.root is du
    assert reloaded.install_stage_label("handoff_requested", "preparing") == "Preparing update"
    assert (
        desktop_update_state_mod.default_status_payload(current_version="1.4.0")["helperVersion"]
        == du.DESKTOP_UPDATER_VERSION
    )
