import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from src.ship import desktop_update_shared as desktop_update_shared_mod
from src.ship import desktop_update_state as desktop_update_state_mod
from src.ship import desktop_updater as updater
from src.ship.desktop_update_manifest import DESKTOP_UPDATER_VERSION
from tests.helpers.temp_paths import workspace_tmpdir


def test_helper_entrypoint_does_not_rebind_leaf_desktop_update_dependencies() -> None:
    assert not hasattr(desktop_update_shared_mod, "root")
    assert not hasattr(desktop_update_state_mod, "root")

    reloaded = importlib.reload(updater)

    assert not hasattr(desktop_update_shared_mod, "root")
    assert not hasattr(desktop_update_state_mod, "root")
    assert reloaded.install_stage_label("handoff_requested", "preparing") == "Preparing update"
    assert (
        desktop_update_state_mod.default_status_payload(current_version="1.4.0")["helperVersion"]
        == DESKTOP_UPDATER_VERSION
    )


def test_main_failure_path_still_uses_native_error_message(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater") as tmp:
        plan_path = Path(tmp) / "install-plan.json"
        plan_path.write_text("{}", encoding="utf-8")
        show_message = mock.Mock()

        class ImmediateThread:
            def __init__(self, *, target, daemon, name) -> None:
                self._target = target

            def start(self) -> None:
                self._target()

            def join(self) -> None:
                return None

        monkeypatch.setattr(
            updater,
            "parse_args",
            lambda argv=None: SimpleNamespace(install_plan=str(plan_path)),
        )
        monkeypatch.setattr(updater, "_show_message", show_message)
        monkeypatch.setattr(
            updater,
            "HelperProgressWindow",
            mock.Mock(return_value=mock.Mock(run=mock.Mock(), close=mock.Mock())),
        )
        monkeypatch.setattr(
            updater,
            "run_install",
            mock.Mock(side_effect=RuntimeError("boom during install")),
        )
        monkeypatch.setattr(updater.threading, "Thread", ImmediateThread)

        result = updater.main([])

    assert result == 1
    show_message.assert_called_once_with("Baluffo Update Failed", "boom during install")
