import importlib
import json
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


def test_main_dispatches_a_terminal_install_repair(monkeypatch, capsys) -> None:
    repair = mock.Mock(
        return_value={
            "ok": True,
            "action": "restore-snapshot",
            "version": "0.1.0",
            "snapshotRemoved": True,
            "snapshotPath": "rollback/1.4.0",
        }
    )
    show_message = mock.Mock()
    monkeypatch.setattr(updater, "repair_terminal_install", repair)
    monkeypatch.setattr(
        updater.DesktopUpdatePaths,
        "from_data_dir",
        mock.Mock(return_value=object()),
    )
    monkeypatch.setattr(updater, "_show_message", show_message)
    monkeypatch.setattr(
        updater,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            repair_desktop_install=True,
            data_dir="data",
            install_root="portable",
            mode="restore-snapshot",
            expect_version="0.1.0",
            discard_snapshot=True,
        ),
    )

    assert updater.main([]) == 0

    repair.assert_called_once()
    payload = json.loads(capsys.readouterr().out)
    assert payload["version"] == "0.1.0"
    show_message.assert_called_once_with(
        "Baluffo Update Repaired",
        "Restored Baluffo 0.1.0 from the retained rollback snapshot.",
    )


def test_main_reports_a_failed_terminal_install_repair(monkeypatch, capsys) -> None:
    error = "No terminal desktop install failure is recorded; there is nothing to repair."
    show_message = mock.Mock()
    monkeypatch.setattr(
        updater,
        "repair_terminal_install",
        mock.Mock(side_effect=RuntimeError(error)),
    )
    monkeypatch.setattr(
        updater.DesktopUpdatePaths,
        "from_data_dir",
        mock.Mock(return_value=object()),
    )
    monkeypatch.setattr(updater, "_show_message", show_message)
    monkeypatch.setattr(
        updater,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            repair_desktop_install=True,
            data_dir="data",
            install_root="portable",
            mode="restore-snapshot",
            expect_version="",
            discard_snapshot=False,
        ),
    )

    assert updater.main([]) == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload == {"ok": False, "error": error}
    show_message.assert_called_once_with("Baluffo Update Repair Failed", error)


def test_panel_repair_waits_for_the_launcher_before_restoring_and_relaunches(
    monkeypatch, tmp_path
) -> None:
    data_dir = tmp_path / "portable" / "ship" / "data"
    install_root = tmp_path / "portable"
    data_dir.mkdir(parents=True)
    plan_path = tmp_path / "repair-plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "planVersion": 1,
                "mode": "restore-snapshot",
                "dataDir": str(data_dir),
                "installRoot": str(install_root),
                "launcherPid": 4242,
                "desktopSessionRoot": str(tmp_path / "session"),
                "relaunch": True,
                "panel": True,
            }
        ),
        encoding="utf-8",
    )
    order: list[str] = []

    def fake_repair(*_args, **_kwargs):
        order.append("repair")
        return {
            "ok": True,
            "action": "restore-snapshot",
            "version": "0.3.064",
            "snapshotRemoved": True,
            "snapshotPath": "rollback/0.3.065",
        }

    def fake_wait(*_args, **_kwargs):
        order.append("wait")

    def fake_launch(*_args, **_kwargs):
        order.append("launch")

    repair = mock.Mock(side_effect=fake_repair)
    wait = mock.Mock(side_effect=fake_wait)
    launch = mock.Mock(side_effect=fake_launch)
    show_message = mock.Mock()
    monkeypatch.setattr(updater, "repair_terminal_install", repair)
    monkeypatch.setattr(updater, "_wait_for_launcher_exit", wait)
    monkeypatch.setattr(updater, "_launch_executable", launch)
    monkeypatch.setattr(updater, "_show_message", show_message)
    monkeypatch.setattr(
        updater,
        "parse_args",
        lambda argv=None: SimpleNamespace(repair_desktop_install=True, repair_plan=str(plan_path)),
    )

    assert updater.main([]) == 0

    assert order == ["wait", "repair", "launch"]
    wait.assert_called_once()
    assert wait.call_args.args[0]["launcherPid"] == 4242
    repair.assert_called_once()
    launch.assert_called_once()
    show_message.assert_not_called()
    assert not plan_path.exists()


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
