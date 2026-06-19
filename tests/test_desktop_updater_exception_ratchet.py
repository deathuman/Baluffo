from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship import desktop_updater as updater
from tests.helpers.temp_paths import workspace_tmpdir


class ImmediateThread:
    def __init__(self, *, target, daemon, name) -> None:
        self._target = target

    def start(self) -> None:
        self._target()

    def join(self) -> None:
        return None


def _configure_main(monkeypatch, plan_path: Path, progress) -> mock.Mock:
    show_message = mock.Mock()
    monkeypatch.setattr(
        updater,
        "parse_args",
        lambda argv=None: SimpleNamespace(install_plan=str(plan_path)),
    )
    monkeypatch.setattr(updater, "_show_message", show_message)
    monkeypatch.setattr(updater, "HelperProgressWindow", mock.Mock(return_value=progress))
    monkeypatch.setattr(updater.threading, "Thread", ImmediateThread)
    return show_message


def test_helper_main_still_handles_expected_install_failure(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater-exception-ratchet") as tmp:
        plan_path = Path(tmp) / "install-plan.json"
        plan_path.write_text("{}", encoding="utf-8")
        progress = mock.Mock(run=mock.Mock(), close=mock.Mock())
        show_message = _configure_main(monkeypatch, plan_path, progress)
        monkeypatch.setattr(
            updater,
            "run_install",
            mock.Mock(side_effect=ValueError("bad install plan")),
        )

        result = updater.main([])

        assert result == 1
        progress.close.assert_called_once()
        show_message.assert_called_once_with("Baluffo Update Failed", "bad install plan")


def test_helper_main_does_not_hide_unexpected_worker_bug(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater-exception-ratchet") as tmp:
        plan_path = Path(tmp) / "install-plan.json"
        plan_path.write_text("{}", encoding="utf-8")
        progress = mock.Mock(run=mock.Mock(), close=mock.Mock())
        _configure_main(monkeypatch, plan_path, progress)
        monkeypatch.setattr(
            updater,
            "run_install",
            mock.Mock(side_effect=AssertionError("unexpected install bug")),
        )

        with pytest.raises(AssertionError, match="unexpected install bug"):
            updater.main([])


def test_helper_main_does_not_hide_unexpected_progress_loop_bug(monkeypatch) -> None:
    with workspace_tmpdir("desktop-updater-exception-ratchet") as tmp:
        plan_path = Path(tmp) / "install-plan.json"
        plan_path.write_text("{}", encoding="utf-8")
        progress = mock.Mock(run=mock.Mock(side_effect=AssertionError("unexpected ui bug")))
        _configure_main(monkeypatch, plan_path, progress)
        monkeypatch.setattr(
            updater,
            "run_install",
            mock.Mock(return_value={"ok": True, "installedVersion": "1.4.0"}),
        )

        with pytest.raises(AssertionError, match="unexpected ui bug"):
            updater.main([])
