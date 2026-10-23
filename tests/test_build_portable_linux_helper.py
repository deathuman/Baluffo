"""The AppImage must ship the one-shot helper that swaps it in place."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

from scripts import build_portable_linux as linux_build
from tests.helpers.temp_paths import workspace_tmpdir


def test_helper_build_targets_the_appimage_swap_entrypoint() -> None:
    with workspace_tmpdir("linux-helper-build") as tmp:
        output_dir = Path(tmp)

        def fake_run(command: list[str], **kwargs: object) -> None:
            dist_path = Path(command[command.index("--distpath") + 1])
            dist_path.mkdir(parents=True, exist_ok=True)
            (dist_path / "baluffo-updater").write_text("helper", encoding="utf-8")

        with mock.patch.object(linux_build.subprocess, "run", side_effect=fake_run) as run_mock:
            helper = linux_build.run_helper_pyinstaller(output_dir)

        command = run_mock.call_args.args[0]
        assert "--onefile" in command
        assert "--windowed" not in command
        assert command[command.index("--name") + 1] == "baluffo-updater"
        assert Path(command[-1]).parts[-3:] == ("src", "ship", "desktop_updater.py")
        assert helper.is_file()


def test_helper_is_copied_next_to_the_payload_binary() -> None:
    with workspace_tmpdir("linux-helper-copy") as tmp:
        built_dir = Path(tmp) / "baluffo"
        built_dir.mkdir(parents=True)
        helper = Path(tmp) / "baluffo-updater"
        helper.write_text("helper", encoding="utf-8")

        target = linux_build.copy_helper_into_payload(built_dir, helper)

        assert target == built_dir / "baluffo-updater"
        assert target.read_text(encoding="utf-8") == "helper"
