from __future__ import annotations

import subprocess
from pathlib import Path
from unittest import mock

import pytest

from src import packaged_desktop_smoke as smoke
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


class _MemorySampler:
    def start(self) -> None:
        return None

    def stop(self) -> dict[str, object]:
        return {}


def test_run_packaged_smoke_does_not_swallow_unexpected_snapshot_bug() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock(spec=subprocess.Popen)
        process.pid = 4242
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
            ]
        )

        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": str(root), "temp": str(root), "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(process, stdout_handle, stderr_handle),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={"health": {"ok": True}, "session": {"ok": True}},
            ),
            mock.patch.object(smoke, "ProcessMemorySampler", return_value=_MemorySampler()),
            mock.patch.object(
                smoke,
                "capture_runtime_snapshot",
                side_effect=AssertionError("unexpected snapshot bug"),
            ),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            with pytest.raises(AssertionError, match="unexpected snapshot bug"):
                smoke.run_packaged_smoke(args)

    terminate_mock.assert_called_once_with(process)
    stdout_handle.close.assert_called_once()
    stderr_handle.close.assert_called_once()
