from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship.packaged_smoke import runtime_snapshot
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def _probe_deps(runtime_failure: BaseException) -> SimpleNamespace:
    process = mock.Mock()
    process.pid = 123
    process.poll.return_value = None
    stdout_handle = mock.Mock()
    stderr_handle = mock.Mock()
    return SimpleNamespace(
        STARTUP_REQUIRED_EVENTS=("jobs_auth_ready",),
        choose_free_port=mock.Mock(side_effect=[52001, 52002]),
        cleanup_orphaned_desktop_ports_nt=mock.Mock(),
        clear_packaged_desktop_session_state=mock.Mock(),
        launch_packaged_exe=mock.Mock(return_value=(process, stdout_handle, stderr_handle)),
        os=SimpleNamespace(environ={}, name="posix"),
        packaged_runtime_env_overrides=mock.Mock(return_value={}),
        slugify_token=lambda value: str(value).lower().replace(" ", "-"),
        terminate_process_tree=mock.Mock(),
        wait_for_packaged_runtime=mock.Mock(side_effect=runtime_failure),
    )


def _run_probe(deps: SimpleNamespace, artifacts_root: Path) -> dict[str, object]:
    return runtime_snapshot.run_embedded_runtime_probe(
        deps,
        exe_path=artifacts_root / "Baluffo.exe",
        probe={"name": "Startup Probe", "openPath": "jobs.html"},
        artifacts_root=artifacts_root,
        runtime_timeout_s=5.0,
        startup_probe=False,
        profile_mode="cold",
        env={},
    )


@pytest.mark.windows
def test_embedded_runtime_probe_reports_expected_runtime_failure() -> None:
    with workspace_tmpdir("embedded-runtime-probe") as tmp:
        deps = _probe_deps(TimeoutError("timed out waiting for bridge"))

        result = _run_probe(deps, Path(tmp) / "artifacts")

        assert result["status"] == "failed"
        assert result["error"] == "timed out waiting for bridge"
        deps.terminate_process_tree.assert_called_once()
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(52001, 52002)
        _, stdout_handle, stderr_handle = deps.launch_packaged_exe.return_value
        stdout_handle.close.assert_called_once()
        stderr_handle.close.assert_called_once()


@pytest.mark.windows
def test_embedded_runtime_probe_does_not_hide_programming_failures() -> None:
    with workspace_tmpdir("embedded-runtime-probe") as tmp:
        deps = _probe_deps(AssertionError("bad probe invariant"))

        with pytest.raises(AssertionError, match="bad probe invariant"):
            _run_probe(deps, Path(tmp) / "artifacts")

        deps.terminate_process_tree.assert_called_once()
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(52001, 52002)
