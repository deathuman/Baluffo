from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship.packaged_smoke import rehearsal_sync
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def _sync_rehearsal_deps(runtime_failure: BaseException) -> SimpleNamespace:
    process = mock.Mock()
    process.pid = 4321
    stdout_handle = mock.Mock()
    stderr_handle = mock.Mock()
    memory_sampler = mock.Mock()
    memory_sampler.stop.return_value = {"sampleCount": 1, "peakWorkingSetBytes": 4096}
    server = mock.Mock()
    server_thread = mock.Mock()
    return SimpleNamespace(
        ProcessMemorySampler=mock.Mock(return_value=memory_sampler),
        _load_portable_packaged_sync_rehearsal_config=mock.Mock(
            return_value=(
                Path("github-app-sync-config.json"),
                {"keyDerivation": "embedded"},
                mock.Mock(),
            )
        ),
        _start_packaged_sync_rehearsal_server=mock.Mock(
            return_value=(
                "http://127.0.0.1:45000",
                {"tokenRequests": 0, "contentRequests": 0, "putRequests": 0, "deleteRequests": 0},
                server,
                server_thread,
            )
        ),
        choose_free_port=mock.Mock(side_effect=[51001, 51002]),
        clear_packaged_desktop_session_state=mock.Mock(),
        cleanup_orphaned_desktop_ports_nt=mock.Mock(),
        launch_packaged_exe=mock.Mock(return_value=(process, stdout_handle, stderr_handle)),
        os=SimpleNamespace(environ={}),
        packaged_runtime_env_overrides=mock.Mock(return_value={}),
        source_sync_mod=SimpleNamespace(
            GITHUB_API_BASE_ENV="BALUFFO_GITHUB_API_BASE",
            SYNC_SCHEMA_VERSION=1,
        ),
        terminate_process_tree=mock.Mock(),
        utc_now_iso=mock.Mock(return_value="2026-06-19T00:00:00+00:00"),
        wait_for_packaged_runtime=mock.Mock(side_effect=runtime_failure),
    )


def _run_sync_rehearsal(deps: SimpleNamespace, root: Path) -> dict[str, object]:
    original_root = rehearsal_sync.root
    rehearsal_sync.root = deps
    try:
        return rehearsal_sync.run_packaged_sync_rehearsal(
            exe_path=root / "Baluffo.exe",
            artifacts_dir=root / "artifacts",
            runtime_timeout_s=5.0,
        )
    finally:
        rehearsal_sync.root = original_root


def test_packaged_sync_rehearsal_reports_expected_runtime_failure() -> None:
    with workspace_tmpdir("packaged-sync-rehearsal") as tmp:
        root = Path(tmp)
        (root / "artifacts").mkdir()
        deps = _sync_rehearsal_deps(TimeoutError("timed out waiting for sync runtime"))

        result = _run_sync_rehearsal(deps, root)

        assert result["status"] == "failed"
        assert result["error"] == "timed out waiting for sync runtime"
        assert result["memoryMetrics"] == {"sampleCount": 1, "peakWorkingSetBytes": 4096}
        deps.ProcessMemorySampler.return_value.start.assert_called_once()
        deps.ProcessMemorySampler.return_value.stop.assert_called_once()
        deps.terminate_process_tree.assert_called_once()
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(51001, 51002)


def test_packaged_sync_rehearsal_does_not_hide_programming_failures() -> None:
    with workspace_tmpdir("packaged-sync-rehearsal") as tmp:
        root = Path(tmp)
        (root / "artifacts").mkdir()
        deps = _sync_rehearsal_deps(AssertionError("bad sync invariant"))

        with pytest.raises(AssertionError, match="bad sync invariant"):
            _run_sync_rehearsal(deps, root)

        deps.ProcessMemorySampler.return_value.start.assert_called_once()
        deps.terminate_process_tree.assert_called_once()
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(51001, 51002)
