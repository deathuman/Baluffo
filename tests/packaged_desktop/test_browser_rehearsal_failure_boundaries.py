from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship.packaged_smoke import rehearsal_browser
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def _run_with_root(function, deps: SimpleNamespace, root: Path) -> None:
    original_root = rehearsal_browser.root
    rehearsal_browser.root = deps
    try:
        function(
            exe_path=root / "Baluffo.exe",
            artifacts_dir=root / "artifacts",
            runtime_timeout_s=5.0,
        )
    finally:
        rehearsal_browser.root = original_root


def _browser_base_deps(root: Path, *, ports: list[int]) -> SimpleNamespace:
    session_root = root / "session"
    return SimpleNamespace(
        ACTIVE_TASK_CLOSE_NODE_SMOKE_SCRIPT="active-task-close.mjs",
        UTC=dt.UTC,
        choose_free_port=mock.Mock(side_effect=ports),
        cleanup_orphaned_desktop_ports_nt=mock.Mock(),
        clear_packaged_desktop_session_state=mock.Mock(),
        datetime=dt.datetime,
        generate_packaged_smoke_run_token=mock.Mock(side_effect=["owner", "launcher", "desktop"]),
        os=SimpleNamespace(environ={}),
        packaged_desktop_session_paths=mock.Mock(
            return_value={
                "sessionRoot": session_root,
                "sessionState": session_root / "state.json",
                "instanceLock": session_root / "lock.json",
            }
        ),
        packaged_runtime_env_overrides=mock.Mock(return_value={}),
        sys=SimpleNamespace(platform="win32"),
        terminate_process_tree=mock.Mock(),
        utc_now_iso=mock.Mock(return_value="2026-06-19T00:00:00+00:00"),
    )


def test_browser_job_rehearsal_does_not_hide_programming_failures() -> None:
    with workspace_tmpdir("browser-rehearsal") as tmp:
        root = Path(tmp)
        (root / "artifacts").mkdir()
        deps = _browser_base_deps(root, ports=[51001, 51002])
        deps._select_packaged_browser_job_browser = mock.Mock(
            side_effect=AssertionError("bad browser invariant")
        )

        with pytest.raises(AssertionError, match="bad browser invariant"):
            _run_with_root(rehearsal_browser.run_packaged_browser_job_rehearsal, deps, root)

        deps.terminate_process_tree.assert_called_once_with(None)
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(51001, 51002, 51001, 51002)


def test_lifecycle_rehearsal_does_not_hide_programming_failures() -> None:
    with workspace_tmpdir("browser-rehearsal") as tmp:
        root = Path(tmp)
        (root / "artifacts").mkdir()
        deps = _browser_base_deps(root, ports=[51001, 51002, 51003, 51004, 51005])
        deps._seed_jobs_pipeline_smoke_feed = mock.Mock()
        deps.launch_packaged_exe = mock.Mock(side_effect=AssertionError("bad lifecycle invariant"))

        with pytest.raises(AssertionError, match="bad lifecycle invariant"):
            _run_with_root(
                rehearsal_browser.run_packaged_desktop_lifecycle_rehearsal,
                deps,
                root,
            )

        assert deps.terminate_process_tree.call_count == 2
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(
            51001,
            51002,
            51001,
            51002,
            51003,
            51004,
            51005,
            51003,
            51004,
        )


def test_active_task_close_rehearsal_does_not_hide_programming_failures() -> None:
    with workspace_tmpdir("browser-rehearsal") as tmp:
        root = Path(tmp)
        (root / "artifacts").mkdir()
        deps = _browser_base_deps(root, ports=[51001, 51002, 51003])
        deps.launch_packaged_exe = mock.Mock(side_effect=AssertionError("bad active invariant"))

        with pytest.raises(AssertionError, match="bad active invariant"):
            _run_with_root(
                rehearsal_browser.run_packaged_active_task_close_rehearsal,
                deps,
                root,
            )

        deps.terminate_process_tree.assert_called_once_with(None)
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(
            51001,
            51002,
            51001,
            51002,
            51003,
        )


def test_orphan_reclaim_rehearsal_does_not_hide_programming_failures() -> None:
    with workspace_tmpdir("browser-rehearsal") as tmp:
        root = Path(tmp)
        (root / "artifacts").mkdir()
        deps = _browser_base_deps(root, ports=[51001, 51002])
        deps.desktop_update_mod = SimpleNamespace(get_app_version=mock.Mock(return_value="1.2.3"))
        deps.launch_packaged_desktop_child = mock.Mock(
            side_effect=AssertionError("bad reclaim invariant")
        )

        with pytest.raises(AssertionError, match="bad reclaim invariant"):
            _run_with_root(rehearsal_browser.run_packaged_orphan_reclaim_rehearsal, deps, root)

        assert deps.terminate_process_tree.call_count == 3
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(51001, 51002, 0, 0)
