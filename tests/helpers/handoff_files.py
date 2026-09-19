from __future__ import annotations

from pathlib import Path

from src import admin_bridge
from tests.admin._runtime_helpers import (
    active_progress,
    fetch_report,
    history_row,
    task_state_entry,
)
from tests.helpers.desktop_update_leaf_namespace import du


def write_credible_handoff_request(
    paths: du.DesktopUpdatePaths,
    session_root: Path,
    *,
    install_state: str = "handoff_requested",
    launcher_pid: int = 1234,
    launcher_token: str = "token-1",
) -> None:
    session_root.mkdir(parents=True, exist_ok=True)
    du.write_json_atomic(
        session_root / "desktop-session.json",
        {
            "launcherPid": int(launcher_pid),
            "launcherToken": str(launcher_token),
        },
    )
    du.write_json_atomic(
        paths.install_plan_path,
        {
            "planVersion": 1,
            "installRoot": str(paths.install_root),
            "dataDir": str(paths.data_dir),
            "tempHelperPath": str(paths.install_root / du.DESKTOP_UPDATE_HELPER_NAME),
            "targetVersion": "1.4.0",
            "currentVersion": "0.1.0",
            "manifestPath": str(paths.manifest_cache_path),
            "downloadedZipPath": str(paths.downloads_dir / "baluffo-portable-1.4.0.zip"),
            "expectedZipSha256": "a" * 64,
            "manifestKeyId": "desktop-ed25519-test",
            "rollbackPath": str(paths.rollback_root / "rollback-1"),
            "updaterWorkingDir": str(paths.updater_dir),
            "createdAt": "2026-04-19T12:00:00Z",
            "launcherPid": int(launcher_pid),
            "launcherToken": str(launcher_token),
            "desktopSessionRoot": str(session_root),
        },
    )
    du.write_json_atomic(paths.handoff_request_path, {"requestedAt": "2026-04-19T12:00:00Z"})
    du.save_status(
        paths,
        {
            **du.default_status_payload(current_version="0.1.0"),
            "installState": str(install_state),
        },
    )


def setup_report_finished_while_owner_active() -> None:
    started_at = admin_bridge.now_iso()
    finished_at = admin_bridge.now_iso()
    run_id = "fetch_report_finished_1"
    admin_bridge.save_json_atomic(
        admin_bridge.TASK_STATE_PATH,
        {
            "fetch": task_state_entry("fetch", run_id=run_id, started_at=started_at),
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_REPORT_PATH,
        fetch_report(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            runtime={"lifecycle": {"owner": "fetch_report", "heartbeatAt": started_at}},
            task_progress=active_progress(
                "executing_sources",
                "Executing sources",
                {"resolvedSources": 5, "sourceCount": 10},
            ),
            summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
        ),
    )
    admin_bridge.save_json_atomic(
        admin_bridge.JOBS_FETCH_TASKS_PATH,
        {
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": "",
            "heartbeatAt": started_at,
            "taskProgress": {"active": True},
            "summary": {"queued": 0, "running": 1, "ok": 0, "error": 0},
            "tasks": [],
        },
    )
    admin_bridge.save_json_atomic(
        admin_bridge.OPS_HISTORY_PATH,
        [
            history_row(
                row_id=run_id,
                run_id=run_id,
                status="warning",
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=123,
                summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
            )
        ],
    )
    admin_bridge.start_lifecycle_run(
        run_id=run_id,
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
        progress=active_progress(
            "executing_sources",
            "Executing sources",
            {"resolvedSources": 5, "sourceCount": 10},
        ),
        summary={"outputCount": 10, "failedSources": 1, "sourceCount": 10},
    )
