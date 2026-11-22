from unittest import mock

from src import admin_bridge
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT


def test_owner_session_waits_for_a_running_storage_reclaim(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=ADMIN_BRIDGE_TEST_PORT,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=False,
        owner_mode="dev-supervisor",
        owner_token="owner-1",
        started_by="test",
        owner_idle_timeout_s=10.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    admin_bridge.bridge_runtime_state.OWNER_STATE["lastActivityAt"] = "2026-03-01T00:00:00+00:00"
    task_state = mock.Mock()
    task_state.get_current_task_state_payload = mock.Mock(
        return_value={
            "tasks": [
                {
                    "taskType": "storage_reclaim",
                    "runId": "storage_reclaim_1",
                    "active": True,
                    "status": "running",
                }
            ]
        }
    )

    with (
        mock.patch.object(admin_bridge, "_get_ops_api", return_value=task_state),
        mock.patch.object(
            admin_bridge,
            "now_utc",
            return_value=admin_bridge.parse_iso("2026-03-01T00:00:15+00:00"),
        ),
    ):
        # A reclaim copy survives the page closing; it must survive the bridge's
        # own idle exit too, or the copy is wasted mid-run.
        assert admin_bridge.owner_session_should_exit() is False
