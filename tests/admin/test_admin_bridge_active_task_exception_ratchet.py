from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest import mock

import pytest

from src import admin_bridge
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT


def _configure_expired_regular_close(admin_bridge_entrypoint_root) -> None:
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=ADMIN_BRIDGE_TEST_PORT,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=True,
        owner_mode="desktop-window",
        owner_token="owner-1",
        desktop_session_id="session-1",
        started_by="test",
        owner_idle_timeout_s=15.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    with mock.patch.object(admin_bridge, "now_iso", return_value="2026-03-01T00:00:00+00:00"):
        admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="closing",
            reason="beforeunload",
        )


@contextmanager
def _owner_exit_context(
    *,
    task_payload: Any = None,
    task_failure: BaseException | None = None,
) -> Iterator[None]:
    task_state = mock.Mock()
    if task_failure is not None:
        task_state.get_current_task_state_payload = mock.Mock(side_effect=task_failure)
    else:
        task_state.get_current_task_state_payload = mock.Mock(return_value=task_payload)
    with (
        mock.patch.object(
            admin_bridge,
            "now_utc",
            return_value=admin_bridge.parse_iso("2026-03-01T00:00:20+00:00"),
        ),
        mock.patch.object(admin_bridge, "_get_ops_api", return_value=task_state),
        mock.patch.object(
            admin_bridge,
            "_get_desktop_update_service",
            return_value=mock.Mock(
                get_status_payload=mock.Mock(
                    return_value={"downloadState": "idle", "installState": "idle"}
                )
            ),
        ),
    ):
        yield


def test_active_task_expected_failure_does_not_block_owner_exit(
    admin_bridge_entrypoint_root,
) -> None:
    _configure_expired_regular_close(admin_bridge_entrypoint_root)

    with _owner_exit_context(task_failure=OSError("task state unavailable")):
        assert admin_bridge.owner_session_should_exit() is True


def test_active_task_malformed_payload_does_not_block_owner_exit(
    admin_bridge_entrypoint_root,
) -> None:
    _configure_expired_regular_close(admin_bridge_entrypoint_root)

    with _owner_exit_context(task_payload=["not", "a", "dict"]):
        assert admin_bridge.owner_session_should_exit() is True


def test_active_task_unexpected_failure_propagates(admin_bridge_entrypoint_root) -> None:
    _configure_expired_regular_close(admin_bridge_entrypoint_root)

    with (
        _owner_exit_context(task_failure=AssertionError("task state bug")),
        pytest.raises(AssertionError, match="task state bug"),
    ):
        admin_bridge.owner_session_should_exit()
