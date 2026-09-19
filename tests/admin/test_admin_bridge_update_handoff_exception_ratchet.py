from collections.abc import Iterator
from contextlib import contextmanager
from unittest import mock

import pytest

from src import admin_bridge
from tests.helpers.bridge_fakes import configure_expired_regular_close

_configure_expired_regular_close = configure_expired_regular_close


@contextmanager
def _owner_exit_context(status_failure: BaseException) -> Iterator[None]:
    with (
        mock.patch.object(
            admin_bridge,
            "now_utc",
            return_value=admin_bridge.parse_iso("2026-03-01T00:00:20+00:00"),
        ),
        mock.patch.object(
            admin_bridge,
            "_get_ops_api",
            return_value=mock.Mock(
                get_current_task_state_payload=mock.Mock(return_value={"tasks": []})
            ),
        ),
        mock.patch.object(
            admin_bridge,
            "_get_desktop_update_service",
            return_value=mock.Mock(get_status_payload=mock.Mock(side_effect=status_failure)),
        ),
    ):
        yield


def test_desktop_update_handoff_status_expected_failure_does_not_block_owner_exit(
    admin_bridge_entrypoint_root,
) -> None:
    _configure_expired_regular_close(admin_bridge_entrypoint_root)

    with _owner_exit_context(OSError("status unavailable")):
        assert admin_bridge.owner_session_should_exit() is True


def test_desktop_update_handoff_status_unexpected_failure_propagates(
    admin_bridge_entrypoint_root,
) -> None:
    _configure_expired_regular_close(admin_bridge_entrypoint_root)

    with (
        _owner_exit_context(AssertionError("status bug")),
        pytest.raises(AssertionError, match="status bug"),
    ):
        admin_bridge.owner_session_should_exit()
