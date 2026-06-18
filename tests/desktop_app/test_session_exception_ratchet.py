from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_app
from tests.helpers.temp_paths import workspace_tmpdir


@pytest.fixture
def stale_lock_workspace():
    with workspace_tmpdir("desktop-session-ratchet") as tmp:
        root = Path(tmp) / "session-root"
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / "desktop-instance.lock"
        lock_path.write_text("not-json", encoding="utf-8")
        yield lock_path


def test_acquire_instance_lock_suppresses_expected_reclaim_callback_failure(
    stale_lock_workspace: Path,
) -> None:
    callback = mock.Mock(side_effect=OSError("trace write failed"))

    with (
        mock.patch.object(
            desktop_app, "resolve_instance_lock_path", return_value=stale_lock_workspace
        ),
        mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
    ):
        lock = desktop_app.acquire_instance_lock(timeout_s=0.5, on_reclaim=callback)

    assert lock is not None
    callback.assert_called_once_with("stale_lock_owner")
    desktop_app.release_instance_lock(lock)


def test_acquire_instance_lock_does_not_swallow_unexpected_reclaim_callback_failure(
    stale_lock_workspace: Path,
) -> None:
    callback = mock.Mock(side_effect=AssertionError("unexpected reclaim callback bug"))

    with (
        mock.patch.object(
            desktop_app, "resolve_instance_lock_path", return_value=stale_lock_workspace
        ),
        mock.patch.object(desktop_app, "_process_identity_matches", return_value=False),
        pytest.raises(AssertionError, match="unexpected reclaim callback bug"),
    ):
        desktop_app.acquire_instance_lock(timeout_s=0.5, on_reclaim=callback)
