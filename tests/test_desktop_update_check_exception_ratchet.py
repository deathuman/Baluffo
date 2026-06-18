from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_service as du_service
from tests.helpers.temp_paths import workspace_tmpdir


def _service(data_dir: Path) -> du_service.DesktopUpdateService:
    return du_service.DesktopUpdateService(
        data_dir=data_dir,
        current_version_getter=lambda: "0.1.0",
    )


def test_check_for_update_returns_error_for_expected_release_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        service = _service(Path(tmp) / "portable" / "ship" / "data")

        with mock.patch.object(
            service,
            "_resolve_latest_release",
            side_effect=RuntimeError("github unavailable"),
        ):
            status = service.check_for_update(force=True)

        assert status["availability"] == "error"
        assert status["updateAvailable"] is False
        assert status["lastError"] == "github unavailable"


def test_check_for_update_does_not_suppress_unexpected_release_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        service = _service(Path(tmp) / "portable" / "ship" / "data")

        with (
            mock.patch.object(
                service,
                "_resolve_latest_release",
                side_effect=AssertionError("unexpected release bug"),
            ),
            pytest.raises(AssertionError, match="unexpected release bug"),
        ):
            service.check_for_update(force=True)
