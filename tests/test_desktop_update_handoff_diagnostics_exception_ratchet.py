from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_service as du_service
from src.ship import desktop_update_state as update_state
from tests.helpers.temp_paths import workspace_tmpdir


def _service_and_zip(
    data_dir: Path,
) -> tuple[du_service.DesktopUpdateService, Path, dict[str, object]]:
    service = du_service.DesktopUpdateService(
        data_dir=data_dir,
        current_version_getter=lambda: "0.1.0",
    )
    zip_path = service.paths.downloads_dir / "baluffo-portable-1.4.0.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    zip_path.write_bytes(b"portable update")
    status = {
        **update_state.default_status_payload(current_version="0.1.0"),
        "downloadState": "downloaded",
        "downloadedZipPath": str(zip_path),
    }
    return service, zip_path, status


def test_handoff_unconfirmed_suppresses_expected_diagnostics_write_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        service, zip_path, status = _service_and_zip(Path(tmp) / "portable" / "ship" / "data")

        with mock.patch.object(
            du_service,
            "write_handoff_diagnostics",
            side_effect=OSError("diagnostics unavailable"),
        ):
            result = service._install_handoff_unconfirmed_locked(
                status=status,
                zip_path=zip_path,
                temp_helper=None,
            )

        assert result["started"] is False
        assert result["errorCode"] == "install_handoff_unconfirmed"


def test_handoff_unconfirmed_does_not_suppress_unexpected_diagnostics_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        service, zip_path, status = _service_and_zip(Path(tmp) / "portable" / "ship" / "data")

        with (
            mock.patch.object(
                du_service,
                "write_handoff_diagnostics",
                side_effect=AssertionError("unexpected diagnostics bug"),
            ),
            pytest.raises(AssertionError, match="unexpected diagnostics bug"),
        ):
            service._install_handoff_unconfirmed_locked(
                status=status,
                zip_path=zip_path,
                temp_helper=None,
            )
