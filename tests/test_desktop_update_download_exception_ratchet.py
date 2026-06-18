from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_service as du_service
from tests.helpers.temp_paths import workspace_tmpdir


def test_run_download_worker_does_not_suppress_unexpected_download_failures() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        service = du_service.DesktopUpdateService(
            data_dir=data_dir,
            current_version_getter=lambda: "0.1.0",
        )
        manifest = {
            "version": "1.4.0",
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.4.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
        }

        with (
            mock.patch.object(
                du_service,
                "download_file",
                side_effect=AssertionError("unexpected download bug"),
            ),
            pytest.raises(AssertionError, match="unexpected download bug"),
        ):
            service._run_download_worker(manifest)
