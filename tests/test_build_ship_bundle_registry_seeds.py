from pathlib import Path
from unittest import mock

import pytest

from scripts.build_ship_bundle import build_bundle
from tests.helpers.ship_bundle import copy_minimal_app_version
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = pytest.mark.packaging


def test_bundle_includes_registry_seeds_and_excludes_local_runtime_registries() -> None:
    with (
        workspace_tmpdir("build-ship-bundle") as tmp,
        mock.patch(
            "scripts.build_ship_bundle._copy_app_version",
            side_effect=copy_minimal_app_version,
        ),
        mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
        mock.patch("scripts.build_ship_bundle._resolve_packaged_sync_config", return_value=None),
    ):
        output = build_bundle(Path(tmp) / "dist" / "baluffo-ship", "1.2.3")

        assert not (output / "data" / "source-registry-active.json").exists()
        assert not (output / "data" / "source-registry-pending.json").exists()
        assert not (output / "data" / "source-approval-state.json").exists()
        assert (output / "data" / "defaults" / "source-registry-active.seed.json").exists()
        assert (output / "data" / "defaults" / "source-registry-pending.seed.json").exists()
