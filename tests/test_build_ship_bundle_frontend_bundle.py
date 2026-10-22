from __future__ import annotations

import hashlib
from pathlib import Path
from unittest import mock

import pytest

from scripts.build_ship_bundle import build_bundle
from src.shared.json_io import read_json
from tests.helpers.ship_bundle import copy_minimal_app_version
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = pytest.mark.packaging

BUNDLED_PAGES = ("admin.html", "jobs.html", "saved.html")


def test_build_bundles_every_desktop_page_before_hashing_the_version() -> None:
    """The manifest must record the bundled pages, not the unbundled sources."""

    def fake_bundle(version_dir: Path) -> dict[str, object]:
        pages = []
        for name in BUNDLED_PAGES:
            bundled_page = f"<!-- bundled {name} -->\n".encode()
            (Path(version_dir) / name).write_bytes(bundled_page)
            pages.append({"name": name, "html": name, "asset": f"frontend/{name}"})
        return {"pages": pages}

    with workspace_tmpdir("build-ship-bundle") as tmp:
        with (
            mock.patch(
                "scripts.build_ship_bundle._copy_app_version",
                side_effect=copy_minimal_app_version,
            ),
            mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
            mock.patch(
                "scripts.build_ship_bundle._resolve_packaged_sync_config", return_value=None
            ),
            mock.patch(
                "scripts.build_ship_bundle.bundle_desktop_pages", side_effect=fake_bundle
            ) as bundle_call,
        ):
            output = build_bundle(Path(tmp) / "dist" / "baluffo-ship", "1.2.3")

        version_root = output / "app" / "versions" / "1.2.3"
        assert bundle_call.call_args.args[0] == version_root
        manifest = read_json(output / "app" / "version-hashes.json", {})
        for name in BUNDLED_PAGES:
            bundled_page = f"<!-- bundled {name} -->\n".encode()
            assert manifest["files"][name] == hashlib.sha256(bundled_page).hexdigest()
