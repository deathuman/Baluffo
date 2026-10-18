import contextlib
import json
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from unittest import mock

from scripts import build_ship_bundle
from tests.helpers.temp_paths import workspace_tmpdir

SHIP_BUNDLE_VERSION = "1.2.3"

_PACKAGED_SYNC_CONFIG_TEMPLATE_PATH = (
    build_ship_bundle.ROOT / "packaging" / "github-app-sync-config.template.json"
)
_PACKAGED_SYNC_CONFIG_PAYLOAD = {
    "schemaVersion": 1,
    "appId": "123456",
    "installationId": "999999",
    "repo": "owner/repo",
    "branch": "main",
    "path": "baluffo/source-sync.json",
    "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
}
# Staged next to the build output so the "no startup preview from local job rows"
# guard builds while local rows are on disk.
_LOCAL_JOB_ROWS = [{"title": f"Role {index}", "company": "Studio"} for index in range(300)]


def copy_minimal_app_version(version_dir: Path) -> None:
    packaged_sync_config = build_ship_bundle._resolve_packaged_sync_config()
    desktop_update_repo = build_ship_bundle._resolve_desktop_update_repo()

    src_dir = version_dir / "src"
    ship_dir = src_dir / "ship"
    ship_dir.mkdir(parents=True, exist_ok=True)
    (version_dir / "packaging").mkdir(parents=True, exist_ok=True)
    shutil.copytree(build_ship_bundle.ROOT / "src" / "storage", src_dir / "storage")
    # `src.storage.baluffo_store` imports these sibling leaves, so the storage probe in
    # `validate_app_version_python_imports` cannot run without them.
    for rel in ("storage_metrics.py", "source_registry_identity.py"):
        shutil.copy2(build_ship_bundle.ROOT / "src" / rel, src_dir / rel)
    for path, text in (
        (src_dir / "__init__.py", ""),
        (src_dir / "admin_bridge.py", "# test stub\n"),
        (ship_dir / "__init__.py", ""),
        (ship_dir / "runtime_launcher.py", "# test stub\n"),
    ):
        path.write_text(text, encoding="utf-8")
    if packaged_sync_config is not None:
        shutil.copy2(
            packaged_sync_config, version_dir / "packaging" / "github-app-sync-config.json"
        )
    if desktop_update_repo:
        (version_dir / "packaging" / build_ship_bundle.DESKTOP_UPDATE_CONFIG_FILE).write_text(
            json.dumps({"repo": desktop_update_repo}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )


def write_packaged_sync_config(root: Path) -> Path:
    """Write the packaged sync config the build copies into a bundle."""
    config_path = Path(root) / "packaging" / "github-app-sync-config.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(_PACKAGED_SYNC_CONFIG_PAYLOAD), encoding="utf-8")
    return config_path


def _link_or_copy(src: str, dst: str) -> str:
    try:
        os.link(src, dst)
    except OSError:  # different volume, or a filesystem without hardlinks
        return shutil.copy2(src, dst)
    return dst


def copy_ship_bundle(source: Path, destination: Path) -> Path:
    """Give one test its own copy of an already-built bundle.

    A real copy of the full bundle (~3.4k files, 53 MB) costs ~2s, which would eat most
    of what sharing the build saves; hardlinked files cost ~0.8s. Links also keep
    deletions local: the import-closure tests unlink files from their copy to prove the
    build validation fails, and the shared build stays intact. Only an in-place rewrite
    of bundle content would leak between tests, and no packaging test does that.
    """
    shutil.copytree(source, destination, copy_function=_link_or_copy)
    return destination


def build_full_ship_bundle(root: Path) -> Path:
    """Build the full-fidelity bundle (real `_copy_app_version`) under `root`."""
    config_path = write_packaged_sync_config(root)
    data_dir = Path(root) / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "jobs-unified-light.json").write_text(json.dumps(_LOCAL_JOB_ROWS), encoding="utf-8")
    with (
        mock.patch("scripts.build_ship_bundle.PACKAGED_SYNC_CONFIG_PATH", config_path),
        mock.patch(
            "scripts.build_ship_bundle.PACKAGED_SYNC_CONFIG_TEMPLATE_PATH",
            _PACKAGED_SYNC_CONFIG_TEMPLATE_PATH,
        ),
    ):
        return build_ship_bundle.build_bundle(
            Path(root) / "dist" / "baluffo-ship", SHIP_BUNDLE_VERSION
        )


@contextlib.contextmanager
def shared_full_ship_bundle() -> Iterator[Path]:
    """Build the full bundle once per test module; each test copies it."""
    with workspace_tmpdir("build-ship-bundle-shared") as root:
        yield build_full_ship_bundle(root)
