from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src.ship.packaged_smoke import rehearsal_update
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def _update_rehearsal_deps(release_server_failure: BaseException) -> SimpleNamespace:
    public_key = mock.Mock()
    public_key.public_bytes_raw.return_value = b"public-key"
    private_key = mock.Mock()
    private_key.public_key.return_value = public_key
    private_key.private_bytes_raw.return_value = b"private-key"
    signing_class = SimpleNamespace(generate=mock.Mock(return_value=private_key))
    return SimpleNamespace(
        DESKTOP_SESSION_STATE_FILE="desktop-session.json",
        DESKTOP_UPDATE_SCHEMA_VERSION=1,
        DESKTOP_UPDATER_VERSION="1.0.0",
        Ed25519SigningClass=signing_class,
        _archive_portable_dir=mock.Mock(),
        _preferred_desktop_browser_env=mock.Mock(return_value={}),
        _seed_rehearsal_local_data=mock.Mock(return_value={}),
        _start_desktop_update_release_server=mock.Mock(side_effect=release_server_failure),
        cleanup_orphaned_desktop_ports_nt=mock.Mock(),
        compute_sha256=mock.Mock(return_value="0" * 64),
        get_app_version=mock.Mock(return_value="1.2.3"),
        os=SimpleNamespace(environ={}, name="posix"),
        sign_manifest=mock.Mock(return_value="signature"),
        subprocess=SimpleNamespace(DEVNULL=-3, run=mock.Mock()),
        terminate_process_tree=mock.Mock(),
        update_manifest_helpers_mod=SimpleNamespace(DESKTOP_UPDATE_CHANNEL="stable"),
        utc_now_iso=mock.Mock(return_value="2026-06-19T00:00:00+00:00"),
    )


def _run_update_rehearsal(deps: SimpleNamespace, root: Path) -> dict[str, object]:
    artifacts_dir = root / "artifacts"
    install_root = artifacts_dir / "portable-install"
    target_root = artifacts_dir / "portable-update-target"
    target_zip = artifacts_dir / "baluffo-portable-update.zip"
    install_root.mkdir(parents=True)
    target_root.mkdir(parents=True)
    target_zip.write_bytes(b"portable-update")
    deps._archive_portable_dir.return_value = target_zip

    original_root = rehearsal_update.root
    rehearsal_update.root = deps
    try:
        with mock.patch.object(
            rehearsal_update,
            "_prepare_desktop_update_rehearsal_roots",
            return_value=(install_root, target_root, []),
        ):
            return rehearsal_update.run_desktop_update_rehearsal(
                exe_path=root / "Baluffo.exe",
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )
    finally:
        rehearsal_update.root = original_root


def test_desktop_update_rehearsal_reports_expected_release_server_failure() -> None:
    with workspace_tmpdir("desktop-update-rehearsal") as tmp:
        deps = _update_rehearsal_deps(RuntimeError("release server unavailable"))

        result = _run_update_rehearsal(deps, Path(tmp))

        assert result["status"] == "failed"
        assert result["error"] == "release server unavailable"
        deps._start_desktop_update_release_server.assert_called_once()
        deps.terminate_process_tree.assert_called_once_with(None)
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(0, 0, 0, 0)


def test_desktop_update_rehearsal_does_not_hide_programming_failures() -> None:
    with workspace_tmpdir("desktop-update-rehearsal") as tmp:
        deps = _update_rehearsal_deps(AssertionError("bad update invariant"))

        with pytest.raises(AssertionError, match="bad update invariant"):
            _run_update_rehearsal(deps, Path(tmp))

        deps._start_desktop_update_release_server.assert_called_once()
        deps.terminate_process_tree.assert_called_once_with(None)
        deps.cleanup_orphaned_desktop_ports_nt.assert_called_once_with(0, 0, 0, 0)
