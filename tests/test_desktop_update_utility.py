"""Tests for desktop update utility behavior."""

import base64
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_update_shared as du_shared
from tests.helpers.desktop_update_leaf_namespace import du


def test_canonical_manifest_bytes_sorts_keys_and_omits_signature() -> None:
    manifest = {
        "signature": "ignored",
        "version": "1.2.3",
        "portable_artifact": {
            "size_bytes": 3,
            "url": "https://example.com/app.zip",
            "sha256": "a" * 64,
        },
        "channel": "stable",
    }

    payload = du.canonical_manifest_bytes(manifest).decode("utf-8")
    assert payload == (
        '{"channel":"stable","portable_artifact":{"sha256":"'
        + ("a" * 64)
        + '","size_bytes":3,"url":"https://example.com/app.zip"},"version":"1.2.3"}'
    )


def test_validate_install_plan_requires_core_fields() -> None:
    with pytest.raises(ValueError, match="Install plan missing fields"):
        du.validate_install_plan({"planVersion": 1})


@pytest.mark.skipif(du.Ed25519PrivateKey is None, reason="cryptography not available")
def test_verify_manifest_signature_accepts_valid_ed25519_signature() -> None:
    private_key = du.Ed25519PrivateKey.generate()
    public_key_bytes = private_key.public_key().public_bytes_raw()
    manifest = {
        "schema_version": 2,
        "key_id": "desktop-ed25519-test",
        "channel": "stable",
        "version": "1.4.0",
        "published_at": "2026-04-14T12:00:00Z",
        "release_notes_url": "https://example.com/release",
        "min_desktop_updater_version": "2.0.0",
        "min_supported_current_version": "1.0.0",
        "data_schema_version": "2",
        "rollback_allowed": True,
        "portable_artifact": {
            "url": "https://example.com/baluffo-portable-1.4.0.zip",
            "sha256": "b" * 64,
            "size_bytes": 123,
        },
        "migration_plan": [],
    }
    manifest["signature"] = base64.b64encode(
        private_key.sign(du.canonical_manifest_bytes(manifest))
    ).decode("ascii")

    du.verify_manifest_signature(
        manifest,
        public_keys={"desktop-ed25519-test": public_key_bytes},
    )


def test_desktop_update_paths_resolve_install_root() -> None:
    paths = du.DesktopUpdatePaths.from_data_dir(Path("C:/Portable/Baluffo/ship/data"))

    assert paths.install_root == Path("C:/Portable/Baluffo")
    assert paths.ship_root == Path("C:/Portable/Baluffo/ship")
    assert paths.install_state_path == Path(
        "C:/Portable/Baluffo/ship/data/updater/install-state.json"
    )


def test_desktop_update_paths_support_external_windows_data_root() -> None:
    data_dir = Path("C:/Users/Andrea/AppData/Roaming/Baluffo")
    paths = du.DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=Path("C:/Portable/Baluffo"),
        ship_root=Path("C:/Portable/Baluffo/ship"),
    )

    assert paths.install_root == Path("C:/Portable/Baluffo")
    assert paths.ship_root == Path("C:/Portable/Baluffo/ship")
    assert paths.data_dir == data_dir
    assert paths.install_state_path == data_dir / "updater" / "install-state.json"


def test_compare_versions_uses_baluffo_release_ordering() -> None:
    assert du.compare_versions("0.1.3", "0.1.23") > 0
    assert du.compare_versions("0.1.3", "0.1.29") > 0
    assert du.compare_versions("0.1.30", "0.1.29") > 0


def test_pid_is_running_prefers_psutil_when_available() -> None:
    process = mock.Mock()
    process.is_running.return_value = True
    process.status.return_value = "running"
    fake_psutil = mock.Mock()
    fake_psutil.Process.return_value = process
    fake_psutil.STATUS_ZOMBIE = "zombie"

    with mock.patch.object(du_shared, "psutil", fake_psutil):
        assert du.pid_is_running(4242) is True


def test_pid_is_running_rejects_zombie_psutil_processes() -> None:
    process = mock.Mock()
    process.is_running.return_value = True
    process.status.return_value = "zombie"
    fake_psutil = mock.Mock()
    fake_psutil.Process.return_value = process
    fake_psutil.STATUS_ZOMBIE = "zombie"

    with mock.patch.object(du_shared, "psutil", fake_psutil):
        assert du.pid_is_running(4242) is False


@pytest.mark.windows
def test_pid_is_running_windows_fallback_accepts_live_pid_without_psutil() -> None:
    kernel32 = mock.Mock()
    kernel32.OpenProcess.return_value = 99

    def fake_get_exit_code_process(_handle, exit_code_ref) -> int:
        pointer = du_shared.ctypes.cast(
            exit_code_ref,
            du_shared.ctypes.POINTER(du_shared.wintypes.DWORD),
        )
        pointer.contents.value = 259
        return 1

    kernel32.GetExitCodeProcess.side_effect = fake_get_exit_code_process

    with (
        mock.patch.object(du_shared, "psutil", None),
        mock.patch.object(du_shared.sys, "platform", "win32"),
        mock.patch.object(
            du_shared.ctypes,
            "windll",
            mock.Mock(kernel32=kernel32),
            create=True,
        ),
    ):
        assert du.pid_is_running(4242) is True

    kernel32.OpenProcess.assert_called_once_with(0x1000, False, 4242)
    kernel32.CloseHandle.assert_called_once_with(99)


@pytest.mark.windows
def test_pid_is_running_windows_fallback_rejects_open_failed_pid_without_psutil() -> None:
    kernel32 = mock.Mock()
    kernel32.OpenProcess.return_value = 0

    with (
        mock.patch.object(du_shared, "psutil", None),
        mock.patch.object(du_shared.sys, "platform", "win32"),
        mock.patch.object(
            du_shared.ctypes,
            "windll",
            mock.Mock(kernel32=kernel32),
            create=True,
        ),
    ):
        assert du.pid_is_running(4242) is False

    kernel32.GetExitCodeProcess.assert_not_called()
    kernel32.CloseHandle.assert_not_called()
