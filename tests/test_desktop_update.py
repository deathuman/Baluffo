import base64
import json
import ssl
from hashlib import sha256
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import pytest

from src import app_version
from src.ship import desktop_update as du
from src.ship import desktop_update_service as du_service
from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state
from src.ship.desktop_app import config as desktop_app_config
from tests.helpers.temp_paths import workspace_tmpdir


def _write_required_root_html(version_root: Path) -> None:
    for name in ("index.html", "jobs.html", "saved.html", "admin.html"):
        (version_root / name).write_text("<html></html>\n", encoding="utf-8")


def _write_credible_handoff_request(
    paths: du.DesktopUpdatePaths,
    session_root: Path,
    *,
    install_state: str = "handoff_requested",
    launcher_pid: int = 1234,
    launcher_token: str = "token-1",
) -> None:
    session_root.mkdir(parents=True, exist_ok=True)
    du.write_json_atomic(
        session_root / "desktop-session.json",
        {
            "launcherPid": int(launcher_pid),
            "launcherToken": str(launcher_token),
        },
    )
    du.write_json_atomic(
        paths.install_plan_path,
        {
            "planVersion": 1,
            "installRoot": str(paths.install_root),
            "dataDir": str(paths.data_dir),
            "tempHelperPath": str(paths.install_root / du.DESKTOP_UPDATE_HELPER_NAME),
            "targetVersion": "1.4.0",
            "currentVersion": "0.1.0",
            "manifestPath": str(paths.manifest_cache_path),
            "downloadedZipPath": str(paths.downloads_dir / "baluffo-portable-1.4.0.zip"),
            "expectedZipSha256": "a" * 64,
            "manifestKeyId": "desktop-ed25519-test",
            "rollbackPath": str(paths.rollback_root / "rollback-1"),
            "updaterWorkingDir": str(paths.updater_dir),
            "createdAt": "2026-04-19T12:00:00Z",
            "launcherPid": int(launcher_pid),
            "launcherToken": str(launcher_token),
            "desktopSessionRoot": str(session_root),
        },
    )
    du.write_json_atomic(paths.handoff_request_path, {"requestedAt": "2026-04-19T12:00:00Z"})
    du.save_status(
        paths,
        {
            **du.default_status_payload(current_version="0.1.0"),
            "installState": str(install_state),
        },
    )


def _prepare_ready_install(
    data_dir: Path,
    *,
    install_root: Path | None = None,
    ship_root: Path | None = None,
) -> tuple[du.DesktopUpdatePaths, du.DesktopUpdateService, Path]:
    paths = du.DesktopUpdatePaths.from_data_dir(
        data_dir,
        install_root=install_root,
        ship_root=ship_root,
    )
    helper_path = paths.install_root / du.DESKTOP_UPDATE_HELPER_NAME
    download_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
    helper_path.parent.mkdir(parents=True, exist_ok=True)
    helper_path.write_text("helper", encoding="utf-8")
    download_path.parent.mkdir(parents=True, exist_ok=True)
    download_path.write_text("zip", encoding="utf-8")
    du.write_json_atomic(
        paths.manifest_cache_path,
        {
            "manifest": {
                "schema_version": 2,
                "key_id": "desktop-ed25519-test",
                "channel": "stable",
                "version": "1.4.0",
                "published_at": "2026-04-14T12:00:00Z",
                "release_notes_url": "https://example.com/release",
                "min_desktop_updater_version": "2.0.0",
                "min_supported_current_version": "0.1.0",
                "data_schema_version": "2",
                "rollback_allowed": True,
                "portable_artifact": {
                    "url": "https://example.com/baluffo-portable-1.4.0.zip",
                    "sha256": du.compute_sha256(download_path),
                    "size_bytes": int(download_path.stat().st_size),
                },
                "migration_plan": [],
                "signature": "ignored-for-test",
            }
        },
    )
    du.save_status(
        paths,
        {
            **du.default_status_payload(current_version="0.1.0"),
            "availability": "available",
            "updateAvailable": True,
            "downloadState": "downloaded",
            "downloadedZipPath": str(download_path),
        },
    )
    service = du.DesktopUpdateService(
        data_dir=data_dir,
        install_root=install_root,
        ship_root=ship_root,
        current_version_getter=lambda: "0.1.0",
    )
    return paths, service, download_path


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

    with mock.patch.object(du, "psutil", fake_psutil):
        assert du.pid_is_running(4242) is True


def test_pid_is_running_rejects_zombie_psutil_processes() -> None:
    process = mock.Mock()
    process.is_running.return_value = True
    process.status.return_value = "zombie"
    fake_psutil = mock.Mock()
    fake_psutil.Process.return_value = process
    fake_psutil.STATUS_ZOMBIE = "zombie"

    with mock.patch.object(du, "psutil", fake_psutil):
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
        mock.patch.object(du, "psutil", None),
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
        mock.patch.object(du, "psutil", None),
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


def test_manifest_to_status_marks_0_1_3_available_over_0_1_23() -> None:
    status = du._manifest_to_status(
        current_version="0.1.23",
        manifest={
            "version": "0.1.3",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.23"),
    )

    assert status["availability"] == "available"
    assert status["updateAvailable"] is True
    assert status["latestVersion"] == "0.1.3"


def test_manifest_to_status_marks_matching_0_1_3_up_to_date() -> None:
    status = du._manifest_to_status(
        current_version="0.1.3",
        manifest={
            "version": "0.1.3",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.3"),
    )

    assert status["availability"] == "up_to_date"
    assert status["updateAvailable"] is False


def test_manifest_to_status_marks_0_1_31_available_over_0_1_23() -> None:
    status = du._manifest_to_status(
        current_version="0.1.23",
        manifest={
            "version": "0.1.31",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.23"),
    )

    assert status["availability"] == "available"
    assert status["updateAvailable"] is True
    assert status["latestVersion"] == "0.1.31"


def test_manifest_to_status_marks_0_1_31_available_over_0_1_3() -> None:
    status = du._manifest_to_status(
        current_version="0.1.3",
        manifest={
            "version": "0.1.31",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.3"),
    )

    assert status["availability"] == "available"
    assert status["updateAvailable"] is True
    assert status["latestVersion"] == "0.1.31"


def test_manifest_to_status_marks_matching_0_1_31_up_to_date() -> None:
    status = du._manifest_to_status(
        current_version="0.1.31",
        manifest={
            "version": "0.1.31",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.31"),
    )

    assert status["availability"] == "up_to_date"
    assert status["updateAvailable"] is False


def test_updater_install_requested_clears_stale_persisted_handoff_state() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "installState": "handoff_requested",
            },
        )

        assert du.updater_install_requested(data_dir) is False

        status = du.load_status(paths, current_version="0.1.0")
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["lastError"] == "Stale desktop update handoff state was cleared."


def test_updater_install_requested_ignores_installing_without_handoff_marker() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "installState": "installing",
                "installStage": "verifying",
            },
        )

        assert du.updater_install_requested(data_dir) is False


def test_updater_install_requested_clears_stale_handoff_marker_without_plan() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        du.write_json_atomic(paths.handoff_request_path, {"requestedAt": "2026-04-14T12:00:00Z"})

        assert du.updater_install_requested(data_dir) is False
        assert not paths.handoff_request_path.exists()


def test_updater_install_requested_accepts_credible_handoff_state() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        session_root = Path(tmp) / "session"
        _write_credible_handoff_request(
            paths,
            session_root,
            install_state="waiting_for_exit",
        )
        with mock.patch.object(update_state, "pid_is_running", return_value=True):
            assert du.updater_install_requested(data_dir) is True
            status = du.load_status(paths, current_version="0.1.0")

        assert status["installState"] == "waiting_for_exit"
        assert status["installStage"] == "waiting_for_exit"
        assert status["installStageLabel"] == "Closing Baluffo"


def test_updater_install_requested_ignores_default_state_after_pipeline_completion() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"

        assert du.updater_install_requested(data_dir) is False


def test_load_status_derives_install_stage_label_from_install_state() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        session_root = Path(tmp) / "session"
        _write_credible_handoff_request(paths, session_root, install_state="waiting_for_exit")

        with mock.patch.object(update_state, "pid_is_running", return_value=True):
            status = du.load_status(paths, current_version="0.1.0")

        assert status["installStage"] == "waiting_for_exit"
        assert status["installStageLabel"] == "Closing Baluffo"


def test_load_desktop_update_public_keys_reads_packaged_fallback() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        app_dir = ship_root / "app"
        version_dir = app_dir / "versions" / "0.1.0" / "packaging"
        version_dir.mkdir(parents=True, exist_ok=True)
        (app_dir / "current.txt").write_text("0.1.0\n", encoding="utf-8")
        (app_dir / "desktop-update-public-keys.json").write_text(
            json.dumps({"desktop-ed25519-2026-01": base64.b64encode(b"p" * 32).decode("ascii")}),
            encoding="utf-8",
        )

        keys = du.load_desktop_update_public_keys(
            candidate_paths=du.desktop_update_public_key_candidate_paths(ship_root)
        )

        assert keys["desktop-ed25519-2026-01"] == b"p" * 32


def test_load_desktop_update_public_keys_repairs_missing_current_pointer() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        app_dir = ship_root / "app"
        version_root = app_dir / "versions" / "0.1.0"
        packaging_dir = version_root / "packaging"
        (version_root / "src").mkdir(parents=True, exist_ok=True)
        packaging_dir.mkdir(parents=True, exist_ok=True)
        (version_root / "src" / "admin_bridge.py").write_text("print('ok')\n", encoding="utf-8")
        _write_required_root_html(version_root)
        (app_dir / "update-state.json").write_text(
            json.dumps(
                {
                    "current_version": "0.1.0",
                    "previous_version": "",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": du.iso_now(),
                }
            ),
            encoding="utf-8",
        )
        (packaging_dir / du.PUBLIC_KEYS_FILE).write_text(
            json.dumps({"desktop-ed25519-2026-01": base64.b64encode(b"k" * 32).decode("ascii")}),
            encoding="utf-8",
        )

        keys = du.load_desktop_update_public_keys(
            candidate_paths=du.desktop_update_public_key_candidate_paths(ship_root)
        )

        assert keys["desktop-ed25519-2026-01"] == b"k" * 32
        assert (app_dir / "current.txt").read_text(encoding="utf-8").strip() == "0.1.0"


def test_get_app_version_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(app_version.APP_VERSION_OVERRIDE_ENV, "0.0.9")

    assert app_version.get_app_version() == "0.0.9"


def test_write_json_atomic_retries_transient_permission_error() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        target = Path(tmp) / "portable" / "ship" / "data" / "updater" / "install-state.json"
        calls = {"count": 0}
        original_replace = du_shared.os.replace

        def flaky_replace(src, dst):  # noqa: ANN001
            calls["count"] += 1
            if calls["count"] == 1:
                raise PermissionError(32, "sharing violation")
            return original_replace(src, dst)

        with mock.patch.object(du_shared.os, "replace", side_effect=flaky_replace):
            du.write_json_atomic(target, {"ok": True})

        assert json.loads(target.read_text(encoding="utf-8"))["ok"] is True
        assert calls["count"] == 2


def test_download_file_retries_transient_permission_error_on_finalize() -> None:
    class FakeResponse:
        def __init__(self, content: bytes) -> None:
            self._content = content
            self._offset = 0
            self.headers = {"Content-Length": str(len(content))}

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

        def read(self, size: int) -> bytes:
            if self._offset >= len(self._content):
                return b""
            chunk = self._content[self._offset : self._offset + size]
            self._offset += len(chunk)
            return chunk

    with workspace_tmpdir("desktop-update") as tmp:
        target = Path(tmp) / "portable" / "ship" / "data" / "updater" / "downloads" / "app.zip"
        content = b"portable-zip"
        calls = {"count": 0}
        seen = {}
        original_replace = du_shared.os.replace

        def flaky_replace(src, dst):  # noqa: ANN001
            calls["count"] += 1
            if calls["count"] == 1:
                raise PermissionError(32, "sharing violation")
            return original_replace(src, dst)

        def fake_urlopen(request, timeout=300.0, context=None):  # noqa: ANN001
            seen["timeout"] = timeout
            seen["context"] = context
            return FakeResponse(content)

        with (
            mock.patch.object(du, "urlopen", side_effect=fake_urlopen),
            mock.patch.object(du_shared.os, "replace", side_effect=flaky_replace),
        ):
            result = du.download_file("https://example.com/app.zip", target)

        assert result == target
        assert target.read_bytes() == content
        assert calls["count"] == 2
        assert seen["timeout"] == 300.0
        assert isinstance(seen["context"], ssl.SSLContext)
        assert list(target.parent.glob("*.download")) == []


def test_fetch_json_uses_ssl_context_for_default_https_urlopen() -> None:
    seen = {}

    class FakeResponse:
        headers = {}

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

        def read(self) -> bytes:
            return b'{"ok": true}'

    def fake_urlopen(request, timeout=20.0, context=None):  # noqa: ANN001
        seen["timeout"] = timeout
        seen["context"] = context
        return FakeResponse()

    with mock.patch.object(du, "urlopen", side_effect=fake_urlopen):
        payload = du.fetch_json("https://api.github.com/repos/example/app/releases", timeout_s=12.0)

    assert payload == {"ok": True}
    assert seen["timeout"] == 12.0
    assert isinstance(seen["context"], ssl.SSLContext)


def test_fetch_json_wraps_certificate_verify_failures_for_https() -> None:
    def failing_urlopen(_request, timeout=20.0, context=None):  # noqa: ANN001,ARG001
        raise URLError(ssl.SSLError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"))

    with (
        mock.patch.object(du, "urlopen", side_effect=failing_urlopen),
        pytest.raises(
            RuntimeError,
            match="SSL certificate verification failed while connecting to GitHub",
        ),
    ):
        du.fetch_json("https://api.github.com/repos/example/app/releases", timeout_s=12.0)


def test_download_file_wraps_certificate_verify_failures_for_https(tmp_path: Path) -> None:
    target = tmp_path / "app.zip"

    def failing_urlopen(_request, timeout=300.0, context=None):  # noqa: ANN001,ARG001
        raise URLError(ssl.SSLError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"))

    with (
        mock.patch.object(du, "urlopen", side_effect=failing_urlopen),
        pytest.raises(
            RuntimeError,
            match="SSL certificate verification failed while connecting to GitHub",
        ),
    ):
        du.download_file("https://example.com/app.zip", target)


def test_resolve_github_api_base_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(du.GITHUB_API_BASE_ENV, "http://127.0.0.1:9000/api/")

    assert du.resolve_github_api_base() == "http://127.0.0.1:9000/api"


def test_resolve_release_repo_prefers_packaged_desktop_update_config() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        packaging_dir = ship_root / "app" / "versions" / "0.1.0" / "packaging"
        packaging_dir.mkdir(parents=True, exist_ok=True)
        (ship_root / "app" / "current.txt").write_text("0.1.0\n", encoding="utf-8")
        (packaging_dir / "desktop-update-config.json").write_text(
            json.dumps({"repo": "owner/app-release"}),
            encoding="utf-8",
        )
        (packaging_dir / "github-app-sync-config.json").write_text(
            json.dumps({"repo": "owner/sync-backup"}),
            encoding="utf-8",
        )

        repo = du.resolve_release_repo(
            install_root=ship_root.parent,
            ship_root=ship_root,
        )

        assert repo == "owner/app-release"


def test_resolve_release_repo_repairs_missing_current_pointer() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        app_dir = ship_root / "app"
        version_root = app_dir / "versions" / "0.1.0"
        packaging_dir = version_root / "packaging"
        (version_root / "src").mkdir(parents=True, exist_ok=True)
        packaging_dir.mkdir(parents=True, exist_ok=True)
        (version_root / "src" / "admin_bridge.py").write_text("print('ok')\n", encoding="utf-8")
        _write_required_root_html(version_root)
        (app_dir / "update-state.json").write_text(
            json.dumps(
                {
                    "current_version": "0.1.0",
                    "previous_version": "",
                    "last_update_status": "ready",
                    "last_error_code": "",
                    "updated_at": du.iso_now(),
                }
            ),
            encoding="utf-8",
        )
        (packaging_dir / "desktop-update-config.json").write_text(
            json.dumps({"repo": "owner/repaired-release"}),
            encoding="utf-8",
        )

        repo = du.resolve_release_repo(
            install_root=ship_root.parent,
            ship_root=ship_root,
        )

        assert repo == "owner/repaired-release"
        assert (app_dir / "current.txt").read_text(encoding="utf-8").strip() == "0.1.0"


def test_resolve_desktop_session_root_falls_back_to_temp_when_primary_is_not_writable() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        local_app_data = Path(tmp) / "local-app-data"
        xdg_data = Path(tmp) / "xdg-data"
        temp_root = Path(tmp) / "temp-root"
        original_write_text = Path.write_text

        def flaky_write_text(
            self: Path,
            data: str,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
        ) -> int:
            path_text = str(self)
            blocked = {"local-app-data", "xdg-data"}
            if ".baluffo-write-probe" in path_text and any(p in path_text for p in blocked):
                raise OSError("read-only")
            return original_write_text(
                self, data, encoding=encoding, errors=errors, newline=newline
            )

        with (
            mock.patch.dict(
                du_shared.os.environ,
                {
                    "LOCALAPPDATA": str(local_app_data),
                    "XDG_DATA_HOME": str(xdg_data),
                    "USERNAME": "tester",
                    "TEMP": str(temp_root),
                    "TMP": str(temp_root),
                },
                clear=False,
            ),
            mock.patch.object(du_shared.tempfile, "gettempdir", return_value=str(temp_root)),
            mock.patch.object(Path, "write_text", new=flaky_write_text),
        ):
            session_root = du.resolve_desktop_session_root()

        assert session_root == (temp_root / "Baluffo-tester").resolve()


def test_resolve_desktop_session_root_honors_env_override() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        override_root = Path(tmp) / "override-session-root"

        session_root = du.resolve_desktop_session_root(
            {
                "BALUFFO_DESKTOP_SESSION_ROOT": str(override_root),
                "LOCALAPPDATA": str(Path(tmp) / "local-app-data"),
                "USERNAME": "tester",
            }
        )

        assert session_root == override_root.resolve()


def test_resolve_desktop_session_root_falls_back_to_runtime_temp_when_standard_locations_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        root = Path(tmp)
        temp_root = root / "temp"
        localappdata_root = root / "localappdata"
        xdg_root = root / "xdg-data"
        env = {
            "LOCALAPPDATA": str(localappdata_root),
            "USERNAME": "tester",
            "XDG_DATA_HOME": str(xdg_root),
        }
        local_candidate = (localappdata_root / "Baluffo").resolve()
        xdg_candidate = (xdg_root / "Baluffo").resolve()
        temp_candidate = (temp_root / "Baluffo-tester").resolve()
        blocked_parents = {local_candidate, xdg_candidate, temp_candidate}
        original_write_text = Path.write_text

        monkeypatch.setattr(desktop_app_config.tempfile, "gettempdir", lambda: str(temp_root))
        monkeypatch.setattr(desktop_app_config, "_RUNTIME_SESSION_ROOT", None)

        def blocked_write_text(self: Path, *args: object, **kwargs: object) -> int:
            if self.name == ".baluffo-write-probe" and self.parent in blocked_parents:
                raise OSError("blocked for test")
            return original_write_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", blocked_write_text)

        resolved = du.resolve_desktop_session_root(env)

    assert "BaluffoRuntime" in str(resolved)


def test_check_for_update_clears_stale_downloaded_state_for_newer_manifest() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        stale_zip = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        stale_zip.parent.mkdir(parents=True, exist_ok=True)
        stale_zip.write_text("stale-zip", encoding="utf-8")
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.4.0",
                "targetVersion": "1.4.0",
                "downloadState": "downloaded",
                "installState": "ready",
                "downloadedZipPath": str(stale_zip),
            },
        )
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }

        with (
            mock.patch.object(
                service, "_resolve_latest_release", return_value={"id": 123, "tag_name": "v1.5.0"}
            ),
            mock.patch.object(service, "_resolve_manifest_from_release", return_value=manifest),
        ):
            status = service.check_for_update(force=True)

        assert status["targetVersion"] == "1.5.0"
        assert status["downloadState"] == "idle"
        assert status["installState"] == "idle"
        assert status["downloadedZipPath"] == ""


def test_get_status_payload_promotes_completed_stale_download_to_ready() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        zip_path = paths.downloads_dir / "baluffo-portable-1.5.0.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_text("portable-zip", encoding="utf-8")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": du.compute_sha256(zip_path),
                "size_bytes": int(zip_path.stat().st_size),
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "downloadState": "downloading",
                "downloadedBytes": 12,
                "totalBytes": 100,
                "downloadPercent": 12,
            },
        )

        status = service.get_status_payload()

        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "ready"
        assert status["downloadPercent"] == 100
        assert status["downloadedZipPath"] == str(zip_path)


def test_get_status_payload_preserves_handoff_state_with_downloaded_zip() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(
            data_dir=data_dir, current_version_getter=lambda: "0.1.31"
        )
        session_root = Path(tmp) / "session"
        _write_credible_handoff_request(paths, session_root, install_state="handoff_requested")
        zip_path = paths.downloads_dir / "baluffo-portable-0.1.32.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_text("portable-zip", encoding="utf-8")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "0.1.32",
            "published_at": "2026-04-19T19:49:37Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-0.1.32.zip",
                "sha256": du.compute_sha256(zip_path),
                "size_bytes": int(zip_path.stat().st_size),
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.31"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "0.1.32",
                "targetVersion": "0.1.32",
                "downloadState": "downloaded",
                "installState": "ready",
                "installStage": "idle",
                "installStageLabel": "",
                "downloadedZipPath": str(zip_path),
                "helperUpdatedAt": "2026-04-19T22:26:21.821985+00:00",
                "rollbackPath": str(paths.rollback_root / "0.1.32-20260419-222621"),
            },
        )

        with mock.patch.object(update_state, "pid_is_running", return_value=True):
            status = service.get_status_payload()

        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "handoff_requested"
        assert status["installStage"] == "preparing"
        assert status["installStageLabel"] == "Preparing update"
        assert status["helperUpdatedAt"] == "2026-04-19T22:26:21.821985+00:00"
        assert status["rollbackPath"] == str(paths.rollback_root / "0.1.32-20260419-222621")


def test_get_status_payload_marks_interrupted_download_as_failed() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "downloadState": "downloading",
                "downloadedBytes": 23,
                "totalBytes": 137,
                "downloadPercent": 16,
            },
        )

        status = service.get_status_payload()

        assert status["downloadState"] == "failed"
        assert status["installState"] == "idle"
        assert status["downloadPercent"] == 0
        assert "stopped before it finished" in status["lastError"]


def test_get_status_payload_normalizes_installed_target_to_up_to_date() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "1.5.0")
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "installState": "installed",
                "installStage": "installed",
                "downloadState": "idle",
            },
        )

        status = service.get_status_payload()

        assert status["currentVersion"] == "1.5.0"
        assert status["availability"] == "up_to_date"
        assert status["updateAvailable"] is False
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"


def test_download_update_returns_structured_ready_failure() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        zip_path = paths.downloads_dir / "baluffo-portable-1.5.0.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_text("portable-zip", encoding="utf-8")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": du.compute_sha256(zip_path),
                "size_bytes": int(zip_path.stat().st_size),
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "lastCheckedAt": du.iso_now(),
                "downloadState": "downloaded",
                "installState": "ready",
                "downloadedZipPath": str(zip_path),
            },
        )

        result = service.download_update()

        assert result["started"] is False
        assert result["errorCode"] == "update_ready_to_install"
        assert result["status"]["installState"] == "ready"


def test_download_update_returns_structured_start_failure() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "lastCheckedAt": du.iso_now(),
            },
        )

        with mock.patch.object(du.threading.Thread, "start", side_effect=RuntimeError("boom")):
            result = service.download_update()

        status = du.load_status(paths, current_version="0.1.0")
        assert result["started"] is False
        assert result["errorCode"] == "download_start_failed"
        assert status["downloadState"] == "failed"
        assert "Could not start the desktop update download" in status["lastError"]


def test_get_status_payload_preserves_failed_download_error_after_reconciliation() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {"cachedAt": du.iso_now(), "manifest": manifest},
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.5.0",
                "targetVersion": "1.5.0",
                "downloadState": "failed",
                "lastError": "socket timeout",
            },
        )

        status = service.get_status_payload()

        assert status["downloadState"] == "failed"
        assert status["lastError"] == "socket timeout"


def test_run_download_worker_does_not_abort_when_progress_status_write_fails(monkeypatch) -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        content = b"portable-zip"
        expected_hash = sha256(content).hexdigest()
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/release",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": expected_hash,
                "size_bytes": len(content),
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        original_save_status = du.save_status

        def flaky_save_status(
            update_paths: du.DesktopUpdatePaths, payload: dict[str, object]
        ) -> dict[str, object]:
            if payload.get("downloadState") == "downloading":
                raise OSError("status file busy")
            return original_save_status(update_paths, payload)

        def fake_download(
            url: str,
            target: Path,
            *,
            on_progress=None,
            timeout_s: float = 300.0,
        ) -> Path:
            if callable(on_progress):
                on_progress(5, len(content))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            return target

        monkeypatch.setattr(du_service, "save_status", flaky_save_status)
        monkeypatch.setattr(du_service, "download_file", fake_download)

        service._run_download_worker(manifest)

        status = du.load_status(paths, current_version="0.1.0")
        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "ready"
        assert status["downloadedZipPath"]


def test_check_for_update_persists_release_notes_metadata_from_release_payload() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        release = {
            "id": 123,
            "tag_name": "v1.5.0",
            "name": "Baluffo v1.5.0",
            "body": "### Fixed\n- Release notes modal",
            "published_at": "2026-04-15T12:00:00Z",
            "html_url": "https://example.com/releases/v1.5.0",
        }
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/releases/v1.5.0",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }

        with (
            mock.patch.object(service, "_resolve_latest_release", return_value=release),
            mock.patch.object(service, "_resolve_manifest_from_release", return_value=manifest),
        ):
            status = service.check_for_update(force=True)

        cached = du.read_json(paths.manifest_cache_path, {})

        assert status["releaseNotesUrl"] == "https://example.com/releases/v1.5.0"
        assert status["releaseNotesTitle"] == "Baluffo v1.5.0"
        assert status["releaseNotesBody"] == "### Fixed\n- Release notes modal"
        assert status["releaseNotesPublishedAt"] == "2026-04-15T12:00:00Z"
        assert cached["manifest"] == manifest
        assert cached["releaseNotes"] == {
            "releaseNotesUrl": "https://example.com/releases/v1.5.0",
            "releaseNotesTitle": "Baluffo v1.5.0",
            "releaseNotesBody": "### Fixed\n- Release notes modal",
            "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
        }
        assert "releaseNotesBody" not in cached["manifest"]


def test_check_for_update_throttle_reuses_cached_release_notes() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/releases/v1.5.0",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "cachedAt": du.iso_now(),
                "releaseId": 123,
                "releaseTag": "v1.5.0",
                "manifest": manifest,
                "releaseNotes": {
                    "releaseNotesUrl": "https://example.com/releases/v1.5.0",
                    "releaseNotesTitle": "Baluffo v1.5.0",
                    "releaseNotesBody": "### Fixed\n- Cached notes",
                    "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
                },
            },
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "lastCheckedAt": du.iso_now(),
            },
        )

        with (
            mock.patch.object(service, "_resolve_latest_release") as latest_release_mock,
            mock.patch.object(service, "_resolve_manifest_from_release") as manifest_mock,
        ):
            status = service.check_for_update(force=False)

        latest_release_mock.assert_not_called()
        manifest_mock.assert_not_called()
        assert status["releaseNotesTitle"] == "Baluffo v1.5.0"
        assert status["releaseNotesBody"] == "### Fixed\n- Cached notes"
        assert status["releaseNotesPublishedAt"] == "2026-04-15T12:00:00Z"


def test_get_status_payload_backfills_release_notes_from_cache() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "schema_version": 2,
            "key_id": "desktop-ed25519-test",
            "channel": "stable",
            "version": "1.5.0",
            "published_at": "2026-04-15T12:00:00Z",
            "release_notes_url": "https://example.com/releases/v1.5.0",
            "min_desktop_updater_version": "2.0.0",
            "min_supported_current_version": "0.1.0",
            "data_schema_version": "2",
            "rollback_allowed": True,
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.5.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
            "migration_plan": [],
            "signature": "ignored-for-test",
        }
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "cachedAt": du.iso_now(),
                "releaseId": 123,
                "releaseTag": "v1.5.0",
                "manifest": manifest,
                "releaseNotes": {
                    "releaseNotesUrl": "https://example.com/releases/v1.5.0",
                    "releaseNotesTitle": "",
                    "releaseNotesBody": "### Fixed\n- Cached after restart",
                    "releaseNotesPublishedAt": "2026-04-15T12:00:00Z",
                },
            },
        )

        status = service.get_status_payload()

        assert status["releaseNotesUrl"] == "https://example.com/releases/v1.5.0"
        assert status["releaseNotesTitle"] == "1.5.0"
        assert status["releaseNotesBody"] == "### Fixed\n- Cached after restart"
        assert status["releaseNotesPublishedAt"] == "2026-04-15T12:00:00Z"


def test_request_install_writes_plan_and_launches_helper() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        helper_path = install_root / du.DESKTOP_UPDATE_HELPER_NAME
        download_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        helper_path.parent.mkdir(parents=True, exist_ok=True)
        helper_path.write_text("helper", encoding="utf-8")
        download_path.parent.mkdir(parents=True, exist_ok=True)
        download_path.write_text("zip", encoding="utf-8")
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": {
                    "schema_version": 2,
                    "key_id": "desktop-ed25519-test",
                    "channel": "stable",
                    "version": "1.4.0",
                    "published_at": "2026-04-14T12:00:00Z",
                    "release_notes_url": "https://example.com/release",
                    "min_desktop_updater_version": "2.0.0",
                    "min_supported_current_version": "0.1.0",
                    "data_schema_version": "2",
                    "rollback_allowed": True,
                    "portable_artifact": {
                        "url": "https://example.com/baluffo-portable-1.4.0.zip",
                        "sha256": du.compute_sha256(download_path),
                        "size_bytes": int(download_path.stat().st_size),
                    },
                    "migration_plan": [],
                    "signature": "ignored-for-test",
                }
            },
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "downloadState": "downloaded",
                "downloadedZipPath": str(download_path),
            },
        )
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        (session_root / "desktop-session.json").write_text(
            json.dumps({"launcherPid": 1234, "launcherToken": "token-1"}),
            encoding="utf-8",
        )
        du.write_success_marker(
            paths,
            app_version="0.1.0",
            bridge_port=8877,
            launcher_token="stale-token",
        )

        with (
            mock.patch.object(du_service, "resolve_desktop_session_root", lambda: session_root),
            mock.patch.object(du_service.shutil, "disk_usage", return_value=mock.Mock(free=10**9)),
            mock.patch.object(update_state, "pid_is_running", return_value=True),
            mock.patch.object(du_service, "verify_manifest_signature"),
        ):
            result = service.request_install()
        assert result["started"] is True
        plan = json.loads(paths.install_plan_path.read_text(encoding="utf-8"))
        assert plan["launcherPid"] == 1234
        assert plan["launcherToken"] == "token-1"
        assert plan["targetVersion"] == "1.4.0"
        assert plan["dataDir"] == str(paths.data_dir)
        assert plan["helperStdoutPath"] == str(paths.helper_stdout_log_path)
        assert plan["helperStderrPath"] == str(paths.helper_stderr_log_path)
        assert plan["helperDiagnosticsPath"] == str(paths.helper_diagnostics_log_path)
        assert result["status"]["installStage"] == "preparing"
        assert result["status"]["installStageLabel"] == "Preparing update"
        assert result["status"]["rollbackPath"]
        assert paths.handoff_request_path.is_file()
        assert not paths.success_marker_path.exists()
        assert not paths.handoff_diagnostics_path.exists()


def test_request_install_not_ready_leaves_no_partial_handoff_artifacts() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")

        result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_not_ready"
        assert not paths.install_plan_path.exists()
        assert not paths.handoff_request_path.exists()


def test_request_install_fails_preflight_when_data_root_lacks_space() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths, service, _download_path = _prepare_ready_install(data_dir)

        with mock.patch.object(
            du_service.shutil,
            "disk_usage",
            return_value=mock.Mock(free=1),
        ):
            result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_preflight_failed"
        assert "data root" in result["error"]
        assert not paths.install_plan_path.exists()
        assert not paths.handoff_request_path.exists()


def test_request_install_fails_preflight_when_install_root_lacks_space() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = Path(tmp) / "AppData" / "Roaming" / "Baluffo"
        paths, service, _download_path = _prepare_ready_install(
            data_dir,
            install_root=install_root,
            ship_root=ship_root,
        )
        high_space = mock.Mock(free=10**9)
        low_space = mock.Mock(free=1)

        with mock.patch.object(
            du_service.shutil,
            "disk_usage",
            side_effect=[high_space, low_space],
        ) as disk_usage_mock:
            result = service.request_install()

        assert result["started"] is False
        assert result["errorCode"] == "install_preflight_failed"
        assert "install root" in result["error"]
        assert disk_usage_mock.call_args_list[0].args[0] == paths.updater_dir
        assert disk_usage_mock.call_args_list[1].args[0] == paths.install_root
        assert not paths.install_plan_path.exists()
        assert not paths.handoff_request_path.exists()


def test_request_install_returns_handoff_unconfirmed_when_post_write_verification_fails() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        install_root = Path(tmp) / "portable"
        ship_root = install_root / "ship"
        data_dir = ship_root / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        helper_path = install_root / du.DESKTOP_UPDATE_HELPER_NAME
        download_path = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        helper_path.parent.mkdir(parents=True, exist_ok=True)
        helper_path.write_text("helper", encoding="utf-8")
        download_path.parent.mkdir(parents=True, exist_ok=True)
        download_path.write_text("zip", encoding="utf-8")
        du.write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": {
                    "schema_version": 2,
                    "key_id": "desktop-ed25519-test",
                    "channel": "stable",
                    "version": "1.4.0",
                    "published_at": "2026-04-14T12:00:00Z",
                    "release_notes_url": "https://example.com/release",
                    "min_desktop_updater_version": "2.0.0",
                    "min_supported_current_version": "0.1.0",
                    "data_schema_version": "2",
                    "rollback_allowed": True,
                    "portable_artifact": {
                        "url": "https://example.com/baluffo-portable-1.4.0.zip",
                        "sha256": du.compute_sha256(download_path),
                        "size_bytes": int(download_path.stat().st_size),
                    },
                    "migration_plan": [],
                    "signature": "ignored-for-test",
                }
            },
        )
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "downloadState": "downloaded",
                "downloadedZipPath": str(download_path),
            },
        )
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        session_root = Path(tmp) / "session"
        session_root.mkdir(parents=True, exist_ok=True)
        (session_root / "desktop-session.json").write_text(
            json.dumps({"launcherPid": 1234, "launcherToken": "token-1"}),
            encoding="utf-8",
        )

        with (
            mock.patch.object(du_service, "resolve_desktop_session_root", lambda: session_root),
            mock.patch.object(du_service.shutil, "disk_usage", return_value=mock.Mock(free=10**9)),
            mock.patch.object(update_state, "pid_is_running", return_value=False),
            mock.patch.object(du_service, "verify_manifest_signature"),
        ):
            result = service.request_install()

        status = du.load_status(paths, current_version="0.1.0")
        diagnostics_raw = paths.handoff_diagnostics_path.read_text(encoding="utf-8")
        diagnostics = json.loads(diagnostics_raw)
        assert result["started"] is False
        assert result["errorCode"] == "install_handoff_unconfirmed"
        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "ready"
        assert status["downloadedZipPath"] == str(download_path)
        assert not paths.install_plan_path.exists()
        assert not paths.handoff_request_path.exists()
        assert diagnostics["handoffRequestPresent"] is True
        assert diagnostics["installPlanValid"] is True
        assert diagnostics["launcherPid"] == 1234
        assert diagnostics["launcherPidRunning"] is False
        assert diagnostics["desktopSessionFilePresent"] is True
        assert diagnostics["launcherPidMatchesSession"] is True
        assert diagnostics["launcherTokenMatchesSession"] is True
        assert "token-1" not in diagnostics_raw


def test_run_download_worker_failure_clears_install_ready_state_and_bad_zip() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        target = paths.downloads_dir / "baluffo-portable-1.4.0.zip"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("bad-zip", encoding="utf-8")
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "availability": "available",
                "updateAvailable": True,
                "latestVersion": "1.4.0",
                "targetVersion": "1.4.0",
                "downloadState": "downloading",
                "downloadedBytes": int(target.stat().st_size),
                "totalBytes": 123,
                "downloadPercent": 100,
                "installState": "ready",
                "installStage": "idle",
                "downloadedZipPath": str(target),
            },
        )
        service = du.DesktopUpdateService(data_dir=data_dir, current_version_getter=lambda: "0.1.0")
        manifest = {
            "version": "1.4.0",
            "portable_artifact": {
                "url": "https://example.com/baluffo-portable-1.4.0.zip",
                "sha256": "a" * 64,
                "size_bytes": 123,
            },
        }

        with mock.patch.object(du_service, "download_file", return_value=target):
            service._run_download_worker(manifest)

        status = du.load_status(paths, current_version="0.1.0")
        assert status["availability"] == "available"
        assert status["updateAvailable"] is True
        assert status["targetVersion"] == "1.4.0"
        assert status["downloadState"] == "failed"
        assert status["installState"] == "idle"
        assert status["installStage"] == "idle"
        assert status["downloadedBytes"] == 0
        assert status["downloadPercent"] == 0
        assert status["downloadedZipPath"] == ""
        assert "checksum mismatch" in str(status["lastError"]).lower()
        assert not target.exists()


@pytest.mark.windows
def test_launch_staged_update_helper_uses_logged_spawn_contract() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        install_root = Path(tmp) / "portable"
        data_dir = install_root / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        temp_helper = Path(tmp) / "BaluffoUpdater-temp.exe"
        temp_helper.write_text("helper", encoding="utf-8")
        du.write_json_atomic(
            paths.install_plan_path,
            {
                "planVersion": 1,
                "installRoot": str(install_root),
                "dataDir": str(paths.data_dir),
                "tempHelperPath": str(temp_helper),
                "targetVersion": "1.4.0",
                "currentVersion": "0.1.0",
                "manifestPath": str(paths.manifest_cache_path),
                "downloadedZipPath": str(paths.downloads_dir / "baluffo-portable-1.4.0.zip"),
                "expectedZipSha256": "abc",
                "manifestKeyId": "desktop-ed25519-test",
                "rollbackPath": str(paths.rollback_root / "1.4.0-20260414-120000"),
                "updaterWorkingDir": str(paths.updater_dir),
                "helperStdoutPath": str(paths.helper_stdout_log_path),
                "helperStderrPath": str(paths.helper_stderr_log_path),
                "helperDiagnosticsPath": str(paths.helper_diagnostics_log_path),
                "createdAt": "2026-04-14T12:00:00Z",
                "launcherPid": 1234,
                "launcherToken": "token-1",
                "desktopSessionRoot": str(Path(tmp) / "session"),
            },
        )

        with mock.patch.object(update_state.subprocess, "Popen") as popen_mock:
            du.launch_staged_update_helper(paths)

        popen_mock.assert_called_once()
        _, kwargs = popen_mock.call_args
        expected_flags = 0
        if update_state.os.name == "nt":
            expected_flags = int(getattr(update_state.subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        assert int(kwargs.get("creationflags") or 0) == expected_flags
        assert kwargs["cwd"] == str(paths.updater_dir)
        assert isinstance(kwargs["env"], dict)
        assert kwargs["env"]["TEMP"] == str(du.helper_runtime_tmpdir())
        assert kwargs["env"]["TMP"] == str(du.helper_runtime_tmpdir())
        assert kwargs["env"]["BALUFFO_DATA_DIR"] == str(paths.data_dir)
        assert kwargs["env"]["BALUFFO_INSTALL_ROOT"] == str(paths.install_root)
        assert kwargs["env"]["BALUFFO_SHIP_ROOT"] == str(paths.ship_root)
        assert Path(str(kwargs["stdout"].name)).resolve() == paths.helper_stdout_log_path.resolve()
        assert Path(str(kwargs["stderr"].name)).resolve() == paths.helper_stderr_log_path.resolve()
