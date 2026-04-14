import base64
import json
from pathlib import Path
from unittest import mock

import pytest

from src import app_version
from src.ship import desktop_update as du
from tests.helpers.temp_paths import workspace_tmpdir


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
    assert paths.install_state_path == Path("C:/Portable/Baluffo/ship/data/updater/install-state.json")


def test_updater_install_requested_reads_persisted_state() -> None:
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

        assert du.updater_install_requested(data_dir) is True


def test_updater_install_requested_reads_handoff_marker() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        du.write_json_atomic(paths.handoff_request_path, {"requestedAt": "2026-04-14T12:00:00Z"})

        assert du.updater_install_requested(data_dir) is True


def test_load_status_derives_install_stage_label_from_install_state() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = du.DesktopUpdatePaths.from_data_dir(data_dir)
        du.save_status(
            paths,
            {
                **du.default_status_payload(current_version="0.1.0"),
                "installState": "waiting_for_exit",
            },
        )

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


def test_get_app_version_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(app_version.APP_VERSION_OVERRIDE_ENV, "0.0.9")

    assert app_version.get_app_version() == "0.0.9"


def test_resolve_github_api_base_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(du.GITHUB_API_BASE_ENV, "http://127.0.0.1:9000/api/")

    assert du.resolve_github_api_base() == "http://127.0.0.1:9000/api"


def test_resolve_desktop_session_root_falls_back_to_temp_when_primary_is_not_writable() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        local_app_data = Path(tmp) / "local-app-data"
        temp_root = Path(tmp) / "temp-root"
        original_write_text = Path.write_text

        def flaky_write_text(self: Path, data: str, encoding: str | None = None, errors: str | None = None, newline: str | None = None) -> int:
            path_text = str(self)
            if ".baluffo-write-probe" in path_text and "local-app-data" in path_text:
                raise OSError("read-only")
            return original_write_text(self, data, encoding=encoding, errors=errors, newline=newline)

        with (
            mock.patch.dict(
                du.os.environ,
                {
                    "LOCALAPPDATA": str(local_app_data),
                    "USERNAME": "tester",
                    "TEMP": str(temp_root),
                    "TMP": str(temp_root),
                },
                clear=False,
            ),
            mock.patch.object(du.tempfile, "gettempdir", return_value=str(temp_root)),
            mock.patch.object(Path, "write_text", new=flaky_write_text),
        ):
            session_root = du.resolve_desktop_session_root()

        assert session_root == (temp_root / "Baluffo-tester").resolve()


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

        with (
            mock.patch.object(du, "resolve_desktop_session_root", return_value=session_root),
            mock.patch.object(du.shutil, "disk_usage", return_value=mock.Mock(free=10**9)),
            mock.patch.object(du, "verify_manifest_signature"),
        ):
            result = service.request_install()

        assert result["started"] is True
        plan = json.loads(paths.install_plan_path.read_text(encoding="utf-8"))
        assert plan["launcherPid"] == 1234
        assert plan["launcherToken"] == "token-1"
        assert plan["targetVersion"] == "1.4.0"
        assert plan["helperStdoutPath"] == str(paths.helper_stdout_log_path)
        assert plan["helperStderrPath"] == str(paths.helper_stderr_log_path)
        assert plan["helperDiagnosticsPath"] == str(paths.helper_diagnostics_log_path)
        assert result["status"]["installStage"] == "preparing"
        assert result["status"]["installStageLabel"] == "Preparing update"
        assert result["status"]["rollbackPath"]
        assert paths.handoff_request_path.is_file()


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

        with mock.patch.object(du.subprocess, "Popen") as popen_mock:
            du.launch_staged_update_helper(paths)

        popen_mock.assert_called_once()
        _, kwargs = popen_mock.call_args
        expected_flags = 0
        if du.os.name == "nt":
            expected_flags = int(getattr(du.subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        assert int(kwargs.get("creationflags") or 0) == expected_flags
        assert kwargs["cwd"] == str(paths.updater_dir)
        assert isinstance(kwargs["env"], dict)
        assert kwargs["env"]["TEMP"] == str((paths.updater_dir / "runtime-tmp").resolve())
        assert kwargs["env"]["TMP"] == str((paths.updater_dir / "runtime-tmp").resolve())
        assert Path(str(kwargs["stdout"].name)).resolve() == paths.helper_stdout_log_path.resolve()
        assert Path(str(kwargs["stderr"].name)).resolve() == paths.helper_stderr_log_path.resolve()
