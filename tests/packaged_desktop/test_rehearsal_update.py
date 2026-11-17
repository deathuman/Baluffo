"""Packaged desktop rehearsal tests for desktop update."""

from src.ship.packaged_smoke import rehearsal_update as rehearsal_update_mod

from ._rehearsal_shared import (
    Path,
    SimpleNamespace,
    _PathReadFailure,
    json,
    mock,
    pytest,
    smoke,
    workspace_tmpdir,
)

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_run_packaged_smoke_can_run_desktop_update_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--desktop-update-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_desktop_update_rehearsal",
                return_value={
                    "name": "Packaged desktop updater rehearsal",
                    "slug": "desktop-update-rehearsal",
                    "status": "passed",
                    "durationMs": 1500,
                    "error": "",
                    "details": {
                        "helperStdoutLog": str(artifacts_dir / "helper.stdout.log"),
                        "helperStderrLog": str(artifacts_dir / "helper.stderr.log"),
                        "helperDiagnosticsLog": str(artifacts_dir / "helper.diagnostics.jsonl"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "desktop-update-rehearsal"
        assert payload["artifacts"]["helperStdout"] == str(artifacts_dir / "helper.stdout.log")
        assert payload["artifacts"]["helperStderr"] == str(artifacts_dir / "helper.stderr.log")
        assert payload["artifacts"]["helperDiagnostics"] == str(
            artifacts_dir / "helper.diagnostics.jsonl"
        )
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True


def test_run_packaged_smoke_can_run_desktop_update_path_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--desktop-update-path-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_desktop_update_path_rehearsal",
                return_value={
                    "name": "Packaged desktop update path rehearsal",
                    "slug": "desktop-update-path-rehearsal",
                    "status": "passed",
                    "durationMs": 1500,
                    "error": "",
                    "details": {},
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "desktop-update-path-rehearsal"
        rehearsal_mock.assert_called_once()


def test_run_packaged_smoke_can_run_desktop_update_rollback_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--desktop-update-rollback-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_desktop_update_rollback_rehearsal",
                return_value={
                    "name": "Packaged desktop update rollback rehearsal",
                    "slug": "desktop-update-rollback-rehearsal",
                    "status": "passed",
                    "durationMs": 1500,
                    "error": "",
                    "details": {},
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "desktop-update-rollback-rehearsal"
        rehearsal_mock.assert_called_once()


def test_rollback_rehearsal_runs_the_shared_lane_in_rollback_mode() -> None:
    with mock.patch.object(
        rehearsal_update_mod,
        "run_desktop_update_rehearsal",
        return_value={
            "status": "passed",
            "details": {
                "rollback": {"rollbackVerified": True},
                "relaunchHealth": {"startupReady": True},
            },
        },
    ) as run_mock:
        result = rehearsal_update_mod.run_desktop_update_rollback_rehearsal(
            exe_path=Path("Baluffo.exe"),
            artifacts_dir=Path("artifacts"),
            runtime_timeout_s=60.0,
        )

    assert run_mock.call_args.kwargs["mode"] == "rollback"
    assert result["slug"] == "desktop-update-rollback-rehearsal"
    assert result["status"] == "passed"


def test_rollback_rehearsal_fails_when_the_helper_did_not_verify_the_rollback() -> None:
    with mock.patch.object(
        rehearsal_update_mod,
        "run_desktop_update_rehearsal",
        return_value={
            "status": "passed",
            "details": {
                "rollback": {"rollbackVerified": False},
                "relaunchHealth": {"startupReady": True},
            },
        },
    ):
        result = rehearsal_update_mod.run_desktop_update_rollback_rehearsal(
            exe_path=Path("Baluffo.exe"),
            artifacts_dir=Path("artifacts"),
            runtime_timeout_s=60.0,
        )

    assert result["status"] == "failed"
    assert "Rollback was not verified" in str(result["error"])


def test_desktop_update_rehearsal_removes_optional_psutil_from_source_runtime() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        portable_root = Path(tmp) / "portable"
        internal_dir = portable_root / "_internal"
        (internal_dir / "psutil").mkdir(parents=True)
        (internal_dir / "psutil" / "__init__.py").write_text("", encoding="utf-8")
        (internal_dir / "psutil-6.0.0.dist-info").mkdir()
        (internal_dir / "_psutil_windows.pyd").write_bytes(b"pyd")
        (internal_dir / "keep.txt").write_text("keep", encoding="utf-8")

        removed = smoke.packaged_smoke_rehearsal_update_mod._remove_optional_psutil_runtime(
            portable_root
        )

        assert sorted(Path(item).as_posix() for item in removed) == [
            "_internal/_psutil_windows.pyd",
            "_internal/psutil",
            "_internal/psutil-6.0.0.dist-info",
        ]
        assert not (internal_dir / "psutil").exists()
        assert not (internal_dir / "psutil-6.0.0.dist-info").exists()
        assert not (internal_dir / "_psutil_windows.pyd").exists()
        assert (internal_dir / "keep.txt").is_file()


def test_desktop_update_rehearsal_prepares_copies_without_mutating_source_portable() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        portable_root = root / "portable"
        artifacts_dir = root / "artifacts"
        internal_dir = portable_root / "_internal"
        app_dir = portable_root / "ship" / "app"
        version_dir = app_dir / "versions" / "1.2.3" / "packaging"
        internal_dir.mkdir(parents=True)
        version_dir.mkdir(parents=True)
        (internal_dir / "psutil").mkdir()
        (internal_dir / "psutil" / "__init__.py").write_text("", encoding="utf-8")
        (internal_dir / "_psutil_windows.pyd").write_bytes(b"pyd")
        (app_dir / "current.txt").write_text("1.2.3\n", encoding="utf-8")
        public_keys_name = smoke.desktop_update_mod.PUBLIC_KEYS_FILE

        install_root, target_root, removed = (
            smoke.packaged_smoke_rehearsal_update_mod._prepare_desktop_update_rehearsal_roots(
                portable_root=portable_root,
                artifacts_dir=artifacts_dir,
                public_keys={"test-key": "cHVibGlj"},
            )
        )

        assert sorted(Path(item).as_posix() for item in removed) == [
            "_internal/_psutil_windows.pyd",
            "_internal/psutil",
        ]
        assert (portable_root / "_internal" / "psutil").is_dir()
        assert not (app_dir / public_keys_name).exists()
        assert not (version_dir / public_keys_name).exists()
        assert not (install_root / "_internal" / "psutil").exists()
        assert (target_root / "_internal" / "psutil").is_dir()
        assert (install_root / "ship" / "app" / public_keys_name).is_file()
        assert (target_root / "ship" / "app" / public_keys_name).is_file()


def test_desktop_update_rehearsal_json_fallbacks_ignore_malformed_payloads() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        paths.handoff_request_path.parent.mkdir(parents=True, exist_ok=True)
        paths.handoff_request_path.write_text("{}", encoding="utf-8")
        paths.install_state_path.parent.mkdir(parents=True, exist_ok=True)
        paths.install_state_path.write_text("{", encoding="utf-8")
        paths.helper_stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
        paths.helper_stdout_log_path.write_text("{", encoding="utf-8")

        assert (
            smoke.packaged_smoke_rehearsal_update_mod._confirmed_install_handoff_status(paths) == {}
        )
        assert smoke.packaged_smoke_rehearsal_update_mod._read_helper_stdout_payload(paths) == {}
        assert smoke.packaged_smoke_rehearsal_update_mod._load_update_install_state(paths) == {}


def test_desktop_update_rehearsal_json_fallbacks_propagate_unexpected_read_errors() -> None:
    paths = SimpleNamespace(
        handoff_request_path=_PathReadFailure(RuntimeError("handoff read bug")),
        install_state_path=_PathReadFailure(RuntimeError("install read bug")),
        helper_stdout_log_path=_PathReadFailure(RuntimeError("stdout read bug")),
    )

    with pytest.raises(RuntimeError, match="install read bug"):
        smoke.packaged_smoke_rehearsal_update_mod._confirmed_install_handoff_status(paths)
    with pytest.raises(RuntimeError, match="stdout read bug"):
        smoke.packaged_smoke_rehearsal_update_mod._read_helper_stdout_payload(paths)
    with pytest.raises(RuntimeError, match="install read bug"):
        smoke.packaged_smoke_rehearsal_update_mod._load_update_install_state(paths)


def test_run_packaged_smoke_can_run_desktop_update_repair_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--desktop-update-repair-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_desktop_update_repair_rehearsal",
                return_value={
                    "name": "Packaged desktop update repair rehearsal",
                    "slug": "desktop-update-repair-rehearsal",
                    "status": "passed",
                    "durationMs": 1500,
                    "error": "",
                    "details": {},
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "desktop-update-repair-rehearsal"
        rehearsal_mock.assert_called_once()


def test_run_packaged_smoke_can_run_desktop_update_reextract_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--desktop-update-reextract-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_desktop_update_reextract_rehearsal",
                return_value={
                    "name": "Packaged desktop update re-extract rehearsal",
                    "slug": "desktop-update-reextract-rehearsal",
                    "status": "passed",
                    "durationMs": 1500,
                    "error": "",
                    "details": {},
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "desktop-update-reextract-rehearsal"
        rehearsal_mock.assert_called_once()


def test_repair_rehearsal_runs_the_snapshot_lane_and_requires_the_snapshot_removed() -> None:
    with mock.patch.object(
        rehearsal_update_mod,
        "_run_desktop_repair_rehearsal",
        return_value={
            "status": "passed",
            "details": {
                "repair": {"action": "restore-snapshot", "snapshotRemoved": True},
                "relaunchHealth": {"startupReady": True},
            },
        },
    ) as run_mock:
        result = rehearsal_update_mod.run_desktop_update_repair_rehearsal(
            exe_path=Path("Baluffo.exe"),
            artifacts_dir=Path("artifacts"),
            runtime_timeout_s=60.0,
        )

    assert run_mock.call_args.kwargs["mode"] == "restore-snapshot"
    assert result["slug"] == "desktop-update-repair-rehearsal"
    assert result["status"] == "passed"


def test_repair_rehearsal_fails_when_the_snapshot_was_kept() -> None:
    with mock.patch.object(
        rehearsal_update_mod,
        "_run_desktop_repair_rehearsal",
        return_value={
            "status": "passed",
            "details": {
                "repair": {"action": "restore-snapshot", "snapshotRemoved": False},
                "relaunchHealth": {"startupReady": True},
            },
        },
    ):
        result = rehearsal_update_mod.run_desktop_update_repair_rehearsal(
            exe_path=Path("Baluffo.exe"),
            artifacts_dir=Path("artifacts"),
            runtime_timeout_s=60.0,
        )

    assert result["status"] == "failed"
    assert "rollback snapshot" in str(result["error"])


def test_reextract_rehearsal_runs_the_zip_lane() -> None:
    with mock.patch.object(
        rehearsal_update_mod,
        "_run_desktop_repair_rehearsal",
        return_value={
            "status": "passed",
            "details": {
                "repair": {"action": "reextract-zip", "snapshotRemoved": True},
                "relaunchHealth": {"startupReady": True},
            },
        },
    ) as run_mock:
        result = rehearsal_update_mod.run_desktop_update_reextract_rehearsal(
            exe_path=Path("Baluffo.exe"),
            artifacts_dir=Path("artifacts"),
            runtime_timeout_s=60.0,
        )

    assert run_mock.call_args.kwargs["mode"] == "reextract-zip"
    assert result["slug"] == "desktop-update-reextract-rehearsal"
    assert result["status"] == "passed"


def test_reextract_rehearsal_fails_when_the_repair_used_the_snapshot_lane() -> None:
    with mock.patch.object(
        rehearsal_update_mod,
        "_run_desktop_repair_rehearsal",
        return_value={
            "status": "passed",
            "details": {
                "repair": {"action": "restore-snapshot", "snapshotRemoved": True},
                "relaunchHealth": {"startupReady": True},
            },
        },
    ):
        result = rehearsal_update_mod.run_desktop_update_reextract_rehearsal(
            exe_path=Path("Baluffo.exe"),
            artifacts_dir=Path("artifacts"),
            runtime_timeout_s=60.0,
        )

    assert result["status"] == "failed"
    assert "reextract-zip" in str(result["error"])


def test_terminal_repair_snapshot_excludes_the_external_data_root() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        install_root = root / "install"
        (install_root / "ship" / "app").mkdir(parents=True)
        (install_root / "ship" / "data").mkdir(parents=True)
        (install_root / "Baluffo.exe").write_bytes(b"exe")
        (install_root / "ship" / "app" / "current.txt").write_text(
            "1.2.3\n",
            encoding="utf-8",
        )
        (install_root / "ship" / "data" / "local-data.json").write_text(
            "{}",
            encoding="utf-8",
        )

        copied = rehearsal_update_mod._create_terminal_repair_snapshot(
            install_root,
            root / "rollback",
        )

        snapshot_root = root / "rollback" / "runtime"
        assert copied == 2
        assert (snapshot_root / "Baluffo.exe").is_file()
        assert (snapshot_root / "ship" / "app" / "current.txt").is_file()
        assert not (snapshot_root / "ship" / "data").exists()


def test_terminal_rollback_status_seeds_the_repair_contract() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        data_dir = root / "install" / "ship" / "data"
        rollback_root = root / "rollback"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)

        status = rehearsal_update_mod._seed_terminal_rollback_status(
            paths,
            rollback_path=rollback_root,
            target_version="1.2.3",
            previous_version="0.0.9",
        )

        assert status["installState"] == "failed"
        assert status["installStage"] == "rollback_failed"
        assert status["rollbackPath"] == str(rollback_root)
        assert status["repairAvailable"] is True
        assert status["targetVersion"] == "1.2.3"
        assert str(status["lastError"]).startswith("install_restore_failed:")
        saved = json.loads(paths.install_state_path.read_text(encoding="utf-8"))
        assert saved["installStage"] == "rollback_failed"
