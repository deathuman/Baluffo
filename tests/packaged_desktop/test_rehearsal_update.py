"""Packaged desktop rehearsal tests for desktop update."""

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
