import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from src import packaged_desktop_smoke as smoke
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_is_windows_process_elevated_treats_expected_api_failure_as_not_elevated() -> None:
    fake_ctypes = SimpleNamespace(
        windll=SimpleNamespace(
            shell32=SimpleNamespace(IsUserAnAdmin=mock.Mock(side_effect=OSError("denied")))
        )
    )

    with (
        mock.patch.object(smoke, "os", SimpleNamespace(name="nt")),
        mock.patch.object(smoke, "ctypes", fake_ctypes, create=True),
    ):
        assert smoke.is_windows_process_elevated() is False


def test_is_windows_process_elevated_does_not_swallow_unexpected_api_failure() -> None:
    fake_ctypes = SimpleNamespace(
        windll=SimpleNamespace(
            shell32=SimpleNamespace(
                IsUserAnAdmin=mock.Mock(side_effect=RuntimeError("unexpected elevation bug"))
            )
        )
    )

    with (
        mock.patch.object(smoke, "os", SimpleNamespace(name="nt")),
        mock.patch.object(smoke, "ctypes", fake_ctypes, create=True),
        pytest.raises(RuntimeError, match="unexpected elevation bug"),
    ):
        smoke.is_windows_process_elevated()


def test_ensure_portable_exe_raises_when_missing_and_build_still_missing() -> None:
    with (
        workspace_tmpdir("packaged-smoke") as tmp,
    ):
        exe_path = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "run_portable_build") as build_mock,
        ):
            with pytest.raises(RuntimeError, match="Packaged desktop executable not found"):
                smoke.ensure_portable_exe(exe_path, rebuild=False)
            build_mock.assert_called_once()


def test_ensure_portable_exe_uses_rebuild_output_dir_when_requested() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        requested_exe = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        rebuilt_dir = root / "artifacts" / "portable-build"
        rebuilt_exe = rebuilt_dir / "Baluffo.exe"
        rebuilt_dir.mkdir(parents=True, exist_ok=True)
        rebuilt_exe.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", requested_exe),
            mock.patch.object(smoke, "run_portable_build", return_value=rebuilt_exe) as build_mock,
        ):
            resolved = smoke.ensure_portable_exe(
                requested_exe, rebuild=True, rebuild_output_dir=rebuilt_dir
            )
        assert resolved == rebuilt_exe.resolve()
        build_mock.assert_called_once_with(rebuilt_dir, force=True)


def test_run_portable_build_cleans_pyinstaller_scratch_dirs_for_explicit_output_dir() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        output_dir = root / "artifacts" / "portable-build"
        output_dir.mkdir(parents=True, exist_ok=True)
        for name in smoke.PORTABLE_BUILD_SCRATCH_NAMES:
            candidate = output_dir.parent / name
            candidate.mkdir(parents=True, exist_ok=True)
            (candidate / "marker.txt").write_text("x", encoding="utf-8")
        with mock.patch.object(smoke.subprocess, "run") as run_mock:
            exe_path = smoke.run_portable_build(output_dir)
        assert exe_path == output_dir / "Baluffo.exe"
        run_mock.assert_called_once()
        command = run_mock.call_args.args[0]
        assert "--bundle-version" in command
        assert (
            command[command.index("--bundle-version") + 1]
            == smoke.expected_portable_build_version()
        )
        assert "--force" not in command
        for name in smoke.PORTABLE_BUILD_SCRATCH_NAMES:
            assert not (output_dir.parent / name).exists()

        with mock.patch.object(smoke.subprocess, "run") as forced_run_mock:
            smoke.run_portable_build(output_dir, force=True)
        forced_command = forced_run_mock.call_args.args[0]
        assert "--force" in forced_command


def test_prune_packaged_smoke_artifacts_keeps_recent_runs_and_current_dir() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp) / "packaged-desktop-smoke"
        root.mkdir(parents=True, exist_ok=True)
        current_dir = root / "20260416-120003"
        retained_dir = root / "20260416-120002"
        stale_dir_a = root / "20260416-120001"
        stale_dir_b = root / "20260416-120000"
        old_file = root / "jobs-pipeline-report.json"
        for path in (current_dir, retained_dir, stale_dir_a, stale_dir_b):
            path.mkdir(parents=True, exist_ok=True)
            (path / "marker.txt").write_text(path.name, encoding="utf-8")
        old_file.write_text("{}", encoding="utf-8")
        os.utime(retained_dir, (200.0, 200.0))
        os.utime(stale_dir_a, (100.0, 100.0))
        os.utime(stale_dir_b, (50.0, 50.0))
        os.utime(old_file, (10.0, 10.0))
        with mock.patch.object(smoke.time, "time", return_value=10_000.0):
            removed = smoke.prune_packaged_smoke_artifacts(
                root,
                keep_recent_runs=2,
                file_retention_s=60,
                current_artifacts_dir=current_dir,
            )
        assert current_dir.exists()
        assert retained_dir.exists()
        assert not stale_dir_a.exists()
        assert not stale_dir_b.exists()
        assert not old_file.exists()
        assert {path.name for path in removed} == {
            stale_dir_a.name,
            stale_dir_b.name,
            old_file.name,
        }


def test_generate_packaged_smoke_run_token_is_collision_safe_with_entropy() -> None:
    now = smoke.datetime(2026, 4, 16, 12, 0, 0, 123456, tzinfo=smoke.UTC)
    first = smoke.generate_packaged_smoke_run_token(now=now, entropy_ns=101)
    second = smoke.generate_packaged_smoke_run_token(now=now, entropy_ns=202)

    assert first != second
    assert first.startswith("20260416-120000-123456-")
    assert second.startswith("20260416-120000-123456-")


def test_select_startup_probe_browser_prefers_chrome_then_brave_then_edge() -> None:
    candidates = [
        {"name": "chrome", "path": "C:/Chrome/chrome.exe"},
        {"name": "brave", "path": "C:/Brave/brave.exe"},
        {"name": "msedge", "path": "C:/Edge/msedge.exe"},
    ]
    with (
        mock.patch.object(
            smoke.desktop_app_mod,
            "resolve_chromium_browser_candidates",
            return_value=candidates,
        ),
        mock.patch.object(smoke.desktop_app_mod, "chromium_app_mode_supported", return_value=True),
    ):
        selected = smoke.select_startup_probe_browser({})

    assert selected == {
        "browserName": "chrome",
        "browserPath": "C:/Chrome/chrome.exe",
    }


def test_select_startup_probe_browser_prefers_explicit_browser_path() -> None:
    with mock.patch.object(
        smoke.desktop_app_mod,
        "chromium_app_mode_supported",
        return_value=True,
    ):
        selected = smoke.select_startup_probe_browser(
            {smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: "C:/Playwright/chrome.exe"}
        )

    assert selected == {
        "browserName": "chrome",
        "browserPath": "C:/Playwright/chrome.exe",
    }


def test_resolve_playwright_chromium_uses_host_env_when_runtime_localappdata_is_isolated() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        chromium_path = root / "ms-playwright" / "chromium" / "chrome.exe"
        chromium_path.parent.mkdir(parents=True)
        chromium_path.write_text("chrome", encoding="utf-8")
        completed = smoke.subprocess.CompletedProcess(
            ["node"],
            0,
            stdout=f"{chromium_path}\n",
            stderr="",
        )
        smoke.packaged_smoke_build_env_mod._PLAYWRIGHT_CHROMIUM_PATH_CACHE = None
        with (
            mock.patch.object(smoke, "resolve_node_command", return_value=["node"]),
            mock.patch.object(smoke.subprocess, "run", return_value=completed) as run_mock,
            mock.patch.dict(smoke.os.environ, {"LOCALAPPDATA": "C:/host-localappdata"}),
        ):
            resolved = smoke.resolve_playwright_chromium_executable(
                {"LOCALAPPDATA": "C:/isolated-runtime-localappdata"}
            )

    assert resolved == str(chromium_path.resolve())
    assert run_mock.call_args.kwargs["env"]["LOCALAPPDATA"] == "C:/host-localappdata"


def test_select_startup_probe_browser_uses_edge_only_when_other_candidates_unavailable() -> None:
    candidates = [
        {"name": "chrome", "path": "C:/Chrome/chrome.exe"},
        {"name": "brave", "path": "C:/Brave/brave.exe"},
        {"name": "msedge", "path": "C:/Edge/msedge.exe"},
    ]

    def fake_supported(candidate, env=None):  # noqa: ANN001, ANN202
        return str(candidate.get("name")) == "msedge"

    with (
        mock.patch.object(
            smoke.desktop_app_mod,
            "resolve_chromium_browser_candidates",
            return_value=candidates,
        ),
        mock.patch.object(
            smoke.desktop_app_mod,
            "chromium_app_mode_supported",
            side_effect=fake_supported,
        ),
    ):
        selected = smoke.select_startup_probe_browser({"BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE": "1"})

    assert selected == {
        "browserName": "msedge",
        "browserPath": "C:/Edge/msedge.exe",
    }


def test_select_startup_probe_browser_fails_when_no_supported_candidate_exists() -> None:
    with (
        mock.patch.object(
            smoke.desktop_app_mod,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "msedge", "path": "C:/Edge/msedge.exe"}],
        ),
        mock.patch.object(smoke.desktop_app_mod, "chromium_app_mode_supported", return_value=False),
    ):
        with pytest.raises(
            RuntimeError, match="No supported managed Chromium probe browser available"
        ):
            smoke.select_startup_probe_browser({})


def test_ensure_portable_exe_rebuilds_default_dist_when_fingerprint_is_stale() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        fake_default = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
        fake_default.parent.mkdir(parents=True, exist_ok=True)
        fake_default.write_text("old", encoding="utf-8")
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", fake_default),
            mock.patch.object(
                smoke,
                "portable_build_status",
                return_value={"fresh": False, "status": "stale"},
            ),
            mock.patch.object(smoke, "run_portable_build", return_value=fake_default) as build_mock,
        ):
            smoke.ensure_portable_exe(fake_default, rebuild=False)
        build_mock.assert_called_once_with(None, force=False)


def test_default_portable_exe_becomes_stale_when_fingerprint_is_not_current() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        fake_default = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
        fake_default.parent.mkdir(parents=True, exist_ok=True)
        fake_default.write_text("old", encoding="utf-8")

        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", fake_default),
            mock.patch.object(
                smoke,
                "portable_build_status",
                return_value={"fresh": False, "status": "stale"},
            ),
        ):
            assert smoke._default_portable_exe_stale(fake_default) is True


def test_ensure_portable_exe_honors_explicit_path_without_rebuilding() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        explicit_exe = Path(tmp) / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        explicit_exe.parent.mkdir(parents=True, exist_ok=True)
        explicit_exe.write_text("exe", encoding="utf-8")
        with mock.patch.object(smoke, "run_portable_build") as build_mock:
            resolved = smoke.ensure_portable_exe(explicit_exe, rebuild=True)
        assert resolved == explicit_exe.resolve()
        build_mock.assert_not_called()


def test_ensure_portable_exe_rejects_missing_explicit_path_instead_of_building_default() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        explicit_exe = Path(tmp) / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        with mock.patch.object(smoke, "run_portable_build") as build_mock:
            with pytest.raises(RuntimeError, match="Packaged desktop executable not found"):
                smoke.ensure_portable_exe(explicit_exe, rebuild=True)
        build_mock.assert_not_called()


def test_parse_packaged_node_smoke_report_reads_scenarios() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        report_path = Path(tmp) / "smoke-report.json"
        report_path.write_text(
            json.dumps(
                {
                    "ok": False,
                    "scenarios": [
                        {
                            "name": "Jobs startup",
                            "status": "passed",
                            "durationMs": 1200,
                            "error": "",
                        },
                        {
                            "name": "Admin action",
                            "status": "failed",
                            "durationMs": 500,
                            "error": "unlock failed",
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        rows = smoke.parse_packaged_node_smoke_report(report_path)
        assert len(rows) == 2
        assert rows[0]["name"] == "Jobs startup"
        assert rows[1]["error"] == "unlock failed"


@pytest.mark.windows
def test_collect_packaged_smoke_env_diagnostics_reports_paths_and_elevation() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        exe_path.parent.mkdir(parents=True, exist_ok=True)
        exe_path.write_text("exe", encoding="utf-8")
        env = {"TMP": str(root / "tmp"), "TEMP": str(root / "temp")}
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=True),
            mock.patch.object(
                smoke,
                "portable_build_status",
                return_value={
                    "status": "fresh",
                    "expectedFingerprint": "expected-fingerprint",
                    "actualFingerprint": "actual-fingerprint",
                },
            ),
            mock.patch.object(
                smoke,
                "read_portable_build_provenance",
                return_value={
                    "fingerprint": "actual-fingerprint",
                    "cacheStatus": "hit",
                },
            ),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=exe_path,
                exe_path=exe_path,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                node_command=["C:/Program Files/nodejs/node.exe"],
                env=env,
            )
        assert diagnostics["requestedExePath"] == str(exe_path.resolve())
        assert diagnostics["defaultExePath"] == str(exe_path.resolve())
        assert diagnostics["exePathMode"] == "default-dist"
        assert diagnostics["exePathSource"] == "default-dist"
        assert diagnostics["explicitExePathFreshness"] == "n/a"
        assert diagnostics["rebuiltPortableExe"] is False
        assert diagnostics["portableBuildFingerprint"] == "actual-fingerprint"
        assert diagnostics["portableBuildCacheStatus"] == "hit"
        assert diagnostics["portableBuildFreshness"] == "fresh"
        assert diagnostics["portableBuildExpectedFingerprint"] == "expected-fingerprint"
        assert diagnostics["portableBuildActualFingerprint"] == "actual-fingerprint"
        assert diagnostics["artifactsDirWritable"]
        assert diagnostics["exeParentWritable"]
        assert diagnostics["nodePath"] == "C:/Program Files/nodejs/node.exe"
        assert diagnostics["tmp"] == str(root / "tmp")
        assert diagnostics["temp"] == str(root / "temp")
        assert diagnostics["isElevated"]


@pytest.mark.windows
def test_collect_packaged_smoke_env_diagnostics_reports_explicit_path_freshness() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        explicit_exe = root / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        explicit_exe.parent.mkdir(parents=True, exist_ok=True)
        explicit_exe.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "_portable_exe_marker_staleness", return_value="stale"),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=False),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=explicit_exe,
                exe_path=explicit_exe,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                node_command=["node"],
                env={},
            )
        assert diagnostics["exePathMode"] == "explicit-path"
        assert diagnostics["exePathSource"] == "explicit-path"
        assert diagnostics["explicitExePathFreshness"] == "stale"
        assert diagnostics["rebuiltPortableExe"] is False


@pytest.mark.windows
def test_collect_packaged_smoke_env_diagnostics_reports_rebuilt_default_dist() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        exe_path.parent.mkdir(parents=True, exist_ok=True)
        exe_path.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=False),
            mock.patch.object(
                smoke,
                "portable_build_status",
                return_value={
                    "status": "fresh",
                    "expectedFingerprint": "expected-fingerprint",
                    "actualFingerprint": "actual-fingerprint",
                },
            ),
            mock.patch.object(
                smoke,
                "read_portable_build_provenance",
                return_value={
                    "fingerprint": "actual-fingerprint",
                    "cacheStatus": "miss",
                },
            ),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=exe_path,
                exe_path=exe_path,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                rebuilt_portable_dir=root / "artifacts" / "portable-build",
                node_command=["node"],
                env={},
            )
        assert diagnostics["exePathMode"] == "default-dist"
        assert diagnostics["exePathSource"] == "rebuilt-dist"
        assert diagnostics["explicitExePathFreshness"] == "n/a"
        assert diagnostics["rebuiltPortableExe"] is True
        assert diagnostics["portableBuildCacheStatus"] == "miss"


def test_packaged_pipeline_smoke_mode_is_enabled_for_pipeline_rehearsal_scripts() -> None:
    assert (
        smoke.packaged_pipeline_smoke_mode(smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT) == "stub-success"
    )
    assert (
        smoke.packaged_pipeline_smoke_mode(smoke.TASK_ABORT_SCHEDULE_NODE_SMOKE_SCRIPT)
        == "stub-success"
    )
    assert smoke.packaged_pipeline_smoke_mode(smoke.DEFAULT_NODE_SMOKE_SCRIPT) == ""
    assert smoke.packaged_runtime_env_overrides(smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT) == {
        "BALUFFO_PACKAGED_SMOKE_FETCH_MODE": "source-runs",
        "BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE": "stub-success",
    }
    assert smoke.packaged_runtime_env_overrides(smoke.TASK_ABORT_SCHEDULE_NODE_SMOKE_SCRIPT) == {
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_DELAY_MS": "12000",
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_HEARTBEAT_MS": "1000",
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE": "controlled-heartbeat-success",
        "BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE": "stub-success",
        "BALUFFO_PACKAGED_SMOKE_RUNTIME": "1",
    }
    assert smoke.packaged_runtime_env_overrides(smoke.DEFAULT_NODE_SMOKE_SCRIPT) == {}


def test_packaged_first_run_bootstrap_smoke_mode_is_script_scoped() -> None:
    admin_active_run_script = smoke.SMOKE_DIR / "packaged-desktop-smoke.admin-active-run.mjs"
    assert (
        smoke.packaged_bootstrap_smoke_mode(smoke.FIRST_RUN_JOBS_NODE_SMOKE_SCRIPT)
        == "controlled-heartbeat-success"
    )
    assert (
        smoke.packaged_bootstrap_smoke_mode(smoke.TASK_ABORT_SCHEDULE_NODE_SMOKE_SCRIPT)
        == "controlled-heartbeat-success"
    )
    assert (
        smoke.packaged_bootstrap_smoke_mode(admin_active_run_script)
        == "controlled-heartbeat-success"
    )
    assert smoke.packaged_bootstrap_smoke_mode(smoke.DEFAULT_NODE_SMOKE_SCRIPT) == ""
    assert smoke.packaged_runtime_env_overrides(smoke.FIRST_RUN_JOBS_NODE_SMOKE_SCRIPT) == {
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_DELAY_MS": "12000",
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_HEARTBEAT_MS": "1000",
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE": "controlled-heartbeat-success",
        "BALUFFO_PACKAGED_SMOKE_RUNTIME": "1",
    }
    assert smoke.packaged_runtime_env_overrides(admin_active_run_script) == {
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_DELAY_MS": "12000",
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_HEARTBEAT_MS": "1000",
        "BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE": "controlled-heartbeat-success",
        "BALUFFO_PACKAGED_SMOKE_RUNTIME": "1",
    }
    assert smoke.packaged_runtime_env_overrides(smoke.DEFAULT_NODE_SMOKE_SCRIPT) == {}


def test_packaged_fetch_evidence_smoke_mode_is_deterministic_by_default() -> None:
    assert smoke.packaged_runtime_env_overrides(smoke.FETCH_EVIDENCE_NODE_SMOKE_SCRIPT) == {
        "BALUFFO_PACKAGED_SMOKE_FETCH_MODE": "source-runs",
    }
    assert (
        smoke.packaged_runtime_env_overrides(
            smoke.FETCH_EVIDENCE_NODE_SMOKE_SCRIPT,
            fetch_evidence_mode="real",
        )
        == {}
    )


def test_packaged_runtime_env_overrides_can_isolate_appdata_per_run() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        artifacts_dir = Path(tmp) / "artifacts"
        overrides = smoke.packaged_runtime_env_overrides(
            smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT,
            artifacts_dir=artifacts_dir,
            session_scope="jobs-pipeline",
        )

        assert overrides["BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE"] == "stub-success"
        assert overrides["BALUFFO_PACKAGED_SMOKE_FETCH_MODE"] == "source-runs"
        assert Path(overrides["APPDATA"]).resolve() == (
            smoke.packaged_desktop_roaming_appdata_root(
                artifacts_dir, session_scope="jobs-pipeline"
            ).resolve()
        )
        assert Path(overrides["LOCALAPPDATA"]).resolve() == (
            smoke.packaged_desktop_local_appdata_root(
                artifacts_dir, session_scope="jobs-pipeline"
            ).resolve()
        )


def test_packaged_runtime_env_overrides_sets_startup_profile_mode_for_probes() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        with mock.patch.object(smoke, "preferred_packaged_desktop_browser_env", return_value={}):
            overrides = smoke.packaged_runtime_env_overrides(
                artifacts_dir=Path(tmp) / "artifacts",
                startup_probe=True,
                profile_mode="warm",
            )

        assert overrides["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] == "1"
        assert overrides[smoke.desktop_app_mod.STARTUP_PROFILE_MODE_ENV] == "warm"


def test_packaged_runtime_env_overrides_prefers_playwright_chromium_for_probes() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        browser_path = str(Path(tmp) / "chromium" / "chrome.exe")
        with mock.patch.object(
            smoke,
            "preferred_packaged_desktop_browser_env",
            return_value={smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV: browser_path},
        ):
            overrides = smoke.packaged_runtime_env_overrides(
                artifacts_dir=Path(tmp) / "artifacts",
                startup_probe=True,
            )

    assert overrides[smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV] == browser_path


def test_classify_subprocess_error_marks_spawn_eperm() -> None:
    error = PermissionError("spawn EPERM")
    assert smoke.classify_subprocess_error(error) == "node_process_spawn_blocked"
    assert (
        smoke.classify_subprocess_error("Error: spawn EPERM") == "playwright_worker_spawn_blocked"
    )
    assert (
        smoke.classify_subprocess_error("browserType.launch: spawn EPERM")
        == "node_process_spawn_blocked"
    )
