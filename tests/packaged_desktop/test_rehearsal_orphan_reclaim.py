"""Packaged desktop rehearsal tests for orphan reclaim."""

from ._rehearsal_shared import (
    Path,
    json,
    mock,
    pytest,
    smoke,
    subprocess,
    workspace_tmpdir,
)

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


@pytest.mark.windows
def test_run_packaged_orphan_reclaim_rehearsal_passes_when_metrics_prove_reclaim() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        stale_site_process = mock.Mock(spec=subprocess.Popen)
        stale_site_process.pid = 101
        stale_bridge_process = mock.Mock(spec=subprocess.Popen)
        stale_bridge_process.pid = 202
        runtime_process = mock.Mock(spec=subprocess.Popen)
        handle_a = mock.Mock()
        handle_b = mock.Mock()
        handle_c = mock.Mock()
        handle_d = mock.Mock()
        handle_e = mock.Mock()
        handle_f = mock.Mock()

        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_desktop_child",
                side_effect=[
                    (stale_site_process, handle_a, handle_b),
                    (stale_bridge_process, handle_c, handle_d),
                ],
            ),
            mock.patch.object(smoke, "wait_for_packaged_child_runtime"),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, handle_e, handle_f),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 8080,
                    "actualBridgePort": 8877,
                    "portRetryObserved": False,
                    "startupMetrics": [
                        {"event": "desktop_stale_runtime_reclaim_started"},
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "bridge", "outcome": "killed"},
                        },
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "site", "outcome": "killed"},
                        },
                    ],
                },
            ),
            mock.patch.object(smoke, "_wait_for_process_exit") as wait_exit_mock,
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_orphan_reclaim_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "passed"
    assert payload["slug"] == "packaged-orphan-reclaim-rehearsal"
    assert payload["details"]["actualSitePort"] == 8080
    assert payload["details"]["actualBridgePort"] == 8877
    assert payload["details"]["portRetryObserved"] is False
    wait_exit_mock.assert_any_call(stale_site_process, timeout_s=15.0)
    wait_exit_mock.assert_any_call(stale_bridge_process, timeout_s=15.0)


@pytest.mark.windows
def test_run_packaged_orphan_reclaim_rehearsal_fails_when_site_reclaim_metric_is_missing() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        stale_site_process = mock.Mock(spec=subprocess.Popen)
        stale_site_process.pid = 101
        stale_bridge_process = mock.Mock(spec=subprocess.Popen)
        stale_bridge_process.pid = 202
        runtime_process = mock.Mock(spec=subprocess.Popen)
        handle_a = mock.Mock()
        handle_b = mock.Mock()
        handle_c = mock.Mock()
        handle_d = mock.Mock()
        handle_e = mock.Mock()
        handle_f = mock.Mock()

        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_desktop_child",
                side_effect=[
                    (stale_site_process, handle_a, handle_b),
                    (stale_bridge_process, handle_c, handle_d),
                ],
            ),
            mock.patch.object(smoke, "wait_for_packaged_child_runtime"),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, handle_e, handle_f),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 8080,
                    "actualBridgePort": 8877,
                    "portRetryObserved": False,
                    "startupMetrics": [
                        {"event": "desktop_stale_runtime_reclaim_started"},
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "bridge", "outcome": "killed"},
                        },
                    ],
                },
            ),
            mock.patch.object(smoke, "_wait_for_process_exit"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_orphan_reclaim_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "failed"
    assert "site reclaim" in payload["error"]


@pytest.mark.windows
def test_run_packaged_orphan_reclaim_rehearsal_fails_when_runtime_retries_ports() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        stale_site_process = mock.Mock(spec=subprocess.Popen)
        stale_site_process.pid = 101
        stale_bridge_process = mock.Mock(spec=subprocess.Popen)
        stale_bridge_process.pid = 202
        runtime_process = mock.Mock(spec=subprocess.Popen)
        handle_a = mock.Mock()
        handle_b = mock.Mock()
        handle_c = mock.Mock()
        handle_d = mock.Mock()
        handle_e = mock.Mock()
        handle_f = mock.Mock()

        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[8080, 8877]),
            mock.patch.object(
                smoke,
                "launch_packaged_desktop_child",
                side_effect=[
                    (stale_site_process, handle_a, handle_b),
                    (stale_bridge_process, handle_c, handle_d),
                ],
            ),
            mock.patch.object(smoke, "wait_for_packaged_child_runtime"),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(runtime_process, handle_e, handle_f),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime_with_port_pivot",
                return_value={
                    "actualSitePort": 9001,
                    "actualBridgePort": 9002,
                    "portRetryObserved": True,
                    "startupMetrics": [
                        {"event": "desktop_stale_runtime_reclaim_started"},
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "bridge", "outcome": "killed"},
                        },
                        {
                            "event": "desktop_stale_runtime_reclaim_result",
                            "fields": {"target": "site", "outcome": "killed"},
                        },
                    ],
                },
            ),
            mock.patch.object(smoke, "_wait_for_process_exit"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_orphan_reclaim_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

    assert payload["status"] == "failed"
    assert "requested ports" in payload["error"]


def test_run_packaged_smoke_can_run_orphan_reclaim_rehearsal_mode() -> None:
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
                "--orphan-reclaim-rehearsal",
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
                "run_packaged_orphan_reclaim_rehearsal",
                return_value={
                    "name": "Packaged orphan reclaim rehearsal",
                    "slug": "packaged-orphan-reclaim-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "runtimeStdout": str(artifacts_dir / "orphan-runtime.stdout.log"),
                        "runtimeStderr": str(artifacts_dir / "orphan-runtime.stderr.log"),
                        "staleSiteStdout": str(artifacts_dir / "orphan-site.stdout.log"),
                        "staleSiteStderr": str(artifacts_dir / "orphan-site.stderr.log"),
                        "staleBridgeStdout": str(artifacts_dir / "orphan-bridge.stdout.log"),
                        "staleBridgeStderr": str(artifacts_dir / "orphan-bridge.stderr.log"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "packaged-orphan-reclaim-rehearsal"
        assert payload["artifacts"]["orphanRehearsalRuntimeStdout"] == str(
            artifacts_dir / "orphan-runtime.stdout.log"
        )
        assert payload["artifacts"]["orphanRehearsalSiteStdout"] == str(
            artifacts_dir / "orphan-site.stdout.log"
        )
        assert payload["artifacts"]["orphanRehearsalBridgeStdout"] == str(
            artifacts_dir / "orphan-bridge.stdout.log"
        )
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True
