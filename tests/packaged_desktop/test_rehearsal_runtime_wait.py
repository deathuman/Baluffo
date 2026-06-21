"""Packaged desktop rehearsal tests for runtime wait."""

from ._rehearsal_shared import (
    ADMIN_BRIDGE_TEST_PORT,
    Path,
    json,
    mock,
    os,
    pytest,
    smoke,
    subprocess,
    workspace_tmpdir,
)

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_wait_for_packaged_runtime_with_port_pivot_prefers_env_scoped_session_root() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        expected_data_dir = root / "run-data"
        expected_data_dir.mkdir(parents=True, exist_ok=True)
        global_env = {"LOCALAPPDATA": str(root / "global-localappdata")}
        global_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(global_env)
        global_session_root.mkdir(parents=True, exist_ok=True)
        (global_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"sitePort": 7001, "bridgePort": 7002, "dataDir": str(root / "wrong-data")}),
            encoding="utf-8",
        )
        run_env = {"LOCALAPPDATA": str(root / "run-localappdata")}
        run_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(run_env)
        run_session_root.mkdir(parents=True, exist_ok=True)
        (run_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"sitePort": 9001, "bridgePort": 9002, "dataDir": str(expected_data_dir)}),
            encoding="utf-8",
        )
        process = mock.Mock(spec=subprocess.Popen)
        process.poll.return_value = None

        def fake_fetch_json(url: str, timeout_s: float = 10.0):  # noqa: ANN001
            if url == "http://127.0.0.1:9002/ops/health":
                return {"desktopMode": True, "startupReady": True}
            if url == "http://127.0.0.1:9002/desktop-local-data/session":
                return {"sitePort": 9001, "bridgePort": 9002}
            raise AssertionError(f"unexpected url {url}")

        with (
            mock.patch.dict(os.environ, global_env, clear=False),
            mock.patch.object(smoke, "fetch_json", side_effect=fake_fetch_json) as fetch_mock,
            mock.patch.object(
                smoke,
                "fetch_startup_metrics",
                return_value=[
                    {"event": "desktop_browser_process_spawn_started"},
                    {
                        "event": "desktop_browser_launch_selected",
                        "fields": {"mode": "chromium-app"},
                    },
                    {"event": "desktop_runtime_port_retry"},
                ],
            ),
            mock.patch.object(smoke, "_packaged_runtime_page_ready", return_value=True),
            mock.patch.object(smoke, "_required_startup_event_present", return_value=True),
        ):
            runtime_state = smoke.wait_for_packaged_runtime_with_port_pivot(
                process,
                requested_site_port=8080,
                requested_bridge_port=ADMIN_BRIDGE_TEST_PORT,
                expected_data_dir=expected_data_dir,
                timeout_s=0.2,
                env=run_env,
            )

        assert runtime_state["actualSitePort"] == 9001
        assert runtime_state["actualBridgePort"] == 9002
        assert runtime_state["portRetryObserved"] is True
        fetch_mock.assert_any_call("http://127.0.0.1:9002/ops/health", timeout_s=1.0)


def test_wait_for_packaged_runtime_with_port_pivot_tolerates_optional_status_fetch_errors() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        expected_data_dir = root / "run-data"
        expected_data_dir.mkdir(parents=True, exist_ok=True)
        run_env = {"LOCALAPPDATA": str(root / "run-localappdata")}
        run_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(run_env)
        run_session_root.mkdir(parents=True, exist_ok=True)
        (run_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"sitePort": 9001, "bridgePort": 9002, "dataDir": str(expected_data_dir)}),
            encoding="utf-8",
        )
        process = mock.Mock(spec=subprocess.Popen)
        process.poll.return_value = None
        rows = [{"event": "desktop_runtime_port_retry"}]

        with (
            mock.patch.object(
                smoke,
                "fetch_json",
                side_effect=[OSError("health reset"), ValueError("session malformed")],
            ),
            mock.patch.object(smoke, "fetch_startup_metrics", return_value=rows),
            mock.patch.object(smoke, "_packaged_runtime_page_ready", return_value=True),
        ):
            runtime_state = smoke.wait_for_packaged_runtime_with_port_pivot(
                process,
                requested_site_port=8080,
                requested_bridge_port=ADMIN_BRIDGE_TEST_PORT,
                expected_data_dir=expected_data_dir,
                timeout_s=0.2,
                required_events=("desktop_runtime_port_retry",),
                env=run_env,
            )

    assert runtime_state["health"] == {}
    assert runtime_state["session"] == {}
    assert runtime_state["actualSitePort"] == 9001
    assert runtime_state["actualBridgePort"] == 9002


def test_wait_for_relaunched_runtime_retries_expected_health_poll_failure() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        expected_data_dir = root / "data"
        expected_data_dir.mkdir(parents=True, exist_ok=True)
        env = {"LOCALAPPDATA": str(root / "localappdata")}
        session_root = smoke.desktop_update_mod.resolve_desktop_session_root(env)
        session_root.mkdir(parents=True, exist_ok=True)
        (session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"bridgePort": 4567, "sitePort": 4568, "dataDir": str(expected_data_dir)}),
            encoding="utf-8",
        )

        fetch_results = [
            OSError("bridge not ready"),
            {"desktopMode": True, "startupReady": True, "appVersion": "1.2.3"},
        ]

        def fake_fetch_json(url: str, timeout_s: float = 5.0):  # noqa: ANN001
            assert url == "http://127.0.0.1:4567/ops/health"
            result = fetch_results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        with (
            mock.patch.object(smoke, "fetch_json", side_effect=fake_fetch_json) as fetch_mock,
            mock.patch.object(smoke.time, "sleep", return_value=None),
        ):
            relaunched = smoke._wait_for_relaunched_runtime(
                expected_data_dir=expected_data_dir,
                expected_version="1.2.3",
                timeout_s=0.1,
                env=env,
            )

        assert relaunched["session"]["bridgePort"] == 4567
        assert relaunched["health"]["startupReady"] is True
        assert fetch_mock.call_count == 2


def test_wait_for_relaunched_runtime_does_not_swallow_unexpected_health_bug() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        expected_data_dir = root / "data"
        expected_data_dir.mkdir(parents=True, exist_ok=True)
        env = {"LOCALAPPDATA": str(root / "localappdata")}
        session_root = smoke.desktop_update_mod.resolve_desktop_session_root(env)
        session_root.mkdir(parents=True, exist_ok=True)
        (session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"bridgePort": 4567, "sitePort": 4568, "dataDir": str(expected_data_dir)}),
            encoding="utf-8",
        )

        with pytest.raises(RuntimeError, match="unexpected health bug"):
            with mock.patch.object(
                smoke,
                "fetch_json",
                side_effect=RuntimeError("unexpected health bug"),
            ):
                smoke._wait_for_relaunched_runtime(
                    expected_data_dir=expected_data_dir,
                    expected_version="1.2.3",
                    timeout_s=0.1,
                    env=env,
                )
