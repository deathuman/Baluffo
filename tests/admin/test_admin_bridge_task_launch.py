from dataclasses import dataclass
from unittest import mock

import pytest

from src import admin_bridge

pytestmark = pytest.mark.usefixtures("admin_bridge_entrypoint_root")


@dataclass(frozen=True)
class _FetcherArgsCase:
    name: str
    payload: dict[str, object]
    expected_preset: str
    report_payload: dict[str, object] | None = None
    expected_present: tuple[str, ...] = ()
    expected_absent: tuple[str, ...] = ()
    expected_values: tuple[tuple[str, str], ...] = ()


def _run_fetcher_args_case(case: _FetcherArgsCase) -> None:
    if case.report_payload is not None:
        admin_bridge.save_json_atomic(
            admin_bridge.JOBS_FETCH_REPORT_PATH,
            case.report_payload,
        )
    args, preset = admin_bridge.build_fetcher_args_from_payload(case.payload)
    assert preset == case.expected_preset
    for option in case.expected_present:
        assert option in args
    for option in case.expected_absent:
        assert option not in args
    for option, value in case.expected_values:
        assert option in args
        assert args[args.index(option) + 1] == value


FETCHER_ARGS_CASES = [
    pytest.param(
        _FetcherArgsCase(
            name="retry-failed-filters-unknown",
            payload={"preset": "retry_failed"},
            expected_preset="retry_failed",
            report_payload={
                "startedAt": "2026-03-01T00:00:00+00:00",
                "finishedAt": "2026-03-01T00:10:00+00:00",
                "summary": {"failedSources": 3, "sourceCount": 4},
                "sources": [
                    {"name": "remote_ok", "status": "error"},
                    {"name": "unknown_custom_source", "status": "error"},
                    {"name": "google_sheets", "status": "error"},
                    {"name": "remote_ok", "status": "error"},
                ],
            },
            expected_present=(
                "--only-sources",
                "--ignore-circuit-breaker",
                "--fetch-strategy",
                "--adapter-http-concurrency",
            ),
            expected_absent=("--quiet",),
            expected_values=(("--only-sources", "google_sheets,remote_ok"),),
        ),
        id="retry-failed-filters-unknown",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="retry-failed-without-known-failures",
            payload={"preset": "retry_failed"},
            expected_preset="retry_failed",
            report_payload={
                "sources": [
                    {"name": "unknown_custom_source_a", "status": "error"},
                    {"name": "unknown_custom_source_b", "status": "error"},
                ]
            },
            expected_present=("--ignore-circuit-breaker",),
            expected_absent=("--only-sources", "--quiet"),
        ),
        id="retry-failed-without-known-failures",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="cadence-and-strategy-overrides",
            payload={
                "preset": "default",
                "fetchStrategy": "http",
                "adapterHttpConcurrency": 48,
                "respectSourceCadence": True,
                "hotSourceCadenceMinutes": 20,
                "coldSourceCadenceMinutes": 90,
            },
            expected_preset="default",
            expected_present=(
                "--fetch-strategy",
                "--adapter-http-concurrency",
                "--respect-source-cadence",
                "--hot-source-cadence-minutes",
                "--cold-source-cadence-minutes",
            ),
            expected_values=(
                ("--fetch-strategy", "http"),
                ("--adapter-http-concurrency", "48"),
                ("--hot-source-cadence-minutes", "20"),
                ("--cold-source-cadence-minutes", "90"),
            ),
        ),
        id="cadence-and-strategy-overrides",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="social-enabled-default",
            payload={"preset": "default"},
            expected_preset="default",
            expected_present=(
                "--social-enabled",
                "--max-workers",
                "--max-per-domain",
                "--adapter-http-concurrency",
            ),
            expected_values=(
                ("--max-workers", "12"),
                ("--max-per-domain", "3"),
                ("--adapter-http-concurrency", "48"),
            ),
        ),
        id="social-enabled-default",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="social-opt-out",
            payload={"preset": "default", "socialEnabled": False},
            expected_preset="default",
            expected_absent=("--social-enabled",),
        ),
        id="social-opt-out",
    ),
    pytest.param(
        _FetcherArgsCase(
            name="uncapped-keeps-social",
            payload={"preset": "uncapped"},
            expected_preset="uncapped",
            expected_present=(
                "--force-refresh-all",
                "--ignore-circuit-breaker",
                "--max-workers",
                "--max-per-domain",
                "--static-detail-concurrency",
                "--source-ttl-minutes",
                "--fetch-strategy",
                "--adapter-http-concurrency",
                "--circuit-breaker-failures",
                "--circuit-breaker-cooldown-minutes",
                "--browser-fallback-cooldown-minutes",
                "--hot-source-cadence-minutes",
                "--cold-source-cadence-minutes",
                "--social-enabled",
            ),
            expected_values=(
                ("--max-workers", "50"),
                ("--max-per-domain", "5"),
                ("--static-detail-concurrency", "10"),
                ("--source-ttl-minutes", "0"),
                ("--fetch-strategy", "auto"),
                ("--adapter-http-concurrency", "48"),
                ("--circuit-breaker-failures", "3"),
                ("--circuit-breaker-cooldown-minutes", "180"),
                ("--browser-fallback-cooldown-minutes", "30"),
                ("--hot-source-cadence-minutes", "15"),
                ("--cold-source-cadence-minutes", "60"),
            ),
        ),
        id="uncapped-keeps-social",
    ),
]


@pytest.mark.parametrize("case", FETCHER_ARGS_CASES, ids=lambda case: case.name)
def test_build_fetcher_args_matrix(case: _FetcherArgsCase) -> None:
    _run_fetcher_args_case(case)


def _configure_background_script_runtime(
    admin_bridge_entrypoint_root, *, desktop_mode: bool
) -> None:
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=True,
        desktop_mode=desktop_mode,
    )
    admin_bridge.configure_runtime_paths(cfg)


@dataclass(frozen=True)
class _BackgroundScriptCommandCase:
    name: str
    frozen: bool
    executable: str
    desktop_mode: bool
    fake_pid: int
    expected_prefix: tuple[str, ...]
    expected_script_arg: str | None = None
    expected_python_script_path: bool = False
    expect_unbuffered_env: bool = False


BACKGROUND_SCRIPT_COMMAND_CASES = [
    pytest.param(
        _BackgroundScriptCommandCase(
            name="frozen-child-script",
            frozen=True,
            executable="C:/tmp/Baluffo.exe",
            desktop_mode=True,
            fake_pid=12345,
            expected_prefix=("C:/tmp/Baluffo.exe", "__child_script__", "--root"),
            expected_script_arg="source_discovery.py",
        ),
        id="frozen-child-script",
    ),
    pytest.param(
        _BackgroundScriptCommandCase(
            name="unbuffered-python",
            frozen=False,
            executable="C:/Python313/python.exe",
            desktop_mode=False,
            fake_pid=24680,
            expected_prefix=("C:/Python313/python.exe", "-u"),
            expected_python_script_path=True,
            expect_unbuffered_env=True,
        ),
        id="unbuffered-python",
    ),
]


@pytest.mark.parametrize("case", BACKGROUND_SCRIPT_COMMAND_CASES, ids=lambda case: case.name)
def test_run_background_script_command_shape(
    case: _BackgroundScriptCommandCase,
    admin_bridge_entrypoint_root,
) -> None:
    _configure_background_script_runtime(
        admin_bridge_entrypoint_root,
        desktop_mode=case.desktop_mode,
    )
    fake_proc = type("FakeProc", (), {"pid": case.fake_pid})()
    with (
        mock.patch.object(admin_bridge.sys, "frozen", case.frozen, create=True),
        mock.patch.object(admin_bridge.sys, "executable", case.executable),
        mock.patch.object(admin_bridge.subprocess, "Popen", return_value=fake_proc) as popen_mock,
    ):
        admin_bridge.run_background_script("source_discovery.py", ["--mode", "dynamic"])
    command = popen_mock.call_args.args[0]
    assert command[: len(case.expected_prefix)] == list(case.expected_prefix)
    if case.expected_script_arg is not None:
        assert command[:5] == [
            case.executable,
            "__child_script__",
            "--root",
            str(admin_bridge_entrypoint_root),
            "--script",
        ]
        assert case.expected_script_arg in command
    if case.expected_python_script_path:
        assert command[2] == str(admin_bridge_entrypoint_root / "src" / "source_discovery.py")
    assert command[-2:] == ["--mode", "dynamic"]
    kwargs = popen_mock.call_args.kwargs
    if case.expect_unbuffered_env:
        assert kwargs["env"]["PYTHONUNBUFFERED"] == "1"
        assert kwargs["stdout"] is kwargs["stderr"]


def test_run_background_script_does_not_write_legacy_task_state(admin_bridge_entrypoint_root):
    _configure_background_script_runtime(admin_bridge_entrypoint_root, desktop_mode=False)
    fake_proc = type("FakeProc", (), {"pid": 24680})()
    with (
        mock.patch.object(admin_bridge.sys, "frozen", False, create=True),
        mock.patch.object(admin_bridge.sys, "executable", "C:/Python313/python.exe"),
        mock.patch.object(admin_bridge.subprocess, "Popen", return_value=fake_proc),
    ):
        admin_bridge.run_background_script(
            "jobs_fetcher.py",
            [],
            extra_env={
                "BALUFFO_FETCH_RUN_ID": "fetch_state_1",
                "BALUFFO_FETCH_STARTED_AT": "2026-03-27T14:00:00+00:00",
            },
        )
    assert admin_bridge.load_json_object(admin_bridge.TASK_STATE_PATH, {}) == {}


def test_start_fetcher_task_writes_report_shell_with_run_id():
    with (
        mock.patch.object(admin_bridge, "pid_is_running", return_value=True),
        mock.patch.object(admin_bridge, "run_background_script", return_value=24680),
    ):
        result = admin_bridge.start_fetcher_task({})

    assert result["started"] is True
    run_id = str(result.get("runId") or "")
    assert run_id.startswith("fetch_")

    report = admin_bridge.load_json_object(admin_bridge.JOBS_FETCH_REPORT_PATH, {})
    assert str(report.get("runId") or "") == run_id
    assert str(report.get("startedAt") or "") == str(result.get("startedAt") or "")
    assert str(report.get("finishedAt") or "") == ""

    current_rows = admin_bridge.get_lifecycle_current_runs()
    matching = [row for row in current_rows if str(row.get("runId") or "") == run_id]
    assert len(matching) == 1
    assert str(matching[0].get("taskType") or "") == "fetch"
    assert str(matching[0].get("lifecycleStatus") or "") == "running"
    assert admin_bridge.load_run_history() == []


def test_start_fetcher_task_does_not_overwrite_fast_terminal_report():
    def fast_child_write(_script, _args, *, extra_env):
        run_id = str(extra_env.get("BALUFFO_FETCH_RUN_ID") or "")
        started_at = str(extra_env.get("BALUFFO_FETCH_STARTED_AT") or "")
        admin_bridge.save_json_atomic(
            admin_bridge.JOBS_FETCH_REPORT_PATH,
            {
                "runId": run_id,
                "startedAt": started_at,
                "finishedAt": "2026-03-27T14:00:02+00:00",
                "summary": {"outputCount": 7, "failedSources": 0, "sourceCount": 3},
            },
        )
        return 24680

    with mock.patch.object(admin_bridge, "run_background_script", side_effect=fast_child_write):
        result = admin_bridge.start_fetcher_task({})

    assert result["started"] is True
    report = admin_bridge.load_json_object(admin_bridge.JOBS_FETCH_REPORT_PATH, {})
    assert str(report.get("runId") or "") == str(result.get("runId") or "")
    assert str(report.get("finishedAt") or "") == "2026-03-27T14:00:02+00:00"
    assert int((report.get("summary") or {}).get("outputCount") or 0) == 7


def test_fetch_lifecycle_terminal_report_with_failed_sources_still_succeeds():
    api = admin_bridge._get_task_launch_api()  # noqa: SLF001
    finished: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []

    closed = api._close_fetch_lifecycle_from_report(  # noqa: SLF001
        run_id="fetch_done_1",
        normalize_fetch_report_contract=lambda payload: payload,
        load_json_object=lambda _path, _default: {
            "runId": "fetch_done_1",
            "startedAt": "2026-03-27T14:00:00+00:00",
            "finishedAt": "2026-03-27T14:20:00+00:00",
            "summary": {"outputCount": 34822, "failedSources": 313, "sourceCount": 2102},
        },
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: (
            finished.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: (
            failed.append({"runId": run_id, "taskType": task_type, **kwargs}) or {}
        ),
    )

    assert closed is True
    assert failed == []
    assert finished == [
        {
            "runId": "fetch_done_1",
            "taskType": "fetch",
            "finished_at": "2026-03-27T14:20:00+00:00",
            "terminal_reason": "completed",
            "summary": {"outputCount": 34822, "failedSources": 313, "sourceCount": 2102},
        }
    ]


def test_start_fetcher_task_spawn_failure_writes_terminal_error_report():
    with mock.patch.object(
        admin_bridge,
        "run_background_script",
        side_effect=RuntimeError("spawn denied"),
    ):
        result = admin_bridge.start_fetcher_task({})

    assert result["started"] is False
    assert str(result.get("error") or "") == "spawn denied"
    run_id = str(result.get("runId") or "")
    report = admin_bridge.load_json_object(admin_bridge.JOBS_FETCH_REPORT_PATH, {})
    assert str(report.get("runId") or "") == run_id
    assert str(report.get("finishedAt") or "")
    assert str((report.get("summary") or {}).get("error") or "") == "spawn denied"
    recent_rows = admin_bridge.get_lifecycle_recent_runs()
    matching = [row for row in recent_rows if str(row.get("runId") or "") == run_id]
    assert len(matching) == 1
    assert str(matching[0].get("taskType") or "") == "fetch"
    assert str(matching[0].get("lifecycleStatus") or "") == "failed"
    assert str((matching[0].get("summary") or {}).get("error") or "") == "spawn denied"
    assert admin_bridge.load_run_history() == []


def test_start_fetcher_task_sets_uncapped_static_budget_env():
    with mock.patch.object(admin_bridge, "run_background_script", return_value=24680) as spawn:
        result = admin_bridge.start_fetcher_task({"preset": "uncapped"})

    run_id = str(result.get("runId") or "")
    assert run_id.startswith("fetch_")
    args, kwargs = spawn.call_args
    assert args[0] == "jobs_fetcher.py"
    assert "--force-refresh-all" in args[1]
    assert kwargs["extra_env"]["BALUFFO_FETCH_RUN_ID"] == run_id
    assert kwargs["extra_env"]["BALUFFO_FETCH_SEED_EXISTING_OUTPUT"] == "1"
    assert kwargs["extra_env"]["BALUFFO_STATIC_SOURCE_TIME_BUDGET_S"] == "180"
    assert kwargs["extra_env"]["BALUFFO_STATIC_LOW_YIELD_DETAIL_CAP"] == "0"
    assert kwargs["extra_env"]["BALUFFO_STATIC_VERY_LOW_YIELD_DETAIL_CAP"] == "0"
    assert kwargs["extra_env"]["BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE"] == "broad"
    assert kwargs["extra_env"]["BALUFFO_UNCAPPED_DEEP_STATIC"] == "1"


def test_start_fetcher_task_default_preset_omits_static_budget_env():
    with mock.patch.object(admin_bridge, "run_background_script", return_value=24680) as spawn:
        admin_bridge.start_fetcher_task({})

    _args, kwargs = spawn.call_args
    assert "BALUFFO_FETCH_SEED_EXISTING_OUTPUT" not in kwargs["extra_env"]
    assert "BALUFFO_STATIC_SOURCE_TIME_BUDGET_S" not in kwargs["extra_env"]


def test_start_fetcher_task_returns_conflict_for_active_fetch():
    started_at = "2026-03-27T14:00:00+00:00"
    admin_bridge.start_lifecycle_run(
        run_id="fetch_live_1",
        task_type="fetch",
        started_at=started_at,
        owner_kind="process",
        owner_pid=111,
    )

    with (
        mock.patch.object(admin_bridge, "pid_is_running", return_value=True),
        mock.patch.object(admin_bridge, "run_background_script") as spawn,
    ):
        result = admin_bridge.start_fetcher_task({})

    assert result == {
        "started": False,
        "alreadyRunning": True,
        "task": "jobs_fetcher",
        "taskType": "fetch",
        "runId": "fetch_live_1",
        "startedAt": started_at,
        "pid": 111,
        "status": "running",
    }
    spawn.assert_not_called()
    assert admin_bridge.load_run_history() == []
    assert not admin_bridge.JOBS_FETCH_REPORT_PATH.exists()


def test_ops_routes_show_completed_for_fetch_with_failed_sources():
    """/ops/task-state and /ops/history must show succeeded/completed when a fetch
    report has failedSources > 0 but no terminal error status."""
    ops_api = admin_bridge._get_ops_api()  # noqa: SLF001
    lifecycle_service = admin_bridge._TASK_LIFECYCLE  # noqa: SLF001

    fetch_run_id = "fetch_failed_src_1"
    started = "2026-05-11T11:00:00Z"
    finished = "2026-05-11T11:20:00Z"

    # Start a lifecycle run.
    lifecycle_service.start_run(
        run_id=fetch_run_id,
        task_type="fetch",
        started_at=started,
        owner_kind="standalone",
        owner_pid=99998,
    )

    # Close out via the task launch API with a report having failedSources.
    terminal_report = {
        "runId": fetch_run_id,
        "startedAt": started,
        "finishedAt": finished,
        "status": "ok",
        "summary": {"outputCount": 1000, "failedSources": 42, "sourceCount": 200},
    }
    task_launch = admin_bridge._get_task_launch_api()  # noqa: SLF001
    task_launch._close_fetch_lifecycle_from_report(  # noqa: SLF001
        run_id=fetch_run_id,
        normalize_fetch_report_contract=lambda p: p,
        load_json_object=lambda _p, _d: terminal_report,
        finish_lifecycle_run=lifecycle_service.finish_run,
        fail_lifecycle_run=lifecycle_service.fail_run,
    )

    # Verify via /ops/history.
    history_payload = ops_api.get_lifecycle_run_history_rows()
    history_rows = [r for r in history_payload if str(r.get("runId") or "") == fetch_run_id]
    assert len(history_rows) >= 1, f"expected fetch row in history, got {history_rows}"
    row = history_rows[0]
    status = str(row.get("lifecycleStatus") or row.get("status") or "").strip().lower()
    assert status in {"completed", "succeeded", "ok"}, (
        f"expected completed/succeeded/ok, got {status}"
    )
    assert str(row.get("finishedAt") or "").strip() == finished
