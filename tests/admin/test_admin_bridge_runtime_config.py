import io
import json
from contextlib import ExitStack, redirect_stdout
from dataclasses import dataclass
from unittest import mock

import pytest

from src import admin_bridge


@dataclass(frozen=True)
class _RuntimeConfigCase:
    name: str
    cli_args: list[str]
    env: dict[str, str]
    bridge_defaults: dict[str, object] | None = None
    storage_defaults: dict[str, object] | None = None
    expected_host: str | None = None
    expected_port: int | None = None
    expected_data_dir_token: str | None = None
    expected_data_dir_suffix: str | None = None
    expected_log_format: str | None = None
    expected_log_level: str | None = None
    expected_owner_mode: str | None = None
    expected_owner_token: str | None = None
    expected_desktop_session_id: str | None = None
    expected_started_by: str | None = None
    expected_owner_idle_timeout_s: float | None = None
    expected_quiet_requests: bool | None = None


def _run_runtime_config_case(case: _RuntimeConfigCase, admin_bridge_entrypoint_root) -> None:
    entrypoint_root = str(admin_bridge_entrypoint_root.resolve())
    cli_args = [entrypoint_root if arg == "__ENTRYPOINT_ROOT__" else arg for arg in case.cli_args]
    env = {
        key: entrypoint_root if value == "__ENTRYPOINT_ROOT__" else value
        for key, value in case.env.items()
    }
    with ExitStack() as stack:
        if case.bridge_defaults is not None:
            stack.enter_context(
                mock.patch.object(
                    admin_bridge,
                    "get_bridge_defaults",
                    return_value=case.bridge_defaults,
                )
            )
        if case.storage_defaults is not None:
            translated_storage_defaults = {}
            for key, value in case.storage_defaults.items():
                if isinstance(value, str) and value.startswith("__ENTRYPOINT_ROOT__/"):
                    translated_storage_defaults[key] = (
                        admin_bridge_entrypoint_root / value.split("/", 1)[1]
                    )
                elif value == "__ENTRYPOINT_ROOT__":
                    translated_storage_defaults[key] = admin_bridge_entrypoint_root
                else:
                    translated_storage_defaults[key] = value
            stack.enter_context(
                mock.patch.object(
                    admin_bridge,
                    "get_storage_defaults",
                    return_value=translated_storage_defaults,
                )
            )
        cfg = admin_bridge.resolve_runtime_config(cli_args, env=env)

    if case.expected_host is not None:
        assert cfg.host == case.expected_host
    if case.expected_port is not None:
        assert cfg.port == case.expected_port
    if case.expected_data_dir_token == "entrypoint-root":
        assert str(cfg.data_dir) == entrypoint_root
    elif case.expected_data_dir_suffix is not None:
        assert str(cfg.data_dir) == str(
            (admin_bridge_entrypoint_root / case.expected_data_dir_suffix).resolve()
        )
    if case.expected_log_format is not None:
        assert cfg.log_format == case.expected_log_format
    if case.expected_log_level is not None:
        assert cfg.log_level == case.expected_log_level
    if case.expected_owner_mode is not None:
        assert cfg.owner_mode == case.expected_owner_mode
    if case.expected_owner_token is not None:
        assert cfg.owner_token == case.expected_owner_token
    if case.expected_desktop_session_id is not None:
        assert cfg.desktop_session_id == case.expected_desktop_session_id
    if case.expected_started_by is not None:
        assert cfg.started_by == case.expected_started_by
    if case.expected_owner_idle_timeout_s is not None:
        assert cfg.owner_idle_timeout_s == case.expected_owner_idle_timeout_s
    if case.expected_quiet_requests is not None:
        assert cfg.quiet_requests is case.expected_quiet_requests


RESOLVE_RUNTIME_CONFIG_CASES = [
    pytest.param(
        _RuntimeConfigCase(
            name="cli-env-precedence",
            cli_args=[
                "--port",
                "9001",
                "--host",
                "127.0.0.9",
                "--data-dir",
                "__ENTRYPOINT_ROOT__",
                "--log-format",
                "jsonl",
                "--log-level",
                "debug",
            ],
            env={
                "BALUFFO_BRIDGE_HOST": "1.2.3.4",
                "BALUFFO_BRIDGE_PORT": "9999",
                "BALUFFO_DATA_DIR": "C:\\should-not-win",
                "BALUFFO_BRIDGE_LOG_FORMAT": "human",
                "BALUFFO_BRIDGE_LOG_LEVEL": "info",
                "BALUFFO_BRIDGE_OWNER_MODE": "env-owner",
            },
            expected_host="127.0.0.9",
            expected_port=9001,
            expected_data_dir_token="entrypoint-root",
            expected_log_format="jsonl",
            expected_log_level="debug",
            expected_owner_mode="env-owner",
        ),
        id="cli-env-precedence",
    ),
    pytest.param(
        _RuntimeConfigCase(
            name="env-defaults-when-cli-missing",
            cli_args=[],
            env={
                "BALUFFO_BRIDGE_HOST": "0.0.0.0",
                "BALUFFO_BRIDGE_PORT": "9911",
                "BALUFFO_DATA_DIR": "__ENTRYPOINT_ROOT__",
                "BALUFFO_BRIDGE_LOG_FORMAT": "jsonl",
                "BALUFFO_BRIDGE_LOG_LEVEL": "debug",
                "BALUFFO_BRIDGE_OWNER_MODE": "dev-supervisor",
                "BALUFFO_BRIDGE_OWNER_TOKEN": "token-123",
                "BALUFFO_BRIDGE_SESSION_ID": "session-123",
                "BALUFFO_BRIDGE_STARTED_BY": "unit-test",
                "BALUFFO_BRIDGE_OWNER_IDLE_TIMEOUT_S": "45",
            },
            bridge_defaults={
                "host": "127.0.0.2",
                "port": 8878,
                "log_format": "human",
                "log_level": "info",
                "quiet_requests": False,
            },
            storage_defaults={"data_dir": "__ENTRYPOINT_ROOT__/from-file"},
            expected_host="0.0.0.0",
            expected_port=9911,
            expected_data_dir_token="entrypoint-root",
            expected_log_format="jsonl",
            expected_log_level="debug",
            expected_owner_mode="dev-supervisor",
            expected_owner_token="token-123",
            expected_desktop_session_id="session-123",
            expected_started_by="unit-test",
            expected_owner_idle_timeout_s=45.0,
        ),
        id="env-defaults-when-cli-missing",
    ),
    pytest.param(
        _RuntimeConfigCase(
            name="file-defaults-when-env-missing",
            cli_args=[],
            env={},
            bridge_defaults={
                "host": "127.0.0.5",
                "port": 9915,
                "log_format": "jsonl",
                "log_level": "debug",
                "quiet_requests": True,
            },
            storage_defaults={"data_dir": "__ENTRYPOINT_ROOT__/from-file"},
            expected_host="127.0.0.5",
            expected_port=9915,
            expected_data_dir_suffix="from-file",
            expected_log_format="jsonl",
            expected_log_level="debug",
            expected_quiet_requests=True,
        ),
        id="file-defaults-when-env-missing",
    ),
]


@pytest.mark.parametrize("case", RESOLVE_RUNTIME_CONFIG_CASES, ids=lambda case: case.name)
def test_resolve_runtime_config_uses_precedence_matrix(
    case: _RuntimeConfigCase,
    admin_bridge_entrypoint_root,
) -> None:
    _run_runtime_config_case(case, admin_bridge_entrypoint_root)


def test_bridge_log_jsonl_output_is_valid_json(admin_bridge_entrypoint_root):
    data_dir = admin_bridge_entrypoint_root / "runtime-data"
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=data_dir,
        host="127.0.0.1",
        port=8877,
        log_format="jsonl",
        log_level="info",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    buf = io.StringIO()
    with redirect_stdout(buf):
        admin_bridge.bridge_log("info", "hello_bridge", runId="abc123")
    line = buf.getvalue().strip()
    payload = json.loads(line)
    assert str(payload.get("message") or "") == "hello_bridge"
    assert str(payload.get("runId") or "") == "abc123"
    assert str(payload.get("level") or "") == "info"
    assert "schemaVersion" not in payload
    assert "fields" not in payload

    retained_rows = admin_bridge.diagnostic_events.read_bridge_events(
        data_dir / "admin-bridge-events.jsonl"
    )
    assert len(retained_rows) == 1
    assert retained_rows[0]["schemaVersion"] == 1
    assert retained_rows[0]["event"] == "hello_bridge"
    assert retained_rows[0]["fields"]["runId"] == "abc123"


def test_bridge_log_human_output_still_writes_retained_event(admin_bridge_entrypoint_root):
    data_dir = admin_bridge_entrypoint_root / "runtime-data"
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=data_dir,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    buf = io.StringIO()
    with redirect_stdout(buf):
        admin_bridge.bridge_log("info", "hello_bridge", runId="abc123")

    assert buf.getvalue().strip() == "[admin_bridge][INFO] hello_bridge runId=abc123"
    retained_rows = admin_bridge.diagnostic_events.read_bridge_events(
        data_dir / "admin-bridge-events.jsonl"
    )
    assert retained_rows[0]["event"] == "hello_bridge"
    assert retained_rows[0]["fields"]["runId"] == "abc123"


def test_bridge_log_respects_log_level_for_retained_events(admin_bridge_entrypoint_root):
    data_dir = admin_bridge_entrypoint_root / "runtime-data"
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=data_dir,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="warn",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    buf = io.StringIO()
    with redirect_stdout(buf):
        admin_bridge.bridge_log("info", "debug_only", runId="abc123")

    assert buf.getvalue() == ""
    assert not (data_dir / "admin-bridge-events.jsonl").exists()


def test_bridge_log_retained_write_failure_does_not_break_console_output(
    admin_bridge_entrypoint_root,
):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root / "runtime-data",
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    buf = io.StringIO()
    with (
        mock.patch.object(
            admin_bridge.diagnostic_events,
            "append_bridge_event",
            side_effect=OSError("diagnostic write failed"),
        ),
        redirect_stdout(buf),
    ):
        admin_bridge.bridge_log("info", "hello_bridge", runId="abc123")

    assert buf.getvalue().strip() == "[admin_bridge][INFO] hello_bridge runId=abc123"


def test_startup_banner_redacts_owner_token_in_retained_events(admin_bridge_entrypoint_root):
    data_dir = admin_bridge_entrypoint_root / "runtime-data"
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=data_dir,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        owner_mode="desktop-window",
        owner_token="owner-secret",
        desktop_session_id="session-123",
        started_by="unit-test",
    )
    admin_bridge.configure_runtime_paths(cfg)
    with redirect_stdout(io.StringIO()):
        admin_bridge.startup_banner(cfg)

    rows = admin_bridge.diagnostic_events.read_bridge_events(data_dir / "admin-bridge-events.jsonl")
    started = next(row for row in rows if row["event"] == "admin_bridge_started")
    assert started["fields"]["owner_token"] == "[redacted]"
    assert started["fields"]["desktop_session_id"] == "session-123"


def test_bridge_log_swallows_broken_windows_pipe(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    with mock.patch(
        "builtins.print", side_effect=OSError(233, "No process is on the other end of the pipe")
    ):
        admin_bridge.bridge_log("info", "hello_bridge", runId="abc123")


def test_configure_runtime_paths_updates_bridge_paths(admin_bridge_entrypoint_root):
    data_dir = admin_bridge_entrypoint_root / "runtime-data"
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=data_dir,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
    )
    admin_bridge.configure_runtime_paths(cfg)
    assert admin_bridge.ACTIVE_PATH == data_dir / "source-registry-active.json"
    assert admin_bridge.TASK_STATE_PATH == data_dir / "admin-task-state.json"
    assert admin_bridge.ADMIN_BRIDGE_EVENTS_PATH == data_dir / "admin-bridge-events.jsonl"
    assert admin_bridge.source_registry_module.DATA_DIR == data_dir.resolve()


def test_owner_session_should_exit_when_supervised_bridge_is_idle(admin_bridge_entrypoint_root):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=False,
        owner_mode="dev-supervisor",
        owner_token="owner-1",
        started_by="test",
        owner_idle_timeout_s=10.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    admin_bridge.bridge_runtime_state.OWNER_STATE["lastActivityAt"] = "2026-03-01T00:00:00+00:00"

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:15+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is True


def test_owner_session_should_exit_after_last_desktop_page_closing_grace(
    admin_bridge_entrypoint_root,
):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=True,
        owner_mode="desktop-window",
        owner_token="owner-1",
        desktop_session_id="session-1",
        started_by="test",
        owner_idle_timeout_s=15.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    with mock.patch.object(
        admin_bridge,
        "now_iso",
        side_effect=["2026-03-01T00:00:00+00:00", "2026-03-01T00:00:01+00:00"],
    ):
        status_code, payload = admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="alive",
        )
        admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="closing",
        )
    assert status_code == 200
    assert payload["ok"] is True

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:05+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is False

    with mock.patch.object(
        admin_bridge,
        "now_utc",
        return_value=admin_bridge.parse_iso("2026-03-01T00:00:20+00:00"),
    ):
        assert admin_bridge.owner_session_should_exit() is True


def test_owner_session_should_not_exit_while_active_critical_tasks_exist(
    admin_bridge_entrypoint_root,
):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=True,
        owner_mode="desktop-window",
        owner_token="owner-1",
        desktop_session_id="session-1",
        started_by="test",
        owner_idle_timeout_s=15.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    admin_bridge.bridge_runtime_state.OWNER_STATE["lastActivityAt"] = "2026-03-01T00:00:00+00:00"

    with (
        mock.patch.object(
            admin_bridge,
            "now_utc",
            return_value=admin_bridge.parse_iso("2026-03-01T00:00:20+00:00"),
        ),
        mock.patch.object(
            admin_bridge,
            "_get_ops_api",
            return_value=mock.Mock(
                get_current_task_state_payload=mock.Mock(
                    return_value={
                        "tasks": [
                            {"taskType": "fetch", "runId": "fetch_live_1", "active": True},
                            {"taskType": "pipeline", "runId": "pipeline_1", "active": True},
                        ]
                    }
                )
            ),
        ),
    ):
        assert admin_bridge.owner_session_should_exit() is False


def test_owner_session_exits_for_confirmed_active_work_close(
    admin_bridge_entrypoint_root,
):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=True,
        owner_mode="desktop-window",
        owner_token="owner-1",
        desktop_session_id="session-1",
        started_by="test",
        owner_idle_timeout_s=15.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    status_code, payload = admin_bridge.update_desktop_session_lifecycle(
        owner_token="owner-1",
        session_id="session-1",
        page_id="page-1",
        state="closing",
        reason="confirmed_active_work_close",
    )
    assert status_code == 200
    assert payload["reason"] == "confirmed_active_work_close"

    with (
        mock.patch.object(
            admin_bridge,
            "now_utc",
            return_value=admin_bridge.parse_iso("2026-03-01T00:00:01+00:00"),
        ),
        mock.patch.object(
            admin_bridge,
            "_get_ops_api",
            return_value=mock.Mock(
                get_current_task_state_payload=mock.Mock(
                    return_value={
                        "tasks": [
                            {"taskType": "fetch", "runId": "fetch_live_1", "active": True},
                        ]
                    }
                )
            ),
        ),
    ):
        assert admin_bridge.owner_session_should_exit() is True


def test_regular_desktop_close_stays_alive_for_active_task_or_update_handoff(
    admin_bridge_entrypoint_root,
):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=True,
        owner_mode="desktop-window",
        owner_token="owner-1",
        desktop_session_id="session-1",
        started_by="test",
        owner_idle_timeout_s=15.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    admin_bridge.update_desktop_session_lifecycle(
        owner_token="owner-1",
        session_id="session-1",
        page_id="page-1",
        state="closing",
        reason="beforeunload",
    )

    with (
        mock.patch.object(
            admin_bridge,
            "_get_ops_api",
            return_value=mock.Mock(
                get_current_task_state_payload=mock.Mock(
                    return_value={
                        "tasks": [
                            {"taskType": "fetch", "runId": "fetch_live_1", "active": True},
                        ]
                    }
                )
            ),
        ),
        mock.patch.object(
            admin_bridge,
            "_get_desktop_update_service",
            return_value=mock.Mock(
                get_status_payload=mock.Mock(
                    return_value={"downloadState": "idle", "installState": "idle"}
                )
            ),
        ),
    ):
        assert admin_bridge.owner_session_should_exit() is False

    with (
        mock.patch.object(
            admin_bridge,
            "_get_ops_api",
            return_value=mock.Mock(
                get_current_task_state_payload=mock.Mock(return_value={"tasks": []})
            ),
        ),
        mock.patch.object(
            admin_bridge,
            "_get_desktop_update_service",
            return_value=mock.Mock(
                get_status_payload=mock.Mock(
                    return_value={
                        "downloadState": "idle",
                        "installState": "handoff_requested",
                    }
                )
            ),
        ),
    ):
        assert admin_bridge.owner_session_should_exit() is False


def test_active_work_close_attempt_is_only_cleared_by_same_page(
    admin_bridge_entrypoint_root,
):
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=True,
        owner_mode="desktop-window",
        owner_token="owner-1",
        desktop_session_id="session-1",
        started_by="test",
        owner_idle_timeout_s=15.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    status_code, payload = admin_bridge.update_desktop_session_lifecycle(
        owner_token="owner-1",
        session_id="session-1",
        page_id="page-1",
        state="closing",
        reason="active_work_close_attempt",
    )
    assert status_code == 200
    assert payload["reason"] == "active_work_close_attempt"

    status_code, _ = admin_bridge.update_desktop_session_lifecycle(
        owner_token="owner-1",
        session_id="session-1",
        page_id="page-2",
        state="alive",
    )
    assert status_code == 200
    session_payload = admin_bridge.get_desktop_session_payload()
    assert session_payload["shutdownReason"] == "active_work_close_attempt"
    assert session_payload["shutdownPageId"] == "page-1"

    status_code, _ = admin_bridge.update_desktop_session_lifecycle(
        owner_token="owner-1",
        session_id="session-1",
        page_id="page-1",
        state="alive",
    )
    assert status_code == 200
    assert admin_bridge.get_desktop_session_payload()["shutdownReason"] == ""


def test_infer_studio_name_from_host_skips_www_and_splits_studio_token(
    admin_bridge_entrypoint_root,
):
    studio = admin_bridge.infer_studio_name_from_host("https://www.naconstudiomilan.com/careers/")
    assert studio == "Nacon Studio Milan"


def test_infer_studio_name_from_host_skips_short_placeholder_subdomain(
    admin_bridge_entrypoint_root,
):
    studio = admin_bridge.infer_studio_name_from_host("https://w.nixxes.com/jobs")
    assert studio == "Nixxes"
